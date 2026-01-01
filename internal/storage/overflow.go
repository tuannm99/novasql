package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/tuannm99/novasql/pkg/bx"
)

// OverflowRef points to an overflow chain in a dedicated overflow segment.
//   - FirstPageID: the first page of the chain (1-based page index within the overflow file;
//     page 0 is reserved for meta)
//   - Length:      total logical bytes stored across the chain
type OverflowRef struct {
	FirstPageID uint32
	Length      uint32
}

// OverflowManager manages large byte slices by storing them in a separate
// overflow segment, using a simple linked-page format.
//
// Layout of overflow file (segment 0):
//   - Page 0: meta page
//     [0..3]  uint32 freeHead   // 0 => no free page
//     [4..7]  uint32 nextAlloc  // next pageID to allocate (>= 1)
//   - Page >=1: data/free pages
//
// Data page layout (PageSize bytes total):
//
//	[0..3]   uint32 nextPageID   // 0 => end of chain
//	[4..5]   uint16 used         // number of payload bytes used
//	[6..]    payload bytes       // up to overflowPayloadSize
//
// Free page layout:
//   - we reuse [0..3] as nextFree pointer for free-list
//   - used=0
type OverflowManager struct {
	fs FileSet
}

func NewOverflowManager(fs FileSet) *OverflowManager {
	return &OverflowManager{
		fs: fs,
	}
}

const (
	overflowHeaderSize  = 6
	overflowPayloadSize = PageSize - overflowHeaderSize

	// meta page offsets
	ovfMetaFreeHeadOff  = 0
	ovfMetaNextAllocOff = 4

	// pageID 0 reserved for meta
	ovfFirstDataPageID = 1
)

var (
	ErrOverflowEmptyData   = errors.New("overflow: empty data")
	ErrOverflowZeroRef     = errors.New("overflow: zero-length ref")
	ErrOverflowBadRef      = errors.New("overflow: bad ref")
	ErrOverflowCorruption  = errors.New("overflow: corruption detected")
	ErrOverflowTruncated   = errors.New("overflow: truncated chain")
	ErrOverflowBadMetaPage = errors.New("overflow: bad meta page")
)

func (ovf *OverflowManager) Write(data []byte) (OverflowRef, error) {
	if len(data) == 0 {
		return OverflowRef{}, ErrOverflowEmptyData
	}

	f, err := ovf.fs.OpenSegment(0)
	if err != nil {
		return OverflowRef{}, err
	}
	defer func() { _ = f.Close() }()

	freeHead, nextAlloc, err := ovf.ensureMeta(f)
	if err != nil {
		return OverflowRef{}, err
	}

	total := len(data)
	remaining := total
	offset := 0

	var firstPageID uint32
	var prevPageID uint32
	hasPrev := false

	for remaining > 0 {
		chunk := min(remaining, overflowPayloadSize)

		pageID, nh, na, err := ovf.allocDataPage(f, freeHead, nextAlloc)
		if err != nil {
			return OverflowRef{}, err
		}
		freeHead, nextAlloc = nh, na

		if firstPageID == 0 {
			firstPageID = pageID
		}

		// Build a full page buffer.
		buf := make([]byte, PageSize)
		bx.PutU32(buf[0:4], 0)             // nextPageID patched later
		bx.PutU16(buf[4:6], uint16(chunk)) // used
		copy(buf[overflowHeaderSize:], data[offset:offset+chunk])

		pageOff := int64(pageID) * int64(PageSize)
		if _, err := f.WriteAt(buf, pageOff); err != nil {
			return OverflowRef{}, err
		}

		// Link previous page -> current page
		if hasPrev {
			prevOff := int64(prevPageID) * int64(PageSize)
			var hdr [4]byte
			bx.PutU32(hdr[:], pageID)
			if _, err := f.WriteAt(hdr[:], prevOff); err != nil {
				return OverflowRef{}, err
			}
		} else {
			hasPrev = true
		}

		prevPageID = pageID
		offset += chunk
		remaining -= chunk
	}

	// Persist meta (freeHead/nextAlloc) best-effort; if it fails, it is serious because allocation state changes.
	if err := ovf.writeMeta(f, freeHead, nextAlloc); err != nil {
		return OverflowRef{}, err
	}

	return OverflowRef{
		FirstPageID: firstPageID,
		Length:      uint32(total),
	}, nil
}

func (ovf *OverflowManager) Read(ref OverflowRef) ([]byte, error) {
	if ref.Length == 0 {
		return nil, ErrOverflowZeroRef
	}
	if ref.FirstPageID < ovfFirstDataPageID {
		return nil, ErrOverflowBadRef
	}

	f, err := ovf.fs.OpenSegment(0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	out := make([]byte, 0, ref.Length)
	remaining := int(ref.Length)
	pageID := ref.FirstPageID

	// expected upper bound pages for this ref (plus a small slack)
	maxPages := (remaining + overflowPayloadSize - 1) / overflowPayloadSize
	maxPages += 4

	for range maxPages {
		if remaining <= 0 {
			break
		}

		pageOff := int64(pageID) * int64(PageSize)
		buf := make([]byte, PageSize)
		if _, err := f.ReadAt(buf, pageOff); err != nil {
			return nil, err
		}

		next := bx.U32(buf[0:4])
		used := int(bx.U16(buf[4:6]))

		if used < 0 || used > overflowPayloadSize {
			slog.Warn("overflow: used too large, clamping",
				"pageID", pageID,
				"used_raw", used,
				"payload_max", overflowPayloadSize,
			)
			used = overflowPayloadSize
		}
		if used > remaining {
			slog.Warn("overflow: used > remaining, clamping",
				"pageID", pageID,
				"used_before", used,
				"remaining", remaining,
			)
			used = remaining
		}

		out = append(out, buf[overflowHeaderSize:overflowHeaderSize+used]...)
		remaining -= used

		if remaining > 0 {
			if next == 0 {
				return nil, fmt.Errorf("%w: remaining=%d", ErrOverflowTruncated, remaining)
			}
			if next < ovfFirstDataPageID {
				return nil, ErrOverflowCorruption
			}
			pageID = next
		}
	}

	if remaining != 0 {
		return nil, ErrOverflowTruncated
	}
	return out, nil
}

// Free releases all pages in the overflow chain back to the free list.
// This does NOT shrink the file; it only recycles pages.
func (ovf *OverflowManager) Free(ref OverflowRef) error {
	if ref.Length == 0 {
		return nil
	}
	if ref.FirstPageID < ovfFirstDataPageID {
		return ErrOverflowBadRef
	}

	f, err := ovf.fs.OpenSegment(0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	freeHead, nextAlloc, err := ovf.ensureMeta(f)
	if err != nil {
		return err
	}

	remaining := int(ref.Length)
	maxPages := (remaining + overflowPayloadSize - 1) / overflowPayloadSize
	maxPages += 4

	pageID := ref.FirstPageID

	for range maxPages {
		if remaining <= 0 {
			break
		}

		// read current page header to get next in chain and used bytes
		pageOff := int64(pageID) * int64(PageSize)
		buf := make([]byte, PageSize)
		if _, err := f.ReadAt(buf, pageOff); err != nil {
			return err
		}

		next := bx.U32(buf[0:4])
		used := int(bx.U16(buf[4:6]))
		if used < 0 || used > overflowPayloadSize {
			used = overflowPayloadSize
		}
		if used > remaining {
			used = remaining
		}

		// push this page onto free list:
		// [0..3] nextFree = freeHead
		// [4..5] used = 0
		var hdr [6]byte
		bx.PutU32(hdr[0:4], freeHead)
		bx.PutU16(hdr[4:6], 0)
		if _, err := f.WriteAt(hdr[:], pageOff); err != nil {
			return err
		}
		freeHead = pageID

		remaining -= used

		if remaining > 0 {
			if next == 0 || next < ovfFirstDataPageID {
				return ErrOverflowCorruption
			}
			pageID = next
		}
	}

	if remaining != 0 {
		return ErrOverflowTruncated
	}

	return ovf.writeMeta(f, freeHead, nextAlloc)
}

// ---- meta / alloc helpers ----

func (ovf *OverflowManager) ensureMeta(f *os.File) (freeHead uint32, nextAlloc uint32, err error) {
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}

	// If file is empty, initialize meta page at page 0.
	if info.Size() < int64(PageSize) {
		buf := make([]byte, PageSize)
		bx.PutU32At(buf, ovfMetaFreeHeadOff, 0)
		bx.PutU32At(buf, ovfMetaNextAllocOff, ovfFirstDataPageID)

		if _, err := f.WriteAt(buf, 0); err != nil {
			return 0, 0, err
		}
		return 0, ovfFirstDataPageID, nil
	}

	// read existing meta
	buf := make([]byte, PageSize)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return 0, 0, err
	}
	freeHead = bx.U32At(buf, ovfMetaFreeHeadOff)
	nextAlloc = bx.U32At(buf, ovfMetaNextAllocOff)
	if nextAlloc < ovfFirstDataPageID {
		return 0, 0, ErrOverflowBadMetaPage
	}
	return freeHead, nextAlloc, nil
}

func (ovf *OverflowManager) writeMeta(f *os.File, freeHead, nextAlloc uint32) error {
	buf := make([]byte, PageSize)
	// read current meta first (preserve future fields)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return err
	}
	bx.PutU32At(buf, ovfMetaFreeHeadOff, freeHead)
	bx.PutU32At(buf, ovfMetaNextAllocOff, nextAlloc)
	_, err := f.WriteAt(buf, 0)
	return err
}

func (ovf *OverflowManager) allocDataPage(
	f *os.File,
	freeHead, nextAlloc uint32,
) (pageID uint32, newFreeHead uint32, newNextAlloc uint32, err error) {
	if freeHead != 0 {
		// pop
		pageID = freeHead

		// read nextFree from the free page's [0..3]
		pageOff := int64(pageID) * int64(PageSize)
		var b [4]byte
		if _, err := f.ReadAt(b[:], pageOff); err != nil {
			return 0, 0, 0, err
		}
		newFreeHead = bx.U32(b[:])
		newNextAlloc = nextAlloc
		return pageID, newFreeHead, newNextAlloc, nil
	}

	// allocate fresh
	pageID = nextAlloc
	newFreeHead = 0
	newNextAlloc = nextAlloc + 1
	return pageID, newFreeHead, newNextAlloc, nil
}
