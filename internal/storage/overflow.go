package storage

import (
	"fmt"
	"log/slog"

	"github.com/tuannm99/novasql/pkg/bx"
)

// OverflowRef points to an overflow chain in a dedicated overflow segment.
// - FirstPageID: the first page of the chain (0-based page index within the overflow file)
// - Length:      total logical bytes stored across the chain
type OverflowRef struct {
	FirstPageID uint32
	Length      uint32
}

// OverflowManager manages large byte slices by storing them in a separate
// overflow segment, using a simple linked-page format.
type OverflowManager struct {
	sm *StorageManager
	fs FileSet
}

func NewOverflowManager(sm *StorageManager, fs FileSet) *OverflowManager {
	return &OverflowManager{
		sm: sm,
		fs: fs,
	}
}

// Overflow page layout (PageSize bytes total):
//
//	[0..3]   uint32 nextPageID   // 0 => end of chain
//	[4..5]   uint16 used         // number of payload bytes used
//	[6..]    payload bytes       // up to overflowPayloadSize
//
// PageSize is shared with normal pages, but the overflow segment is a
// completely separate file.
const (
	overflowHeaderSize  = 6
	overflowPayloadSize = PageSize - overflowHeaderSize
)

// Write stores `data` as a linked list of overflow pages in fs.OpenSegment(0).
// It always appends new pages at the end of the file and returns an OverflowRef.
func (ovf *OverflowManager) Write(data []byte) (OverflowRef, error) {
	if len(data) == 0 {
		return OverflowRef{}, fmt.Errorf("overflow: empty data")
	}

	// Always use segment 0 as the overflow file.
	f, err := ovf.fs.OpenSegment(0)
	if err != nil {
		return OverflowRef{}, err
	}
	defer func() { _ = f.Close() }()

	total := len(data)
	remaining := total
	offset := 0

	// Determine the starting page index for newly appended overflow pages.
	info, err := f.Stat()
	if err != nil {
		return OverflowRef{}, err
	}
	startPageID := uint32(info.Size() / int64(PageSize))

	slog.Debug("overflow: Write start",
		"len", total,
		"startPageID", startPageID,
	)

	var firstPageID uint32
	var prevPageID uint32
	hasPrev := false

	curPageID := startPageID

	for remaining > 0 {
		chunk := min(remaining, overflowPayloadSize)

		// Build a full page buffer.
		buf := make([]byte, PageSize)

		// nextPageID = 0 for now, will be patched on the previous page.
		bx.PutU32(buf[0:4], 0)
		// used bytes on this page
		bx.PutU16(buf[4:6], uint16(chunk))
		// copy payload
		copy(buf[overflowHeaderSize:overflowHeaderSize+chunk], data[offset:offset+chunk])

		pageOff := int64(curPageID) * int64(PageSize)
		if _, err := f.WriteAt(buf, pageOff); err != nil {
			return OverflowRef{}, err
		}

		slog.Debug("overflow: wrote page",
			"pageID", curPageID,
			"offset", pageOff,
			"chunk", chunk,
			"remaining_before", remaining,
		)

		// Link previous page to this one.
		if hasPrev {
			prevOff := int64(prevPageID) * int64(PageSize)
			var hdr [4]byte
			bx.PutU32(hdr[:], curPageID) // set nextPageID of prev page
			if _, err := f.WriteAt(hdr[:], prevOff); err != nil {
				return OverflowRef{}, err
			}

			slog.Debug("overflow: updated prev.next",
				"prevPageID", prevPageID,
				"nextPageID", curPageID,
				"prevOff", prevOff,
			)
		} else {
			// First page of this chain.
			firstPageID = curPageID
			hasPrev = true
		}

		prevPageID = curPageID
		curPageID++
		offset += chunk
		remaining -= chunk
	}

	ref := OverflowRef{
		FirstPageID: firstPageID,
		Length:      uint32(total),
	}

	slog.Debug("overflow: Write done",
		"firstPageID", ref.FirstPageID,
		"length", ref.Length,
	)

	return ref, nil
}

// Read loads the full logical byte slice from an overflow chain.
func (ovf *OverflowManager) Read(ref OverflowRef) ([]byte, error) {
	if ref.Length == 0 {
		return nil, fmt.Errorf("overflow: zero-length ref")
	}

	f, err := ovf.fs.OpenSegment(0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	slog.Debug("overflow: Read start",
		"firstPageID", ref.FirstPageID,
		"length", ref.Length,
	)

	out := make([]byte, 0, ref.Length)
	remaining := int(ref.Length)
	pageID := ref.FirstPageID

	for remaining > 0 {
		pageOff := int64(pageID) * int64(PageSize)
		buf := make([]byte, PageSize)

		if _, err := f.ReadAt(buf, pageOff); err != nil {
			return nil, err
		}

		next := bx.U32(buf[0:4])
		used := int(bx.U16(buf[4:6]))

		// Safety clamps for corrupted headers.
		if used > overflowPayloadSize {
			slog.Warn("overflow: used field too large, clamping",
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

		slog.Debug("overflow: read page",
			"pageID", pageID,
			"offset", pageOff,
			"nextPageID", next,
			"used", used,
			"remaining_before", remaining,
		)

		out = append(out, buf[overflowHeaderSize:overflowHeaderSize+used]...)
		remaining -= used

		if remaining > 0 {
			if next == 0 {
				// We still expect more data but chain ended.
				return nil, fmt.Errorf("overflow: truncated chain, remaining=%d", remaining)
			}
			pageID = next
		}
	}

	slog.Debug("overflow: Read done",
		"out_len", len(out),
	)

	return out, nil
}
