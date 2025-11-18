package storage

import "github.com/tuannm99/novasql/pkg/bx"

// offset Size Field
// 0      4    nextPageID
// 4      2    usedBytes
// 6      n    dataChunk -- max(n) = 8192 - 6 (nextPageID+usedBytes)
// -> if dataChunk greater than 8186 -> split to multiple pages -> linked by nextPageID
//
const (
	overflowOffNext           = 0
	overflowOffLen            = 4
	overflowHeaderSize        = 6
	overflowNoNext     uint32 = 0xFFFFFFFF
)

// OverflowRef describes an overflowed large value that is stored
// outside of the normal heap page as a linked list of overflow pages.
type OverflowRef struct {
	FirstPageID uint32 `json:"first_page_id"`
	Length      uint32 `json:"length"`
}

// OverflowManager manages reading/writing large values that do not fit
// into a single normal tuple. It uses a dedicated FileSet and the
// StorageManager to allocate and chain pages on disk.
type OverflowManager struct {
	sm *StorageManager
	fs FileSet
}

// NewOverflowManager creates a new overflow manager bound to a FileSet.
//
// In many designs you will use a separate FileSet for overflow data,
// e.g. table "users" uses:
//
//	data:      LocalFileSet{Dir: ".../tables", Base: "users"}
//	overflow:  LocalFileSet{Dir: ".../tables", Base: "users_overflow"}
func NewOverflowManager(sm *StorageManager, fs FileSet) *OverflowManager {
	return &OverflowManager{
		sm: sm,
		fs: fs,
	}
}

// allocatePage finds the next free page ID by counting existing pages.
// This is simple but not very efficient and not thread-safe; good enough
// for an educational engine V1.
func (om *OverflowManager) allocatePage() (uint32, error) {
	n, err := om.sm.CountPages(om.fs)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Write stores an arbitrary-length value into one or more overflow pages
// and returns an OverflowRef that can be used to read it back later.
func (om *OverflowManager) Write(value []byte) (OverflowRef, error) {
	totalLen := len(value)

	// For simplicity we always create at least one page, even if len == 0.
	var firstPageID uint32
	var prevPageID uint32
	var prevBuf []byte
	var havePrev bool

	payloadMax := PageSize - overflowHeaderSize

	offset := 0
	for offset <= totalLen {
		chunkLen := totalLen - offset
		if chunkLen > payloadMax {
			chunkLen = payloadMax
		}

		// Allocate a new page.
		pageID, err := om.allocatePage()
		if err != nil {
			return OverflowRef{}, err
		}

		buf := make([]byte, PageSize)

		// Write nextPageID as "no-next" for now; we will patch previous page
		// to point to this page if needed.
		bx.PutU32(buf[overflowOffNext:], overflowNoNext)
		bx.PutU16(buf[overflowOffLen:], uint16(chunkLen))

		if chunkLen > 0 {
			copy(buf[overflowHeaderSize:overflowHeaderSize+chunkLen], value[offset:offset+chunkLen])
		}

		// If we had a previous page, patch its nextPageID and write it out.
		if havePrev {
			bx.PutU32(prevBuf[overflowOffNext:], pageID)
			if err := om.sm.WritePage(om.fs, int32(prevPageID), prevBuf); err != nil {
				return OverflowRef{}, err
			}
		} else {
			// This is the first page in the chain.
			firstPageID = pageID
		}

		// Prepare for next iteration.
		prevPageID = pageID
		prevBuf = buf
		havePrev = true

		offset += chunkLen

		if chunkLen == 0 {
			// We only create one empty page when len == 0.
			break
		}
	}

	// Write the last page (its nextPageID is overflowNoNext).
	if havePrev {
		if err := om.sm.WritePage(om.fs, int32(prevPageID), prevBuf); err != nil {
			return OverflowRef{}, err
		}
	}

	return OverflowRef{
		FirstPageID: firstPageID,
		Length:      uint32(totalLen),
	}, nil
}

// Read loads the full value described by the reference by walking the
// linked list of overflow pages.
func (om *OverflowManager) Read(ref OverflowRef) ([]byte, error) {
	if ref.Length == 0 {
		// Empty value; either no pages or a single empty page.
		return []byte{}, nil
	}

	result := make([]byte, int(ref.Length))
	remaining := int(ref.Length)

	pageID := ref.FirstPageID
	writePos := 0

	for {
		buf := make([]byte, PageSize)
		if err := om.sm.ReadPage(om.fs, int32(pageID), buf); err != nil {
			return nil, err
		}

		nextID := bx.U32(buf[overflowOffNext : overflowOffNext+4])
		used := int(bx.U16(buf[overflowOffLen : overflowOffLen+2]))
		if used > PageSize-overflowHeaderSize {
			used = PageSize - overflowHeaderSize
		}
		if used > remaining {
			used = remaining
		}

		if used > 0 {
			copy(result[writePos:writePos+used], buf[overflowHeaderSize:overflowHeaderSize+used])
			writePos += used
			remaining -= used
		}

		if remaining <= 0 || nextID == overflowNoNext {
			break
		}
		pageID = nextID
	}

	// In case of inconsistent chain, we may not fill everything; but for V1 we assume
	// storage is correct and result is fully populated.
	return result, nil
}
