package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/tuannm99/novasql/internal/alias/util"
)

var (
	// currently unused; if you later decide to distinguish between "zero page"
	// and "beyond EOF", you can return this from ReadPage.
	ErrPageNotFound = errors.New("storage_manager: page not found")

	// currently unused in this file; reserved for higher-level "append" logic.
	ErrPageFull = errors.New("storage_manager: write would exceed page data length")
)

type FileSet interface {
	OpenSegment(segNo int32) (*os.File, error)
}

var _ FileSet = (*LocalFileSet)(nil)

// LocalFileSet represents a local directory + base file name.
// Segments are stored as: Base, Base.1, Base.2, ...
type LocalFileSet struct {
	Dir  string
	Base string
}

func (lfs LocalFileSet) OpenSegment(segNo int32) (*os.File, error) {
	name := lfs.Base
	if segNo > 0 {
		name = fmt.Sprintf("%s.%d", lfs.Base, segNo)
	}
	path := filepath.Join(lfs.Dir, name)
	if err := os.MkdirAll(lfs.Dir, 0o755); err != nil {
		return nil, err
	}
	// RDWR | CREATE (no truncate)
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}

// StorageManager maps a logical pageID -> (segment, offset).
type StorageManager struct{}

func NewStorageManager() *StorageManager {
	return &StorageManager{}
}

func (sm *StorageManager) pagesPerSegment() int {
	// total 1 GiB / 8 KiB = 131072 pages per segment
	return SegmentSize / PageSize
}

func (sm *StorageManager) locate(pageID int32) (segNo int32, offset int32) {
	pps := sm.pagesPerSegment()
	segNo = pageID / int32(pps)
	pageInSeg := pageID % int32(pps)
	offset = pageInSeg * PageSize
	return segNo, offset
}

// ReadPage reads exactly one page (PageSize bytes) into dst.
// If the underlying file is smaller than the requested offset+PageSize,
// the remainder is zero-filled. This allows "sparse" pages that are
// lazily initialized by higher layers.
func (sm *StorageManager) ReadPage(fs FileSet, pageID int32, dst []byte) error {
	if len(dst) != PageSize {
		return fmt.Errorf("dst must be exactly %d bytes", PageSize)
	}
	segNo, off := sm.locate(pageID)
	f, err := fs.OpenSegment(segNo)
	if err != nil {
		return err
	}
	defer util.CloseFileFunc(f)

	n, err := f.ReadAt(dst, int64(off))
	if err != nil && err != io.EOF {
		return err
	}
	// Zero-fill the rest of the page if we hit EOF early or a short read.
	for i := n; i < PageSize; i++ {
		dst[i] = 0
	}
	return nil
}

// WritePage writes exactly one page (PageSize bytes) from src to disk
// at the location computed from pageID.
func (sm *StorageManager) WritePage(fs FileSet, pageID int32, src []byte) error {
	if len(src) != PageSize {
		return fmt.Errorf("src must be exactly %d bytes", PageSize)
	}
	segNo, off := sm.locate(pageID)
	f, err := fs.OpenSegment(segNo)
	if err != nil {
		return err
	}
	defer util.CloseFileFunc(f)

	n, err := f.WriteAt(src, int64(off))
	if err != nil {
		return err
	}
	if n != PageSize {
		return io.ErrShortWrite
	}
	return nil
}

// LoadPage reads a page into memory and returns a Page wrapper.
// If the on-disk bytes are all zero, the page is treated as uninitialized
// and is initialized with the given pageID.
func (sm *StorageManager) LoadPage(fs FileSet, pageID uint32) (*Page, error) {
	buf := make([]byte, PageSize)
	if err := sm.ReadPage(fs, int32(pageID), buf); err != nil {
		return nil, err
	}
	p := &Page{Buf: buf}
	if p.IsUninitialized() {
		p.init(pageID)
	}
	return p, nil
}

// SavePage writes the in-memory Page back to disk.
func (sm *StorageManager) SavePage(fs FileSet, pageID uint32, p Page) error {
	if len(p.Buf) != PageSize {
		return fmt.Errorf("page buffer must be %d bytes", PageSize)
	}
	return sm.WritePage(fs, int32(pageID), p.Buf)
}

// CountPages computes total pages for a given FileSet by scanning all segments.
func (sm *StorageManager) CountPages(fs FileSet) (uint32, error) {
	var total uint32

	// We assume segments are named: Base, Base.1, Base.2, ...
	for segNo := int32(0); ; segNo++ {
		f, err := fs.OpenSegment(segNo)
		if err != nil {
			// Stop when the segment file does not exist
			if os.IsNotExist(err) {
				break
			}
			return 0, err
		}

		info, statErr := f.Stat()
		_ = f.Close()
		if statErr != nil {
			return 0, statErr
		}

		size := info.Size()
		if size <= 0 {
			// Empty segment – no pages here
			continue
		}

		pages := uint32(size / int64(PageSize))
		total += pages
	}

	return total, nil
}
