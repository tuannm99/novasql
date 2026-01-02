package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var (
	ErrPageNotFound = errors.New("storage_manager: page not found")
	ErrPageFull     = errors.New("storage_manager: write would exceed page data length")
)

type FileSet interface {
	OpenSegment(segNo int32) (*os.File, error)
}

var _ FileSet = (*LocalFileSet)(nil)

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
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}

type StorageManager struct{}

func NewStorageManager() *StorageManager { return &StorageManager{} }

func (sm *StorageManager) pagesPerSegment() int {
	return SegmentSize / PageSize
}

func (sm *StorageManager) locate(pageID int32) (segNo int32, offset int32) {
	pps := sm.pagesPerSegment()
	segNo = pageID / int32(pps)
	pageInSeg := pageID % int32(pps)
	offset = pageInSeg * PageSize
	return segNo, offset
}

func (sm *StorageManager) ReadPage(fs FileSet, pageID int32, dst []byte) error {
	if pageID < 0 {
		return fmt.Errorf("pageID must be >= 0, got %d", pageID)
	}
	if len(dst) != PageSize {
		return fmt.Errorf("dst must be exactly %d bytes", PageSize)
	}
	segNo, off := sm.locate(pageID)
	f, err := fs.OpenSegment(segNo)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	n, err := f.ReadAt(dst, int64(off))
	if err != nil && err != io.EOF {
		return err
	}
	for i := n; i < PageSize; i++ {
		dst[i] = 0
	}
	return nil
}

func (sm *StorageManager) WritePage(fs FileSet, pageID int32, src []byte) error {
	if pageID < 0 {
		return fmt.Errorf("pageID must be >= 0, got %d", pageID)
	}
	if len(src) != PageSize {
		return fmt.Errorf("src must be exactly %d bytes", PageSize)
	}
	segNo, off := sm.locate(pageID)
	f, err := fs.OpenSegment(segNo)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	n, err := f.WriteAt(src, int64(off))
	if err != nil {
		return err
	}
	if n != PageSize {
		return io.ErrShortWrite
	}
	return nil
}

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

func (sm *StorageManager) SavePage(fs FileSet, pageID uint32, p Page) error {
	if len(p.Buf) != PageSize {
		return fmt.Errorf("page buffer must be %d bytes", PageSize)
	}
	return sm.WritePage(fs, int32(pageID), p.Buf)
}

func (sm *StorageManager) CountPages(fs FileSet) (uint32, error) {
	if lfs, ok := fs.(LocalFileSet); ok {
		return countPagesLocalFileSet(lfs)
	}
	return 0, nil
}

func countPagesLocalFileSet(lfs LocalFileSet) (uint32, error) {
	if err := os.MkdirAll(lfs.Dir, 0o755); err != nil {
		return 0, err
	}

	segs, err := listSegmentsLocal(lfs)
	if err != nil {
		return 0, err
	}

	var total uint32
	for _, segNo := range segs {
		path := filepath.Join(lfs.Dir, SegFileName(lfs.Base, segNo))
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}

		size := info.Size()
		if size <= 0 {
			continue
		}

		// WritePage always writes full pages, so floor is fine.
		total += uint32(size / int64(PageSize))
	}

	return total, nil
}
