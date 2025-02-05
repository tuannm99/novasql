package storage

import (
	// "encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	// Avoid large file issues (some OS/filesystems limit file sizes).
	// Faster backups & recovery (smaller file chunks).
	// Better parallel I/O (multiple segments can be read/written independently).
	SegmentSize = 1 * 1024 * 1024 * 1024 // 1GB per segment
)

// managing database files
type StorageManager struct {
	dir       string
	pageCount uint32
}

// NewStorageManager initializes storage in a directory
func NewStorageManager(dir string) *StorageManager {
	return &StorageManager{dir: dir}
}

// GetSegmentPath returns the file path of a segment based on the page number
func (sm *StorageManager) GetSegmentPath(pageID uint32) string {
	segmentID := pageID / (SegmentSize / PageSize)
	return filepath.Join(sm.dir, fmt.Sprintf("segment_%d", segmentID))
}

// WritePage writes a page to its corresponding segment file
func (sm *StorageManager) WritePage(pageID uint32, data []byte) error {
	if len(data) > PageSize {
		return fmt.Errorf("page must smaller than %d bytes", PageSize)
	}

	segmentPath := sm.GetSegmentPath(pageID)
	offset := int64((pageID % (SegmentSize / PageSize)) * PageSize)

	// Ensure the directory exists
	dir := filepath.Dir(segmentPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Open file (create if not exists)
	file, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Seek to the correct offset
	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}

	// Write page data
	_, err = file.Write(data)
	return err
}

// ReadPage reads a page from disk
func (sm *StorageManager) ReadPage(pageID uint32) ([]byte, error) {
	segmentPath := sm.GetSegmentPath(pageID)
	offset := int64((pageID % (SegmentSize / PageSize)) * PageSize)

	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]byte, PageSize)
	_, err = file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (sm *StorageManager) LoadPage(pageID uint32) (*Page, error) {
	data, err := sm.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	page := &Page{}
	err = page.Deserialize(data)
	if err != nil {
		return nil, err
	}

	return page, nil
}

func (sm *StorageManager) SavePage(page *Page) error {
	data := page.Serialize()
	return sm.WritePage(page.ID, data)
}
