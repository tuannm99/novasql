package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// Avoid large file issues (some OS/filesystems limit file sizes).
	// Faster backups & recovery (smaller file chunks).
	// Better parallel I/O (multiple segments can be read/written independently).
	SegmentSize = 1 * 1024 * 1024 * 1024 // 1GB per segment

	FileMode0644 = 0644 // rw-r--r--
	FileMode0664 = 0664 // rw-rw-r--
	FileMode0600 = 0600 // rw-------
	FileMode0755 = 0755 // rwxr-xr-x
	FileMode0777 = 0777 // rwxrwxrwx
	FileMode0700 = 0700 // rwx------
	FileMode0555 = 0555 // r-xr-xr-x
	FileMode0400 = 0400 // r--------
	FileMode0200 = 0200 // -w-------
	FileMode0111 = 0111 // --x--x--x)
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
		return fmt.Errorf("StorageManager::WritePage page must smaller than %d bytes", PageSize)
	}

	segmentPath := sm.GetSegmentPath(pageID)
	offset := int64((pageID % (SegmentSize / PageSize)) * PageSize)

	// Ensure the directory exists
	dir := filepath.Dir(segmentPath)
	if err := os.MkdirAll(dir, FileMode0755); err != nil {
		return fmt.Errorf("StorageManager::WritePage makedir %v", err)
	}

	// Open file (create if not exists)
	file, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE, FileMode0664)
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
	if err != nil {
		return fmt.Errorf("StorageManager::WritePage %v", err)
	}
	return nil
}

// ReadPage reads a page from disk
func (sm *StorageManager) ReadPage(pageID uint32) ([]byte, error) {
	segmentPath := sm.GetSegmentPath(pageID)
	offset := int64((pageID % (SegmentSize / PageSize)) * PageSize)

	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("StorageManager::ReadPage %v", err)
	}
	defer file.Close()

	data := make([]byte, PageSize)
	_, err = file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("StorageManager::ReadPage %v", err)
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
	data, err := page.Serialize()
	if err != nil {
		return fmt.Errorf("StorageManager::SavePage %v", err)
	}
	return sm.WritePage(page.ID, data)
}
