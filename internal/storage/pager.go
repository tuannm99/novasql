package storage

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// Pager manages the database file and provides direct page access
type Pager struct {
	file      *os.File     // Database file
	fileSize  int64        // Size of the database file
	pageSize  int          // Size of each page
	pageCount int          // Number of pages in the database
	mu        sync.RWMutex // Mutex for thread safety
}

// NewPager creates a new pager for the given database file
func NewPager(filename string, pageSize int) (*Pager, error) {
	// Open or create the database file
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, FileMode0664)
	if err != nil {
		return nil, fmt.Errorf("open database file: %w", err)
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("get file info: %w", err)
	}

	pager := &Pager{
		file:      file,
		fileSize:  fileInfo.Size(),
		pageSize:  pageSize,
		pageCount: int(fileInfo.Size()) / pageSize,
	}

	return pager, nil
}

// GetPage retrieves a page from disk
func (p *Pager) GetPage(pageNum int) (*Page, error) {
	if pageNum < 0 {
		return nil, fmt.Errorf("invalid page number: %d", pageNum)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Create a new page
	page := &Page{
		Data: make([]byte, p.pageSize),
	}

	// Read page from disk if it exists
	if pageNum < p.pageCount {
		offset := int64(pageNum * p.pageSize)
		if _, err := p.file.Seek(offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to page: %w", err)
		}

		if _, err := io.ReadFull(p.file, page.Data); err != nil {
			return nil, fmt.Errorf("read page: %w", err)
		}
	} else {
		// New page, zero it out
		for i := range page.Data {
			page.Data[i] = 0
		}
	}

	return page, nil
}

// WritePage writes a page to disk
func (p *Pager) WritePage(pageNum int, data []byte) error {
	if pageNum < 0 {
		return fmt.Errorf("invalid page number: %d", pageNum)
	}

	if len(data) != p.pageSize {
		return fmt.Errorf("invalid page size: expected %d, got %d", p.pageSize, len(data))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	offset := int64(pageNum * p.pageSize)
	if _, err := p.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek to page: %w", err)
	}

	if _, err := p.file.Write(data); err != nil {
		return fmt.Errorf("write page: %w", err)
	}

	// Update page count if we're writing beyond the current file size
	if pageNum >= p.pageCount {
		p.pageCount = pageNum + 1
	}

	return nil
}

// Close closes the database file
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Close()
}

// PageCount returns the number of pages in the database
func (p *Pager) PageCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pageCount
}

// PageSize returns the size of each page
func (p *Pager) PageSize() int {
	return p.pageSize
}

// File returns the underlying database file
func (p *Pager) File() *os.File {
	return p.file
}
