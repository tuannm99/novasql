package embedded

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/tuannm99/novasql/internal/storage"
)

type Pager struct {
	file      *os.File
	fileSize  int64
	pageSize  int
	pageCount int
	mu        sync.RWMutex
}

func NewPager(filename string, pageSize int) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, storage.FileMode0664)
	if err != nil {
		return nil, fmt.Errorf("open database file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
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

func (p *Pager) GetPage(pageNum int) (*Page, error) {
	if pageNum < 0 {
		return nil, fmt.Errorf("invalid page number: %d", pageNum)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	page := &Page{
		Data: make([]byte, p.pageSize),
	}

	if pageNum < p.pageCount {
		offset := int64(pageNum * p.pageSize)
		if _, err := p.file.Seek(offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to page: %w", err)
		}

		if _, err := io.ReadFull(p.file, page.Data); err != nil {
			return nil, fmt.Errorf("read page: %w", err)
		}
	} else {
		for i := range page.Data {
			page.Data[i] = 0
		}
	}

	return page, nil
}

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

	if pageNum >= p.pageCount {
		p.pageCount = pageNum + 1
	}

	return nil
}

func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Close()
}

func (p *Pager) PageCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pageCount
}

func (p *Pager) PageSize() int {
	return p.pageSize
}

func (p *Pager) File() *os.File {
	return p.file
}
