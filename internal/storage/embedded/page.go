package embedded 

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/tuannm99/novasql/internal/storage"
)

const (
	PageHeaderSize  = 32
	DefaultCellPointerSize = 8
)

const PageSize = storage.PageSize

type PageHeader struct {
	ID             uint32
	Type           storage.PageType
	FreeStart      uint32
	FreeEnd        uint32
	TotalFreeSpace uint32
	Flags          uint8
}

type Page struct {
	pageNum int
	Data    []byte
	dirty   bool
	mu      sync.RWMutex
}

func (p *Page) Write(offset int, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset+len(data) > len(p.Data) {
		return storage.ErrPageFull
	}
	copy(p.Data[offset:], data)
	p.dirty = true
	return nil
}

func (p *Page) Read(offset, length int) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset+length > len(p.Data) {
		return nil, storage.ErrPageFull
	}
	return p.Data[offset : offset+length], nil
}

func (p *Page) WriteUint32(offset int, val uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset+4 > len(p.Data) {
		return storage.ErrPageFull
	}
	binary.LittleEndian.PutUint32(p.Data[offset:], val)
	p.dirty = true
	return nil
}

func (p *Page) ReadUint32(offset int) (uint32, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset+4 > len(p.Data) {
		return 0, storage.ErrPageFull
	}
	return binary.LittleEndian.Uint32(p.Data[offset:]), nil
}

func (p *Page) IsDirty() bool {
	return p.dirty
}

func (p *Page) GetPageNum() int {
	return p.pageNum
}

func (p *Page) GetData() []byte {
	return p.Data
}

func (p *Page) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, PageHeader{
		ID:             uint32(p.pageNum),
		Type:           storage.PageType(p.Data[0]),
		FreeStart:      uint32(PageHeaderSize),
		FreeEnd:        uint32(len(p.Data)),
		TotalFreeSpace: uint32(len(p.Data) - PageHeaderSize),
		Flags:          0,
	}); err != nil {
		return nil, fmt.Errorf("serialize page header: %w", err)
	}

	if _, err := buf.Write(p.Data); err != nil {
		return nil, fmt.Errorf("serialize page data: %w", err)
	}

	result := buf.Bytes()
	if len(result) < PageSize {
		padding := make([]byte, PageSize-len(result))
		result = append(result, padding...)
	} else if len(result) > PageSize {
		result = result[:PageSize]
	}

	return result, nil
}

func (p *Page) Deserialize(data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("deserialize: invalid page size: expected %d, got %d", PageSize, len(data))
	}

	buf := bytes.NewReader(data)

	var header PageHeader
	if err := binary.Read(buf, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("deserialize page header: %w", err)
	}

	p.pageNum = int(header.ID)
	p.Data = make([]byte, PageSize-PageHeaderSize)
	if _, err := buf.Read(p.Data); err != nil {
		return fmt.Errorf("deserialize page data: %w", err)
	}

	p.dirty = false
	return nil
}
