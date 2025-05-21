package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

const (
	DefaultPageHeaderSize  = 32 // Size of page header in bytes
	DefaultCellPointerSize = 8  // Size of cell pointer in bytes
)

var (
	ErrWriteExceedPageSize = errors.New("write would exceed page size")
	ErrReadExceedPageSize  = errors.New("read would exceed page size")
	ErrPageFull            = errors.New("write would exceed page data length")
)

// PageHeader contains metadata about the page
type PageHeader struct {
	ID             uint32   // Page ID
	Type           PageType // Type of page
	FreeStart      uint32   // Start of free space
	FreeEnd        uint32   // End of free space
	TotalFreeSpace uint32   // Total free space in page
	Flags          uint8    // Page flags
}

// Page represents a database page
type Page struct {
	pageNum  int          // Page number
	Data     []byte       // Page data
	dirty    bool         // Whether the page has been modified
	pinCount int          // Number of references to this page
	mu       sync.RWMutex // Protects concurrent access to page data
}

// Write writes data at the given offset
func (p *Page) Write(offset int, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset+len(data) > len(p.Data) {
		return ErrPageFull
	}
	copy(p.Data[offset:], data)
	p.dirty = true
	return nil
}

// Read reads data from the given offset and length
func (p *Page) Read(offset, length int) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset+length > len(p.Data) {
		return nil, ErrPageFull
	}
	return p.Data[offset : offset+length], nil
}

// WriteUint32 writes a uint32 value at the given offset
func (p *Page) WriteUint32(offset int, val uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset+4 > len(p.Data) {
		return ErrPageFull
	}
	binary.LittleEndian.PutUint32(p.Data[offset:], val)
	p.dirty = true
	return nil
}

// ReadUint32 reads a uint32 value from the given offset
func (p *Page) ReadUint32(offset int) (uint32, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset+4 > len(p.Data) {
		return 0, ErrPageFull
	}
	return binary.LittleEndian.Uint32(p.Data[offset:]), nil
}

// IsDirty returns whether the page has been modified
func (p *Page) IsDirty() bool {
	return p.dirty
}

// GetPageNum returns the page number
func (p *Page) GetPageNum() int {
	return p.pageNum
}

// GetData returns the raw page data
func (p *Page) GetData() []byte {
	return p.Data
}

// Serialize converts the page to a byte array
func (p *Page) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write header
	if err := binary.Write(buf, binary.LittleEndian, PageHeader{
		ID:             uint32(p.pageNum),
		Type:           PageType(p.Data[0]),
		FreeStart:      uint32(DefaultPageHeaderSize),
		FreeEnd:        uint32(len(p.Data)),
		TotalFreeSpace: uint32(len(p.Data) - DefaultPageHeaderSize),
		Flags:          0,
	}); err != nil {
		return nil, fmt.Errorf("serialize page header: %w", err)
	}

	// Write data
	if _, err := buf.Write(p.Data); err != nil {
		return nil, fmt.Errorf("serialize page data: %w", err)
	}

	// Ensure the buffer is PageSize bytes
	result := buf.Bytes()
	if len(result) < PageSize {
		padding := make([]byte, PageSize-len(result))
		result = append(result, padding...)
	} else if len(result) > PageSize {
		result = result[:PageSize]
	}

	return result, nil
}

// Deserialize reads the page from a byte array
func (p *Page) Deserialize(data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("deserialize: invalid page size: expected %d, got %d", PageSize, len(data))
	}

	buf := bytes.NewReader(data)

	// Read header
	var header PageHeader
	if err := binary.Read(buf, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("deserialize page header: %w", err)
	}

	p.pageNum = int(header.ID)
	p.Data = make([]byte, PageSize-DefaultPageHeaderSize)
	if _, err := buf.Read(p.Data); err != nil {
		return fmt.Errorf("deserialize page data: %w", err)
	}

	p.dirty = false
	return nil
}
