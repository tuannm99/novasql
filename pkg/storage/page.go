package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

const (
	OneB  = 1
	OneKB = 1024
	OneMB = OneKB * 1024
	OneGB = OneMB * 1024
)

const (
	// 8KB page size, similar to PostgreSQL
	PageSize   = OneKB * 8
	CanCompact = 0x01
)

// PageType enum
type PageType uint8

const (
	Root PageType = iota + 1
	Interior
	Leaf
)

type PageDirectory struct{}

// PageHeader represents metadata for a page
type PageHeader struct {
	ID             uint32
	Type           PageType
	FreeStart      uint32 // Offset to start of free space
	FreeEnd        uint32 // Offset to end of free space
	TotalFreeSpace uint32 // FreeEnd - FreeStart
	Flags          uint8

	// LogSequenceNumber interface{}
	// Lsn               interface{}
	// Checksum          interface{}
	// Special           interface{}
}

// Page structure to hold data and metadata
type Page struct {
	ID     uint32
	Header PageHeader
	Data   [PageSize - uint32(unsafe.Sizeof(PageHeader{}))]byte
}

// CellPointer represents a pointer to a cell in a page
type CellPointer struct {
	Location uint32
	Size     uint32
}

// PointerList represents a list of cell pointers in a page
type PointerList struct {
	Start *CellPointer
	Size  uint32
}

var (
	PageHeaderSize  = uint32(binary.Size(PageHeader{}))
	CellPointerSize = uint32(binary.Size(CellPointer{}))
)

// Create a new page with an empty header
func NewPage(sm *StorageManager, pageType PageType, id uint32) (*Page, error) {
	// Try to load from disk
	page, err := sm.LoadPage(id)
	if err == nil {
		return page, nil
	}

	// Create a new page if not found
	page = &Page{
		ID: id,
		Header: PageHeader{
			ID:             id,
			Type:           pageType,
			FreeStart:      PageHeaderSize,
			FreeEnd:        PageSize - 1,
			TotalFreeSpace: PageSize - PageHeaderSize - 1,
		},
	}

	// Save new page to disk
	err = sm.SavePage(page)
	if err != nil {
		return nil, err
	}

	return page, nil
}

// Compute index from cell pointer offset
func GetIdFromCellPointerOffset(offset uint32) uint32 {
	return (offset - PageHeaderSize) / CellPointerSize
}

// Compute offset from cell pointer index
func GetOffsetCellPointerFromId(id uint32) uint32 {
	return id*CellPointerSize + PageHeaderSize
}

// Add a cell to the page, return index
func AddCell(page *Page, cell []byte) uint32 {
	header := &page.Header

	cellSize := uint32(len(cell))
	if header.TotalFreeSpace < cellSize+CellPointerSize {
		panic("Not enough space in page")
	}

	// Create cell pointer
	cellPointer := CellPointer{
		Location: header.FreeEnd - cellSize,
		Size:     cellSize,
	}

	// Copy cell into page (simulated with a slice)
	copy(page.Data[cellPointer.Location:], cell)

	// Copy cell pointer to start of free space
	pointerOffset := header.FreeStart
	copy(page.Data[pointerOffset:], (*(*[unsafe.Sizeof(CellPointer{})]byte)(unsafe.Pointer(&cellPointer)))[:])

	// Update page metadata
	header.FreeEnd -= cellSize
	header.FreeStart += CellPointerSize
	header.TotalFreeSpace = header.FreeEnd - header.FreeStart

	return GetIdFromCellPointerOffset(pointerOffset)
}

// Remove a cell from the page
func RemoveCell(page *Page, index uint32) {
	pointerOffset := GetOffsetCellPointerFromId(uint32(index))

	header := &page.Header
	header.Flags |= CanCompact

	// Mark cell as deleted by setting its location to 0
	cellPointer := (*CellPointer)(unsafe.Pointer(&page.Data[pointerOffset]))
	cellPointer.Location = 0
}

// Get the list of cell pointers in the page
func GetPointerList(page *Page) PointerList {
	header := &page.Header
	start := (*CellPointer)(unsafe.Pointer(&page.Data[unsafe.Sizeof(PageHeader{})]))

	size := (header.FreeStart - PageHeaderSize) / CellPointerSize
	return PointerList{Start: start, Size: size}
}

// Compact the page, removing deleted cells
// POSTGRESQL VACUMM
func Compact(sm *StorageManager, page *Page) error {
	header := &page.Header
	if header.Flags&CanCompact == 0 {
		// No need to compact
		return nil
	}

	// Create a temporary page with the same ID
	newPage, err := NewPage(sm, page.Header.Type, page.ID)
	if err != nil {
		return err
	}

	// Get existing cell pointers
	pointerList := GetPointerList(page)

	for i := uint32(0); i < pointerList.Size; i++ {
		curPointer := (*CellPointer)(unsafe.Pointer(
			uintptr(unsafe.Pointer(pointerList.Start)) + uintptr(i*uint32(unsafe.Sizeof(CellPointer{})))))

		if curPointer.Location != 0 {
			cellData := page.Data[curPointer.Location : curPointer.Location+curPointer.Size]
			AddCell(newPage, cellData)
		}
	}

	// Copy metadata from newPage
	header.FreeStart = newPage.Header.FreeStart
	header.FreeEnd = newPage.Header.FreeEnd
	header.TotalFreeSpace = header.FreeEnd - header.FreeStart
	header.Flags &^= CanCompact

	// Copy compacted data back
	copy(page.Data[:], newPage.Data[:])

	// Save updated page to disk
	return sm.SavePage(page)
}

func (p *Page) Serialize() []byte {
	// Allocate buffer size, without padding
	buf := make([]byte, PageSize)

	// Write PageHeader manually
	offset := 0
	copy(buf[offset:], (*(*[unsafe.Sizeof(PageHeader{})]byte)(unsafe.Pointer(&p.Header)))[:])
	offset += int(unsafe.Sizeof(p.Header))

	// Write Data (no padding)
	copy(buf[offset:], p.Data[:])

	return buf
}

func (p *Page) Deserialize(data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("invalid page size")
	}

	buf := bytes.NewReader(data)

	// Read PageHeader
	binary.Read(buf, binary.LittleEndian, &p.Header)

	// Read Data
	copy(p.Data[:], data[binary.Size(p.Header):])

	return nil
}
