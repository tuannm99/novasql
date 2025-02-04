package storage

import (
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

// Create a new page with an empty header
func NewPage(pageType PageType, id uint32) *Page {
	page := &Page{
		ID: id,
		Header: PageHeader{
			ID:             id,
			Type:           pageType,
			FreeStart:      uint32(unsafe.Sizeof(PageHeader{})),
			FreeEnd:        PageSize - 1,
			TotalFreeSpace: PageSize - uint32(unsafe.Sizeof(PageHeader{})) - 1,
		},
	}
	return page
}

// Compute index from cell pointer offset
func GetIdFromCellPointerOffset(offset uint32) uint32 {
	return (offset - uint32(unsafe.Sizeof(PageHeader{}))) / uint32(unsafe.Sizeof(CellPointer{}))
}

// Compute offset from cell pointer index
func GetOffsetCellPointerFromId(id uint32) uint32 {
	return id*uint32(unsafe.Sizeof(CellPointer{})) + uint32(unsafe.Sizeof(PageHeader{}))
}

// Add a cell to the page, return index
func AddCell(page *Page, cell []byte) uint32 {
	header := &page.Header

	cellSize := uint32(len(cell))
	if header.TotalFreeSpace < cellSize+uint32(unsafe.Sizeof(CellPointer{})) {
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
	header.FreeStart += uint32(unsafe.Sizeof(CellPointer{}))
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

	size := (header.FreeStart - uint32(unsafe.Sizeof(PageHeader{}))) / uint32(unsafe.Sizeof(CellPointer{}))
	return PointerList{Start: start, Size: size}
}

// Compact the page, removing deleted cells
// POSTGRESQL VACUMM
func Compact(page *Page) {
	header := &page.Header
	if header.Flags&CanCompact == 0 {
		return
	}

	newPage := NewPage(Root, 0)
	pointerList := GetPointerList(page)

	for i := uint32(0); i < pointerList.Size; i++ {
		curPointer := (*CellPointer)(unsafe.Pointer(uintptr(unsafe.Pointer(pointerList.Start)) + uintptr(i*uint32(unsafe.Sizeof(CellPointer{})))))

		if curPointer.Location != 0 {
			cellData := page.Data[curPointer.Location : curPointer.Location+curPointer.Size]
			AddCell(newPage, cellData)
		}
	}

	header.FreeStart = newPage.Header.FreeStart
	header.FreeEnd = newPage.Header.FreeEnd
	header.TotalFreeSpace = header.FreeEnd - header.FreeStart
	header.Flags &^= CanCompact

	copy(page.Data[unsafe.Sizeof(PageHeader{}):], newPage.Data[unsafe.Sizeof(PageHeader{}):])
}

// Page structure to hold data and metadata
type Page struct {
	ID     uint32
	Header PageHeader
	Data   [PageSize]byte
}
