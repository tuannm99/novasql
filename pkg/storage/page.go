package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	Slotted PageType = iota + 1
	Directory
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
	Data   []byte
}

var (
	DefaultPageHeaderSize  = uint32(binary.Size(PageHeader{}))
	DefaultCellPointerSize = uint32(binary.Size(CellPointer{}))
	DefaultPageDataSize    = PageSize - DefaultPageHeaderSize
)

func (p *Page) GetHeaderSize() int {
	return binary.Size(p.Header)
}

func (p *Page) GetDataSize() int {
	return PageSize - p.GetHeaderSize()
}

// CellPointer represents a pointer to a cell in a page
type CellPointer struct {
	Location uint32
	Size     uint32
}

// PointerList represents a list of cell pointers in a page
type PointerList struct {
	Start []CellPointer
	Size  uint32
}

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
			FreeStart:      DefaultPageHeaderSize,
			FreeEnd:        PageSize - 1,
			TotalFreeSpace: PageSize - DefaultPageHeaderSize - 1,
		},
		Data: []byte{},
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
	return (offset - DefaultPageHeaderSize) / DefaultCellPointerSize
}

// Compute offset from cell pointer index
func GetOffsetCellPointerFromId(id uint32) uint32 {
	return id*DefaultCellPointerSize + DefaultPageHeaderSize
}

// Add a cell to the page, return index
func AddCell(page *Page, cell []byte) uint32 {
	header := &page.Header

	cellSize := uint32(len(cell))
	if header.TotalFreeSpace < cellSize+DefaultCellPointerSize {
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
	cellPointerBytes := make([]byte, DefaultCellPointerSize)
	binary.LittleEndian.PutUint32(cellPointerBytes[:4], cellPointer.Location)
	binary.LittleEndian.PutUint32(cellPointerBytes[4:], cellPointer.Size)
	copy(page.Data[pointerOffset:], cellPointerBytes)

	// Update page metadata
	header.FreeEnd -= cellSize
	header.FreeStart += DefaultCellPointerSize
	header.TotalFreeSpace = header.FreeEnd - header.FreeStart

	return GetIdFromCellPointerOffset(pointerOffset)
}

// Remove a cell from the page
func RemoveCell(page *Page, index uint32) {
	pointerOffset := GetOffsetCellPointerFromId(index)

	header := &page.Header
	header.Flags |= CanCompact

	// Mark cell as deleted by setting its location to 0
	cellPointerBytes := page.Data[pointerOffset : pointerOffset+DefaultCellPointerSize]
	// cellPointer := CellPointer{}
	binary.LittleEndian.PutUint32(cellPointerBytes[:4], 0)
	binary.LittleEndian.PutUint32(cellPointerBytes[4:], 0)
}

// Get the list of cell pointers in the page
func GetPointerList(page *Page) PointerList {
	header := &page.Header
	start := []CellPointer{}
	offset := DefaultPageHeaderSize

	for offset < uint32(header.FreeStart) {
		cellPointer := CellPointer{}
		binary.Read(bytes.NewReader(page.Data[offset:offset+DefaultCellPointerSize]), binary.LittleEndian, &cellPointer)
		start = append(start, cellPointer)
		offset += uint32(DefaultCellPointerSize)
	}

	size := (header.FreeStart - DefaultPageHeaderSize) / DefaultCellPointerSize
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

	for _, curPointer := range pointerList.Start {
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

func (p *Page) Serialize() ([]byte, error) {
	// Preallocated buffer
	buf := bytes.NewBuffer(make([]byte, 0, PageSize))

	// Write PageHeader
	if err := binary.Write(buf, binary.LittleEndian, &p.Header); err != nil {
		return nil, err
	}

	// Write Data
	if err := binary.Write(buf, binary.LittleEndian, p.Data[:]); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (p *Page) Deserialize(data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("invalid page size")
	}

	buf := bytes.NewReader(data)

	// Read PageHeader
	binary.Read(buf, binary.LittleEndian, &p.Header)

	// Read Data
	headerSize := p.GetHeaderSize()
	p.Data = data[headerSize:]

	return nil
}
