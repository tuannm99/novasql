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
	BTree
	Overflow
)

// PageHeader represents metadata for a page
type PageHeader struct {
	ID             uint32
	Type           PageType
	FreeStart      uint32 // Offset to start of free space
	FreeEnd        uint32 // Offset to end of free space
	TotalFreeSpace uint32 // FreeEnd - FreeStart
	Flags          uint8
	LSN            uint64 // Log Sequence Number for WAL
	Checksum       uint32 // CRC32 checksum
}

// Page structure to hold data and metadata
type Page struct {
	ID     uint32
	Header PageHeader
	Data   []byte
	dirty  bool // Track if page has been modified
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

var (
	DefaultPageHeaderSize  = uint32(binary.Size(PageHeader{}))
	DefaultCellPointerSize = uint32(binary.Size(CellPointer{}))
	DefaultPageDataSize    = PageSize - DefaultPageHeaderSize
)

// GetHeaderSize returns the size of the page header
func (p *Page) GetHeaderSize() int {
	return binary.Size(p.Header)
}

// GetDataSize returns the size of the page data
func (p *Page) GetDataSize() int {
	return PageSize - p.GetHeaderSize()
}

// Create a new page with an empty header
func NewPage(sm *StorageManager, pageType PageType, id uint32) (*Page, error) {
	// Try to load from disk
	page, err := sm.LoadPage(id)
	if err == nil {
		return page, nil
	}

	// Create a new page if not found
	headerSize := uint32(binary.Size(PageHeader{}))
	page = &Page{
		ID: id,
		Header: PageHeader{
			ID:             id,
			Type:           pageType,
			FreeStart:      headerSize,
			FreeEnd:        PageSize,
			TotalFreeSpace: PageSize - headerSize,
			Flags:          0,
			LSN:            0,
			Checksum:       0,
		},
		Data:  make([]byte, PageSize-headerSize),
		dirty: true,
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
func AddCell(page *Page, cell []byte) (uint32, error) {
	header := &page.Header
	cellSize := uint32(len(cell))

	// Check if there's enough space
	if header.TotalFreeSpace < cellSize+DefaultCellPointerSize {
		return 0, fmt.Errorf("not enough space in page")
	}

	// Create cell pointer
	cellPointer := CellPointer{
		Location: header.FreeEnd - cellSize,
		Size:     cellSize,
	}

	// Copy cell into page at the end (growing downward)
	copy(page.Data[cellPointer.Location-DefaultPageHeaderSize:], cell)

	// Copy cell pointer to start of free space
	pointerOffset := header.FreeStart
	cellPointerBytes := make([]byte, DefaultCellPointerSize)
	binary.LittleEndian.PutUint32(cellPointerBytes[:4], cellPointer.Location)
	binary.LittleEndian.PutUint32(cellPointerBytes[4:], cellPointer.Size)
	copy(page.Data[pointerOffset-DefaultPageHeaderSize:], cellPointerBytes)

	// Update page metadata
	header.FreeEnd -= cellSize
	header.FreeStart += DefaultCellPointerSize
	header.TotalFreeSpace = header.FreeEnd - header.FreeStart
	page.dirty = true

	return GetIdFromCellPointerOffset(pointerOffset), nil
}

// GetCell retrieves a cell by its index
func GetCell(page *Page, index uint32) ([]byte, error) {
	pointerOffset := GetOffsetCellPointerFromId(index)

	if pointerOffset >= page.Header.FreeStart {
		return nil, fmt.Errorf("invalid cell index: %d", index)
	}

	// Read cell pointer
	pointerData := page.Data[pointerOffset-DefaultPageHeaderSize : pointerOffset-DefaultPageHeaderSize+DefaultCellPointerSize]
	location := binary.LittleEndian.Uint32(pointerData[:4])
	size := binary.LittleEndian.Uint32(pointerData[4:])

	if location == 0 || size == 0 {
		return nil, fmt.Errorf("cell at index %d has been deleted", index)
	}

	// Read cell data
	return page.Data[location-DefaultPageHeaderSize : location-DefaultPageHeaderSize+size], nil
}

// Remove a cell from the page
func RemoveCell(page *Page, index uint32) error {
	pointerOffset := GetOffsetCellPointerFromId(index)

	if pointerOffset >= page.Header.FreeStart {
		return fmt.Errorf("invalid cell index: %d", index)
	}

	header := &page.Header
	header.Flags |= CanCompact
	page.dirty = true

	// Mark cell as deleted by setting its location and size to 0
	cellPointerBytes := page.Data[pointerOffset-DefaultPageHeaderSize : pointerOffset-DefaultPageHeaderSize+DefaultCellPointerSize]
	binary.LittleEndian.PutUint32(cellPointerBytes[:4], 0)
	binary.LittleEndian.PutUint32(cellPointerBytes[4:], 0)

	return nil
}

// Get the list of cell pointers in the page
func GetPointerList(page *Page) (PointerList, error) {
	header := &page.Header
	start := []CellPointer{}
	offset := DefaultPageHeaderSize

	for offset < header.FreeStart {
		if offset+DefaultCellPointerSize > uint32(len(page.Data))+DefaultPageHeaderSize {
			return PointerList{}, fmt.Errorf("pointer offset out of bounds")
		}

		dataOffset := offset - DefaultPageHeaderSize

		if dataOffset+DefaultCellPointerSize > uint32(len(page.Data)) {
			return PointerList{}, fmt.Errorf("pointer data offset out of bounds")
		}

		location := binary.LittleEndian.Uint32(page.Data[dataOffset : dataOffset+4])
		size := binary.LittleEndian.Uint32(page.Data[dataOffset+4 : dataOffset+8])

		cellPointer := CellPointer{
			Location: location,
			Size:     size,
		}

		start = append(start, cellPointer)
		offset += DefaultCellPointerSize
	}

	size := (header.FreeStart - DefaultPageHeaderSize) / DefaultCellPointerSize
	return PointerList{Start: start, Size: size}, nil
}

// Compact the page, removing deleted cells
func Compact(sm *StorageManager, page *Page) error {
	header := &page.Header
	if header.Flags&CanCompact == 0 {
		// No need to compact
		return nil
	}

	// Get existing cell pointers
	pointerList, err := GetPointerList(page)
	if err != nil {
		return fmt.Errorf("compact: %v", err)
	}

	// Create a temporary page with the same ID and type
	tempPage := &Page{
		ID: page.ID,
		Header: PageHeader{
			ID:             page.ID,
			Type:           page.Header.Type,
			FreeStart:      DefaultPageHeaderSize,
			FreeEnd:        PageSize,
			TotalFreeSpace: PageSize - DefaultPageHeaderSize,
			Flags:          0,
			LSN:            page.Header.LSN,
			Checksum:       page.Header.Checksum,
		},
		Data:  make([]byte, PageSize-DefaultPageHeaderSize),
		dirty: true,
	}

	// Copy valid cells to the temporary page
	for _, curPointer := range pointerList.Start {
		if curPointer.Location != 0 && curPointer.Size != 0 {
			cellData := page.Data[curPointer.Location-DefaultPageHeaderSize : curPointer.Location-DefaultPageHeaderSize+curPointer.Size]
			_, err := AddCell(tempPage, cellData)
			if err != nil {
				return fmt.Errorf("compact - adding cell: %v", err)
			}
		}
	}

	// Copy metadata and data from temporary page back to original
	page.Header.FreeStart = tempPage.Header.FreeStart
	page.Header.FreeEnd = tempPage.Header.FreeEnd
	page.Header.TotalFreeSpace = tempPage.Header.TotalFreeSpace
	page.Header.Flags &^= CanCompact // Clear the CanCompact flag
	copy(page.Data, tempPage.Data)
	page.dirty = true

	// Save updated page to disk
	return sm.SavePage(page)
}

// Serialize converts the page to a byte array
func (p *Page) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write header
	if err := binary.Write(buf, binary.LittleEndian, p.Header); err != nil {
		return nil, fmt.Errorf("serialize header: %v", err)
	}

	// Write data
	if _, err := buf.Write(p.Data); err != nil {
		return nil, fmt.Errorf("serialize data: %v", err)
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
		return fmt.Errorf("invalid page size: expected %d, got %d", PageSize, len(data))
	}

	buf := bytes.NewReader(data)

	// Read header
	if err := binary.Read(buf, binary.LittleEndian, &p.Header); err != nil {
		return fmt.Errorf("deserialize header: %v", err)
	}

	// Read data
	headerSize := DefaultPageHeaderSize
	p.Data = make([]byte, PageSize-headerSize)
	if _, err := buf.Read(p.Data); err != nil {
		return fmt.Errorf("deserialize data: %v", err)
	}

	p.dirty = false
	return nil
}

