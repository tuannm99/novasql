package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// PageHeader represents metadata for a page
type PageHeader struct {
	ID             uint32
	Type           PageType
	FreeStart      uint32 // Offset to start of free space
	FreeEnd        uint32 // Offset to end of free space
	TotalFreeSpace uint32 // FreeEnd - FreeStart
	Flags          uint8
	// LSN            uint64 // Log Sequence Number for WAL
	// Checksum       uint32 // CRC32 checksum
	// TransactionID  uint64 // ID of last transaction that modified the page
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
		},
		Data:  make([]byte, PageSize-headerSize),
		dirty: true,
	}

	// Add the page to the buffer pool
	err = sm.bufferPool.AddPage(page)
	if err != nil {
		return nil, NewStorageError("add new page to buffer pool", err)
	}

	return page, nil
}

// Compute index from cell pointer offset
func getIdFromCellPointerOffset(offset uint32) uint32 {
	return (offset - DefaultPageHeaderSize) / DefaultCellPointerSize
}

// Compute offset from cell pointer index
func getOffsetCellPointerFromId(id uint32) uint32 {
	return id*DefaultCellPointerSize + DefaultPageHeaderSize
}

// AddCell adds a cell to the page and returns its index
func AddCell(page *Page, cell []byte) (uint32, error) {
	header := &page.Header
	cellSize := uint32(len(cell))

	// Check if there's enough space
	if header.TotalFreeSpace < cellSize+DefaultCellPointerSize {
		return 0, NewStorageError("add cell", fmt.Errorf("not enough space in page"))
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

	return getIdFromCellPointerOffset(pointerOffset), nil
}

// GetCell retrieves a cell by its index
func GetCell(page *Page, index uint32) ([]byte, error) {
	offset := getOffsetCellPointerFromId(index)

	if offset >= page.Header.FreeStart {
		return nil, NewStorageError("get cell", fmt.Errorf("invalid cell index: %d", index))
	}

	// Read cell pointer
	startCell := offset - DefaultPageHeaderSize
	pointerData := page.Data[startCell : startCell+DefaultCellPointerSize]
	location := binary.LittleEndian.Uint32(pointerData[:4])
	size := binary.LittleEndian.Uint32(pointerData[4:])

	if location == 0 || size == 0 {
		return nil, NewStorageError("get cell", fmt.Errorf("cell at index %d has been deleted", index))
	}

	// Read cell data
	return page.Data[location-DefaultPageHeaderSize : location-DefaultPageHeaderSize+size], nil
}

// RemoveCell removes a cell from the page
func RemoveCell(page *Page, index uint32) error {
	offset := getOffsetCellPointerFromId(index)

	if offset >= page.Header.FreeStart {
		return NewStorageError("remove cell", fmt.Errorf("invalid cell index: %d", index))
	}

	header := &page.Header
	header.Flags |= CanCompact
	page.dirty = true

	// Mark cell as deleted by setting its location and size to 0
	startCell := offset - DefaultPageHeaderSize
	cellPointerBytes := page.Data[startCell : startCell+DefaultCellPointerSize]
	binary.LittleEndian.PutUint32(cellPointerBytes[:4], 0)
	binary.LittleEndian.PutUint32(cellPointerBytes[4:], 0)

	return nil
}

// GetPointerList returns the list of cell pointers in the page
func GetPointerList(page *Page) (PointerList, error) {
	header := &page.Header
	start := []CellPointer{}
	offset := DefaultPageHeaderSize

	for offset < header.FreeStart {
		if offset+DefaultCellPointerSize > uint32(len(page.Data))+DefaultPageHeaderSize {
			return PointerList{}, NewStorageError("get pointer list", fmt.Errorf("pointer offset out of bounds"))
		}

		dataOffset := offset - DefaultPageHeaderSize

		if dataOffset+DefaultCellPointerSize > uint32(len(page.Data)) {
			return PointerList{}, NewStorageError("get pointer list", fmt.Errorf("pointer data offset out of bounds"))
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

// Compact removes deleted cells and reorganizes the page
func Compact(sm *StorageManager, page *Page) error {
	header := &page.Header
	if header.Flags&CanCompact == 0 {
		// No need to compact
		return nil
	}

	// Get existing cell pointers
	pointerList, err := GetPointerList(page)
	if err != nil {
		return NewStorageError("compact", fmt.Errorf("failed to get pointer list during compaction: %w", err))
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
		},
		Data:  make([]byte, PageSize-DefaultPageHeaderSize),
		dirty: true,
	}

	// Copy valid cells to the temporary page
	for _, ptr := range pointerList.Start {
		if ptr.Location != 0 && ptr.Size != 0 {
			cellData := page.Data[ptr.Location-DefaultPageHeaderSize : ptr.Location-DefaultPageHeaderSize+ptr.Size]
			_, err := AddCell(tempPage, cellData)
			if err != nil {
				return NewStorageError("compact", fmt.Errorf("failed to add cell during compaction: %w", err))
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
		return nil, NewStorageError("serialize page header", err)
	}

	// Write data
	if _, err := buf.Write(p.Data); err != nil {
		return nil, NewStorageError("serialize page data", err)
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
		return NewStorageError("deserialize", fmt.Errorf("invalid page size: expected %d, got %d", PageSize, len(data)))
	}

	buf := bytes.NewReader(data)

	// Read header
	if err := binary.Read(buf, binary.LittleEndian, &p.Header); err != nil {
		return NewStorageError("deserialize page header", err)
	}

	// Read data
	headerSize := DefaultPageHeaderSize
	p.Data = make([]byte, PageSize-headerSize)
	if _, err := buf.Read(p.Data); err != nil {
		return NewStorageError("deserialize page data", err)
	}

	p.dirty = false
	return nil
}

// Helper function that must be added to Page struct
func (p *Page) serializeHeader() ([]byte, error) {
	headerSize := binary.Size(p.Header)
	if headerSize < 0 {
		return nil, NewStorageError("serialize header", fmt.Errorf("invalid header size"))
	}

	headerBytes := make([]byte, headerSize)
	buf := bytes.NewBuffer(headerBytes[:0])

	err := binary.Write(buf, binary.LittleEndian, &p.Header)
	if err != nil {
		return nil, NewStorageError("serialize header", err)
	}

	return buf.Bytes(), nil
}
