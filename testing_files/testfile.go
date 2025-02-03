package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

// Constants
const (
	PageSize       = 8192 // 8 KB page size
	PageHeaderSize = 16   // Metadata size in the header
)

// PageHeader structure
type PageHeader struct {
	FreeSpaceOffset uint16 // Offset of free space in the page
	TupleCount      uint16 // Number of tuples in the page
}

// LinePointer structure
type LinePointer struct {
	Offset uint16 // Offset of the tuple in the page
	Length uint16 // Length of the tuple
}

// Page structure
type Page struct {
	Header       PageHeader     // Page header
	LinePointers []LinePointer  // Array of line pointers
	Data         [PageSize]byte // Page data
}

// NewPage creates a new empty page
func NewPage() *Page {
	return &Page{
		Header: PageHeader{
			FreeSpaceOffset: PageSize,
			TupleCount:      0,
		},
		LinePointers: []LinePointer{},
	}
}

// InsertTuple inserts a tuple into the page
func (p *Page) InsertTuple(data []byte) error {
	tupleLength := len(data)
	requiredSpace := tupleLength + 4 // Line pointer (4 bytes: Offset + Length)

	// Check if there's enough free space
	freeSpace := int(p.Header.FreeSpaceOffset) - PageHeaderSize - (len(p.LinePointers) * 4)
	if freeSpace < requiredSpace {
		return fmt.Errorf("not enough space to insert tuple")
	}

	// Update FreeSpaceOffset and LinePointer
	p.Header.FreeSpaceOffset -= uint16(tupleLength)
	offset := p.Header.FreeSpaceOffset

	// Copy data into the page
	copy(p.Data[offset:], data)

	// Add line pointer
	p.LinePointers = append(p.LinePointers, LinePointer{
		Offset: offset,
		Length: uint16(tupleLength),
	})
	p.Header.TupleCount++

	return nil
}

// SaveToFile writes the Page struct to a binary file
func (p *Page) SaveToFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	// Write the PageHeader
	err = binary.Write(file, binary.LittleEndian, p.Header)
	if err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	// Write the LinePointers
	for _, lp := range p.LinePointers {
		err = binary.Write(file, binary.LittleEndian, lp)
		if err != nil {
			return fmt.Errorf("failed to write line pointer: %v", err)
		}
	}

	// Write the Data
	_, err = file.Write(p.Data[:])
	if err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	return nil
}

// LoadFromFile reads the Page struct from a binary file
func LoadFromFile(filename string) (*Page, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	page := NewPage()

	// Read the PageHeader
	err = binary.Read(file, binary.LittleEndian, &page.Header)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %v", err)
	}

	// Read the LinePointers
	for i := 0; i < int(page.Header.TupleCount); i++ {
		var lp LinePointer
		err = binary.Read(file, binary.LittleEndian, &lp)
		if err != nil {
			return nil, fmt.Errorf("failed to read line pointer: %v", err)
		}
		page.LinePointers = append(page.LinePointers, lp)
	}

	// Read the Data
	_, err = file.Read(page.Data[:])
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %v", err)
	}

	return page, nil
}

func main() {
	page := NewPage()

	// Insert tuples
	tuples := []string{"Hello", "World", "Slotted", "Page"}
	for _, t := range tuples {
		err := page.InsertTuple([]byte(t))
		if err != nil {
			log.Fatalf("Error inserting tuple: %v\n", err)
		}
	}

	// Save the page to a file
	err := page.SaveToFile("page.bat")
	if err != nil {
		log.Fatalf("Error saving page to file: %v\n", err)
	}

	// Load the page from the file
	loadedPage, err := LoadFromFile("page.bat")
	if err != nil {
		log.Fatalf("Error loading page from file: %v\n", err)
	}

	// Display loaded page content
	fmt.Printf("Loaded Page Header: %+v\n", loadedPage.Header)
	for i, lp := range loadedPage.LinePointers {
		fmt.Printf("LinePointer[%d]: %+v\n", i, lp)
		if lp.Length > 0 {
			fmt.Printf("  Data: %s\n", string(loadedPage.Data[lp.Offset:lp.Offset+lp.Length]))
		}
	}
}
