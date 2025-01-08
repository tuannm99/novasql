package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const PageSize = 8192 // 8KB page size

// PageHeader represents metadata for the page.
type PageHeader struct {
	PageType   uint16
	NumTuples  uint16
	FreeSpace  uint16
	Checksum   uint32
	LinkToNext uint64
}

type Tuple struct {
	Data []byte
}

// Page represents a database page with all necessary components.
type Page struct {
	Header       PageHeader
	FreeSpaceMap []byte
	ItemPointers []ItemPointer
	Tuples       []Tuple
	Footer       Footer
}

// ItemPointer is a pointer to a tuple on the page.
type ItemPointer struct {
	Offset uint32
	Length uint32
}

// Footer contains additional metadata.
type Footer struct {
	Checksum uint32
}

// WritePage writes the page to a file (simulating disk storage).
func (p *Page) WritePage(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := new(bytes.Buffer)

	// Serialize PageHeader
	if err := binary.Write(buffer, binary.LittleEndian, p.Header); err != nil {
		return fmt.Errorf("failed to write page header: %v", err)
	}

	// Serialize FreeSpaceMap
	if _, err := buffer.Write(p.FreeSpaceMap); err != nil {
		return fmt.Errorf("failed to write free space map: %v", err)
	}

	// Serialize ItemPointers
	for _, itemPointer := range p.ItemPointers {
		if err := binary.Write(buffer, binary.LittleEndian, itemPointer); err != nil {
			return fmt.Errorf("failed to write item pointer: %v", err)
		}
	}

	// Serialize Tuples
	for _, tuple := range p.Tuples {
		if _, err := buffer.Write(tuple.Data); err != nil {
			return fmt.Errorf("failed to write tuple data: %v", err)
		}
	}

	// Serialize Footer
	if err := binary.Write(buffer, binary.LittleEndian, p.Footer); err != nil {
		return fmt.Errorf("failed to write footer: %v", err)
	}

	// Write to file
	if _, err := file.WriteAt(buffer.Bytes(), 0); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

// ReadPage reads a page from a file (simulating disk storage).
func (p *Page) ReadPage(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, PageSize)
	_, err = file.Read(buffer)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	// Deserialize the PageHeader
	if err := binary.Read(bytes.NewReader(buffer[:16]), binary.LittleEndian, &p.Header); err != nil {
		return fmt.Errorf("failed to read page header: %v", err)
	}

	// Deserialize other components...
	// Continue the deserialization process...

	return nil
}
