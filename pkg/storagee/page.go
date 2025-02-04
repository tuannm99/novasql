package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

type PageDirectory struct {
}

type PageHeader struct {
	PageNumber   int32
	PageType     string
	FreeSpacePtr int32
}

type Item struct {
	Data []byte
}

type Page struct {
	ID           int
	Header       PageHeader
	ItemIds      []int32
	FreeSpace    []byte
	Items        []Item
	SpecialSpace []byte
}

// Function to serialize Page struct and write to file
func (p *Page) WriteToFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	err = binary.Write(file, binary.LittleEndian, p.Header.PageNumber)
	if err != nil {
		return err
	}

	pageTypeLen := int32(len(p.Header.PageType))
	err = binary.Write(file, binary.LittleEndian, pageTypeLen)
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, []byte(p.Header.PageType))
	if err != nil {
		return err
	}

	err = binary.Write(file, binary.LittleEndian, p.Header.FreeSpacePtr)
	if err != nil {
		return err
	}

	itemIdsLen := int32(len(p.ItemIds))
	err = binary.Write(file, binary.LittleEndian, itemIdsLen)
	if err != nil {
		return err
	}

	for _, itemId := range p.ItemIds {
		err = binary.Write(file, binary.LittleEndian, itemId)
		if err != nil {
			return err
		}
	}

	itemsLen := int32(len(p.Items))
	err = binary.Write(file, binary.LittleEndian, itemsLen)
	if err != nil {
		return err
	}

	for _, item := range p.Items {
		itemLen := int32(len(item.Data))
		err = binary.Write(file, binary.LittleEndian, itemLen)
		if err != nil {
			return err
		}
		err = binary.Write(file, binary.LittleEndian, item.Data)
		if err != nil {
			return err
		}
	}

	freeSpaceLen := int32(len(p.FreeSpace))
	err = binary.Write(file, binary.LittleEndian, freeSpaceLen)
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, p.FreeSpace)
	if err != nil {
		return err
	}

	specialSpaceLen := int32(len(p.SpecialSpace))
	err = binary.Write(file, binary.LittleEndian, specialSpaceLen)
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, p.SpecialSpace)
	if err != nil {
		return err
	}

	return nil
}

// Function to read a Page struct from file
func ReadFromFile(filename string) (*Page, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	p := &Page{}

	err = binary.Read(file, binary.LittleEndian, &p.Header.PageNumber)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading PageNumber")
	}

	var pageTypeLen int32
	err = binary.Read(file, binary.LittleEndian, &pageTypeLen)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading PageTypeLen")
	}

	pageTypeBytes := make([]byte, pageTypeLen)
	err = binary.Read(file, binary.LittleEndian, &pageTypeBytes)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading PageType string")
	}
	p.Header.PageType = string(pageTypeBytes)

	err = binary.Read(file, binary.LittleEndian, &p.Header.FreeSpacePtr)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading FreeSpacePtr")
	}

	var itemIdsLen int32
	err = binary.Read(file, binary.LittleEndian, &itemIdsLen)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading ItemIdsLen")
	}

	for i := int32(0); i < itemIdsLen; i++ {
		var itemId int32
		err = binary.Read(file, binary.LittleEndian, &itemId)
		if err != nil {
			return nil, fmt.Errorf("unexpected EOF while reading ItemId %d", i)
		}
		p.ItemIds = append(p.ItemIds, itemId)
	}

	var itemsLen int32
	err = binary.Read(file, binary.LittleEndian, &itemsLen)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading Items length")
	}

	for i := int32(0); i < itemsLen; i++ {
		var itemLen int32
		err = binary.Read(file, binary.LittleEndian, &itemLen)
		if err != nil {
			return nil, fmt.Errorf("unexpected EOF while reading Items length")
		}
		data := make([]byte, itemLen)
		err = binary.Read(file, binary.LittleEndian, &data)
		if err != nil {
			return nil, fmt.Errorf("unexpected EOF while reading Items data")
		}
		p.Items = append(p.Items, Item{Data: data})
	}

	var freeSpaceLen int32
	err = binary.Read(file, binary.LittleEndian, &freeSpaceLen)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading FreeSpace length")
	}

	p.FreeSpace = make([]byte, freeSpaceLen)
	err = binary.Read(file, binary.LittleEndian, &p.FreeSpace)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading FreeSpace data")
	}

	var specialSpaceLen int32
	err = binary.Read(file, binary.LittleEndian, &specialSpaceLen)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading SpecialSpace length")
	}

	p.SpecialSpace = make([]byte, specialSpaceLen)
	err = binary.Read(file, binary.LittleEndian, &p.SpecialSpace)
	if err != nil {
		return nil, fmt.Errorf("unexpected EOF while reading SpecialSpace data")
	}

	return p, nil
}
