// package main
//
// import (
// 	"encoding/binary"
// 	"fmt"
// 	// "log"
// 	"os"
// )
//
// const (
// 	BlockSize = 8192
// )
//
// type LinePointer struct {
// 	Offset int32
// 	Length int32
// }
//
// type PageHeader struct {
// 	PageNumber   int32
// 	PageType     string
// 	FreeSpacePtr int32
// }
//
// type Tuple struct {
// 	Data []byte
// }
//
// type Page struct {
// 	Header       PageHeader
// 	ItemIds      []int32
// 	FreeSpace    []byte
// 	Tuple        []Tuple
// 	SpecialSpace []byte
// 	LinePointers []LinePointer
// }
//
// // Function to serialize Page struct and write to file
// func (p *Page) WriteToFile(filename string) error {
// 	file, err := os.Create(filename)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()
//
// 	err = binary.Write(file, binary.LittleEndian, p.Header.PageNumber)
// 	if err != nil {
// 		return err
// 	}
//
// 	pageTypeLen := int32(len(p.Header.PageType))
// 	err = binary.Write(file, binary.LittleEndian, pageTypeLen)
// 	if err != nil {
// 		return err
// 	}
// 	err = binary.Write(file, binary.LittleEndian, []byte(p.Header.PageType))
// 	if err != nil {
// 		return err
// 	}
//
// 	err = binary.Write(file, binary.LittleEndian, p.Header.FreeSpacePtr)
// 	if err != nil {
// 		return err
// 	}
//
// 	itemIdsLen := int32(len(p.ItemIds))
// 	err = binary.Write(file, binary.LittleEndian, itemIdsLen)
// 	if err != nil {
// 		return err
// 	}
//
// 	for _, itemId := range p.ItemIds {
// 		err = binary.Write(file, binary.LittleEndian, itemId)
// 		if err != nil {
// 			return err
// 		}
// 	}
//
// 	itemsLen := int32(len(p.Tuple))
// 	err = binary.Write(file, binary.LittleEndian, itemsLen)
// 	if err != nil {
// 		return err
// 	}
//
// 	for _, item := range p.Tuple {
// 		itemLen := int32(len(item.Data))
// 		err = binary.Write(file, binary.LittleEndian, itemLen)
// 		if err != nil {
// 			return err
// 		}
// 		err = binary.Write(file, binary.LittleEndian, item.Data)
// 		if err != nil {
// 			return err
// 		}
// 	}
//
// 	freeSpaceLen := int32(len(p.FreeSpace))
// 	err = binary.Write(file, binary.LittleEndian, freeSpaceLen)
// 	if err != nil {
// 		return err
// 	}
// 	err = binary.Write(file, binary.LittleEndian, p.FreeSpace)
// 	if err != nil {
// 		return err
// 	}
//
// 	specialSpaceLen := int32(len(p.SpecialSpace))
// 	err = binary.Write(file, binary.LittleEndian, specialSpaceLen)
// 	if err != nil {
// 		return err
// 	}
// 	err = binary.Write(file, binary.LittleEndian, p.SpecialSpace)
// 	if err != nil {
// 		return err
// 	}
//
// 	return nil
// }
//
// // Function to read a Page struct from file
// func ReadFromFile(filename string) (*Page, error) {
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()
//
// 	p := &Page{}
//
// 	err = binary.Read(file, binary.LittleEndian, &p.Header.PageNumber)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading PageNumber")
// 	}
//
// 	var pageTypeLen int32
// 	err = binary.Read(file, binary.LittleEndian, &pageTypeLen)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading PageTypeLen")
// 	}
//
// 	pageTypeBytes := make([]byte, pageTypeLen)
// 	err = binary.Read(file, binary.LittleEndian, &pageTypeBytes)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading PageType string")
// 	}
// 	p.Header.PageType = string(pageTypeBytes)
//
// 	err = binary.Read(file, binary.LittleEndian, &p.Header.FreeSpacePtr)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading FreeSpacePtr")
// 	}
//
// 	var itemIdsLen int32
// 	err = binary.Read(file, binary.LittleEndian, &itemIdsLen)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading ItemIdsLen")
// 	}
//
// 	for i := int32(0); i < itemIdsLen; i++ {
// 		var itemId int32
// 		err = binary.Read(file, binary.LittleEndian, &itemId)
// 		if err != nil {
// 			return nil, fmt.Errorf("unexpected EOF while reading ItemId %d", i)
// 		}
// 		p.ItemIds = append(p.ItemIds, itemId)
// 	}
//
// 	var itemsLen int32
// 	err = binary.Read(file, binary.LittleEndian, &itemsLen)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading Items length")
// 	}
//
// 	for i := int32(0); i < itemsLen; i++ {
// 		var itemLen int32
// 		err = binary.Read(file, binary.LittleEndian, &itemLen)
// 		if err != nil {
// 			return nil, fmt.Errorf("unexpected EOF while reading Items length")
// 		}
// 		data := make([]byte, itemLen)
// 		err = binary.Read(file, binary.LittleEndian, &data)
// 		if err != nil {
// 			return nil, fmt.Errorf("unexpected EOF while reading Items data")
// 		}
// 		p.Tuple = append(p.Tuple, Tuple{Data: data})
// 	}
//
// 	var freeSpaceLen int32
// 	err = binary.Read(file, binary.LittleEndian, &freeSpaceLen)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading FreeSpace length")
// 	}
//
// 	p.FreeSpace = make([]byte, freeSpaceLen)
// 	err = binary.Read(file, binary.LittleEndian, &p.FreeSpace)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading FreeSpace data")
// 	}
//
// 	var specialSpaceLen int32
// 	err = binary.Read(file, binary.LittleEndian, &specialSpaceLen)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading SpecialSpace length")
// 	}
//
// 	p.SpecialSpace = make([]byte, specialSpaceLen)
// 	err = binary.Read(file, binary.LittleEndian, &p.SpecialSpace)
// 	if err != nil {
// 		return nil, fmt.Errorf("unexpected EOF while reading SpecialSpace data")
// 	}
//
// 	return p, nil
// }
//
// func (p *Page) InsertRow(data []byte) (int, error) {
// 	if len(data) > len(p.FreeSpace) {
// 		return -1, fmt.Errorf("not enough free space to insert row")
// 	}
//
// 	offset := len(p.Tuple) // Use the length of Tuple slice as the offset
// 	lp := LinePointer{Offset: int32(offset), Length: int32(len(data))}
// 	p.LinePointers = append(p.LinePointers, lp)
// 	p.Tuple = append(p.Tuple, Tuple{Data: data})
//
// 	// Update FreeSpace
// 	fmt.Println(p.FreeSpace[len(data):])
// 	p.FreeSpace = p.FreeSpace[len(data):]
//
// 	return len(p.LinePointers) - 1, nil
// }
//
// func (p *Page) GetRow(index int) ([]byte, error) {
// 	if index < 0 || index >= len(p.LinePointers) {
// 		return nil, fmt.Errorf("index out of range")
// 	}
//
// 	lp := p.LinePointers[index]
// 	if lp.Length == 0 {
// 		return nil, fmt.Errorf("row is deleted")
// 	}
//
// 	tuple := p.Tuple[lp.Offset]
// 	return tuple.Data[:lp.Length], nil
// }
//
// func (p *Page) UpdateRow(index int, newData []byte) error {
// 	if index < 0 || index >= len(p.LinePointers) {
// 		return fmt.Errorf("index out of range")
// 	}
//
// 	lp := p.LinePointers[index]
// 	if int32(len(newData)) > lp.Length {
// 		return fmt.Errorf("new data exceeds original length")
// 	}
//
// 	tuple := &p.Tuple[lp.Offset]
// 	copy(tuple.Data, newData)
// 	return nil
// }
//
// func (p *Page) DeleteRow(index int) error {
// 	if index < 0 || index >= len(p.LinePointers) {
// 		return fmt.Errorf("index out of range")
// 	}
//
// 	// Mark the row as deleted by setting length to 0 (logical deletion)
// 	p.LinePointers[index].Length = 0
// 	return nil
// }
//
// // func main() {
// // 	page := &Page{
// // 		Header: PageHeader{
// // 			PageNumber:   1,
// // 			PageType:     "Data",
// // 			FreeSpacePtr: 100,
// // 		},
// // 		ItemIds:      []int32{0, 1},
// // 		FreeSpace:    []byte{0x00, 0x01, 0x02},
// // 		Tuple:        []Tuple{{Data: []byte("Item1")}, {Data: []byte("Item2")}},
// // 		SpecialSpace: []byte{0xFF},
// // 	}
// //
// // 	err := page.WriteToFile("page_data.dat")
// // 	if err != nil {
// // 		log.Fatal("Error writing to file:", err)
// // 	}
// //
// // 	loadedPage, err := ReadFromFile("page_data.dat")
// // 	if err != nil {
// // 		log.Fatal("Error reading from file:", err)
// // 	}
// //
// // 	fmt.Printf("Loaded Page: %+v\n", loadedPage)
// // 	loadedPage.InsertRow([]byte{11})
// //
// // 	row0, _ := loadedPage.GetRow(0)
// // 	fmt.Printf("❤❤❤ tuannm: [demo_file.go][297][row0]: %+v\n", row0)
// //
// // 	fmt.Printf("Loaded Page: %+v\n", loadedPage)
// // }
