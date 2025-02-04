// package main
//
// import (
// 	"encoding/binary"
// 	"fmt"
// 	"math"
// 	"os"
// )
//
// const (
// 	OneB  = 1
// 	OneKB = 1024
// 	OneMB = OneKB * 1024
// 	OneGB = OneMB * 1024
// )
//
// const (
// 	PageSize   = OneKB * 4
// 	CanCompact = 0x01
// )
//
// type PageType uint8
//
// const (
// 	Root PageType = iota + 1
// 	Interior
// 	Leaf
// )
//
// type PageHeader struct {
// 	ID                uint32
// 	Type              PageType
// 	LogSequenceNumber interface{}
// 	FreeStart         uint16
// 	FreeEnd           uint16
// 	TotalFree         uint16
//
// 	Lsn      interface{}
// 	Checksum interface{}
// 	Flags    uint8
// 	Lower    interface{}
// 	Upper    interface{}
// 	Special  interface{}
// }
//
// type LinePointer struct {
// 	LineLocation uint16
// 	LineSize     uint16
// }
//
// type PointerList struct {
// 	Start []LinePointer
// 	Size  int
// }
//
// type Page struct {
// 	ID           uint32
// 	PageHeader   PageHeader
// 	ItemIdData   []interface{}
// 	FreeSpace    interface{}
// 	items        []interface{}
// 	SpecialSpace interface{}
// }
//
// type Movie struct {
// 	ID      uint32
// 	Title   [100]byte
// 	Rating  float32
// 	Release uint32
// }
//
// func newPage(pageType PageType, id uint32) []byte {
// 	page := make([]byte, PageSize)
// 	header := PageHeader{
// 		ID:        id,
// 		Type:      pageType,
// 		FreeStart: uint16(binary.Size(PageHeader{})),
// 		FreeEnd:   PageSize - 1,
// 		TotalFree: PageSize - uint16(binary.Size(PageHeader{})),
// 	}
// 	copy(page[:binary.Size(header)], headerToBytes(header))
// 	return page
// }
//
// func headerToBytes(header PageHeader) []byte {
// 	buf := make([]byte, binary.Size(header))
// 	binary.LittleEndian.PutUint32(buf[0:4], header.ID)
// 	buf[4] = byte(header.Type)
// 	binary.LittleEndian.PutUint16(buf[5:7], header.FreeStart)
// 	binary.LittleEndian.PutUint16(buf[7:9], header.FreeEnd)
// 	binary.LittleEndian.PutUint16(buf[9:11], header.TotalFree)
// 	buf[11] = header.Flags
//
// 	return buf
// }
//
// func bytesToHeader(data []byte) PageHeader {
// 	return PageHeader{
// 		ID:        binary.LittleEndian.Uint32(data[0:4]),
// 		Type:      PageType(data[4]),
// 		FreeStart: binary.LittleEndian.Uint16(data[5:7]),
// 		FreeEnd:   binary.LittleEndian.Uint16(data[7:9]),
// 		TotalFree: binary.LittleEndian.Uint16(data[9:11]),
// 		Flags:     data[11],
// 	}
// }
//
// func addLine(page []byte, line []byte) uint16 {
// 	header := bytesToHeader(page[:binary.Size(PageHeader{})])
// 	LinePointer := LinePointer{
// 		LineLocation: header.FreeEnd - uint16(len(line)),
// 		LineSize:     uint16(len(line)),
// 	}
// 	if header.TotalFree < LinePointer.LineSize+uint16(binary.Size(LinePointer)) {
// 		panic("Not enough space on page")
// 	}
//
// 	copy(page[LinePointer.LineLocation:], line)
// 	copy(page[header.FreeStart:], LinePointerToBytes(LinePointer))
//
// 	header.FreeEnd -= LinePointer.LineSize
// 	header.FreeStart += uint16(binary.Size(LinePointer))
// 	header.TotalFree = header.FreeEnd - header.FreeStart
// 	copy(page[:binary.Size(header)], headerToBytes(header))
//
// 	return (header.FreeStart - uint16(binary.Size(LinePointer))) / uint16(binary.Size(LinePointer))
// }
//
// func LinePointerToBytes(cp LinePointer) []byte {
// 	buf := make([]byte, binary.Size(cp))
// 	binary.LittleEndian.PutUint16(buf[0:2], cp.LineLocation)
// 	binary.LittleEndian.PutUint16(buf[2:4], cp.LineSize)
// 	return buf
// }
//
// func bytesToLinePointer(data []byte) LinePointer {
// 	return LinePointer{
// 		LineLocation: binary.LittleEndian.Uint16(data[0:2]),
// 		LineSize:     binary.LittleEndian.Uint16(data[2:4]),
// 	}
// }
//
// func savePage(file *os.File, page []byte) {
// 	header := bytesToHeader(page[:binary.Size(PageHeader{})])
// 	_, err := file.Seek(int64(header.ID)*PageSize, 0)
// 	if err != nil {
// 		panic(err)
// 	}
// 	_, err = file.Write(page)
// 	if err != nil {
// 		panic(err)
// 	}
// }
//
// func loadPage(file *os.File, pageID uint32) []byte {
// 	page := make([]byte, PageSize)
// 	_, err := file.Seek(int64(pageID)*PageSize, 0)
// 	if err != nil {
// 		panic(err)
// 	}
// 	_, err = file.Read(page)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return page
// }
//
// func displayHeader(page []byte) {
// 	header := bytesToHeader(page[:binary.Size(PageHeader{})])
// 	fmt.Printf("Page: ID=%d, Type=%d, FreeStart=%d, FreeEnd=%d, TotalFree=%d, Flags=%d\n",
// 		header.ID, header.Type, header.FreeStart, header.FreeEnd, header.TotalFree, header.Flags)
// }
//
// func main() {
// 	file, err := os.OpenFile("movies.db", os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer file.Close()
//
// 	page := newPage(Leaf, 0)
// 	displayHeader(page)
//
// 	movie1 := Movie{ID: 1, Title: [100]byte{'T', 'o', 'y', ' ', 'S', 't', 'o', 'r', 'y'}, Rating: 0.92, Release: 1995}
// 	addLine(page, movieToBytes(movie1))
//
// 	displayHeader(page)
//
// 	savePage(file, page)
//
// 	loadedPage := loadPage(file, 0)
// 	displayHeader(loadedPage)
// }
//
// func movieToBytes(movie Movie) []byte {
// 	buf := make([]byte, binary.Size(movie))
// 	binary.LittleEndian.PutUint32(buf[0:4], movie.ID)
// 	copy(buf[4:104], movie.Title[:])
// 	binary.LittleEndian.PutUint32(buf[104:108], math.Float32bits(movie.Rating))
// 	binary.LittleEndian.PutUint32(buf[108:112], movie.Release)
// 	return buf
// }
