package main

import (
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

type PageType uint8

const (
	Root PageType = iota + 1
	Interior
	Leaf
)

type BufferPool struct {
	pageDirectory PageDirectory
}

func (bp *BufferPool) getPage(pageID int) {
}

type PageDirectory struct {
}

type PageHeader struct {
	ID                uint32
	Type              PageType
	LogSequenceNumber interface{}
	FreeStart         uint16
	FreeEnd           uint16
	TotalFree         uint16

	Lsn      interface{}
	Checksum interface{}
	Flags    uint8
	Lower    interface{}
	Upper    interface{}
	Special  interface{}
}

type Page struct {
	ID     int64
	Header PageHeader
}

// mataining database files
type StorageManager struct {
}

type TupleHeader struct {
}

type Tuple struct {
	header TupleHeader
}

type TableData struct {
	ID          int64
	name        [255]byte
	description [1024]byte
}

func main() {
	testByte := []byte("Here is a string....")
	size := binary.Size(testByte)
	fmt.Printf("❤❤❤ tuannm: [page.go][79][size]: %+v\n", size)

}
