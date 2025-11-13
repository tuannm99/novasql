package main

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/tuannm99/novasql/internal/storage"
)

func must(okSlot int, err error) int {
	if err != nil {
		log.Fatal(err)
	}
	return okSlot
}

func insert(p *storage.Page) {
	// 1) chuỗi (UTF-8)
	_ = must(p.InsertTuple([]byte("chuỗi tuannm99")))

	// 2) chuỗi dài
	_ = must(p.InsertTuple([]byte("cmnaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")))

	// 3) 1 byte: 0x01
	_ = must(p.InsertTuple([]byte{1}))

	// 4) 1 byte: 0xFF (255)
	_ = must(p.InsertTuple([]byte{255}))

	// 5) số 256 -> 2 byte LE: 0x00 0x01
	_ = must(p.InsertTuple([]byte{0x00, 0x01}))
	// or encoding/binary:
	b2 := make([]byte, 2)
	binary.LittleEndian.PutUint16(b2, 256)
	_ = must(p.InsertTuple(b2))
}

func main() {
	buf := make([]byte, storage.PageSize)
	p, err := storage.NewPage(buf, 0)
	if err != nil {
		log.Fatal(err)
	}
	insert(p)
	fmt.Println(p.DebugString())

	// check data
	for i := range 6 {
		byteData, _ := p.ReadTuple(i)
		fmt.Println("slot - bytedata")
		fmt.Println(i, byteData)
	}

	_ = p.UpdateTuple(0, []byte("updated chuỗi tuannm99"))
	fmt.Println(p.DebugString())

	_ = p.DeleteTuple(0)
	fmt.Println(p.DebugString())
}
