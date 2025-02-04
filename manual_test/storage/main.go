package main

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/tuannm99/novasql/pkg/storage"
)

func main() {
	testByte := []byte("Here is a string....")
	size := binary.Size(testByte)
	fmt.Printf("❤❤❤ tuannm: [page.go][79][size]: %+v\n", size)

	fmt.Println(uint32(unsafe.Sizeof(storage.PageHeader{
		ID:             100000,
		TotalFreeSpace: 9999,
	})))
}
