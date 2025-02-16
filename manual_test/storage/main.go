package main

import (
	"os"

	"fmt"
	"github.com/tuannm99/novasql/pkg/storage"
)

func main() {
	sm := storage.NewStorageManager("./data")

	page, err := storage.NewPage(sm, storage.Leaf, 0)
	if err != nil {
		panic(err)
	}

	page.Header.Flags |= storage.CanCompact

	err = sm.SavePage(page)
	if err != nil {
		panic(err)
	}

	loadedPage, err := sm.LoadPage(0)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Loaded Page ID: %d, Flags: %d, Data: %d \n", loadedPage.ID,
		loadedPage.Header.Flags, loadedPage.Data)

	file, err := os.OpenFile("data/segment_0", os.O_RDWR, 0644)

	if err != nil {
		fmt.Printf("❤❤❤ tuannm: [main.go][35][err]: %+v\n", err)
		return
	}

	defer file.Close()

	_, err = file.Seek(18, 0) // seek file at position 18 means got data, not the header

	buffer := make([]byte, 8192)
	n, err := file.Read(buffer)
	if err != nil {
		fmt.Printf("❤❤❤ tuannm: [main.go][46][err]: %+v\n", err)
	}

	fmt.Println(buffer[:n])

	// _, err = file.WriteAt([]byte{111}, 18)
	// fmt.Printf("❤❤❤ tuannm: [main.go][53][err]: %+v\n", err)
	bytesWritten, err := file.WriteAt([]byte{113, 100}, 18) // write file at position 18 -> 0-17 is a header
	if err != nil {
		fmt.Printf("❤❤❤ tuannm: [main.go][err writing file]: %v\n", err)
	} else {
		fmt.Printf("❤❤❤ tuannm: Successfully wrote %d bytes\n", bytesWritten)
	}

}
