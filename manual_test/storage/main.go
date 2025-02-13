package main

import (
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
}
