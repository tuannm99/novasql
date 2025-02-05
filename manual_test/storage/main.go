package main

import (
	"fmt"

	"github.com/tuannm99/novasql/pkg/storage"
)

func main() {

	// Initialize StorageManager
	sm := storage.NewStorageManager("./data")

	// Create a new page
	page, err := storage.NewPage(sm, storage.Leaf, 0)
	if err != nil {
		panic(err)
	}

	// Modify page
	page.Header.Flags |= storage.CanCompact

	// Save updated page
	err = sm.SavePage(page)
	if err != nil {
		panic(err)
	}

	// Load page from disk
	loadedPage, err := sm.LoadPage(0)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Loaded Page ID: %d, Flags: %d\n", loadedPage.ID, loadedPage.Header.Flags)

}
