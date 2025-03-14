package main

import (
	"fmt"
	"os"

	"github.com/tuannm99/novasql/pkg/storage"
)

func main() {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "novasql-test-*")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)
	fmt.Printf("Testing in directory: %s\n", tmpDir)

	// Initialize storage manager
	sm, err := storage.NewStorageManager(tmpDir)
	if err != nil {
		fmt.Printf("Failed to create storage manager: %v\n", err)
		return
	}
	defer sm.Close()

	// Initialize page directory
	pd, err := storage.NewPageDirectory(tmpDir)
	if err != nil {
		fmt.Printf("Failed to create page directory: %v\n", err)
		return
	}

	fmt.Println("\n=== Testing Page Operations ===")
	testPageOperations(sm)

	fmt.Println("\n=== Testing Tuple Operations ===")
	testTupleOperations(sm, pd)

	fmt.Println("\n=== Testing B+ Tree Operations ===")
	testBTreeOperations(sm)

	fmt.Println("\nAll tests completed!")
}

func testPageOperations(sm *storage.StorageManager) {
	// CREATE: Allocate a new page
	fmt.Println("Creating new page...")
	page, err := sm.AllocatePage(storage.Slotted)
	if err != nil {
		fmt.Printf("Failed to allocate page: %v\n", err)
		return
	}
	pageID := page.ID
	fmt.Printf("Created page with ID: %d\n", pageID)

	// CREATE: Add a cell to the page
	fmt.Println("Adding cell to page...")
	testData := []byte("Hello, NovaSQL!")
	cellIndex, err := storage.AddCell(page, testData, 1) // txID = 1
	if err != nil {
		fmt.Printf("Failed to add cell: %v\n", err)
		return
	}
	fmt.Printf("Added cell at index: %d\n", cellIndex)

	// Save the page
	err = sm.SavePage(page)
	if err != nil {
		fmt.Printf("Failed to save page: %v\n", err)
		return
	}

	// READ: Load the page and verify cell content
	fmt.Println("Reading cell from page...")
	loadedPage, err := sm.LoadPage(pageID)
	if err != nil {
		fmt.Printf("Failed to load page: %v\n", err)
		return
	}

	cellData, err := storage.GetCell(loadedPage, cellIndex)
	if err != nil {
		fmt.Printf("Failed to get cell: %v\n", err)
		return
	}
	fmt.Printf("Read cell data: %s\n", string(cellData))

	// UPDATE: Modify the cell
	fmt.Println("Updating cell...")
	// To update a cell, we need to remove it and add a new one
	err = storage.RemoveCell(loadedPage, cellIndex, 2) // txID = 2
	if err != nil {
		fmt.Printf("Failed to remove cell: %v\n", err)
		return
	}

	updatedData := []byte("Updated NovaSQL data!")
	newCellIndex, err := storage.AddCell(loadedPage, updatedData, 2) // txID = 2
	if err != nil {
		fmt.Printf("Failed to add updated cell: %v\n", err)
		return
	}
	fmt.Printf("Added updated cell at index: %d\n", newCellIndex)

	// Save the page
	err = sm.SavePage(loadedPage)
	if err != nil {
		fmt.Printf("Failed to save page: %v\n", err)
		return
	}

	// READ: Verify updated content
	loadedPage, err = sm.LoadPage(pageID)
	if err != nil {
		fmt.Printf("Failed to reload page: %v\n", err)
		return
	}

	updatedCellData, err := storage.GetCell(loadedPage, newCellIndex)
	if err != nil {
		fmt.Printf("Failed to get updated cell: %v\n", err)
		return
	}
	fmt.Printf("Read updated cell data: %s\n", string(updatedCellData))

	// DELETE: Remove the cell
	fmt.Println("Deleting cell...")
	err = storage.RemoveCell(loadedPage, newCellIndex, 3) // txID = 3
	if err != nil {
		fmt.Printf("Failed to delete cell: %v\n", err)
		return
	}

	// Save the page
	err = sm.SavePage(loadedPage)
	if err != nil {
		fmt.Printf("Failed to save page after deletion: %v\n", err)
		return
	}

	// Verify cell is gone
	loadedPage, err = sm.LoadPage(pageID)
	if err != nil {
		fmt.Printf("Failed to reload page: %v\n", err)
		return
	}

	_, err = storage.GetCell(loadedPage, newCellIndex)
	if err != nil {
		fmt.Printf("As expected, cell is gone: %v\n", err)
	} else {
		fmt.Println("ERROR: Cell still exists after deletion!")
	}

	// Test page compaction
	fmt.Println("Testing page compaction...")
	err = storage.Compact(sm, loadedPage)
	if err != nil {
		fmt.Printf("Failed to compact page: %v\n", err)
		return
	}
	fmt.Printf("Page compacted successfully. Free space: %d bytes\n", loadedPage.Header.TotalFreeSpace)
}

func testTupleOperations(sm *storage.StorageManager, pd *storage.PageDirectory) {
	// Create a "table" by adding a page to the directory
	tableName := "test_table"

	// Allocate a page for the table
	page, err := sm.AllocatePage(storage.Slotted)
	if err != nil {
		fmt.Printf("Failed to allocate page for table: %v\n", err)
		return
	}

	// Add the page to the directory
	err = pd.AddPage(tableName, uint32(page.ID))
	if err != nil {
		fmt.Printf("Failed to add page to directory: %v\n", err)
		return
	}
	fmt.Printf("Created table '%s' with page ID: %d\n", tableName, page.ID)

	// CREATE: Insert tuples
	fmt.Println("Inserting tuples...")
	tuple1 := storage.Tuple{
		ID:   1,
		Data: []byte("First record"),
	}

	err = page.InsertTuple(tuple1, 1) // txID = 1
	if err != nil {
		fmt.Printf("Failed to insert tuple: %v\n", err)
		return
	}

	tuple2 := storage.Tuple{
		ID:   2,
		Data: []byte("Second record"),
	}

	err = page.InsertTuple(tuple2, 1) // txID = 1
	if err != nil {
		fmt.Printf("Failed to insert second tuple: %v\n", err)
		return
	}

	// Save the page
	err = sm.SavePage(page)
	if err != nil {
		fmt.Printf("Failed to save page: %v\n", err)
		return
	}

	// READ: Fetch tuples
	fmt.Println("Reading tuples...")
	loadedPage, err := sm.LoadPage(page.ID)
	if err != nil {
		fmt.Printf("Failed to load page: %v\n", err)
		return
	}

	fetchedTuple1, err := loadedPage.FetchTuple(1)
	if err != nil {
		fmt.Printf("Failed to fetch tuple 1: %v\n", err)
		return
	}
	fmt.Printf("Tuple 1: ID=%d, Data=%s\n", fetchedTuple1.ID, string(fetchedTuple1.Data))

	fetchedTuple2, err := loadedPage.FetchTuple(2)
	if err != nil {
		fmt.Printf("Failed to fetch tuple 2: %v\n", err)
		return
	}
	fmt.Printf("Tuple 2: ID=%d, Data=%s\n", fetchedTuple2.ID, string(fetchedTuple2.Data))

	// UPDATE: We need to remove and re-insert to update a tuple
	fmt.Println("Updating tuple...")
	// This is simplified - in real impl, you'd need to locate which cell contains the tuple
	// through scanning and then remove that cell.

	// For now, let's just add a new tuple with updated data
	tuple1Updated := storage.Tuple{
		ID:   1,
		Data: []byte("First record (updated)"),
	}

	err = loadedPage.InsertTuple(tuple1Updated, 2) // txID = 2
	if err != nil {
		fmt.Printf("Failed to insert updated tuple: %v\n", err)
		return
	}

	// Save the page
	err = sm.SavePage(loadedPage)
	if err != nil {
		fmt.Printf("Failed to save page: %v\n", err)
		return
	}

	// READ: Verify update (fetchedTuple should find the latest version)
	loadedPage, err = sm.LoadPage(page.ID)
	if err != nil {
		fmt.Printf("Failed to reload page: %v\n", err)
		return
	}

	updatedTuple, err := loadedPage.FetchTuple(1)
	if err != nil {
		fmt.Printf("Failed to fetch updated tuple: %v\n", err)
		return
	}
	fmt.Printf("Updated tuple: ID=%d, Data=%s\n", updatedTuple.ID, string(updatedTuple.Data))

	// Test page directory operations
	fmt.Println("Testing page directory...")
	pages, err := pd.GetPages(tableName)
	if err != nil {
		fmt.Printf("Failed to get pages for table: %v\n", err)
		return
	}
	fmt.Printf("Table '%s' has %d pages\n", tableName, len(pages))
}

func testBTreeOperations(sm *storage.StorageManager) {
	// Create a new B+ tree
	fmt.Println("Creating B+ tree...")
	btree, err := storage.NewBPlusTree(4, sm) // Order 4
	if err != nil {
		fmt.Printf("Failed to create B+ tree: %v\n", err)
		return
	}

	rootPageID := btree.GetRootPageID()
	fmt.Printf("Created B+ tree with root page ID: %d\n", rootPageID)

	// INSERT: Add key-value pairs
	fmt.Println("Inserting key-value pairs...")
	testData := []struct {
		key   string
		value string
	}{
		{"apple", "red fruit"},
		{"banana", "yellow fruit"},
		{"cherry", "red fruit"},
		{"date", "brown fruit"},
		{"elderberry", "purple fruit"},
	}

	for _, data := range testData {
		err := btree.Insert([]byte(data.key), []byte(data.value))
		if err != nil {
			fmt.Printf("Failed to insert key '%s': %v\n", data.key, err)
			continue
		}
		fmt.Printf("Inserted key: %s\n", data.key)
	}

	// READ: Search for values
	fmt.Println("Searching for values...")
	for _, data := range testData {
		value, err := btree.Search([]byte(data.key))
		if err != nil {
			fmt.Printf("Failed to find key '%s': %v\n", data.key, err)
			continue
		}
		fmt.Printf("Found: %s -> %s\n", data.key, string(value))
	}

	// Search for non-existent key
	_, err = btree.Search([]byte("fig"))
	if err != nil {
		fmt.Printf("As expected, key 'fig' not found: %v\n", err)
	} else {
		fmt.Println("ERROR: Found key 'fig' which shouldn't exist!")
	}

	// DELETE: Remove a key
	fmt.Println("Deleting a key...")
	err = btree.Delete([]byte("banana"))
	if err != nil {
		fmt.Printf("Failed to delete key 'banana': %v\n", err)
	} else {
		fmt.Println("Deleted key: banana")
	}

	// Verify deletion
	_, err = btree.Search([]byte("banana"))
	if err != nil {
		fmt.Printf("As expected, key 'banana' was deleted: %v\n", err)
	} else {
		fmt.Println("ERROR: Key 'banana' still exists after deletion!")
	}

	// Load the tree from its root page ID
	fmt.Println("Loading B+ tree from disk...")
	loadedTree, err := storage.LoadBPlusTree(rootPageID, 4, sm)
	if err != nil {
		fmt.Printf("Failed to load B+ tree: %v\n", err)
		return
	}

	// Verify we can still search
	fmt.Println("Verifying tree after loading...")
	value, err := loadedTree.Search([]byte("cherry"))
	if err != nil {
		fmt.Printf("Failed to find key 'cherry' after loading: %v\n", err)
	} else {
		fmt.Printf("Found after loading: cherry -> %s\n", string(value))
	}
}

