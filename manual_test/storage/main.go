package main

import (
	"fmt"
	"os"

	"github.com/tuannm99/novasql/internal/storage"
)

func main() {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "testdb-*.db")
	if err != nil {
		fmt.Printf("Failed to create temp file: %v\n", err)
		return
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())
	fmt.Printf("Testing with database file: %s\n", tmpfile.Name())

	// Initialize pager
	pager, err := storage.NewPager(tmpfile.Name(), storage.PageSize)
	if err != nil {
		fmt.Printf("Failed to create pager: %v\n", err)
		return
	}
	defer pager.Close()

	fmt.Println("\n=== Testing Basic Page Operations ===")
	testBasicPageOperations(pager)

	fmt.Println("\n=== Testing Multiple Pages ===")
	testMultiplePages(pager)

	fmt.Println("\n=== Testing Page Persistence ===")
	testPagePersistence(pager)

	fmt.Println("\nAll tests completed!")
}

func testBasicPageOperations(pager *storage.Pager) {
	// Test getting a new page
	fmt.Println("Getting page 0...")
	page, err := pager.GetPage(0)
	if err != nil {
		fmt.Printf("Failed to get page: %v\n", err)
		return
	}
	fmt.Printf("Successfully got page 0\n")

	// Write some data to the page
	fmt.Println("Writing data to page...")
	testData := []byte("Hello, NovaSQL!")
	copy(page.Data[:len(testData)], testData)

	// Write the page to disk
	err = pager.WritePage(0, page.Data)
	if err != nil {
		fmt.Printf("Failed to write page: %v\n", err)
		return
	}
	fmt.Printf("Successfully wrote page to disk\n")

	// Read the page back
	fmt.Println("Reading page back...")
	page, err = pager.GetPage(0)
	if err != nil {
		fmt.Printf("Failed to get page: %v\n", err)
		return
	}

	// Verify the data
	readData := page.Data[:len(testData)]
	if string(readData) != string(testData) {
		fmt.Printf("Data mismatch! Expected: %s, Got: %s\n", testData, readData)
	} else {
		fmt.Printf("Successfully verified data: %s\n", readData)
	}
}

func testMultiplePages(pager *storage.Pager) {
	// Write to multiple pages
	fmt.Println("Writing to multiple pages...")
	for i := 0; i < 5; i++ {
		page, err := pager.GetPage(i)
		if err != nil {
			fmt.Printf("Failed to get page %d: %v\n", i, err)
			return
		}

		// Write unique data to each page
		data := fmt.Sprintf("Page %d data", i)
		copy(page.Data[:len(data)], []byte(data))

		// Write the page to disk
		err = pager.WritePage(i, page.Data)
		if err != nil {
			fmt.Printf("Failed to write page %d: %v\n", i, err)
			return
		}
	}

	// Verify all pages
	fmt.Println("Verifying all pages...")
	for i := 0; i < 5; i++ {
		page, err := pager.GetPage(i)
		if err != nil {
			fmt.Printf("Failed to get page %d: %v\n", i, err)
			return
		}

		expectedData := fmt.Sprintf("Page %d data", i)
		readData := string(page.Data[:len(expectedData)])
		if readData != expectedData {
			fmt.Printf("Data mismatch on page %d! Expected: %s, Got: %s\n", i, expectedData, readData)
		} else {
			fmt.Printf("Successfully verified page %d: %s\n", i, readData)
		}
	}
}

func testPagePersistence(pager *storage.Pager) {
	// Write some data
	fmt.Println("Writing data to pages...")
	for i := 0; i < 3; i++ {
		page, err := pager.GetPage(i)
		if err != nil {
			fmt.Printf("Failed to get page %d: %v\n", i, err)
			return
		}

		data := fmt.Sprintf("Persistent data for page %d", i)
		copy(page.Data[:len(data)], []byte(data))

		err = pager.WritePage(i, page.Data)
		if err != nil {
			fmt.Printf("Failed to write page %d: %v\n", i, err)
			return
		}
	}

	// Close and reopen the pager
	fmt.Println("Closing and reopening pager...")
	pager.Close()

	newPager, err := storage.NewPager(pager.File().Name(), storage.PageSize)
	if err != nil {
		fmt.Printf("Failed to create new pager: %v\n", err)
		return
	}
	defer newPager.Close()

	// Verify data persistence
	fmt.Println("Verifying data persistence...")
	for i := 0; i < 3; i++ {
		page, err := newPager.GetPage(i)
		if err != nil {
			fmt.Printf("Failed to get page %d: %v\n", i, err)
			return
		}

		expectedData := fmt.Sprintf("Persistent data for page %d", i)
		readData := string(page.Data[:len(expectedData)])
		if readData != expectedData {
			fmt.Printf("Data mismatch on page %d! Expected: %s, Got: %s\n", i, expectedData, readData)
		} else {
			fmt.Printf("Successfully verified persistent data on page %d: %s\n", i, readData)
		}
	}
}
