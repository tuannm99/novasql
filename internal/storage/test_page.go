package storage

import (
	"os"
	"testing"
)

func TestPageCreation(t *testing.T) {
	pageID := uint64(0)
	page := NewPage(pageID)

	if page.PageID != pageID {
		t.Errorf("Page ID mismatch: got %v, want %v", page.PageID, pageID)
	}

	if len(page.Data) != PAGE_SIZE {
		t.Errorf("Data length mismatch: got %v, want %v", len(page.Data), PAGE_SIZE)
	}

	for _, b := range page.Data {
		if b != 0 {
			t.Errorf("Data should be initialized to zero")
			break
		}
	}
}

func TestWriteAndReadPage(t *testing.T) {
	filePath := "novasql_data.db"
	pageID := uint64(0)

	page := NewPage(pageID)
	if err := page.WriteToFile(filePath); err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}

	readPage := NewPage(pageID)
	if err := readPage.ReadFromFile(filePath); err != nil {
		t.Fatalf("Failed to read from file: %v", err)
	}

	if string(page.Data) != string(readPage.Data) {
		t.Errorf("Data mismatch after reading from file")
	}

	os.Remove(filePath)
}
