package db_test

import (
	"os"
	"testing"

	"github.com/tuannm99/novasql/internal"
)

func TestDatabase(t *testing.T) {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "testdb-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	// Test creating a new database
	db, err := internal.NewDatabase(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Test initial page count
	if count := db.PageCount(); count != 0 {
		t.Errorf("Expected initial page count to be 0, got %d", count)
	}

	// Test page size
	if size := db.PageSize(); size != internal.DefaultPageSize {
		t.Errorf("Expected page size to be %d, got %d", internal.DefaultPageSize, size)
	}

	// Test writing and reading a page
	testData := make([]byte, internal.DefaultPageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write to page 0
	if err := db.WritePage(0, testData); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Read page 0
	page, err := db.GetPage(0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	// Verify data
	for i := range testData {
		if page.Data[i] != testData[i] {
			t.Errorf("Data mismatch at index %d: expected %d, got %d", i, testData[i], page.Data[i])
		}
	}

	// Test closing and reopening
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	db, err = internal.NewDatabase(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify data persists
	page, err = db.GetPage(0)
	if err != nil {
		t.Fatalf("Failed to get page after reopen: %v", err)
	}

	for i := range testData {
		if page.Data[i] != testData[i] {
			t.Errorf("Data mismatch after reopen at index %d: expected %d, got %d", i, testData[i], page.Data[i])
		}
	}

	// Test writing to multiple pages
	for i := range 5 {
		// Create unique data for each page
		pageData := make([]byte, internal.DefaultPageSize)
		for j := range pageData {
			pageData[j] = byte((i*1000 + j) % 256)
		}

		if err := db.WritePage(i, pageData); err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	// Verify all pages
	for i := range 5 {
		page, err := db.GetPage(i)
		if err != nil {
			t.Fatalf("Failed to get page %d: %v", i, err)
		}

		expectedData := make([]byte, internal.DefaultPageSize)
		if i == 0 {
			// First page has the original test data
			for j := range expectedData {
				expectedData[j] = byte(j % 256)
			}
		} else {
			// Other pages have the unique data
			for j := range expectedData {
				expectedData[j] = byte((i*1000 + j) % 256)
			}
		}

		for j := range expectedData {
			if page.Data[j] != expectedData[j] {
				t.Errorf("Data mismatch on page %d at index %d: expected %d, got %d", i, j, expectedData[j], page.Data[j])
			}
		}
	}

	// Test page count after writing multiple pages
	if count := db.PageCount(); count != 5 {
		t.Errorf("Expected page count to be 5, got %d", count)
	}
}

func TestDatabaseErrors(t *testing.T) {
	// Test creating database with invalid filename
	_, err := internal.NewDatabase("")
	if err == nil {
		t.Error("Expected error when creating database with empty filename")
	}

	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "testdb-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	// Create database
	db, err := internal.NewDatabase(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Test writing page with invalid size
	invalidData := make([]byte, internal.DefaultPageSize-1)
	if err := db.WritePage(0, invalidData); err == nil {
		t.Error("Expected error when writing page with invalid size")
	}

	// Test getting page with negative ID
	if _, err := db.GetPage(-1); err == nil {
		t.Error("Expected error when getting page with negative ID")
	}

	// Test operations after closing
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	if _, err := db.GetPage(0); err != internal.ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}

	if err := db.WritePage(0, make([]byte, internal.DefaultPageSize)); err != internal.ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}
}
