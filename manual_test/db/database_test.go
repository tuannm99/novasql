package db_test

import (
	"os"
	"testing"

	"github.com/tuannm99/novasql/internal"
	"github.com/tuannm99/novasql/internal/storage"
)

func TestDatabase(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "testdb-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	db, err := internal.NewDatabase(tmpfile.Name(), storage.Embedded)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	if count := db.PageCount(); count != 0 {
		t.Errorf("Expected initial page count to be 0, got %d", count)
	}

	if size := db.PageSize(); size != storage.PageSize {
		t.Errorf("Expected page size to be %d, got %d", storage.PageSize, size)
	}

	testData := make([]byte, storage.PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	if err := db.WritePage(0, testData); err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	page, err := db.GetPage(0)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	for i := range testData {
		if page.Data[i] != testData[i] {
			t.Errorf("Data mismatch at index %d: expected %d, got %d", i, testData[i], page.Data[i])
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	db, err = internal.NewDatabase(tmpfile.Name(), storage.Embedded)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	page, err = db.GetPage(0)
	if err != nil {
		t.Fatalf("Failed to get page after reopen: %v", err)
	}

	for i := range testData {
		if page.Data[i] != testData[i] {
			t.Errorf("Data mismatch after reopen at index %d: expected %d, got %d", i, testData[i], page.Data[i])
		}
	}

	for i := range 5 {

		pageData := make([]byte, storage.PageSize)
		for j := range pageData {
			pageData[j] = byte((i*1000 + j) % 256)
		}

		if err := db.WritePage(i, pageData); err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	for i := range 5 {
		page, err := db.GetPage(i)
		if err != nil {
			t.Fatalf("Failed to get page %d: %v", i, err)
		}

		expectedData := make([]byte, storage.PageSize)
		if i == 0 {
			for j := range expectedData {
				expectedData[j] = byte(j % 256)
			}
		} else {
			for j := range expectedData {
				expectedData[j] = byte((i*1000 + j) % 256)
			}
		}

		for j := range expectedData {
			if page.Data[j] != expectedData[j] {
				t.Errorf(
					"Data mismatch on page %d at index %d: expected %d, got %d",
					i,
					j,
					expectedData[j],
					page.Data[j],
				)
			}
		}
	}

	if count := db.PageCount(); count != 5 {
		t.Errorf("Expected page count to be 5, got %d", count)
	}
}

func TestDatabaseErrors(t *testing.T) {
	_, err := internal.NewDatabase("", storage.Embedded)
	if err == nil {
		t.Error("Expected error when creating database with empty filename")
	}

	tmpfile, err := os.CreateTemp("", "testdb-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	db, err := internal.NewDatabase(tmpfile.Name(), storage.Embedded)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	invalidData := make([]byte, storage.PageSize-1)
	if err := db.WritePage(0, invalidData); err == nil {
		t.Error("Expected error when writing page with invalid size")
	}

	if _, err := db.GetPage(-1); err == nil {
		t.Error("Expected error when getting page with negative ID")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	if _, err := db.GetPage(0); err != internal.ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}

	if err := db.WritePage(0, make([]byte, storage.PageSize)); err != internal.ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}
}
