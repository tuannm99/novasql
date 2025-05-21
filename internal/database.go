package internal

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/tuannm99/novasql/internal/storage"
)

const (
	DefaultPageSize = 4096 // 4KB pages
)

var (
	ErrDatabaseClosed = errors.New("database is closed")
	ErrInvalidPageID  = errors.New("invalid page ID")
)

// Database represents a database instance
type Database struct {
	mu     sync.RWMutex
	pager  *storage.Pager
	closed bool
}

// NewDatabase creates a new database instance
func NewDatabase(filename string) (*Database, error) {
	// Create pager with default page size
	pager, err := storage.NewPager(filename, DefaultPageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create pager: %w", err)
	}

	return &Database{
		pager:  pager,
		closed: false,
	}, nil
}

// Close closes the database
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	if err := db.pager.Close(); err != nil {
		return fmt.Errorf("failed to close pager: %w", err)
	}

	db.closed = true
	return nil
}

// GetPage retrieves a page from the database
func (db *Database) GetPage(pageID int) (*storage.Page, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	page, err := db.pager.GetPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to get page %d: %w", pageID, err)
	}

	return page, nil
}

// WritePage writes data to a page in the database
func (db *Database) WritePage(pageID int, data []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	if len(data) != DefaultPageSize {
		return fmt.Errorf("invalid page size: expected %d, got %d", DefaultPageSize, len(data))
	}

	// Write the page directly to disk
	if err := db.pager.WritePage(pageID, data); err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageID, err)
	}

	return nil
}

// Delete deletes the database file
func (db *Database) Delete() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.closed {
		if err := db.Close(); err != nil {
			return fmt.Errorf("failed to close database before deletion: %w", err)
		}
	}

	// Get the filename from the pager's file
	fileInfo, err := db.pager.File().Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	if err := os.Remove(fileInfo.Name()); err != nil {
		return fmt.Errorf("failed to delete database file: %w", err)
	}

	return nil
}

// PageCount returns the number of pages in the database
func (db *Database) PageCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.pager.PageCount()
}

// PageSize returns the size of each page
func (db *Database) PageSize() int {
	return db.pager.PageSize()
}
