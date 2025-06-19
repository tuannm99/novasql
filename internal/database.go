package internal

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/tuannm99/novasql/internal/storage"
)

var (
	ErrDatabaseClosed = errors.New("database: database is closed")
	ErrInvalidPageID  = errors.New("database: invalid page ID")
)

type Operator interface{}

type Database struct {
	mu     sync.RWMutex
	pager  *storage.Pager
	mode   storage.StorageMode
	closed bool
}

func NewDatabase(filename string, mode storage.StorageMode) (*Database, error) {
	pager, err := storage.NewPager(filename, storage.PageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create pager: %w", err)
	}

	return &Database{
		pager:  pager,
		mode:   mode,
		closed: false,
	}, nil
}

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

func (db *Database) WritePage(pageID int, data []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	if len(data) != storage.PageSize {
		return fmt.Errorf("invalid page size: expected %d, got %d", storage.PageSize, len(data))
	}

	if err := db.pager.WritePage(pageID, data); err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageID, err)
	}

	return nil
}

func (db *Database) Delete() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.closed {
		if err := db.Close(); err != nil {
			return fmt.Errorf("failed to close database before deletion: %w", err)
		}
	}

	fileInfo, err := db.pager.File().Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	if err := os.Remove(fileInfo.Name()); err != nil {
		return fmt.Errorf("failed to delete database file: %w", err)
	}

	return nil
}

func (db *Database) PageCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.pager.PageCount()
}

func (db *Database) PageSize() int {
	return db.pager.PageSize()
}
