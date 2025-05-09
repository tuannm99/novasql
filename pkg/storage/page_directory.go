package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PageDirectory tracks which pages belong to which tables
type PageDirectory struct {
	entries map[string][]uint32 // Maps table names to page IDs
	path    string              // File path for storing directory data
	mu      sync.RWMutex        // Mutex for thread safety
}

// NewPageDirectory initializes a Page Directory and tries to load existing data
func NewPageDirectory(workDir string) (*PageDirectory, error) {
	pd := &PageDirectory{
		entries: make(map[string][]uint32),
		path:    filepath.Join(workDir, "page_directory"),
	}

	// Load from file if it exists
	if err := pd.Load(); err != nil {
		return nil, NewStorageError("load page directory", err)
	}

	return pd, nil
}

// AddPage adds a new page to the directory and persists it
func (pd *PageDirectory) AddPage(tableName string, pageID uint32) error {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	pd.entries[tableName] = append(pd.entries[tableName], pageID)
	return pd.Save()
}

// GetPages returns all pages for a table
func (pd *PageDirectory) GetPages(tableName string) ([]uint32, error) {
	pd.mu.RLock()
	defer pd.mu.RUnlock()

	pages, exists := pd.entries[tableName]
	if !exists {
		return nil, NewStorageError("get pages", fmt.Errorf("table %s not found", tableName))
	}

	// Return a copy to prevent concurrent modification
	result := make([]uint32, len(pages))
	copy(result, pages)

	return result, nil
}

// RemovePage removes a page and persists changes
func (pd *PageDirectory) RemovePage(tableName string, pageID uint32) error {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	pages, exists := pd.entries[tableName]
	if !exists {
		return NewStorageError("remove page", fmt.Errorf("table %s not found", tableName))
	}

	for i, id := range pages {
		if id == pageID {
			pd.entries[tableName] = append(pages[:i], pages[i+1:]...)
			return pd.Save()
		}
	}

	return NewStorageError("remove page", fmt.Errorf("page %d not found in table %s", pageID, tableName))
}

// Save persists the Page Directory as a binary file
func (pd *PageDirectory) Save() error {
	file, err := os.Create(pd.path)
	if err != nil {
		return NewStorageError("save page directory", err)
	}
	defer func() {
		_ = file.Close()
	}()

	var buf bytes.Buffer

	// Write number of tables
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(pd.entries))); err != nil {
		return NewStorageError("write table count", err)
	}

	// Write table entries
	for table, pages := range pd.entries {
		// Write table name length and name
		tableNameBytes := []byte(table)
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(tableNameBytes))); err != nil {
			return NewStorageError("write table name length", err)
		}
		if _, err := buf.Write(tableNameBytes); err != nil {
			return NewStorageError("write table name", err)
		}

		// Write number of pages
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(pages))); err != nil {
			return NewStorageError("write page count", err)
		}

		// Write page IDs
		for _, pageID := range pages {
			if err := binary.Write(&buf, binary.LittleEndian, pageID); err != nil {
				return NewStorageError("write page ID", err)
			}
		}
	}

	// Write buffer to file
	_, err = file.Write(buf.Bytes())
	if err != nil {
		return NewStorageError("write page directory to disk", err)
	}

	return nil
}

// Load reads the Page Directory from a binary file
func (pd *PageDirectory) Load() error {
	file, err := os.Open(pd.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet, start fresh
		}
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return NewStorageError("get file stats", err)
	}
	if stat.Size() == 0 {
		return nil // Empty file, start fresh
	}

	data := make([]byte, stat.Size())
	if _, err := file.Read(data); err != nil {
		return NewStorageError("read file data", err)
	}

	buf := bytes.NewReader(data)

	// Read number of tables
	var numTables uint32
	if err := binary.Read(buf, binary.LittleEndian, &numTables); err != nil {
		return NewStorageError("read table count", err)
	}

	pd.entries = make(map[string][]uint32)

	for i := uint32(0); i < numTables; i++ {
		// Read table name length
		var tableNameLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &tableNameLen); err != nil {
			return NewStorageError("read table name length", err)
		}

		// Read table name
		tableNameBytes := make([]byte, tableNameLen)
		if _, err := buf.Read(tableNameBytes); err != nil {
			return NewStorageError("read table name", err)
		}
		tableName := string(tableNameBytes)

		// Read number of pages
		var numPages uint32
		if err := binary.Read(buf, binary.LittleEndian, &numPages); err != nil {
			return NewStorageError("read page count", err)
		}

		// Read page IDs
		pages := make([]uint32, numPages)
		for j := uint32(0); j < numPages; j++ {
			if err := binary.Read(buf, binary.LittleEndian, &pages[j]); err != nil {
				return NewStorageError("read page ID", err)
			}
		}

		pd.entries[tableName] = pages
	}

	return nil
}
