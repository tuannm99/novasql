package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

type PageDirectory struct {
	entries map[string][]uint64
	path    string
}

// NewPageDirectory initializes a Page Directory and tries to load existing data
func NewPageDirectory(workDir string) (*PageDirectory, error) {
	pd := &PageDirectory{
		entries: make(map[string][]uint64),
		path:    filepath.Join(workDir, "page_directory"),
	}

	// Load from file if it exists
	if err := pd.Load(); err != nil {
		return nil, fmt.Errorf("failed to load page directory: %v", err)
	}

	return pd, nil
}

// AddPage adds a new page to the directory and persists it
func (pd *PageDirectory) AddPage(tableName string, pageID uint64) error {
	pd.entries[tableName] = append(pd.entries[tableName], pageID)
	return pd.Save()
}

// GetPages returns all pages for a table
func (pd *PageDirectory) GetPages(tableName string) ([]uint64, error) {
	pages, exists := pd.entries[tableName]
	if !exists {
		return nil, fmt.Errorf("table not found")
	}
	return pages, nil
}

// RemovePage removes a page and persists changes
func (pd *PageDirectory) RemovePage(tableName string, pageID uint64) error {
	pages, exists := pd.entries[tableName]
	if !exists {
		return fmt.Errorf("table not found")
	}

	for i, id := range pages {
		if id == pageID {
			pd.entries[tableName] = append(pages[:i], pages[i+1:]...)
			return pd.Save()
		}
	}
	return fmt.Errorf("page not found")
}

// Save persists the Page Directory as a binary file
func (pd *PageDirectory) Save() error {
	file, err := os.Create(pd.path)
	if err != nil {
		return err
	}
	defer file.Close()

	var buf bytes.Buffer

	// Write number of tables
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(pd.entries))); err != nil {
		return err
	}

	// Write table entries
	for table, pages := range pd.entries {
		// Write table name length and name
		tableNameBytes := []byte(table)
		if err := binary.Write(&buf, binary.LittleEndian, uint64(len(tableNameBytes))); err != nil {
			return err
		}
		if _, err := buf.Write(tableNameBytes); err != nil {
			return err
		}

		// Write number of pages
		if err := binary.Write(&buf, binary.LittleEndian, uint64(len(pages))); err != nil {
			return err
		}

		// Write page IDs
		for _, pageID := range pages {
			if err := binary.Write(&buf, binary.LittleEndian, pageID); err != nil {
				return err
			}
		}
	}

	// Write buffer to file
	_, err = file.Write(buf.Bytes())
	return err
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
		return err
	}
	if stat.Size() == 0 {
		return nil // Empty file, start fresh
	}

	data := make([]byte, stat.Size())
	if _, err := file.Read(data); err != nil {
		return err
	}

	buf := bytes.NewReader(data)

	// Read number of tables
	var numTables uint64
	if err := binary.Read(buf, binary.LittleEndian, &numTables); err != nil {
		return err
	}

	pd.entries = make(map[string][]uint64)

	for i := uint64(0); i < numTables; i++ {
		// Read table name length
		var tableNameLen uint64
		if err := binary.Read(buf, binary.LittleEndian, &tableNameLen); err != nil {
			return err
		}

		// Read table name
		tableNameBytes := make([]byte, tableNameLen)
		if _, err := buf.Read(tableNameBytes); err != nil {
			return err
		}
		tableName := string(tableNameBytes)

		// Read number of pages
		var numPages uint64
		if err := binary.Read(buf, binary.LittleEndian, &numPages); err != nil {
			return err
		}

		// Read page IDs
		pages := make([]uint64, numPages)
		for j := uint64(0); j < numPages; j++ {
			if err := binary.Read(buf, binary.LittleEndian, &pages[j]); err != nil {
				return err
			}
		}

		pd.entries[tableName] = pages
	}

	return nil
}
