package database

import (
	"fmt"
	"sync"

	"github.com/tuannm99/novasql/pkg/storage"
)

type Constraint uint8

const (
	PrimaryIndex Constraint = iota + 1
	Unique
)

type (
	NovaSql struct {
		storageManager *storage.StorageManager
		pageDirectory  *storage.PageDirectory
		ctx            Context
		WorkDir        string
		txID           uint64 // Current transaction ID
		operationLock  sync.Mutex
	}

	Context struct {
		tables  map[string]TableMeta
		maxSize uint32
	}

	TableMeta struct {
		root    uint32   // First page
		pages   []uint32 // All pages for this table
		name    string
		schema  Schema
		indexes []IndexMetadata
		rowid   uint64
	}

	Schema struct {
		columns []Column
		index   map[string]uint32
	}

	Column struct {
		name        string
		dataTypes   any
		constraints []Constraint
	}

	IndexMetadata struct {
		root   uint32
		name   string
		column Column
		schema Schema
		unique bool
	}
)

func New(workDir string) (*NovaSql, error) {
	storageManager, err := storage.NewStorageManager(workDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage manager: %v", err)
	}

	pageDirectory, err := storage.NewPageDirectory(workDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize page directory: %v", err)
	}

	return &NovaSql{
		storageManager: storageManager,
		pageDirectory:  pageDirectory,
		ctx:            Context{tables: make(map[string]TableMeta)},
		WorkDir:        workDir,
		txID:           1, // Start with transaction ID 1
	}, nil
}

// NextTxID gets the next transaction ID
func (db *NovaSql) NextTxID() uint64 {
	db.txID++
	return db.txID
}

// CreateTable registers a new table
func (db *NovaSql) CreateTable(name string, schema Schema) error {
	if _, exists := db.ctx.tables[name]; exists {
		return fmt.Errorf("table already exists")
	}

	page, err := db.storageManager.AllocatePage(storage.Slotted)
	if err != nil {
		return fmt.Errorf("failed to allocate page: %v", err)
	}

	// Initialize table metadata
	db.ctx.tables[name] = TableMeta{
		root:   page.ID,
		pages:  []uint32{page.ID}, // Start with one page
		name:   name,
		schema: schema,
		rowid:  0, // Start at 0
	}

	// Update page directory
	if err := db.pageDirectory.AddPage(name, page.ID); err != nil {
		return fmt.Errorf("failed to update page directory: %v", err)
	}

	return nil
}

// InsertTuple inserts a tuple into the specified table
func (db *NovaSql) InsertTuple(tableName string, tuple storage.Tuple) error {
	table, exists := db.ctx.tables[tableName]
	if !exists {
		return fmt.Errorf("table does not exist")
	}

	// txID := db.NextTxID()

	// Try to insert in existing pages
	for _, pageID := range table.pages {
		page, err := db.storageManager.LoadPage(pageID)
		if err != nil {
			continue // Try next page
		}

		// Try to insert tuple
		err = page.InsertTuple(tuple)
		if err == nil {
			// Successfully inserted
			return db.storageManager.SavePage(page)
		}
	}

	// If we get here, we need a new page
	newPage, err := db.storageManager.AllocatePage(storage.Slotted)
	if err != nil {
		return fmt.Errorf("failed to allocate new page: %v", err)
	}

	// Add to table's page list
	tableMeta := db.ctx.tables[tableName]
	tableMeta.pages = append(tableMeta.pages, newPage.ID)
	db.ctx.tables[tableName] = tableMeta

	// Add to page directory
	if err := db.pageDirectory.AddPage(tableName, newPage.ID); err != nil {
		return fmt.Errorf("failed to update page directory: %v", err)
	}

	// Insert into new page
	err = newPage.InsertTuple(tuple)
	if err != nil {
		return fmt.Errorf("failed to insert tuple: %v", err)
	}

	return db.storageManager.SavePage(newPage)
}

// FetchTuple retrieves a tuple by ID from the specified table
func (db *NovaSql) FetchTuple(tableName string, tupleID uint64) (*storage.Tuple, error) {
	table, exists := db.ctx.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table does not exist")
	}

	// Search all pages for this table
	for _, pageID := range table.pages {
		page, err := db.storageManager.LoadPage(pageID)
		if err != nil {
			continue // Try next page
		}

		tuple, err := page.FetchTuple(tupleID)
		if err == nil {
			return tuple, nil
		}
	}

	return nil, fmt.Errorf("tuple not found")
}

// DeleteTuple deletes a tuple by ID from the specified table
func (db *NovaSql) DeleteTuple(tableName string, tupleID uint64) error {
	_, exists := db.ctx.tables[tableName]
	if !exists {
		return fmt.Errorf("table does not exist")
	}

	_ = db.NextTxID()

	// This is a simplified implementation
	// In a real system, you would need to:
	// 1. Find which page contains the tuple
	// 2. Find which cell within that page contains the tuple
	// 3. Use RemoveCell to remove that cell

	// For now we'll just return not implemented
	return fmt.Errorf("delete tuple not implemented")
}

// UpdateTuple updates a tuple by ID in the specified table
func (db *NovaSql) UpdateTuple(tableName string, tuple storage.Tuple) error {
	_, exists := db.ctx.tables[tableName]
	if !exists {
		return fmt.Errorf("table does not exist")
	}

	_ = db.NextTxID()

	// A simple implementation would:
	// 1. Delete the old tuple
	// 2. Insert the new tuple
	// But this doesn't preserve the original position

	// For now we'll just return not implemented
	return fmt.Errorf("update tuple not implemented")
}

// CreateIndex creates a new index on the specified column
func (db *NovaSql) CreateIndex(tableName, indexName, columnName string, unique bool) error {
	table, exists := db.ctx.tables[tableName]
	if !exists {
		return fmt.Errorf("table does not exist")
	}

	// Find the column in the schema
	var column Column
	columnFound := false
	for _, col := range table.schema.columns {
		if col.name == columnName {
			column = col
			columnFound = true
			break
		}
	}

	if !columnFound {
		return fmt.Errorf("column %s not found in table %s", columnName, tableName)
	}

	// Create a B+ tree for the index
	order := 16 // Order 16 is reasonable -> search or tell AI for it
	btree, err := storage.NewBPlusTree(order, db.storageManager)
	if err != nil {
		return fmt.Errorf("failed to create B+ tree: %v", err)
	}

	// Add index metadata
	index := IndexMetadata{
		root:   btree.GetRootPageID(),
		name:   indexName,
		column: column,
		unique: unique,
	}

	tableMeta := db.ctx.tables[tableName]
	tableMeta.indexes = append(tableMeta.indexes, index)
	db.ctx.tables[tableName] = tableMeta

	return nil
}

// Close shuts down the database gracefully
func (db *NovaSql) Close() error {
	// Flush any modified pages
	if err := db.storageManager.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush pages: %v", err)
	}

	// Close storage manager
	if err := db.storageManager.Close(); err != nil {
		return fmt.Errorf("failed to close storage manager: %v", err)
	}

	// TODO: Implement catalog persistence
	return nil
}
