package pkg

import (
	"fmt"

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
	}

	Context struct {
		tables  map[string]TableMeta
		maxSize uint32
	}

	TableMeta struct {
		root    uint32
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
		dataTypes   interface{}
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

func NewNovaSql(workDir string) (*NovaSql, error) {
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
	}, nil
}

// CreateTable registers a new table
func (db *NovaSql) CreateTable(name string, schema Schema) error {
	if _, exists := db.ctx.tables[name]; exists {
		return fmt.Errorf("table already exists")
	}

	page, err := db.storageManager.AllocatePage(1)
	if err != nil {
		return fmt.Errorf("failed to allocate page: %v", err)
	}

	db.ctx.tables[name] = TableMeta{
		root:   page.ID,
		name:   name,
		schema: schema,
	}

	return nil
}
func (db *NovaSql) InsertTuple(tableName string, tuple storage.Tuple) error {
	table, exists := db.ctx.tables[tableName]
	if !exists {
		return fmt.Errorf("table does not exist")
	}

	page, err := db.storageManager.LoadPage(table.root)
	if err != nil {
		return fmt.Errorf("failed to load page: %v", err)
	}

	err = page.InsertTuple(tuple)
	if err != nil {
		newPage, err := db.storageManager.AllocatePage(1)
		if err != nil {
			return fmt.Errorf("failed to allocate new page: %v", err)
		}
		table.root = newPage.ID
		err = newPage.InsertTuple(tuple)
		if err != nil {
			return fmt.Errorf("failed to insert tuple: %v", err)
		}
	}

	return db.storageManager.SavePage(page)
}

// FetchTuple retrieves a tuple by ID
func (db *NovaSql) FetchTuple(tableName string, tupleID uint64) (*storage.Tuple, error) {
	table, exists := db.ctx.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table does not exist")
	}

	page, err := db.storageManager.LoadPage(table.root)
	if err != nil {
		return nil, fmt.Errorf("failed to load page: %v", err)
	}

	return page.FetchTuple(tupleID)
}
