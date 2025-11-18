package engine

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

var (
	ErrDatabaseClosed = errors.New("novasql: database is closed")
	ErrInvalidPageID  = errors.New("novasql: invalid page ID")
)

type DatabaseOperation interface {
	CreateTable(name string, schema record.Schema) (*heap.Table, error)
	OpenTable(name string) (*heap.Table, error)
	Close() error
}

type TableMeta struct {
	Name      string        `json:"name"`
	Schema    record.Schema `json:"schema"`
	PageCount uint32        `json:"page_count"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

var _ DatabaseOperation = (*Database)(nil)

type Database struct {
	DataDir string
	SM      *storage.StorageManager
	// TODO: catalog, bufferpool, locks, ...
}

// NewDatabase creates a new database handle without touching the filesystem.
func NewDatabase(dataDir string) *Database {
	return &Database{
		DataDir: dataDir,
		SM:      storage.NewStorageManager(),
	}
}

func (db *Database) tableDir() string {
	return filepath.Join(db.DataDir, "tables")
}

func (db *Database) tableMetaPath(name string) string {
	return filepath.Join(db.tableDir(), name+".meta.json")
}

// helper: return FileSet for a given table name.
func (db *Database) tableFileSet(name string) storage.FileSet {
	return storage.LocalFileSet{
		Dir:  db.tableDir(),
		Base: name,
	}
}

// writeTableMeta overwrites the meta file for a given table.
func (db *Database) writeTableMeta(meta *TableMeta) error {
	path := db.tableMetaPath(meta.Name)

	if err := os.MkdirAll(db.tableDir(), 0o755); err != nil {
		return err
	}

	meta.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// readTableMeta loads table metadata from JSON file.
func (db *Database) readTableMeta(name string) (*TableMeta, error) {
	path := db.tableMetaPath(name)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var meta TableMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (db *Database) CreateTable(name string, schema record.Schema) (*heap.Table, error) {
	fs := db.tableFileSet(name)
	bp := bufferpool.NewPool(db.SM, fs, bufferpool.DefaultCapacity)

	meta := &TableMeta{
		Name:      name,
		Schema:    schema,
		PageCount: 0,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := db.writeTableMeta(meta); err != nil {
		return nil, err
	}

	// Overflow data for this table is stored in a separate fileset with a
	// deterministic naming convention: "<table>_ovf".
	overflowFS := storage.LocalFileSet{
		Dir:  db.tableDir(),
		Base: name + "_ovf",
	}
	ovf := storage.NewOverflowManager(db.SM, overflowFS)

	tbl := heap.NewTable(name, schema, db.SM, fs, bp, ovf, 0)
	return tbl, nil
}

func (db *Database) OpenTable(name string) (*heap.Table, error) {
	fs := db.tableFileSet(name)

	meta, err := db.readTableMeta(name)
	if err != nil {
		return nil, err
	}

	// Count pages on disk as the single source of truth.
	pageCount, err := db.SM.CountPages(fs)
	if err != nil {
		return nil, err
	}

	// Refresh meta PageCount snapshot.
	meta.PageCount = pageCount
	meta.UpdatedAt = time.Now()

	// Best-effort update; if this fails, we still can open the table.
	if err := db.writeTableMeta(meta); err != nil {
		slog.Info("open table:: error write table meta", "err", err)
	}

	bp := bufferpool.NewPool(db.SM, fs, bufferpool.DefaultCapacity)

	// Rebuild the overflow manager for this table based on the same naming
	// convention used in CreateTable.
	overflowFS := storage.LocalFileSet{
		Dir:  db.tableDir(),
		Base: name + "_ovf",
	}
	ovf := storage.NewOverflowManager(db.SM, overflowFS)

	tbl := heap.NewTable(name, meta.Schema, db.SM, fs, bp, ovf, pageCount)
	return tbl, nil
}

func (db *Database) Close() error {
	// TODO: later - keep track of opened tables and flush all buffer pools.
	return nil
}

// Not supported yet: we do not have a real ALTER TABLE that rewrites data.
// UpdateTableSchema only updates the meta file schema definition.
func (db *Database) UpdateTableSchema(name string, newSchema record.Schema) error {
	meta, err := db.readTableMeta(name)
	if err != nil {
		return err
	}

	meta.Schema = newSchema
	meta.UpdatedAt = time.Now()

	return db.writeTableMeta(meta)
}

// SyncTableMetaPageCount updates the table meta when only PageCount changes.
func (db *Database) SyncTableMetaPageCount(tbl *heap.Table) error {
	meta, err := db.readTableMeta(tbl.Name)
	if err != nil {
		return err
	}
	meta.PageCount = tbl.PageCount
	return db.writeTableMeta(meta)
}
