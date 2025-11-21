package novasql

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

// DatabaseOperation defines the high-level operations that a Database supports.
type DatabaseOperation interface {
	ListDatabase() ([]string, error)
	SelectDatabase(name string) ([]string, error)
	DropDatabase(name string) ([]string, error)

	CreateTable(name string, schema record.Schema) (*heap.Table, error)
	OpenTable(name string) (*heap.Table, error)
	DropTable(name string) error
	ListTables() ([]*TableMeta, error)
	RenameTable(oldName, newName string) error

	Close() error
}

// TableMeta holds table-level metadata persisted as JSON.
type TableMeta struct {
	Name      string        `json:"name"`
	Schema    record.Schema `json:"schema"`
	PageCount uint32        `json:"page_count"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

var _ DatabaseOperation = (*Database)(nil)

// Database is a lightweight handle for a NovaSQL database directory.
type Database struct {
	DataDir string
	SM      *storage.StorageManager
	// TODO: catalog, shared buffer pool, lock manager, WAL manager, tx manager, ...
}

// DropDatabase implements DatabaseOperation.
func (db *Database) DropDatabase(name string) ([]string, error) {
	panic("unimplemented")
}

// ListDatabase implements DatabaseOperation.
func (db *Database) ListDatabase() ([]string, error) {
	panic("unimplemented")
}

// SelectDatabase implements DatabaseOperation.
func (db *Database) SelectDatabase(name string) ([]string, error) {
	panic("unimplemented")
}

// NewDatabase creates a new database handle without touching the filesystem.
func NewDatabase(dataDir string) *Database {
	return &Database{
		DataDir: dataDir,
		SM:      storage.NewStorageManager(),
	}
}

// tableDir returns the directory where table data and meta files live.
func (db *Database) tableDir() string {
	return filepath.Join(db.DataDir, "tables")
}

// tableMetaPath returns the path to the JSON metadata file for a table.
func (db *Database) tableMetaPath(name string) string {
	return filepath.Join(db.tableDir(), name+".meta.json")
}

// tableFileSet returns the FileSet used by the storage manager for a given table.
func (db *Database) tableFileSet(name string) storage.FileSet {
	return storage.LocalFileSet{
		Dir:  db.tableDir(),
		Base: name,
	}
}

// overflowFileSet returns the FileSet used for overflow storage of a given table.
func (db *Database) overflowFileSet(name string) storage.LocalFileSet {
	return storage.LocalFileSet{
		Dir:  db.tableDir(),
		Base: name + "_ovf",
	}
}

// writeTableMeta overwrites the meta JSON file for a given table.
//
// NOTE: We still use os.* here because LocalFileSet currently represents
// data page sets only, not arbitrary metadata files.
func (db *Database) writeTableMeta(meta *TableMeta) error {
	if err := os.MkdirAll(db.tableDir(), 0o755); err != nil {
		return err
	}

	meta.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	path := db.tableMetaPath(meta.Name)
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

// CreateTable creates a new heap table and its associated overflow storage.
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
	overflowFS := db.overflowFileSet(name)
	ovf := storage.NewOverflowManager(db.SM, overflowFS)

	tbl := heap.NewTable(name, schema, db.SM, fs, bp, ovf, 0)
	return tbl, nil
}

// OpenTable opens an existing table using the on-disk metadata and page set.
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
		slog.Info("open table: error writing table meta", "err", err, "table", name)
	}

	bp := bufferpool.NewPool(db.SM, fs, bufferpool.DefaultCapacity)

	// Rebuild the overflow manager for this table based on the same naming
	// convention used in CreateTable.
	overflowFS := db.overflowFileSet(name)
	ovf := storage.NewOverflowManager(db.SM, overflowFS)

	tbl := heap.NewTable(name, meta.Schema, db.SM, fs, bp, ovf, pageCount)
	return tbl, nil
}

// DropTable removes all on-disk data for a table: heap pages, overflow pages,
// and the metadata JSON file.
func (db *Database) DropTable(name string) error {
	// Remove heap page directory.
	tablePath := filepath.Join(db.tableDir(), name)
	if err := os.RemoveAll(tablePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Remove overflow page directory.
	overflowPath := filepath.Join(db.tableDir(), name+"_ovf")
	if err := os.RemoveAll(overflowPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Remove meta file.
	metaPath := db.tableMetaPath(name)
	if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// TODO: later - also update in-memory catalog, drop related indexes, etc.
	return nil
}

// ListTables scans the table directory for *.meta.json files and returns their metadata.
func (db *Database) ListTables() ([]*TableMeta, error) {
	if err := os.MkdirAll(db.tableDir(), 0o755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(db.tableDir())
	if err != nil {
		return nil, err
	}

	var metas []*TableMeta
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".meta.json") {
			continue
		}

		// Strip ".meta.json" to get the table name.
		tableName := strings.TrimSuffix(name, ".meta.json")
		meta, err := db.readTableMeta(tableName)
		if err != nil {
			// Best-effort: log and skip broken meta files.
			slog.Warn("list tables: failed to read table meta", "file", name, "err", err)
			continue
		}
		metas = append(metas, meta)
	}

	return metas, nil
}

// RenameTable renames a table: heap dir, overflow dir, and meta file.
//
// NOTE: This does not handle open table handles or indexes yet.
func (db *Database) RenameTable(oldName, newName string) error {
	// Ensure tableDir exists.
	if err := os.MkdirAll(db.tableDir(), 0o755); err != nil {
		return err
	}

	// Heap dir rename.
	oldTablePath := filepath.Join(db.tableDir(), oldName)
	newTablePath := filepath.Join(db.tableDir(), newName)
	if _, err := os.Stat(oldTablePath); err == nil {
		if err := os.Rename(oldTablePath, newTablePath); err != nil {
			return err
		}
	}

	// Overflow dir rename.
	oldOverflowPath := filepath.Join(db.tableDir(), oldName+"_ovf")
	newOverflowPath := filepath.Join(db.tableDir(), newName+"_ovf")
	if _, err := os.Stat(oldOverflowPath); err == nil {
		if err := os.Rename(oldOverflowPath, newOverflowPath); err != nil {
			return err
		}
	}

	// Meta file rename + update content.
	oldMetaPath := db.tableMetaPath(oldName)
	newMetaPath := db.tableMetaPath(newName)

	if _, err := os.Stat(oldMetaPath); err == nil {
		if err := os.Rename(oldMetaPath, newMetaPath); err != nil {
			return err
		}

		meta, err := db.readTableMeta(newName)
		if err != nil {
			return err
		}
		meta.Name = newName
		meta.UpdatedAt = time.Now()
		if err := db.writeTableMeta(meta); err != nil {
			return err
		}
	}

	// TODO: later - update in-memory catalog, rename related indexes, etc.
	return nil
}

// Close currently does nothing but is kept for future extension.
//
// TODO: later - keep track of opened tables, flush all buffer pools, close WAL, etc.
func (db *Database) Close() error {
	return nil
}

// UpdateTableSchema updates the table metadata schema definition only.
//
// Not supported yet: we do not have a real ALTER TABLE that rewrites data.
// This function does NOT touch any on-disk tuples or pages.
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
