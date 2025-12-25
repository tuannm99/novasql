package novasql

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tuannm99/novasql/internal/btree"
	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

var (
	ErrDatabaseClosed = errors.New("novasql: database is closed")
	ErrInvalidPageID  = errors.New("novasql: invalid page ID")
	ErrBadIdent       = errors.New("novasql: invalid identifier")
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
	Indexes   []IndexMeta   `json:"indexes,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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
	return writeFileAtomic(path, data, 0o644)
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
	if err := validateIdent(name); err != nil {
		return nil, err
	}

	fs := db.tableFileSet(name)
	bp := bufferpool.NewPool(db.SM, fs, bufferpool.DefaultCapacity)

	now := time.Now()
	meta := &TableMeta{
		Name:      name,
		Schema:    schema,
		PageCount: 0,
		Indexes:   nil,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := db.writeTableMeta(meta); err != nil {
		return nil, err
	}

	overflowFS := db.overflowFileSet(name)
	ovf := storage.NewOverflowManager(db.SM, overflowFS)

	tbl := heap.NewTable(name, schema, db.SM, fs, bp, ovf, 0)
	tbl.SetPageCountHook(func(pc uint32) error {
		return db.syncTableMetaPageCountByName(name, pc)
	})
	return tbl, nil
}

// OpenTable opens an existing table using the on-disk metadata and page set.
func (db *Database) OpenTable(name string) (*heap.Table, error) {
	if err := validateIdent(name); err != nil {
		return nil, err
	}

	fs := db.tableFileSet(name)

	meta, err := db.readTableMeta(name)
	if err != nil {
		return nil, err
	}

	pageCount, err := db.SM.CountPages(fs)
	if err != nil {
		return nil, err
	}

	// Refresh meta snapshot (keep Indexes intact).
	meta.PageCount = pageCount
	meta.UpdatedAt = time.Now()

	// Best-effort update.
	if err := db.writeTableMeta(meta); err != nil {
		slog.Info("open table: error writing table meta", "err", err, "table", name)
	}

	bp := bufferpool.NewPool(db.SM, fs, bufferpool.DefaultCapacity)

	overflowFS := db.overflowFileSet(name)
	ovf := storage.NewOverflowManager(db.SM, overflowFS)

	tbl := heap.NewTable(name, meta.Schema, db.SM, fs, bp, ovf, pageCount)
	tbl.SetPageCountHook(func(pc uint32) error {
		return db.syncTableMetaPageCountByName(name, pc)
	})
	return tbl, nil
}

// DropTable removes all on-disk data for a table: indexes, heap pages, overflow pages,
// and the metadata JSON file.
func (db *Database) DropTable(name string) error {
	if err := validateIdent(name); err != nil {
		return err
	}
	if err := os.MkdirAll(db.tableDir(), 0o755); err != nil {
		return err
	}

	// Best-effort: remove indexes first (so you don't leave garbage files).
	if meta, err := db.readTableMeta(name); err == nil && meta != nil {
		for _, im := range meta.Indexes {
			// Only btree for now
			if im.Kind != IndexKindBTree {
				continue
			}
			base := im.FileBase
			// Backward compat (in case FileBase was empty in older meta):
			if base == "" {
				base = db.fmtIndexBase(name, im.Name)
			}
			fs := storage.LocalFileSet{Dir: db.tableDir(), Base: base}
			if err := btree.DropIndex(fs); err != nil {
				return err
			}
		}
	}

	// Remove heap segments: name, name.1, ...
	if err := storage.RemoveAllSegments(storage.LocalFileSet{Dir: db.tableDir(), Base: name}); err != nil {
		return err
	}

	// Remove overflow segments: name_ovf, name_ovf.1, ...
	if err := storage.RemoveAllSegments(storage.LocalFileSet{Dir: db.tableDir(), Base: name + "_ovf"}); err != nil {
		return err
	}

	// Remove meta file
	metaPath := db.tableMetaPath(name)
	if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
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

		tableName := strings.TrimSuffix(name, ".meta.json")
		meta, err := db.readTableMeta(tableName)
		if err != nil {
			slog.Warn("list tables: failed to read table meta", "file", name, "err", err)
			continue
		}
		metas = append(metas, meta)
	}

	return metas, nil
}

// RenameTable renames heap + ovf + meta + index segments and updates registry.
func (db *Database) RenameTable(oldName, newName string) error {
	if err := validateIdent(oldName); err != nil {
		return err
	}
	if err := validateIdent(newName); err != nil {
		return err
	}
	if err := os.MkdirAll(db.tableDir(), 0o755); err != nil {
		return err
	}

	// Prevent accidental overwrite
	newMetaPath := db.tableMetaPath(newName)
	if _, err := os.Stat(newMetaPath); err == nil {
		return fmt.Errorf("novasql: table already exists: %s", newName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Load old meta first (needed to rename index segments)
	meta, err := db.readTableMeta(oldName)
	if err != nil {
		return err
	}

	// 1) Rename heap segments
	if err := storage.RenameAllSegments(
		storage.LocalFileSet{Dir: db.tableDir(), Base: oldName},
		storage.LocalFileSet{Dir: db.tableDir(), Base: newName},
	); err != nil {
		return err
	}

	// 2) Rename overflow segments
	if err := storage.RenameAllSegments(
		storage.LocalFileSet{Dir: db.tableDir(), Base: oldName + "_ovf"},
		storage.LocalFileSet{Dir: db.tableDir(), Base: newName + "_ovf"},
	); err != nil {
		return err
	}

	// 3) Rename index segments + update registry FileBase
	now := time.Now()
	for i := range meta.Indexes {
		im := &meta.Indexes[i]
		if im.Kind != IndexKindBTree {
			continue
		}

		oldBase := im.FileBase
		if oldBase == "" {
			// Backward compat if older meta didn't store FileBase
			oldBase = db.fmtIndexBase(oldName, im.Name)
		}
		newBase := db.fmtIndexBase(newName, im.Name)

		if err := storage.RenameAllSegments(
			storage.LocalFileSet{Dir: db.tableDir(), Base: oldBase},
			storage.LocalFileSet{Dir: db.tableDir(), Base: newBase},
		); err != nil {
			return err
		}

		im.FileBase = newBase
		im.UpdatedAt = now
	}

	// 4) Rename meta file itself (oldName.meta.json -> newName.meta.json)
	oldMetaPath := db.tableMetaPath(oldName)
	if err := os.Rename(oldMetaPath, newMetaPath); err != nil {
		// If rename fails here, you already renamed data files.
		// It's ok for phase2; user can retry. But we return error.
		return err
	}

	// 5) Rewrite meta content with new table name (atomic overwrite)
	meta.Name = newName
	meta.UpdatedAt = now
	return db.writeTableMeta(meta)
}

func (db *Database) syncTableMetaPageCountByName(name string, pageCount uint32) error {
	meta, err := db.readTableMeta(name)
	if err != nil {
		return err
	}
	meta.PageCount = pageCount
	return db.writeTableMeta(meta)
}

// Close currently does nothing but is kept for future extension.
func (db *Database) Close() error {
	return nil
}

func (db *Database) UpdateTableSchema(name string, newSchema record.Schema) error {
	meta, err := db.readTableMeta(name)
	if err != nil {
		return err
	}

	meta.Schema = newSchema
	meta.UpdatedAt = time.Now()

	return db.writeTableMeta(meta)
}

func (db *Database) SyncTableMetaPageCount(tbl *heap.Table) error {
	meta, err := db.readTableMeta(tbl.Name)
	if err != nil {
		return err
	}
	meta.PageCount = tbl.PageCount
	return db.writeTableMeta(meta)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	tmp, err := os.CreateTemp(dir, base+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	ok = true
	return nil
}

// fmtIndexBase returns LocalFileSet.Base for index segments.
// Example: table="users", index="idx_age" -> "users__idx__idx_age"
func (db *Database) fmtIndexBase(table, index string) string {
	table = strings.TrimSpace(table)
	index = strings.TrimSpace(index)
	return table + "__idx__" + index
}

func validateIdent(name string) error {
	if name == "" {
		return ErrBadIdent
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return ErrBadIdent
	}
	if strings.Contains(name, "..") {
		return ErrBadIdent
	}
	return nil
}
