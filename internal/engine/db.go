package engine

import (
	"errors"
	"path/filepath"

	"github.com/tuannm99/novasql/internal/alias/util"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/storage"
)

var (
	ErrDatabaseClosed = errors.New("novasql: database is closed")
	ErrInvalidPageID  = errors.New("novasql: invalid page ID")
)

type Database struct {
	DataDir string
	SM      *storage.StorageManager
	// TODO: catalog, bufferpool, locks, ...
}

// NewDatabase khởi tạo handle, chưa tạo file.
func NewDatabase(dataDir string) *Database {
	return &Database{
		DataDir: dataDir,
		SM:      storage.NewStorageManager(),
	}
}

// helper: trả về FileSet cho 1 table name
func (db *Database) tableFileSet(name string) storage.FileSet {
	// ví dụ: data/tables/<name>
	dir := filepath.Join(db.DataDir, "tables")
	return storage.LocalFileSet{
		Dir:  dir,
		Base: name,
	}
}

// CreateTable: V1 – chỉ tạo handle, schema và đặt PageCount = 0.
// TODO: ghi schema + pageCount ra meta file (JSON/YAML).
func (db *Database) CreateTable(name string, schema storage.Schema) (*heap.Table, error) {
	fs := db.tableFileSet(name)
	tbl := heap.NewTable(name, schema, db.SM, fs, 0)
	return tbl, nil
}

// OpenTable: V1 – chưa có catalog, nên tạm tính PageCount từ kích thước segment 0.
func (db *Database) OpenTable(name string, schema storage.Schema) (*heap.Table, error) {
	fs := db.tableFileSet(name)

	// tạm thời chỉ xem segment 0:
	f, err := fs.OpenSegment(0)
	if err != nil {
		return nil, err
	}
	defer util.CloseFileFunc(f)

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < 0 {
		size = 0
	}

	// mỗi page = PageSize bytes
	pageCount := uint32(size / int64(storage.PageSize))
	tbl := heap.NewTable(name, schema, db.SM, fs, pageCount)
	return tbl, nil
}
