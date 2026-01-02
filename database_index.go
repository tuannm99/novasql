package novasql

import (
	"errors"
	"os"
	"time"

	"github.com/tuannm99/novasql/internal/btree"
	"github.com/tuannm99/novasql/internal/storage"
)

type IndexKind string

const (
	IndexKindBTree IndexKind = "btree"
)

var (
	ErrIndexNotFound  = errors.New("novasql: index not found")
	ErrIndexExists    = errors.New("novasql: index already exists")
	ErrIndexBadColumn = errors.New("novasql: index key column not found")
	ErrIndexBadKind   = errors.New("novasql: unsupported index kind")
	ErrIndexBadName   = errors.New("novasql: invalid index name")
	ErrIndexBadTable  = errors.New("novasql: invalid table name")
	ErrIndexBadKeyCol = errors.New("novasql: invalid key column")
)

// IndexMeta is stored inside TableMeta (table.meta.json).
type IndexMeta struct {
	Name      string    `json:"name"`
	Kind      IndexKind `json:"kind"`
	KeyColumn string    `json:"key_column"`
	FileBase  string    `json:"file_base"` // LocalFileSet.Base (segments live in db.tableDir())
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (db *Database) ListIndexes(table string) ([]IndexMeta, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if err := validateIdent(table); err != nil {
		return nil, ErrIndexBadTable
	}
	meta, err := db.readTableMeta(table)
	if err != nil {
		return nil, err
	}
	return append([]IndexMeta(nil), meta.Indexes...), nil
}

func (db *Database) findIndexMeta(meta *TableMeta, indexName string) (int, *IndexMeta) {
	for i := range meta.Indexes {
		if meta.Indexes[i].Name == indexName {
			return i, &meta.Indexes[i]
		}
	}
	return -1, nil
}

func (db *Database) hasColumn(meta *TableMeta, col string) bool {
	for i := range meta.Schema.Cols {
		if meta.Schema.Cols[i].Name == col {
			return true
		}
	}
	return false
}

func (db *Database) indexFileSet(table, index string) storage.LocalFileSet {
	return storage.LocalFileSet{
		Dir:  db.TableDir(),
		Base: db.fmtIndexBase(table, index),
	}
}

// CreateBTreeIndex registers an index and creates a new BTree handle.
// NOTE: This does not backfill existing rows yet (phase2 minimal).
func (db *Database) CreateBTreeIndex(table, indexName, keyColumn string) (*btree.Tree, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if err := validateIdent(table); err != nil {
		return nil, ErrIndexBadTable
	}
	if err := validateIdent(indexName); err != nil {
		return nil, ErrIndexBadName
	}
	if err := validateIdent(keyColumn); err != nil {
		return nil, ErrIndexBadKeyCol
	}

	tmeta, err := db.readTableMeta(table)
	if err != nil {
		return nil, err
	}
	if !db.hasColumn(tmeta, keyColumn) {
		return nil, ErrIndexBadColumn
	}
	if _, im := db.findIndexMeta(tmeta, indexName); im != nil {
		return nil, ErrIndexExists
	}

	_ = os.MkdirAll(db.TableDir(), 0o755)
	fs := db.indexFileSet(table, indexName)
	bp := db.viewFor(fs)

	tree := btree.NewTree(db.SM, fs, bp)

	now := time.Now()
	tmeta.Indexes = append(tmeta.Indexes, IndexMeta{
		Name:      indexName,
		Kind:      IndexKindBTree,
		KeyColumn: keyColumn,
		FileBase:  fs.Base,
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err := db.writeTableMeta(tmeta); err != nil {
		return nil, err
	}

	return tree, nil
}

func (db *Database) OpenBTreeIndex(table, indexName string) (*btree.Tree, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if err := validateIdent(table); err != nil {
		return nil, ErrIndexBadTable
	}
	if err := validateIdent(indexName); err != nil {
		return nil, ErrIndexBadName
	}

	tmeta, err := db.readTableMeta(table)
	if err != nil {
		return nil, err
	}

	_, im := db.findIndexMeta(tmeta, indexName)
	if im == nil {
		return nil, ErrIndexNotFound
	}
	if im.Kind != IndexKindBTree {
		return nil, ErrIndexBadKind
	}

	base := im.FileBase
	if base == "" {
		// Backward compat
		base = db.fmtIndexBase(table, indexName)
	}

	fs := storage.LocalFileSet{Dir: db.TableDir(), Base: base}
	bp := db.viewFor(fs)

	return btree.OpenTree(db.SM, fs, bp)
}

// IMPORTANT: flush/drop from global pool BEFORE deleting files.
func (db *Database) DropIndex(table, indexName string) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if err := validateIdent(table); err != nil {
		return ErrIndexBadTable
	}
	if err := validateIdent(indexName); err != nil {
		return ErrIndexBadName
	}

	tmeta, err := db.readTableMeta(table)
	if err != nil {
		return err
	}

	pos, im := db.findIndexMeta(tmeta, indexName)
	if im == nil {
		return ErrIndexNotFound
	}
	if im.Kind != IndexKindBTree {
		return ErrIndexBadKind
	}

	base := im.FileBase
	if base == "" {
		base = db.fmtIndexBase(table, indexName)
	}
	fs := storage.LocalFileSet{Dir: db.TableDir(), Base: base}

	// Invalidate cached pages first.
	if err := db.flushAndDropFileSet(fs); err != nil {
		return err
	}

	// Drop index files.
	if err := btree.DropIndex(fs); err != nil {
		return err
	}

	// Remove registry entry.
	last := len(tmeta.Indexes) - 1
	tmeta.Indexes[pos] = tmeta.Indexes[last]
	tmeta.Indexes = tmeta.Indexes[:last]

	return db.writeTableMeta(tmeta)
}
