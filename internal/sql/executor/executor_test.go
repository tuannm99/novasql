package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/btree"
	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/sql/planner"
	"github.com/tuannm99/novasql/internal/storage"
)

// ---- fakes ----

type fakeDB struct {
	metas []*novasql.TableMeta

	// optional: to satisfy executorDB
	dir string
	sm  *storage.StorageManager
	bp  bufferpool.Manager
}

func (f *fakeDB) CreateDatabase(name string) error        { return nil }
func (f *fakeDB) DropDatabase(name string) (any, error)   { return nil, nil }
func (f *fakeDB) SelectDatabase(name string) (any, error) { return nil, nil }
func (f *fakeDB) CreateTable(table string, schema record.Schema) (any, error) {
	return nil, nil
}
func (f *fakeDB) DropTable(table string) error                { return nil }
func (f *fakeDB) OpenTable(table string) (*heap.Table, error) { return nil, nil }
func (f *fakeDB) ListTables() ([]*novasql.TableMeta, error)   { return f.metas, nil }
func (f *fakeDB) TableDir() string                            { return f.dir }
func (f *fakeDB) BufferView(fs storage.FileSet) bufferpool.Manager {
	return f.bp
}
func (f *fakeDB) StorageManager() *storage.StorageManager { return f.sm }

// ---- tests: listBTreeIndexes ----

func TestListBTreeIndexes_TableMetaNotFound(t *testing.T) {
	e := NewExecutorForTest(&fakeDB{metas: []*novasql.TableMeta{}}, nil)

	_, err := e.listBTreeIndexes("users")
	require.Error(t, err)
	require.Contains(t, err.Error(), "table meta not found")
}

func TestListBTreeIndexes_FiltersOnlyBTree(t *testing.T) {
	db := &fakeDB{
		metas: []*novasql.TableMeta{
			{
				Name: "users",
				Indexes: []novasql.IndexMeta{
					{Kind: novasql.IndexKindBTree, Name: "i1", KeyColumn: "id", FileBase: "users__idx__i1"},
					{Kind: "HASH", Name: "i2", KeyColumn: "name", FileBase: "users__idx__i2"},
					{Kind: novasql.IndexKindBTree, Name: "i3", KeyColumn: "age", FileBase: "users__idx__i3"},
				},
			},
		},
	}

	e := NewExecutorForTest(db, nil)
	idxs, err := e.listBTreeIndexes("users")
	require.NoError(t, err)
	require.Len(t, idxs, 2)
	require.Equal(t, "i1", idxs[0].Name)
	require.Equal(t, "i3", idxs[1].Name)
}

// ---- tests: syncBTreeIndexesOnInsert ----

func TestSyncBTreeIndexesOnInsert_NoIndexes(t *testing.T) {
	db := &fakeDB{
		metas: []*novasql.TableMeta{
			{Name: "users", Indexes: []novasql.IndexMeta{}},
		},
	}
	e := NewExecutorForTest(db, nil)

	called := 0
	e.btreeInsertFn = func(im novasql.IndexMeta, key int64, tid heap.TID) error {
		called++
		return nil
	}

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}

	err := e.syncBTreeIndexesOnInsert("users", schema, []any{int64(1)}, heap.TID{PageID: 1, Slot: 2})
	require.NoError(t, err)
	require.Equal(t, 0, called)
}

func TestSyncBTreeIndexesOnInsert_InsertCalled(t *testing.T) {
	db := &fakeDB{
		metas: []*novasql.TableMeta{
			{
				Name: "users",
				Indexes: []novasql.IndexMeta{
					{Kind: novasql.IndexKindBTree, Name: "by_id", KeyColumn: "id", FileBase: "users__idx__by_id"},
				},
			},
		},
	}
	e := NewExecutorForTest(db, nil)

	var gotIM novasql.IndexMeta
	var gotKey int64
	var gotTID heap.TID
	e.btreeInsertFn = func(im novasql.IndexMeta, key int64, tid heap.TID) error {
		gotIM = im
		gotKey = key
		gotTID = tid
		return nil
	}

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
			{Name: "name", Type: record.ColText, Nullable: true},
		},
	}
	values := []any{int64(10), "abc"}
	tid := heap.TID{PageID: 7, Slot: 9}

	err := e.syncBTreeIndexesOnInsert("users", schema, values, tid)
	require.NoError(t, err)

	require.Equal(t, "by_id", gotIM.Name)
	require.Equal(t, int64(10), gotKey)
	require.Equal(t, tid, gotTID)
}

func TestSyncBTreeIndexesOnInsert_SkipNullKey(t *testing.T) {
	db := &fakeDB{
		metas: []*novasql.TableMeta{
			{
				Name: "users",
				Indexes: []novasql.IndexMeta{
					{Kind: novasql.IndexKindBTree, Name: "by_id", KeyColumn: "id", FileBase: "users__idx__by_id"},
				},
			},
		},
	}
	e := NewExecutorForTest(db, nil)

	called := 0
	e.btreeInsertFn = func(im novasql.IndexMeta, key int64, tid heap.TID) error {
		called++
		return nil
	}

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}

	err := e.syncBTreeIndexesOnInsert("users", schema, []any{nil}, heap.TID{PageID: 1, Slot: 1})
	require.NoError(t, err)
	require.Equal(t, 0, called)
}

func TestSyncBTreeIndexesOnInsert_SkipNonInt64KeyColumn(t *testing.T) {
	db := &fakeDB{
		metas: []*novasql.TableMeta{
			{
				Name: "users",
				Indexes: []novasql.IndexMeta{
					{Kind: novasql.IndexKindBTree, Name: "by_name", KeyColumn: "name", FileBase: "users__idx__by_name"},
				},
			},
		},
	}
	e := NewExecutorForTest(db, nil)

	called := 0
	e.btreeInsertFn = func(im novasql.IndexMeta, key int64, tid heap.TID) error {
		called++
		return nil
	}

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "name", Type: record.ColText, Nullable: true},
		},
	}

	// name is TEXT -> should be skipped
	err := e.syncBTreeIndexesOnInsert("users", schema, []any{"abc"}, heap.TID{PageID: 1, Slot: 1})
	require.NoError(t, err)
	require.Equal(t, 0, called)
}

func TestSyncBTreeIndexesOnInsert_UnknownColumnInSchema_Skip(t *testing.T) {
	db := &fakeDB{
		metas: []*novasql.TableMeta{
			{
				Name: "users",
				Indexes: []novasql.IndexMeta{
					{
						Kind:      novasql.IndexKindBTree,
						Name:      "by_missing",
						KeyColumn: "missing",
						FileBase:  "users__idx__by_missing",
					},
				},
			},
		},
	}
	e := NewExecutorForTest(db, nil)

	called := 0
	e.btreeInsertFn = func(im novasql.IndexMeta, key int64, tid heap.TID) error {
		called++
		return nil
	}

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}

	err := e.syncBTreeIndexesOnInsert("users", schema, []any{int64(1)}, heap.TID{PageID: 1, Slot: 1})
	require.NoError(t, err)
	require.Equal(t, 0, called)
}

func TestSyncBTreeIndexesOnInsert_OutOfOrderSkipped_Continue(t *testing.T) {
	db := &fakeDB{
		metas: []*novasql.TableMeta{
			{
				Name: "users",
				Indexes: []novasql.IndexMeta{
					{Kind: novasql.IndexKindBTree, Name: "idx1", KeyColumn: "id", FileBase: "users__idx__idx1"},
					{Kind: novasql.IndexKindBTree, Name: "idx2", KeyColumn: "id", FileBase: "users__idx__idx2"},
				},
			},
		},
	}
	e := NewExecutorForTest(db, nil)

	calls := []string{}
	e.btreeInsertFn = func(im novasql.IndexMeta, key int64, tid heap.TID) error {
		calls = append(calls, im.Name)
		if im.Name == "idx1" {
			return btree.ErrOutOfOrderInsert
		}
		return nil
	}

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}

	err := e.syncBTreeIndexesOnInsert("users", schema, []any{int64(100)}, heap.TID{PageID: 1, Slot: 1})
	require.NoError(t, err)
	require.Equal(t, []string{"idx1", "idx2"}, calls)
}

func TestSyncBTreeIndexesOnInsert_InsertErrorBubbles(t *testing.T) {
	db := &fakeDB{
		metas: []*novasql.TableMeta{
			{
				Name: "users",
				Indexes: []novasql.IndexMeta{
					{Kind: novasql.IndexKindBTree, Name: "by_id", KeyColumn: "id", FileBase: "users__idx__by_id"},
				},
			},
		},
	}
	e := NewExecutorForTest(db, nil)

	wantErr := errors.New("boom")
	e.btreeInsertFn = func(im novasql.IndexMeta, key int64, tid heap.TID) error {
		return wantErr
	}

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}

	err := e.syncBTreeIndexesOnInsert("users", schema, []any{int64(1)}, heap.TID{PageID: 1, Slot: 1})
	require.Error(t, err)
	require.True(t, errors.Is(err, wantErr))
}

// ---- tests: coerceInsertValues ----

func TestCoerceInsertValues_IntToInt64(t *testing.T) {
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
			{Name: "ok", Type: record.ColBool, Nullable: true},
			{Name: "name", Type: record.ColText, Nullable: true},
		},
	}

	out, err := coerceInsertValues(schema, []any{123, true, "abc"})
	require.NoError(t, err)
	require.Equal(t, int64(123), out[0])
	require.Equal(t, true, out[1])
	require.Equal(t, "abc", out[2])
}

func TestCoerceInsertValues_CountMismatch(t *testing.T) {
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}
	_, err := coerceInsertValues(schema, []any{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "values count")
}

func TestCoerceInsertValues_NotNullViolation(t *testing.T) {
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: false},
		},
	}
	_, err := coerceInsertValues(schema, []any{nil})
	require.Error(t, err)
	require.Contains(t, err.Error(), "NOT NULL")
}

func TestCoerceInsertValues_TypeMismatch(t *testing.T) {
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}
	_, err := coerceInsertValues(schema, []any{"abc"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expects INT64")
}

// ---- tests: matchWhere / colPos ----

func TestColPos(t *testing.T) {
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
			{Name: "name", Type: record.ColText, Nullable: true},
		},
	}
	require.Equal(t, 0, colPos(schema, "id"))
	require.Equal(t, 1, colPos(schema, "name"))
	require.Equal(t, -1, colPos(schema, "missing"))
}

func TestMatchWhere_Int64Equal(t *testing.T) {
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}
	w := &planner.WhereEq{Column: "id", Value: int64(10)}

	ok, err := matchWhere(schema, w, []any{int64(10)})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = matchWhere(schema, w, []any{int64(11)})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestMatchWhere_NullHandling(t *testing.T) {
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}
	w := &planner.WhereEq{Column: "id", Value: nil}

	ok, err := matchWhere(schema, w, []any{nil})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = matchWhere(schema, w, []any{int64(1)})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestMatchWhere_UnknownColumn(t *testing.T) {
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: true},
		},
	}
	w := &planner.WhereEq{Column: "missing", Value: int64(1)}

	_, err := matchWhere(schema, w, []any{int64(1)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown column")
}
