package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

// newTestHeapTable creates a minimal heap.Table for testing index integration.
func newTestHeapTable(t *testing.T) (*heap.Table, *storage.StorageManager, storage.FileSet) {
	t.Helper()

	dir := t.TempDir()
	sm := storage.NewStorageManager()
	fs := storage.LocalFileSet{
		Dir:  dir,
		Base: "users",
	}
	bp := bufferpool.NewPool(sm, fs, bufferpool.DefaultCapacity)

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: false},
			{Name: "name", Type: record.ColText, Nullable: false},
			{Name: "active", Type: record.ColBool, Nullable: false},
		},
	}

	tbl := heap.NewTable("users", schema, sm, fs, bp, 0)
	return tbl, sm, fs
}

func TestBTree_InsertAndSearchEqual(t *testing.T) {
	tbl, sm, _ := newTestHeapTable(t)

	// Index file set & buffer pool
	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := bufferpool.NewPool(sm, idxFS, bufferpool.DefaultCapacity)

	tree := NewTree(sm, idxFS, idxBP)

	// Insert some rows into heap + index by id
	const numRows = 10
	for i := 1; i <= numRows; i++ {
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0,
		})
		require.NoError(t, err)

		err = tree.Insert(int64(i), tid)
		require.NoError(t, err)
	}

	// Flush heap + index to disk
	require.NoError(t, tbl.BP.FlushAll())
	require.NoError(t, idxBP.FlushAll())

	// Search for id=7
	tids, err := tree.SearchEqual(7)
	require.NoError(t, err)
	require.Len(t, tids, 1)

	row, err := tbl.Get(tids[0])
	require.NoError(t, err)

	require.Equal(t, int64(7), row[0].(int64))
	require.Equal(t, "user-7", row[1].(string))
	require.False(t, row[2].(bool))
}

func TestBTree_Insert_NodeFull(t *testing.T) {
	tbl, sm, _ := newTestHeapTable(t)

	// Index with very small buffer pool to simulate low capacity, but the actual
	// "full" check is done at the page level using FreeSpace, so we need to fill
	// the leaf page until Insert returns ErrNodeFull.
	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := bufferpool.NewPool(sm, idxFS, bufferpool.DefaultCapacity)

	tree := NewTree(sm, idxFS, idxBP)

	// Repeatedly insert until we hit ErrNodeFull.
	var lastErr error
	for i := 1; i < 10000; i++ {
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0,
		})
		require.NoError(t, err)

		lastErr = tree.Insert(int64(i), tid)
		if lastErr == ErrNodeFull {
			break
		}
		require.NoError(t, lastErr)
	}

	require.ErrorIs(t, lastErr, ErrNodeFull)
}
