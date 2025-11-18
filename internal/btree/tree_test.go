// internal/btree/tree_test.go
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

func newTestHeapTable(t *testing.T) (*heap.Table, *storage.StorageManager, storage.LocalFileSet) {
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

	overflowFS := storage.LocalFileSet{
		Dir:  fs.Dir,
		Base: fs.Base + "_ovf",
	}
	ovf := storage.NewOverflowManager(sm, overflowFS)

	tbl := heap.NewTable("users", schema, sm, fs, bp, ovf, 0)
	return tbl, sm, fs
}

func TestTree_InsertAndSearchEqual(t *testing.T) {
	tbl, sm, _ := newTestHeapTable(t)

	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := bufferpool.NewPool(sm, idxFS, bufferpool.DefaultCapacity)

	tree := NewTree(sm, idxFS, idxBP)

	for i := 1; i <= 10; i++ {
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0,
		})
		require.NoError(t, err)

		err = tree.Insert(int64(i), tid)
		require.NoError(t, err)
	}

	// Flush heap + index
	require.NoError(t, tbl.BP.FlushAll())
	require.NoError(t, idxBP.FlushAll())

	// Search
	tids, err := tree.SearchEqual(7)
	require.NoError(t, err)
	require.Len(t, tids, 1)

	row, err := tbl.Get(tids[0])
	require.NoError(t, err)
	require.Equal(t, int64(7), row[0].(int64))
	require.Equal(t, "user-7", row[1].(string))
}
