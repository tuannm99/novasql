package heap

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

// newTestTable creates a new heap.Table bound to a temp directory and returns it
// along with the underlying StorageManager and FileSet for reopen tests.
func newTestTable(t *testing.T, base string) (*Table, *storage.StorageManager, storage.LocalFileSet) {
	t.Helper()

	dir := t.TempDir()

	sm := storage.NewStorageManager()
	fs := storage.LocalFileSet{
		Dir:  dir,
		Base: base,
	}
	bp := bufferpool.NewPool(sm, fs, bufferpool.DefaultCapacity)

	// Simple schema: (id INT64, name TEXT, active BOOL)
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: false},
			{Name: "name", Type: record.ColText, Nullable: false},
			{Name: "active", Type: record.ColBool, Nullable: false},
		},
	}

	overflowFS := storage.LocalFileSet{
		Dir:  dir,
		Base: base + "_ovf",
	}
	ovf := storage.NewOverflowManager(sm, overflowFS)

	// New table with pageCount=0, Insert will lazily create pages.
	tbl := NewTable(base, schema, sm, fs, bp, ovf, 0)

	return tbl, sm, fs
}

func TestTable_InsertAndScan_Persisted(t *testing.T) {
	tbl, sm, fs := newTestTable(t, "users")

	// Insert some rows
	const numRows = 10
	type rowData struct {
		id     int64
		name   string
		active bool
	}
	expected := make(map[int64]rowData)

	for i := 1; i <= numRows; i++ {
		r := rowData{
			id:     int64(i),
			name:   fmt.Sprintf("user-%d", i),
			active: i%2 == 0,
		}
		_, err := tbl.Insert([]any{r.id, r.name, r.active})
		require.NoError(t, err)
		expected[r.id] = r
	}

	// Flush all dirty pages to disk via buffer pool.
	require.NoError(t, tbl.BP.FlushAll())

	// Reopen table: new buffer pool, page count from storage.
	pageCount, err := sm.CountPages(fs)
	require.NoError(t, err)
	require.Greater(t, pageCount, uint32(0))

	bp2 := bufferpool.NewPool(sm, fs, bufferpool.DefaultCapacity)
	schema := tbl.Schema // reuse schema

	// Rebuild overflow manager using the same naming convention.
	overflowFS := storage.LocalFileSet{
		Dir:  fs.Dir,
		Base: fs.Base + "_ovf",
	}
	ovf := storage.NewOverflowManager(sm, overflowFS)

	tbl2 := NewTable("users", schema, sm, fs, bp2, ovf, pageCount)

	// Scan and reconstruct rows from disk.
	got := make(map[int64]rowData)

	err = tbl2.Scan(func(id TID, row []any) error {
		idVal := row[0].(int64)
		nameVal := row[1].(string)
		activeVal := row[2].(bool)

		got[idVal] = rowData{
			id:     idVal,
			name:   nameVal,
			active: activeVal,
		}
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, expected, got)
}

func TestTable_UpdateRedirect_ScanAndGet(t *testing.T) {
	tbl, sm, fs := newTestTable(t, "users_update")

	type rowData struct {
		id     int64
		name   string
		active bool
	}

	// Insert a few rows and remember TID of the first one.
	var tidFirst TID
	for i := 1; i <= 3; i++ {
		r := rowData{
			id:     int64(i),
			name:   fmt.Sprintf("user-%d", i),
			active: true,
		}
		tid, err := tbl.Insert([]any{r.id, r.name, r.active})
		require.NoError(t, err)
		if i == 1 {
			tidFirst = tid
		}
	}

	// Update the first row with a longer name so that its tuple likely grows
	// and triggers redirect within the page.
	updatedName := "user-1-updated-and-longer"
	err := tbl.Update(tidFirst, []any{
		int64(1),
		updatedName,
		false,
	})
	require.NoError(t, err)

	// Flush to make sure redirect state is persisted.
	require.NoError(t, tbl.BP.FlushAll())

	// Reopen the table with a fresh buffer pool and page count from storage.
	pageCount, err := sm.CountPages(fs)
	require.NoError(t, err)

	bp2 := bufferpool.NewPool(sm, fs, bufferpool.DefaultCapacity)
	schema := tbl.Schema

	overflowFS := storage.LocalFileSet{
		Dir:  fs.Dir,
		Base: fs.Base + "_ovf",
	}
	ovf := storage.NewOverflowManager(sm, overflowFS)

	tbl2 := NewTable("users_update", schema, sm, fs, bp2, ovf, pageCount)

	// 1) Scan: we should see id=1 exactly once with updatedName.
	foundIDs := make(map[int64]string)

	err = tbl2.Scan(func(id TID, row []any) error {
		idVal := row[0].(int64)
		nameVal := row[1].(string)
		foundIDs[idVal] = nameVal
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, "user-1-updated-and-longer", foundIDs[1])
	require.Len(t, foundIDs, 3) // 3 rows, none duplicated

	// 2) Get: using old TID should still see the updated row (redirect).
	row, err := tbl2.Get(tidFirst)
	require.NoError(t, err)
	require.Equal(t, int64(1), row[0].(int64))
	require.Equal(t, "user-1-updated-and-longer", row[1].(string))
	require.False(t, row[2].(bool))
}

func TestTable_DeleteAndScan(t *testing.T) {
	tbl, sm, fs := newTestTable(t, "users_delete")

	// Insert 5 rows, remember TID of id=3.
	var tid3 TID
	for i := 1; i <= 5; i++ {
		active := i%2 == 0
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			active,
		})
		require.NoError(t, err)
		if i == 3 {
			tid3 = tid
		}
	}

	// Delete row with id=3.
	err := tbl.Delete(tid3)
	require.NoError(t, err)

	// Flush to persist delete flags.
	require.NoError(t, tbl.BP.FlushAll())

	// Reopen for reading.
	pageCount, err := sm.CountPages(fs)
	require.NoError(t, err)

	bp2 := bufferpool.NewPool(sm, fs, bufferpool.DefaultCapacity)
	schema := tbl.Schema

	overflowFS := storage.LocalFileSet{
		Dir:  fs.Dir,
		Base: fs.Base + "_ovf",
	}
	ovf := storage.NewOverflowManager(sm, overflowFS)

	tbl2 := NewTable("users_delete", schema, sm, fs, bp2, ovf, pageCount)

	// Scan: id=3 should be missing, others present.
	found := make(map[int64]bool)

	err = tbl2.Scan(func(id TID, row []any) error {
		idVal := row[0].(int64)
		found[idVal] = true
		return nil
	})
	require.NoError(t, err)

	require.False(t, found[3], "id=3 should have been deleted")
	require.True(t, found[1])
	require.True(t, found[2])
	require.True(t, found[4])
	require.True(t, found[5])
	require.Len(t, found, 4)
}
