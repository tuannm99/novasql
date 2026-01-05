package btree

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
	"github.com/tuannm99/novasql/internal/wal"
)

func newTestHeapTable(t *testing.T) (*heap.Table, *storage.StorageManager, *bufferpool.GlobalPool) {
	t.Helper()

	dir := t.TempDir()
	sm := storage.NewStorageManager()

	// Shared/global buffer pool (Postgres-like).
	w, _ := wal.Open(filepath.Join(dir, "wal"))
	gp := bufferpool.NewGlobalPool(sm, bufferpool.DefaultCapacity, w)

	// Heap relation (users)
	fs := storage.LocalFileSet{
		Dir:  dir,
		Base: "users",
	}
	bp := gp.View(fs)

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
	ovf := storage.NewOverflowManager(overflowFS)

	tbl := heap.NewTable("users", schema, sm, fs, bp, ovf, 0)
	t.Cleanup(func() { _ = tbl.Close() })

	return tbl, sm, gp
}

func TestTree_InsertAndSearchEqual(t *testing.T) {
	tbl, sm, gp := newTestHeapTable(t)

	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := gp.View(idxFS)

	tree := NewTree(sm, idxFS, idxBP)
	t.Cleanup(func() { _ = tree.Close() })

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

func TestTree_InsertOutOfOrderShouldError(t *testing.T) {
	tbl, sm, gp := newTestHeapTable(t)

	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := gp.View(idxFS)

	tree := NewTree(sm, idxFS, idxBP)
	t.Cleanup(func() { _ = tree.Close() })

	// Insert first key = 10
	tid1, err := tbl.Insert([]any{
		int64(10),
		"user-10",
		true,
	})
	require.NoError(t, err)
	require.NoError(t, tree.Insert(10, tid1))

	// Insert smaller key = 5 → must fail with ErrOutOfOrderInsert
	tid2, err := tbl.Insert([]any{
		int64(5),
		"user-5",
		false,
	})
	require.NoError(t, err)

	err = tree.Insert(5, tid2)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOutOfOrderInsert)
}

func TestTree_InsertAndSearchEqual_Duplicates(t *testing.T) {
	tbl, sm, gp := newTestHeapTable(t)

	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := gp.View(idxFS)

	tree := NewTree(sm, idxFS, idxBP)
	t.Cleanup(func() { _ = tree.Close() })

	// Insert 2 rows with same id = 5
	tid1, err := tbl.Insert([]any{
		int64(5),
		"user-5-a",
		true,
	})
	require.NoError(t, err)
	require.NoError(t, tree.Insert(5, tid1))

	tid2, err := tbl.Insert([]any{
		int64(5),
		"user-5-b",
		false,
	})
	require.NoError(t, err)
	require.NoError(t, tree.Insert(5, tid2))

	// Flush heap + index
	require.NoError(t, tbl.BP.FlushAll())
	require.NoError(t, idxBP.FlushAll())

	tids, err := tree.SearchEqual(5)
	require.NoError(t, err)
	require.Len(t, tids, 2)

	// Load both rows and ensure id == 5, name in expected set
	names := map[string]bool{
		"user-5-a": false,
		"user-5-b": false,
	}

	for _, tid := range tids {
		row, err := tbl.Get(tid)
		require.NoError(t, err)
		require.Equal(t, int64(5), row[0].(int64))

		name := row[1].(string)
		_, ok := names[name]
		require.True(t, ok, "unexpected name: %s", name)
		names[name] = true
	}

	// Ensure we saw both names
	for k, seen := range names {
		require.True(t, seen, "name %s not seen", k)
	}
}

func TestTree_RangeScan(t *testing.T) {
	tbl, sm, gp := newTestHeapTable(t)

	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := gp.View(idxFS)

	tree := NewTree(sm, idxFS, idxBP)
	t.Cleanup(func() { _ = tree.Close() })

	// Insert 1..10 (sorted)
	for i := 1; i <= 10; i++ {
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0,
		})
		require.NoError(t, err)
		require.NoError(t, tree.Insert(int64(i), tid))
	}

	// Range [3, 7] → expect 5 rows: 3,4,5,6,7
	tids, err := tree.RangeScan(3, 7)
	require.NoError(t, err)
	require.Len(t, tids, 5)

	ids := make([]int64, 0, len(tids))
	for _, tid := range tids {
		row, err := tbl.Get(tid)
		require.NoError(t, err)
		id := row[0].(int64)
		ids = append(ids, id)
		require.GreaterOrEqual(t, id, int64(3))
		require.LessOrEqual(t, id, int64(7))
	}

	require.ElementsMatch(t,
		[]int64{3, 4, 5, 6, 7},
		ids,
	)
}

func TestTree_RangeScan_EmptyAndReverse(t *testing.T) {
	tbl, sm, gp := newTestHeapTable(t)

	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := gp.View(idxFS)

	tree := NewTree(sm, idxFS, idxBP)
	t.Cleanup(func() { _ = tree.Close() })

	// Insert 1..5
	for i := 1; i <= 5; i++ {
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0,
		})
		require.NoError(t, err)
		require.NoError(t, tree.Insert(int64(i), tid))
	}

	// Range completely outside: [10, 20] → empty
	tids, err := tree.RangeScan(10, 20)
	require.NoError(t, err)
	require.Empty(t, tids)

	// Reverse range: min > max → empty
	tids, err = tree.RangeScan(4, 2)
	require.NoError(t, err)
	require.Empty(t, tids)
}

// Optional: stress test to ensure tree can grow height > 1 and still work.
func TestTree_HeightIncreasesWithManyInserts(t *testing.T) {
	tbl, sm, gp := newTestHeapTable(t)

	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := gp.View(idxFS)

	tree := NewTree(sm, idxFS, idxBP)
	t.Cleanup(func() { _ = tree.Close() })

	// Insert enough rows to force multiple splits (likely height >= 2).
	const n = 2000
	for i := 1; i <= n; i++ {
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0,
		})
		require.NoError(t, err)
		require.NoError(t, tree.Insert(int64(i), tid))
	}

	require.GreaterOrEqual(t, tree.Height, 2)

	// Spot check some keys.
	for _, k := range []int64{1, 500, 1000, 1999} {
		tids, err := tree.SearchEqual(k)
		require.NoError(t, err)
		require.Len(t, tids, 1)

		row, err := tbl.Get(tids[0])
		require.NoError(t, err)
		require.Equal(t, k, row[0].(int64))
	}
}

// TestManual_BTreeDeepInsert is a manual stress test for debugging B+Tree
// splits / height growth. It is skipped by default.
func TestManual_BTreeDeepInsert(t *testing.T) {
	t.Skip("manual debug test - remove this Skip() when you want to run it")

	// Enable verbose debug logging for btree.
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))

	tbl, sm, gp := newTestHeapTable(t)

	idxFS := storage.LocalFileSet{
		Dir:  t.TempDir(),
		Base: "users_id_idx",
	}
	idxBP := gp.View(idxFS)

	tree := NewTree(sm, idxFS, idxBP)
	defer func() { _ = tree.Close() }()

	const n = 3000

	for i := 1; i <= n; i++ {
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0,
		})
		require.NoError(t, err, "heap insert failed at i=%d", i)

		err = tree.Insert(int64(i), tid)
		if err != nil {
			t.Fatalf("btree insert failed at key=%d: err=%v (height=%d root=%d)",
				i, err, tree.Height, tree.Root)
		}
	}

	for _, k := range []int64{1, int64(n / 2), int64(n)} {
		tids, err := tree.SearchEqual(k)
		require.NoError(t, err)
		require.Len(t, tids, 1)

		row, err := tbl.Get(tids[0])
		require.NoError(t, err)
		require.Equal(t, k, row[0].(int64))
	}
}

func TestLeaf_AppendOutOfOrderIsAllowed(t *testing.T) {
	sm := storage.NewStorageManager()
	dir := t.TempDir()
	fs := storage.LocalFileSet{
		Dir:  dir,
		Base: "idx",
	}

	w, _ := wal.Open(filepath.Join(dir, "wal"))
	gp := bufferpool.NewGlobalPool(sm, bufferpool.DefaultCapacity, w)
	bp := gp.View(fs)

	p, err := bp.GetPage(0)
	require.NoError(t, err)
	defer func() { _ = bp.Unpin(p, false) }()

	leaf := &LeafNode{Page: p}

	// Insert keys in non-sorted order at the leaf layer.
	err = leaf.AppendEntry(10, heap.TID{PageID: 1, Slot: 1})
	require.NoError(t, err)

	err = leaf.AppendEntry(5, heap.TID{PageID: 1, Slot: 2})
	require.NoError(t, err)

	// Range / FindEqual must still behave correctly thanks to in-memory sort.
	tids, err := leaf.FindEqual(5)
	require.NoError(t, err)
	require.Len(t, tids, 1)
	require.Equal(t, uint32(1), tids[0].PageID)
	require.Equal(t, uint16(2), tids[0].Slot)
}
