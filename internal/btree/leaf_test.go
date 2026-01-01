package btree

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/storage"
)

// newTestLeaf creates a LeafNode backed by a fresh page (pageID=0) in a temp dir.
// Uses GlobalPool + FileSet View so it works with shared buffer design.
func newTestLeaf(t *testing.T) (*LeafNode, *storage.StorageManager, storage.LocalFileSet, bufferpool.Manager) {
	t.Helper()

	dir := t.TempDir()
	sm := storage.NewStorageManager()
	fs := storage.LocalFileSet{
		Dir:  dir,
		Base: "leaf_test",
	}

	// Shared buffer
	gp := bufferpool.NewGlobalPool(sm, bufferpool.DefaultCapacity)

	// Relation-scoped view (implements bufferpool.Manager)
	bp := gp.View(fs)

	p, err := bp.GetPage(0)
	require.NoError(t, err)

	leaf := &LeafNode{Page: p}
	return leaf, sm, fs, bp
}

func TestLeaf_AppendAndEntryAt(t *testing.T) {
	leaf, _, _, bp := newTestLeaf(t)
	defer func() {
		_ = bp.Unpin(leaf.Page, false)
	}()

	// Insert a few entries with increasing keys.
	for i := int64(1); i <= 5; i++ {
		tid := heap.TID{PageID: 123, Slot: uint16(i)}
		err := leaf.AppendEntry(i, tid)
		require.NoError(t, err)
	}

	require.Equal(t, 5, leaf.NumKeys())

	// Verify entries are decoded correctly.
	for i := 0; i < leaf.NumKeys(); i++ {
		k, tid, err := leaf.EntryAt(i)
		require.NoError(t, err)
		require.Equal(t, KeyType(i+1), k)
		require.Equal(t, uint32(123), tid.PageID)
		require.Equal(t, uint16(i+1), tid.Slot)
	}
}

func TestLeaf_FindEqualAndRange(t *testing.T) {
	leaf, _, _, bp := newTestLeaf(t)
	defer func() {
		_ = bp.Unpin(leaf.Page, false)
	}()

	// Insert keys: 1,2,3,3,4,5.
	keys := []KeyType{1, 2, 3, 3, 4, 5}
	for i, k := range keys {
		tid := heap.TID{PageID: 1, Slot: uint16(i)}
		require.NoError(t, leaf.AppendEntry(k, tid))
	}

	// FindEqual(3) → 2 entries.
	tids, err := leaf.FindEqual(3)
	require.NoError(t, err)
	require.Len(t, tids, 2)
	for _, tid := range tids {
		require.Equal(t, uint32(1), tid.PageID)
	}

	// Range [2,4] → keys {2,3,3,4}.
	rangeTIDs, err := leaf.Range(2, 4)
	require.NoError(t, err)
	require.Len(t, rangeTIDs, 4)

	// Check that all keys in range are indeed 2..4.
	for _, tid := range rangeTIDs {
		// recover the key by scanning leaf
		found := false
		for i := 0; i < leaf.NumKeys(); i++ {
			k, tID, err := leaf.EntryAt(i)
			require.NoError(t, err)
			if tID == tid {
				require.GreaterOrEqual(t, k, KeyType(2))
				require.LessOrEqual(t, k, KeyType(4))
				found = true
				break
			}
		}
		require.True(t, found, "tid not found in leaf entries")
	}
}
