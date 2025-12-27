package bufferpool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/storage"
)

// newTestPool creates a temporary directory, StorageManager and buffer pool for testing.
// It returns the pool and a cleanup function.
func newTestPool(t *testing.T, capacity int) (*Pool, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "novasql-bp-*")
	require.NoError(t, err)

	sm := storage.NewStorageManager()
	fs := storage.LocalFileSet{
		Dir:  dir,
		Base: "testtable",
	}

	pool := NewPool(sm, fs, capacity)

	cleanup := func() {
		_ = os.RemoveAll(dir)
	}

	return pool, cleanup
}

func TestPool_GetPage_LoadsAndPins(t *testing.T) {
	pool, cleanup := newTestPool(t, 4)
	defer cleanup()

	// First GetPage should load from disk and put it in a frame.
	page1, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page1)
	require.Equal(t, uint32(0), page1.PageID())

	// Find the frame by pageTable instead of assuming index 0.
	idx, ok := pool.pageTable[0]
	require.True(t, ok)
	require.NotNil(t, pool.frames[idx])

	frame := pool.frames[idx]
	require.Equal(t, uint32(0), frame.PageID)
	require.Equal(t, int32(1), frame.Pin)
	require.False(t, frame.Dirty)

	// Second GetPage for the same page should return the same pointer and increase pin count.
	page2, err := pool.GetPage(0)
	require.NoError(t, err)
	require.Same(t, page1, page2)
	require.Equal(t, int32(2), frame.Pin)
}

func TestPool_GetPage_Full_NoFreeFrameError(t *testing.T) {
	pool, cleanup := newTestPool(t, 1)
	defer cleanup()

	// Fill the only frame with page 0 and keep it pinned.
	page0, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page0)

	require.Len(t, pool.frames, 1)

	idx0, ok := pool.pageTable[0]
	require.True(t, ok)
	require.NotNil(t, pool.frames[idx0])
	require.Equal(t, int32(1), pool.frames[idx0].Pin)

	// Try to get a different page without unpinning the first one -> no free frame.
	_, err = pool.GetPage(1)
	require.ErrorIs(t, err, ErrNoFreeFrame)
}

func TestPool_EvictDirtyFrameAndFlush(t *testing.T) {
	pool, cleanup := newTestPool(t, 1)
	defer cleanup()

	// Step 1: Load page 0 and modify its content.
	page0, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page0)

	// Modify page buffer so we can verify it is flushed to disk.
	buf := page0.Buf
	require.NotEmpty(t, buf)
	buf[0] = 42

	// Unpin with dirty = true so the frame is marked dirty and evictable.
	err = pool.Unpin(page0, true)
	require.NoError(t, err)

	idx0, ok := pool.pageTable[0]
	require.True(t, ok)
	require.Equal(t, int32(0), pool.frames[idx0].Pin)
	require.True(t, pool.frames[idx0].Dirty)

	// Step 2: Request page 1, forcing eviction of page 0.
	page1, err := pool.GetPage(1)
	require.NoError(t, err)
	require.NotNil(t, page1)
	require.Equal(t, uint32(1), page1.PageID())

	// At this point page 0 should have been flushed to disk by eviction.
	// Reload page 0 directly from StorageManager and verify the content.
	sm := pool.sm
	fs := pool.fs

	reloaded, err := sm.LoadPage(fs, 0)
	require.NoError(t, err)
	require.NotNil(t, reloaded)
	require.Equal(t, byte(42), reloaded.Buf[0])
}

func TestPool_FlushAll_WritesDirtyFrames(t *testing.T) {
	pool, cleanup := newTestPool(t, 2)
	defer cleanup()

	// Load two pages and modify them.
	page0, err := pool.GetPage(0)
	require.NoError(t, err)
	page1, err := pool.GetPage(1)
	require.NoError(t, err)

	page0.Buf[10] = 11
	page1.Buf[20] = 22

	// Mark both as dirty and unpin.
	require.NoError(t, pool.Unpin(page0, true))
	require.NoError(t, pool.Unpin(page1, true))

	// FlushAll should write both pages to disk and clear dirty flags.
	err = pool.FlushAll()
	require.NoError(t, err)

	idx0, ok := pool.pageTable[0]
	require.True(t, ok)
	idx1, ok := pool.pageTable[1]
	require.True(t, ok)

	require.False(t, pool.frames[idx0].Dirty)
	require.False(t, pool.frames[idx1].Dirty)

	// Reload from disk and verify the changes are persisted.
	sm := pool.sm
	fs := pool.fs

	reloaded0, err := sm.LoadPage(fs, 0)
	require.NoError(t, err)
	require.Equal(t, byte(11), reloaded0.Buf[10])

	reloaded1, err := sm.LoadPage(fs, 1)
	require.NoError(t, err)
	require.Equal(t, byte(22), reloaded1.Buf[20])
}

// Verify default capacity is used when capacity <= 0.
func TestNewPool_DefaultCapacity(t *testing.T) {
	sm := storage.NewStorageManager()
	dir := t.TempDir()
	fs := storage.LocalFileSet{
		Dir:  dir,
		Base: "testtable",
	}

	pool := NewPool(sm, fs, 0)

	// Sanity: can still use the pool.
	page, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page)
}

func TestPool_DeletePageFromBuffer_Unpinned(t *testing.T) {
	pool, cleanup := newTestPool(t, 2)
	defer cleanup()

	// Load a page
	page0, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page0)

	idx, ok := pool.pageTable[0]
	require.True(t, ok)
	require.NotNil(t, pool.frames[idx])
	require.Equal(t, int32(1), pool.frames[idx].Pin)

	// Unpin then delete from buffer
	require.NoError(t, pool.Unpin(page0, false))
	require.Equal(t, int32(0), pool.frames[idx].Pin)

	err = pool.DeletePageFromBuffer(0)
	require.NoError(t, err)

	// Mapping should be removed and frame slot freed (nil)
	_, ok = pool.pageTable[0]
	require.False(t, ok)
	require.Nil(t, pool.frames[idx])
}

func TestPool_DeletePageFromBuffer_Pinned_ReturnsError(t *testing.T) {
	pool, cleanup := newTestPool(t, 2)
	defer cleanup()

	// Load page 0 and keep it pinned
	page0, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page0)

	idx, ok := pool.pageTable[0]
	require.True(t, ok)
	require.NotNil(t, pool.frames[idx])
	require.Equal(t, int32(1), pool.frames[idx].Pin)

	err = pool.DeletePageFromBuffer(0)
	require.ErrorIs(t, err, ErrPagePinned)

	// Frame and mapping should remain unchanged
	idx2, ok := pool.pageTable[0]
	require.True(t, ok)
	require.Equal(t, idx, idx2)
	require.NotNil(t, pool.frames[idx2])
	require.Equal(t, int32(1), pool.frames[idx2].Pin)
}

func TestPool_ReusesFreedFrameSlot(t *testing.T) {
	pool, cleanup := newTestPool(t, 2)
	defer cleanup()

	// Load page 0
	page0, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page0)

	idx0, ok := pool.pageTable[0]
	require.True(t, ok)
	require.NotNil(t, pool.frames[idx0])

	require.NoError(t, pool.Unpin(page0, false))
	require.NoError(t, pool.DeletePageFromBuffer(0))
	require.Nil(t, pool.frames[idx0])

	// Load page 1, should use the freed slot idx0 (because pool scans nil slots).
	page1, err := pool.GetPage(1)
	require.NoError(t, err)
	require.NotNil(t, page1)

	idx1, ok := pool.pageTable[1]
	require.True(t, ok)
	require.Equal(t, idx0, idx1)
	require.NotNil(t, pool.frames[idx1])
	require.Equal(t, uint32(1), pool.frames[idx1].PageID)
}
