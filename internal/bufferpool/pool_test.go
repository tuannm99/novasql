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

	// First GetPage should load from disk and add one frame.
	page1, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page1)
	require.Equal(t, uint32(0), page1.PageID())
	require.Len(t, pool.frames, 1)

	frame := pool.frames[0]
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
	require.Equal(t, int32(1), pool.frames[0].Pin)

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
	require.Equal(t, int32(0), pool.frames[0].Pin)
	require.True(t, pool.frames[0].Dirty)

	// Step 2: Request page 1, forcing eviction of page 0.
	page1, err := pool.GetPage(1)
	require.NoError(t, err)
	require.NotNil(t, page1)

	// At this point page 0 should have been flushed to disk by eviction.
	// We reload page 0 directly from StorageManager and verify the content.
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
	require.False(t, pool.frames[0].Dirty)
	require.False(t, pool.frames[1].Dirty)

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

// Optional: verify default capacity is used when capacity <= 0.
func TestNewPool_DefaultCapacity(t *testing.T) {
	sm := storage.NewStorageManager()
	dir := t.TempDir()
	fs := storage.LocalFileSet{
		Dir:  dir,
		Base: "testtable",
	}

	pool := NewPool(sm, fs, 0)
	require.Equal(t, 16, pool.capacity)

	// Sanity: can still use the pool.
	page, err := pool.GetPage(0)
	require.NoError(t, err)
	require.NotNil(t, page)
}
