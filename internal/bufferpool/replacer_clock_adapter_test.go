package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClockAdapter_SizeAndEvictable(t *testing.T) {
	r := newClockAdapter(4)

	// Touch frames so they become "present"
	r.RecordAccess(0)
	r.RecordAccess(1)

	require.Equal(t, 0, r.Size())

	// Make frame 0 evictable
	r.SetEvictable(0, true)
	require.Equal(t, 1, r.Size())

	// Make frame 1 evictable
	r.SetEvictable(1, true)
	require.Equal(t, 2, r.Size())

	// Turn frame 0 back to non-evictable
	r.SetEvictable(0, false)
	require.Equal(t, 1, r.Size())

	// Removing a non-present frame should not break
	r.Remove(3)
	require.Equal(t, 1, r.Size())
}

func TestClockAdapter_Evict_NoneEvictable(t *testing.T) {
	r := newClockAdapter(2)

	// present but not evictable
	r.RecordAccess(0)
	r.RecordAccess(1)

	_, ok := r.Evict()
	require.False(t, ok)
	require.Equal(t, 0, r.Size())
}

func TestClockAdapter_Evict_SecondChanceBehavior(t *testing.T) {
	r := newClockAdapter(3)

	// Mark all present and evictable
	for i := 0; i < 3; i++ {
		r.RecordAccess(i)       // sets ref = true
		r.SetEvictable(i, true) // count = 3
	}
	require.Equal(t, 3, r.Size())

	// First Evict():
	// all ref bits are true -> CLOCK should clear refs on first sweep
	// then pick the first encountered victim on second sweep.
	v1, ok := r.Evict()
	require.True(t, ok)
	require.GreaterOrEqual(t, v1, 0)
	require.Less(t, v1, 3)
	require.Equal(t, 2, r.Size())

	// Ensure evicted frame is removed: next evictions should never return v1.
	v2, ok := r.Evict()
	require.True(t, ok)
	require.NotEqual(t, v1, v2)
	require.Equal(t, 1, r.Size())

	v3, ok := r.Evict()
	require.True(t, ok)
	require.NotEqual(t, v1, v3)
	require.NotEqual(t, v2, v3)
	require.Equal(t, 0, r.Size())

	// No more victims
	_, ok = r.Evict()
	require.False(t, ok)
}

func TestClockAdapter_Remove_PreventsEviction(t *testing.T) {
	r := newClockAdapter(2)

	// Make both present + evictable
	r.RecordAccess(0)
	r.RecordAccess(1)
	r.SetEvictable(0, true)
	r.SetEvictable(1, true)
	require.Equal(t, 2, r.Size())

	// Remove frame 0 from tracking
	r.Remove(0)
	require.Equal(t, 1, r.Size())

	// Evict must return only frame 1
	v, ok := r.Evict()
	require.True(t, ok)
	require.Equal(t, 1, v)
	require.Equal(t, 0, r.Size())

	// Further evict should fail
	_, ok = r.Evict()
	require.False(t, ok)
}
