package clockx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClock_New_DefaultCapacity(t *testing.T) {
	c := New(0)
	require.NotNil(t, c)
	require.Equal(t, 1, c.Capacity())
	require.Equal(t, 0, c.Size())
}

func TestClock_Touch_MakesPresent(t *testing.T) {
	c := New(3)

	// Touch an id -> becomes present, ref=true but not evictable yet.
	c.Touch(1)
	require.Equal(t, 0, c.Size())

	// Setting evictable for present slot should increase size.
	c.SetEvictable(1, true)
	require.Equal(t, 1, c.Size())

	// Setting again same value should not change size.
	c.SetEvictable(1, true)
	require.Equal(t, 1, c.Size())

	// Set back to non-evictable
	c.SetEvictable(1, false)
	require.Equal(t, 0, c.Size())
}

func TestClock_SetEvictable_UnknownSlotIgnored(t *testing.T) {
	c := New(2)

	// Not touched yet -> not present, SetEvictable should be ignored.
	c.SetEvictable(0, true)
	require.Equal(t, 0, c.Size())

	// Touch then SetEvictable works.
	c.Touch(0)
	c.SetEvictable(0, true)
	require.Equal(t, 1, c.Size())
}

func TestClock_Evict_NoneEvictable(t *testing.T) {
	c := New(2)

	// Present but not evictable.
	c.Touch(0)
	c.Touch(1)

	id, ok := c.Evict()
	require.False(t, ok)
	require.Equal(t, -1, id)
	require.Equal(t, 0, c.Size())
}

func TestClock_Evict_SecondChanceAndRemovesVictim(t *testing.T) {
	c := New(3)

	// Make 0,1,2 present and evictable, and ref=true via Touch.
	for i := 0; i < 3; i++ {
		c.Touch(i)
		c.SetEvictable(i, true)
	}
	require.Equal(t, 3, c.Size())

	// First eviction:
	// All have ref=true, so clock should clear refs on first pass,
	// then evict the first encountered slot on the next pass.
	v1, ok := c.Evict()
	require.True(t, ok)
	require.GreaterOrEqual(t, v1, 0)
	require.Less(t, v1, 3)
	require.Equal(t, 2, c.Size())

	// Ensure victim is removed: it should never be evicted again.
	v2, ok := c.Evict()
	require.True(t, ok)
	require.NotEqual(t, v1, v2)
	require.Equal(t, 1, c.Size())

	v3, ok := c.Evict()
	require.True(t, ok)
	require.NotEqual(t, v1, v3)
	require.NotEqual(t, v2, v3)
	require.Equal(t, 0, c.Size())

	// Nothing left
	v4, ok := c.Evict()
	require.False(t, ok)
	require.Equal(t, -1, v4)
}

func TestClock_Evict_RespectsRefBit(t *testing.T) {
	c := New(2)

	// Make both present and evictable, both ref=true
	c.Touch(0)
	c.Touch(1)
	c.SetEvictable(0, true)
	c.SetEvictable(1, true)
	require.Equal(t, 2, c.Size())

	// Touch(0) again to make sure it stays recently used.
	c.Touch(0)

	// Evict one.
	// With CLOCK, both ref bits may be cleared first; we only assert correctness:
	// - some victim is returned
	// - size decreases
	// - victim removed and cannot be evicted again
	v, ok := c.Evict()
	require.True(t, ok)
	require.Contains(t, []int{0, 1}, v)
	require.Equal(t, 1, c.Size())

	// Next evict should return the other one
	v2, ok := c.Evict()
	require.True(t, ok)
	require.NotEqual(t, v, v2)
	require.Equal(t, 0, c.Size())
}

func TestClock_Remove_DecrementsSizeIfEvictable(t *testing.T) {
	c := New(3)

	c.Touch(0)
	c.Touch(1)
	c.SetEvictable(0, true)
	c.SetEvictable(1, true)
	require.Equal(t, 2, c.Size())

	// Remove evictable slot -> size decrements
	c.Remove(0)
	require.Equal(t, 1, c.Size())

	// Remove again is no-op
	c.Remove(0)
	require.Equal(t, 1, c.Size())

	// Remove non-evictable present slot -> size unchanged
	c.Touch(2)
	require.Equal(t, 1, c.Size())
	c.Remove(2)
	require.Equal(t, 1, c.Size())
}

func TestClock_BoundsChecks(t *testing.T) {
	c := New(2)

	// Out of range should not panic / change size
	c.Touch(-1)
	c.Touch(2)
	c.SetEvictable(-1, true)
	c.SetEvictable(2, true)
	c.Remove(-1)
	c.Remove(2)

	require.Equal(t, 0, c.Size())
}
