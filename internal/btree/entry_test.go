package btree

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/heap"
)

func TestEncodeDecodeLeafEntry(t *testing.T) {
	tid := heap.TID{PageID: 123, Slot: 7}
	key := KeyType(42)

	b := EncodeLeafEntry(key, tid)
	require.Len(t, b, LeafEntrySize)

	k2, tid2 := DecodeLeafEntry(b)
	require.Equal(t, key, k2)
	require.Equal(t, tid.PageID, tid2.PageID)
	require.Equal(t, tid.Slot, tid2.Slot)
}
