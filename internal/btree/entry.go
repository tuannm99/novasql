package btree

import (
	"github.com/tuannm99/novasql/pkg/bx"
	"github.com/tuannm99/novasql/internal/heap"
)

// KeyType represents the key type supported by this B-Tree.
// For V1 we only support int64 keys.
type KeyType = int64

const (
	// LeafEntrySize is the fixed size of one leaf entry:
	// 8 bytes key + 4 bytes PageID + 2 bytes Slot = 14 bytes.
	LeafEntrySize = 8 + 4 + 2
)

// EncodeLeafEntry encodes (key, TID) into a compact byte slice.
// Layout: [key int64][PageID uint32][Slot uint16]
func EncodeLeafEntry(key KeyType, tid heap.TID) []byte {
	buf := make([]byte, 0, LeafEntrySize)

	var k [8]byte
	bx.PutU64(k[:], uint64(key))
	buf = append(buf, k[:]...)

	var p [4]byte
	bx.PutU32(p[:], tid.PageID)
	buf = append(buf, p[:]...)

	var s [2]byte
	bx.PutU16(s[:], tid.Slot)
	buf = append(buf, s[:]...)

	return buf
}

// DecodeLeafEntry decodes a leaf entry into (key, TID).
func DecodeLeafEntry(b []byte) (KeyType, heap.TID) {
	if len(b) < LeafEntrySize {
		// In V1 we assume page layer guarantees length; return zero on bad size.
		return 0, heap.TID{}
	}
	key := int64(bx.U64(b[0:8]))
	pageID := bx.U32(b[8:12])
	slot := bx.U16(b[12:14])

	return key, heap.TID{
		PageID: pageID,
		Slot:   slot,
	}
}
