package btree

import (
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/pkg/bx"
)

// KeyType represents the key type supported by this B-Tree.
// For V1 we only support int64 keys.
type KeyType = int64

const (
	// LeafEntrySize is the fixed size of one leaf entry:
	// 8 bytes key + 4 bytes PageID + 2 bytes Slot = 14 bytes.
	LeafEntrySize = 8 + 4 + 2

	// InternalEntrySize is 8 bytes key + 4 bytes childPageID = 12 bytes.
	InternalEntrySize = 8 + 4
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
		// We rely on the page layer to guarantee tuple length.
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

// EncodeInternalEntry encodes (minKey, childPageID).
// Layout: [key int64][childPageID uint32]
func EncodeInternalEntry(key KeyType, child uint32) []byte {
	buf := make([]byte, 0, InternalEntrySize)

	var k [8]byte
	bx.PutU64(k[:], uint64(key))
	buf = append(buf, k[:]...)

	var c [4]byte
	bx.PutU32(c[:], child)
	buf = append(buf, c[:]...)

	return buf
}

// DecodeInternalEntry decodes an internal entry into (key, childPageID).
func DecodeInternalEntry(b []byte) (KeyType, uint32) {
	if len(b) < InternalEntrySize {
		return 0, 0
	}
	key := int64(bx.U64(b[0:8]))
	child := bx.U32(b[8:12])
	return key, child
}
