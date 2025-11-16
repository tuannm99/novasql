package btree

import (
	"fmt"

	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/storage"
)

// LeafNode is a thin wrapper around storage.Page for leaf-level index entries.
// It assumes each tuple on the page is a leaf entry encoded by EncodeLeafEntry.
type LeafNode struct {
	Page *storage.Page
}

// NumKeys returns how many entries (slots) are on this leaf.
func (n *LeafNode) NumKeys() int {
	return n.Page.NumSlots()
}

// KeyAt decodes the key at the given slot.
func (n *LeafNode) KeyAt(i int) (KeyType, error) {
	data, err := n.Page.ReadTuple(i)
	if err != nil {
		return 0, err
	}
	key, _ := DecodeLeafEntry(data)
	return key, nil
}

// EntryAt decodes (key, TID) at the given slot.
func (n *LeafNode) EntryAt(i int) (KeyType, heap.TID, error) {
	data, err := n.Page.ReadTuple(i)
	if err != nil {
		return 0, heap.TID{}, err
	}
	key, tid := DecodeLeafEntry(data)
	return key, tid, nil
}

// AppendEntry appends a new (key, TID) at the end of the page.
// V1 assumes keys are inserted in non-decreasing order so the page remains sorted.
func (n *LeafNode) AppendEntry(key KeyType, tid heap.TID) error {
	data := EncodeLeafEntry(key, tid)
	_, err := n.Page.InsertTuple(data)
	return err
}

// FindEqual scans linearly for all TIDs with the given key.
// Later we can upgrade this to binary search + optional duplicates.
func (n *LeafNode) FindEqual(key KeyType) ([]heap.TID, error) {
	var out []heap.TID
	for i := 0; i < n.Page.NumSlots(); i++ {
		k, tid, err := n.EntryAt(i)
		if err != nil {
			return nil, err
		}
		if k == key {
			out = append(out, tid)
		}
	}
	return out, nil
}

// DebugDump prints content of leaf (for manual debug).
func (n *LeafNode) DebugDump() string {
	s := "LeafNode{"
	for i := 0; i < n.Page.NumSlots(); i++ {
		k, tid, err := n.EntryAt(i)
		if err != nil {
			s += fmt.Sprintf(" [err: %v]", err)
			continue
		}
		s += fmt.Sprintf(" (%d -> %+v)", k, tid)
	}
	s += " }"
	return s
}
