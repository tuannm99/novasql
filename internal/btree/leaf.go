package btree

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/storage"
)

// ErrOutOfOrderInsert was originally used at the leaf level to enforce that
// keys are inserted in non-decreasing order. The V2 implementation enforces
// monotonic inserts at the Tree level instead, so leaves are free to accept
// keys in any order.
var ErrOutOfOrderInsert = fmt.Errorf("btree: keys must be inserted in non-decreasing order")

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
// V2 no longer enforces sorted order at the leaf level; leaves are allowed
// to contain keys in any order. Query methods build a sorted view on demand.
func (n *LeafNode) AppendEntry(key KeyType, tid heap.TID) error {
	data := EncodeLeafEntry(key, tid)
	slot, err := n.Page.InsertTuple(data)
	if err == nil {
		slog.Debug("btree.Leaf.AppendEntry",
			"key", key,
			"pageID", n.Page.PageID(),
			"slot", slot,
		)
	}
	return err
}

// leafEntry is an in-memory representation of a leaf tuple.
type leafEntry struct {
	key KeyType
	tid heap.TID
}

// entriesSorted reads all entries from the leaf and returns them sorted by key.
func (n *LeafNode) entriesSorted() ([]leafEntry, error) {
	num := n.NumKeys()
	out := make([]leafEntry, 0, num)
	for i := 0; i < num; i++ {
		k, tid, err := n.EntryAt(i)
		if err != nil {
			return nil, err
		}
		out = append(out, leafEntry{
			key: k,
			tid: tid,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].key < out[j].key
	})
	return out, nil
}

// lowerBoundSorted returns the first index i in entries such that
// entries[i].key >= target. If all keys < target, returns len(entries).
func lowerBoundSorted(entries []leafEntry, target KeyType) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if entries[mid].key < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// FindEqual finds all TIDs with the given key using a sorted in-memory view.
func (n *LeafNode) FindEqual(key KeyType) ([]heap.TID, error) {
	var out []heap.TID

	entries, err := n.entriesSorted()
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return out, nil
	}

	start := lowerBoundSorted(entries, key)
	for i := start; i < len(entries); i++ {
		e := entries[i]
		if e.key != key {
			break
		}
		out = append(out, e.tid)
	}
	slog.Debug("btree.Leaf.FindEqual",
		"pageID", n.Page.PageID(),
		"key", key,
		"numTIDs", len(out),
	)
	return out, nil
}

// FirstGEKey returns the first index in the sorted view whose key >= target.
// Returns -1 if none found.
//
// NOTE: the returned index is in the sorted in-memory view, not a slot index
// on the underlying Page. This method is currently only used in tests.
func (n *LeafNode) FirstGEKey(target KeyType) (int, error) {
	entries, err := n.entriesSorted()
	if err != nil {
		return -1, err
	}
	if len(entries) == 0 {
		return -1, nil
	}
	i := lowerBoundSorted(entries, target)
	if i >= len(entries) {
		return -1, nil
	}
	return i, nil
}

// Range returns all TIDs with minKey <= key <= maxKey in this leaf.
func (n *LeafNode) Range(minKey, maxKey KeyType) ([]heap.TID, error) {
	var out []heap.TID
	if minKey > maxKey {
		return out, nil
	}

	entries, err := n.entriesSorted()
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return out, nil
	}

	i := lowerBoundSorted(entries, minKey)
	for i < len(entries) {
		e := entries[i]
		if e.key > maxKey {
			break
		}
		out = append(out, e.tid)
		i++
	}
	slog.Debug("btree.Leaf.Range",
		"pageID", n.Page.PageID(),
		"minKey", minKey,
		"maxKey", maxKey,
		"numTIDs", len(out),
	)
	return out, nil
}

// DebugDump prints a human-readable representation of the leaf contents
// in physical slot order (not sorted).
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
