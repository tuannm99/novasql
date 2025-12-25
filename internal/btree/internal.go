package btree

import (
	"errors"
	"log/slog"

	"github.com/tuannm99/novasql/internal/storage"
)

// InternalNode is a thin wrapper around a page used as an internal B+Tree node.
// Each entry encodes (minKey, childPageID).
//
// Semantics:
//
//   - For each child subtree we store: minKey(child), childPageID.
//
//   - Entries are kept in ascending order of minKey.
//
//   - To choose a child for search key K:
//
//     Let entries be e[0..n-1], with e[i] = (minKey_i, child_i).
//
//     For i in 0..n-2:
//     if K < minKey_{i+1}: return child_i
//     return child_{n-1}
type InternalNode struct {
	Page *storage.Page
}

func (n *InternalNode) NumKeys() int { return n.Page.NumSlots() }

func (n *InternalNode) EntryAt(i int) (KeyType, uint32, error) {
	data, err := n.Page.ReadTuple(i)
	if err != nil {
		return 0, 0, err
	}
	key, child := DecodeInternalEntry(data)
	return key, child, nil
}

func (n *InternalNode) AppendEntry(key KeyType, child uint32) error {
	data := EncodeInternalEntry(key, child)
	slot, err := n.Page.InsertTuple(data)
	if err == nil {
		slog.Debug("btree.Internal.AppendEntry",
			"pageID", n.Page.PageID(),
			"key", key,
			"child", child,
			"slot", slot,
		)
	}
	return err
}

type internalEntry struct {
	key   KeyType
	child uint32
}

// readEntries reads all entries from the internal node into a slice.
func (n *InternalNode) readEntries() ([]internalEntry, error) {
	num := n.NumKeys()
	out := make([]internalEntry, 0, num)
	for i := range num {
		k, c, err := n.EntryAt(i)
		if err != nil {
			return nil, err
		}
		out = append(out, internalEntry{key: k, child: c})
	}
	return out, nil
}

// findChildIndex returns (index, childPageID) for a given search key using
// the "minKey" semantics described in the type comment above.
//
// This implementation decodes entries once, then uses binary search.
func (n *InternalNode) findChildIndex(key KeyType) (int, uint32, error) {
	entries, err := n.readEntries()
	if err != nil {
		return 0, 0, err
	}
	num := len(entries)
	if num == 0 {
		return 0, 0, errors.New("btree: internal node has no entries")
	}
	if num == 1 {
		return 0, entries[0].child, nil
	}

	// For i in [0..num-2], compare against minKey of the next entry.
	for i := range num - 1 {
		if key < entries[i+1].key {
			return i, entries[i].child, nil
		}
	}
	return num - 1, entries[num-1].child, nil
}
