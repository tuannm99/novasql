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
//     if K < minKey_{i+1}:
//     return child_i
//     return child_{n-1}
//
//   - This is equivalent to classic B+Tree where internal separators are the
//     minimum key values for each child subtree (except the leftmost).
type InternalNode struct {
	Page *storage.Page
}

// NumKeys returns how many entries (slots) are on this internal node.
func (n *InternalNode) NumKeys() int {
	return n.Page.NumSlots()
}

// EntryAt decodes the i-th internal entry into (key, childPageID).
func (n *InternalNode) EntryAt(i int) (KeyType, uint32, error) {
	data, err := n.Page.ReadTuple(i)
	if err != nil {
		return 0, 0, err
	}
	key, child := DecodeInternalEntry(data)
	return key, child, nil
}

// AppendEntry appends a new (key, childPageID) entry at the end of the page.
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

// internalEntry is a convenient in-memory representation of an internal tuple.
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
func (n *InternalNode) findChildIndex(key KeyType) (int, uint32, error) {
	num := n.NumKeys()
	if num == 0 {
		return 0, 0, errors.New("btree: internal node has no entries")
	}
	if num == 1 {
		_, child, err := n.EntryAt(0)
		return 0, child, err
	}

	// For i in [0..num-2], compare against minKey of the *next* entry.
	for i := 0; i < num-1; i++ {
		_, child, err := n.EntryAt(i)
		if err != nil {
			return 0, 0, err
		}
		nextKey, _, err := n.EntryAt(i + 1)
		if err != nil {
			return 0, 0, err
		}
		if key < nextKey {
			return i, child, nil
		}
	}

	// Otherwise use the last child.
	_, child, err := n.EntryAt(num - 1)
	return num - 1, child, err
}

