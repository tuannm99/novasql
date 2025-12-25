package btree

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/storage"
)

// LeafNode is a thin wrapper around storage.Page for leaf-level index entries.
type LeafNode struct {
	Page *storage.Page
}

func (n *LeafNode) NumKeys() int { return n.Page.NumSlots() }

func (n *LeafNode) KeyAt(i int) (KeyType, error) {
	data, err := n.Page.ReadTuple(i)
	if err != nil {
		return 0, err
	}
	key, _ := DecodeLeafEntry(data)
	return key, nil
}

func (n *LeafNode) EntryAt(i int) (KeyType, heap.TID, error) {
	data, err := n.Page.ReadTuple(i)
	if err != nil {
		return 0, heap.TID{}, err
	}
	key, tid := DecodeLeafEntry(data)
	return key, tid, nil
}

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

type leafEntry struct {
	key KeyType
	tid heap.TID
}

// readEntries reads entries in physical slot order (no sorting).
func (n *LeafNode) readEntries() ([]leafEntry, error) {
	num := n.NumKeys()
	out := make([]leafEntry, 0, num)
	for i := range num {
		k, tid, err := n.EntryAt(i)
		if err != nil {
			return nil, err
		}
		out = append(out, leafEntry{key: k, tid: tid})
	}
	return out, nil
}

func sortLeafEntries(entries []leafEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].key != entries[j].key {
			return entries[i].key < entries[j].key
		}
		// deterministic tie-breaker
		if entries[i].tid.PageID != entries[j].tid.PageID {
			return entries[i].tid.PageID < entries[j].tid.PageID
		}
		return entries[i].tid.Slot < entries[j].tid.Slot
	})
}

// rebuildSorted rewrites the whole leaf page in-place in sorted physical order.
func (n *LeafNode) rebuildSorted(entries []leafEntry) error {
	sortLeafEntries(entries)

	// Re-init page header + clear bytes
	n.Page.Reset(n.Page.PageID())

	for _, e := range entries {
		if err := n.AppendEntry(e.key, e.tid); err != nil {
			return err
		}
	}
	return nil
}

// entriesSorted keeps your current behavior for query, but now leaf is already sorted physically
// so this is mostly redundant; still keep it safe for now.
func (n *LeafNode) entriesSorted() ([]leafEntry, error) {
	entries, err := n.readEntries()
	if err != nil {
		return nil, err
	}
	sortLeafEntries(entries)
	return entries, nil
}

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

func (n *LeafNode) FindEqual(key KeyType) ([]heap.TID, error) {
	var out []heap.TID
	entries, err := n.entriesSorted()
	if err != nil {
		return nil, err
	}
	start := lowerBoundSorted(entries, key)
	for i := start; i < len(entries); i++ {
		if entries[i].key != key {
			break
		}
		out = append(out, entries[i].tid)
	}
	return out, nil
}

func (n *LeafNode) Range(minKey, maxKey KeyType) ([]heap.TID, error) {
	var out []heap.TID
	if minKey > maxKey {
		return out, nil
	}
	entries, err := n.entriesSorted()
	if err != nil {
		return nil, err
	}
	i := lowerBoundSorted(entries, minKey)
	for i < len(entries) {
		if entries[i].key > maxKey {
			break
		}
		out = append(out, entries[i].tid)
		i++
	}
	return out, nil
}

func (n *LeafNode) DebugDump() string {
	s := "LeafNode{"
	for i := range n.Page.NumSlots() {
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
