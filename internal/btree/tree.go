package btree

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/storage"
)

// ErrInvalidTreeHeight is returned when the tree height is not supported by the
// current implementation.
var (
	ErrInvalidTreeHeight             = errors.New("btree: invalid tree height")
	ErrInternalNodeHasNoEntries      = errors.New("btree: internal node has no entries")
	ErrLeafHasNoKey                  = errors.New("btree: leaf has no keys")
	ErrCannotSplitLeafGreaterThanTwo = errors.New("btree: cannot split leaf with <2 keys")
	ErrInternalChildIdxOutOfRange    = errors.New("btree: internal child index out of range")
	ErrInternalNodePageHasZeroCap    = errors.New("btree: internal node page has zero capacity")
	ErrSplitRequiredMoreThanTwoPages = errors.New("btree: internal split would require more than two pages")
)

// Meta holds logical information about the tree. Later this can be persisted
// alongside table metadata if you want durable index catalogs.
type Meta struct {
	Root   uint32
	Height int
}

// Tree is a B+Tree implementation with arbitrary height.
//
// Constraints for V1:
//   - Leaf and internal nodes are each backed by exactly one Page.
//   - Only int64 keys are supported.
//   - Inserts must be in non-decreasing key order (see ErrOutOfOrderInsert).
//
// Invariants:
//   - Height >= 1.
//   - Height == 1 → root is a leaf.
//   - Height > 1  → root is an internal node.
type Tree struct {
	SM *storage.StorageManager
	FS storage.FileSet
	BP bufferpool.Manager

	Root   uint32 // root page id
	Height int

	Meta *Meta

	lastKeySet bool
	lastKey    KeyType

	// nextPageID is the next page ID to allocate for this tree.
	// Root is fixed at pageID=0, so we start from 1.
	nextPageID uint32

	// meta persistence
	metaEnabled bool
	metaPath    string
}

// NewTree creates a brand-new tree (no attempt to load persisted meta).
// Use OpenTree if you want restore Root/Height/nextPageID from disk.
func NewTree(sm *storage.StorageManager, fs storage.FileSet, bp bufferpool.Manager) *Tree {
	t := &Tree{SM: sm, FS: fs, BP: bp}

	t.Root = 0
	t.Height = 1
	t.nextPageID = 1 // page 0 reserved as root

	if mp, ok := metaPathForFileSet(fs); ok {
		t.metaEnabled = true
		t.metaPath = mp
	}

	t.Meta = &Meta{Root: t.Root, Height: t.Height}

	// persist initial meta best-effort (optional)
	if err := t.saveMeta(); err != nil {
		slog.Warn("btree.NewTree: saveMeta failed", "err", err)
	}

	return t
}

// OpenTree opens an existing tree:
// - loads meta file if present (Root/Height/NextPageID)
// - ALWAYS restores nextPageID safely using CountPages(fs) so we never overwrite pages.
func OpenTree(sm *storage.StorageManager, fs storage.FileSet, bp bufferpool.Manager) (*Tree, error) {
	t := &Tree{
		SM: sm,
		FS: fs,
		BP: bp,
	}

	// defaults (fresh)
	t.Root = 0
	t.Height = 1
	t.nextPageID = 1

	if mp, ok := metaPathForFileSet(fs); ok {
		t.metaEnabled = true
		t.metaPath = mp
	}

	// Load meta (if any)
	if m, ok, err := t.loadMeta(); err != nil {
		return nil, err
	} else if ok {
		if m.Height >= 1 {
			t.Height = m.Height
		}
		t.Root = m.Root
		t.nextPageID = m.NextPageID
	}

	// Restore nextPageID from on-disk size (single source of truth to avoid overwrite)
	pageCount, err := sm.CountPages(fs)
	if err != nil {
		return nil, err
	}

	// If any pages exist, nextPageID must be >= pageCount (pages are [0..pageCount-1])
	if pageCount > 0 {
		if t.nextPageID < pageCount {
			t.nextPageID = pageCount
		}
	} else {
		// pageCount==0 but root is still page 0 logically; nextPageID must be at least 1
		if t.nextPageID < 1 {
			t.nextPageID = 1
		}
	}

	t.Meta = &Meta{Root: t.Root, Height: t.Height}

	// persist normalized meta best-effort
	if err := t.saveMeta(); err != nil {
		slog.Warn("btree.OpenTree: saveMeta failed", "err", err)
	}

	slog.Debug("btree.OpenTree",
		"root", t.Root,
		"height", t.Height,
		"nextPageID", t.nextPageID,
		"pageCount", pageCount,
	)

	return t, nil
}

// allocPage allocates a new page ID for this tree and returns a pinned page
// from the buffer pool. It does NOT touch the filesystem directly; StorageManager
// will create/extend segments on first flush.
func (t *Tree) allocPage() (uint32, *storage.Page, error) {
	pid := t.nextPageID
	t.nextPageID++

	slog.Debug("btree.allocPage", "pageID", pid)

	p, err := t.BP.GetPage(pid)
	if err != nil {
		return 0, nil, err
	}
	return pid, p, nil
}

func (t *Tree) syncMeta() {
	if t.Meta == nil {
		t.Meta = &Meta{}
	}
	t.Meta.Root = t.Root
	t.Meta.Height = t.Height

	// best-effort persist
	if err := t.saveMeta(); err != nil {
		slog.Warn("btree.syncMeta: saveMeta failed", "err", err)
	}
}

// ---- Public API ----

// Insert inserts (key, tid) into the tree, performing splits as needed.
// Height may increase if the root splits.
//
// V2 enforces non-decreasing keys at the Tree level: if a key smaller than
// the last inserted key is provided, ErrOutOfOrderInsert is returned.
func (t *Tree) Insert(key KeyType, tid heap.TID) error {
	slog.Debug("btree.Insert.start",
		"key", key,
		"tidPage", tid.PageID,
		"tidSlot", tid.Slot,
		"height", t.Height,
		"root", t.Root,
	)

	if t.lastKeySet && key < t.lastKey {
		slog.Debug("btree.Insert.out_of_order", "key", key, "lastKey", t.lastKey)
		return ErrOutOfOrderInsert
	}

	newRootID, didSplit, rightMinKey, rightPageID, err := t.insertAt(t.Root, t.Height, key, tid)
	if err != nil {
		slog.Debug("btree.Insert.insertAt_error", "err", err)
		return err
	}

	if !didSplit {
		// Root did not split. Root page id may have changed if the subtree
		// was rebuilt; we adopt the new id.
		t.Root = newRootID
		t.syncMeta()
		t.lastKey = key
		t.lastKeySet = true
		slog.Debug("btree.Insert.done_no_root_split",
			"root", t.Root,
			"height", t.Height,
		)
		return nil
	}

	// Root split: we must create a new internal root one level above.
	// Children:
	//   - left subtree rooted at newRootID (min key: existing tree min).
	//   - right subtree rooted at rightPageID (min key: rightMinKey).
	rootLevel := t.Height
	slog.Debug("btree.Insert.root_split",
		"oldRoot", t.Root,
		"newLeftRoot", newRootID,
		"rightRoot", rightPageID,
		"rightMinKey", rightMinKey,
		"oldHeight", t.Height,
	)

	rootID, rootPage, err := t.allocPage()
	if err != nil {
		return err
	}
	rootNode := &InternalNode{Page: rootPage}
	defer func() {
		_ = t.BP.Unpin(rootPage, true)
	}()

	leftMinKey, err := t.findMinKeyInSubtree(newRootID, rootLevel)
	if err != nil {
		return err
	}

	if err := rootNode.AppendEntry(leftMinKey, newRootID); err != nil {
		return err
	}
	if err := rootNode.AppendEntry(rightMinKey, rightPageID); err != nil {
		return err
	}

	t.Root = rootID
	t.Height++
	t.syncMeta()

	t.lastKey = key
	t.lastKeySet = true

	slog.Debug("btree.Insert.done_with_new_root",
		"root", t.Root,
		"height", t.Height,
	)

	return nil
}

// SearchEqual returns all TIDs with the given key.
func (t *Tree) SearchEqual(key KeyType) ([]heap.TID, error) {
	if t.Height < 1 {
		return nil, ErrInvalidTreeHeight
	}

	slog.Debug("btree.SearchEqual.start", "key", key, "root", t.Root, "height", t.Height)

	pageID := t.Root
	level := t.Height

	for level > 1 {
		p, err := t.BP.GetPage(pageID)
		if err != nil {
			return nil, err
		}
		node := &InternalNode{Page: p}
		idx, child, err := node.findChildIndex(key)
		_ = idx // kept for future debugging/extension
		_ = t.BP.Unpin(p, false)
		if err != nil {
			return nil, err
		}
		slog.Debug("btree.SearchEqual.descend",
			"level", level,
			"pageID", pageID,
			"child", child,
		)
		pageID = child
		level--
	}

	// Leaf level
	p, err := t.BP.GetPage(pageID)
	if err != nil {
		return nil, err
	}
	leaf := &LeafNode{Page: p}
	defer func() {
		_ = t.BP.Unpin(p, false)
	}()

	tids, err := leaf.FindEqual(key)
	if err != nil {
		return nil, err
	}
	slog.Debug("btree.SearchEqual.done",
		"key", key,
		"numTIDs", len(tids),
	)
	return tids, nil
}

// RangeScan returns all TIDs with minKey <= key <= maxKey.
// This is a simple full-tree range scan: it traverses all leaves.
func (t *Tree) RangeScan(minKey, maxKey KeyType) ([]heap.TID, error) {
	var out []heap.TID
	if t.Height < 1 {
		return out, ErrInvalidTreeHeight
	}
	slog.Debug("btree.RangeScan.start",
		"minKey", minKey,
		"maxKey", maxKey,
		"root", t.Root,
		"height", t.Height,
	)
	err := t.rangeScanAt(t.Root, t.Height, minKey, maxKey, &out)
	if err != nil {
		return nil, err
	}
	slog.Debug("btree.RangeScan.done",
		"minKey", minKey,
		"maxKey", maxKey,
		"numTIDs", len(out),
	)
	return out, nil
}

// ---- Recursive helpers ----

// insertAt inserts (key, tid) into the subtree rooted at pageID with the given
// level (1 = leaf, >1 = internal).
//
// Returns:
//   - newPageID: page id of the (possibly rebuilt) root of this subtree.
//   - didSplit: whether this subtree was split into left/right siblings.
//   - rightMinKey: if didSplit, the min key of the right sibling subtree.
//   - rightPageID: if didSplit, the page id of the right sibling.
func (t *Tree) insertAt(
	pageID uint32,
	level int,
	key KeyType,
	tid heap.TID,
) (newPageID uint32, didSplit bool, rightMinKey KeyType, rightPageID uint32, err error) {
	if level < 1 {
		return 0, false, 0, 0, ErrInvalidTreeHeight
	}

	if level == 1 {
		return t.insertIntoLeaf(pageID, key, tid)
	}
	return t.insertIntoInternal(pageID, level, key, tid)
}

// insertIntoLeaf handles insertion at leaf level (level == 1).
func (t *Tree) insertIntoLeaf(
	pageID uint32,
	key KeyType,
	tid heap.TID,
) (newPageID uint32, didSplit bool, rightMinKey KeyType, rightPageID uint32, err error) {
	p, err := t.BP.GetPage(pageID)
	if err != nil {
		return 0, false, 0, 0, err
	}
	leaf := &LeafNode{Page: p}

	// Load existing physical entries, append new, then sort.
	entries, err := leaf.readEntries()
	if err != nil {
		_ = t.BP.Unpin(p, false)
		return 0, false, 0, 0, err
	}
	entries = append(entries, leafEntry{key: key, tid: tid})
	sortLeafEntries(entries)

	// Compute max entries per empty leaf page (fixed tuple size).
	// Total size = Header + N*SlotSize + N*LeafEntrySize <= PageSize
	maxPerPage := (storage.PageSize - storage.HeaderSize) / (storage.SlotSize + LeafEntrySize)
	if maxPerPage <= 0 {
		_ = t.BP.Unpin(p, false)
		return 0, false, 0, 0, fmt.Errorf("btree: leaf page capacity is zero")
	}

	total := len(entries)

	// Case 1: fits in one page -> rebuild in place (sorted physical order)
	if total <= maxPerPage {
		if err := leaf.rebuildSorted(entries); err != nil {
			_ = t.BP.Unpin(p, false)
			return 0, false, 0, 0, err
		}
		_ = t.BP.Unpin(p, true)
		return pageID, false, 0, 0, nil
	}

	// Case 2: split into two leaves
	if total < 2 {
		_ = t.BP.Unpin(p, false)
		return 0, false, 0, 0, ErrCannotSplitLeafGreaterThanTwo
	}

	mid := total / 2
	leftEnts := entries[:mid]
	rightEnts := entries[mid:]

	// Rebuild left in-place (reuse old pageID)
	if err := leaf.rebuildSorted(leftEnts); err != nil {
		_ = t.BP.Unpin(p, false)
		return 0, false, 0, 0, err
	}

	// Allocate right page
	rightID, rightPage, err := t.allocPage()
	if err != nil {
		_ = t.BP.Unpin(p, true) // left already rebuilt
		return 0, false, 0, 0, err
	}
	rightLeaf := &LeafNode{Page: rightPage}

	if err := rightLeaf.rebuildSorted(rightEnts); err != nil {
		_ = t.BP.Unpin(p, true)
		_ = t.BP.Unpin(rightPage, false)
		return 0, false, 0, 0, err
	}

	rightMin := rightEnts[0].key

	_ = t.BP.Unpin(p, true)
	_ = t.BP.Unpin(rightPage, true)

	return pageID, true, rightMin, rightID, nil
}

// insertIntoInternal handles insertion into an internal node at the given level.
// level > 1.
func (t *Tree) insertIntoInternal(
	pageID uint32,
	level int,
	key KeyType,
	tid heap.TID,
) (newPageID uint32, didSplit bool, rightMinKey KeyType, rightPageID uint32, err error) {
	if level <= 1 {
		return 0, false, 0, 0, ErrInvalidTreeHeight
	}

	// Load current internal node.
	p, err := t.BP.GetPage(pageID)
	if err != nil {
		return 0, false, 0, 0, err
	}
	node := &InternalNode{Page: p}

	// Choose child to descend into using minKey semantics.
	idx, childID, err := node.findChildIndex(key)
	if err != nil {
		_ = t.BP.Unpin(p, false)
		return 0, false, 0, 0, err
	}

	slog.Debug("btree.insertIntoInternal.descend",
		"key", key,
		"pageID", pageID,
		"level", level,
		"childIndex", idx,
		"childID", childID,
	)

	// Recursively insert into child subtree.
	childNewID, childSplit, childRightMin, childRightID, err := t.insertAt(childID, level-1, key, tid)
	if err != nil {
		_ = t.BP.Unpin(p, false)
		return 0, false, 0, 0, err
	}

	// Read all existing entries.
	entries, err := node.readEntries()
	if err != nil {
		_ = t.BP.Unpin(p, false)
		return 0, false, 0, 0, err
	}
	_ = t.BP.Unpin(p, false) // old page is not used anymore (copy-on-write)

	// Update pointer of the child where we descended.
	if idx < 0 || idx >= len(entries) {
		return 0, false, 0, 0, ErrInternalChildIdxOutOfRange
	}
	entries[idx].child = childNewID

	// If the child split, we add an extra (minKey, childID) entry.
	if childSplit {
		slog.Debug("btree.insertIntoInternal.child_split",
			"parentPageID", pageID,
			"childIndex", idx,
			"childRightMin", childRightMin,
			"childRightID", childRightID,
		)
		entries = append(entries, internalEntry{
			key:   childRightMin,
			child: childRightID,
		})
	}

	// Keep entries sorted by key to preserve minKey semantics.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	// Decide whether we can rebuild as a single internal page or must split.
	leftID, leftPage, err := t.allocPage()
	if err != nil {
		return 0, false, 0, 0, err
	}
	leftNode := &InternalNode{Page: leftPage}

	// Compute maximum number of entries one empty internal page can hold.
	// (initial FreeSpace / (entry size + slot header)).
	maxPerPage := leftPage.FreeSpace() / (InternalEntrySize + storage.SlotSize)
	if maxPerPage <= 0 {
		_ = t.BP.Unpin(leftPage, false)
		return 0, false, 0, 0, ErrInternalNodePageHasZeroCap
	}

	total := len(entries)

	// Case 1: everything fits into one page → rebuild a single internal node.
	if total <= maxPerPage {
		for _, e := range entries {
			if err := leftNode.AppendEntry(e.key, e.child); err != nil {
				_ = t.BP.Unpin(leftPage, false)
				return 0, false, 0, 0, err
			}
		}
		_ = t.BP.Unpin(leftPage, true)
		slog.Debug("btree.insertIntoInternal.single_page",
			"oldPageID", pageID,
			"newPageID", leftID,
			"numEntries", total,
		)
		return leftID, false, 0, 0, nil
	}

	// Case 2: we need to split into two internal nodes.
	rightID, rightPage, err := t.allocPage()
	if err != nil {
		_ = t.BP.Unpin(leftPage, false)
		return 0, false, 0, 0, err
	}
	rightNode := &InternalNode{Page: rightPage}

	// Split entries into two groups, with per-page capacity respected.
	leftCount := min(total/2, maxPerPage)
	rightCount := total - leftCount
	if rightCount > maxPerPage {
		_ = t.BP.Unpin(leftPage, false)
		_ = t.BP.Unpin(rightPage, false)
		return 0, false, 0, 0, ErrSplitRequiredMoreThanTwoPages
	}

	leftEnts := entries[:leftCount]
	rightEnts := entries[leftCount:]

	for _, e := range leftEnts {
		if err := leftNode.AppendEntry(e.key, e.child); err != nil {
			_ = t.BP.Unpin(leftPage, false)
			_ = t.BP.Unpin(rightPage, false)
			return 0, false, 0, 0, err
		}
	}
	for _, e := range rightEnts {
		if err := rightNode.AppendEntry(e.key, e.child); err != nil {
			_ = t.BP.Unpin(leftPage, false)
			_ = t.BP.Unpin(rightPage, false)
			return 0, false, 0, 0, err
		}
	}

	rightMin := rightEnts[0].key

	_ = t.BP.Unpin(leftPage, true)
	_ = t.BP.Unpin(rightPage, true)

	slog.Debug("btree.insertIntoInternal.split_done",
		"oldPageID", pageID,
		"leftID", leftID,
		"rightID", rightID,
		"rightMinKey", rightMin,
		"totalEntries", total,
		"leftCount", leftCount,
		"rightCount", rightCount,
	)

	return leftID, true, rightMin, rightID, nil
}

// rangeScanAt recursively traverses the subtree rooted at (pageID, level)
// and appends all TIDs where minKey <= key <= maxKey.
func (t *Tree) rangeScanAt(
	pageID uint32,
	level int,
	minKey, maxKey KeyType,
	out *[]heap.TID,
) error {
	if level < 1 {
		return ErrInvalidTreeHeight
	}

	if level == 1 {
		p, err := t.BP.GetPage(pageID)
		if err != nil {
			return err
		}
		leaf := &LeafNode{Page: p}
		tids, err := leaf.Range(minKey, maxKey)
		_ = t.BP.Unpin(p, false)
		if err != nil {
			return err
		}
		slog.Debug("btree.rangeScanAt.leaf",
			"pageID", pageID,
			"numTIDs", len(tids),
		)
		*out = append(*out, tids...)
		return nil
	}

	p, err := t.BP.GetPage(pageID)
	if err != nil {
		return err
	}
	node := &InternalNode{Page: p}
	num := node.NumKeys()

	slog.Debug("btree.rangeScanAt.internal",
		"pageID", pageID,
		"level", level,
		"numChildren", num,
	)

	for i := range num {
		_, child, err := node.EntryAt(i)
		if err != nil {
			_ = t.BP.Unpin(p, false)
			return err
		}
		if err := t.rangeScanAt(child, level-1, minKey, maxKey, out); err != nil {
			_ = t.BP.Unpin(p, false)
			return err
		}
	}

	_ = t.BP.Unpin(p, false)
	return nil
}

// findMinKeyInSubtree finds the minimum key in the subtree rooted at pageID
// with the given level.
func (t *Tree) findMinKeyInSubtree(pageID uint32, level int) (KeyType, error) {
	if level < 1 {
		return 0, ErrInvalidTreeHeight
	}

	if level == 1 {
		p, err := t.BP.GetPage(pageID)
		if err != nil {
			return 0, err
		}
		leaf := &LeafNode{Page: p}
		defer func() { _ = t.BP.Unpin(p, false) }()

		entries, err := leaf.entriesSorted()
		if err != nil {
			return 0, err
		}
		if len(entries) == 0 {
			return 0, ErrLeafHasNoKey
		}
		return entries[0].key, nil
	}

	p, err := t.BP.GetPage(pageID)
	if err != nil {
		return 0, err
	}
	node := &InternalNode{Page: p}
	if node.NumKeys() == 0 {
		_ = t.BP.Unpin(p, false)
		return 0, ErrInternalNodeHasNoEntries
	}
	_, child, err := node.EntryAt(0)
	_ = t.BP.Unpin(p, false)
	if err != nil {
		return 0, err
	}
	return t.findMinKeyInSubtree(child, level-1)
}
