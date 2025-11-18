package btree

import (
	"errors"

	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/storage"
)

// ErrNodeFull indicates that the root leaf page is full and cannot accept more entries.
// V1 does not support splits yet, so the caller must handle this.
var ErrNodeFull = errors.New("btree: root leaf node is full")

// Tree is a very simple B+Tree V1 where the root is a single leaf page.
// It supports inserting keys in non-decreasing order and searching by key.
type Tree struct {
	SM   *storage.StorageManager
	FS   storage.FileSet
	BP   bufferpool.Manager
	Root uint32 // root page id, for now always 0
}

// NewTree creates a new Tree handle. The root page will be lazily initialized
// when the first insert happens.
func NewTree(sm *storage.StorageManager, fs storage.FileSet, bp bufferpool.Manager) *Tree {
	return &Tree{
		SM:   sm,
		FS:   fs,
		BP:   bp,
		Root: 0,
	}
}

func (t *Tree) getRootLeaf() (*LeafNode, *storage.Page, error) {
	p, err := t.BP.GetPage(t.Root)
	if err != nil {
		return nil, nil, err
	}
	// StorageManager.LoadPage should have initialized the page if it was zeroed.
	return &LeafNode{Page: p}, p, nil
}

// Insert appends a (key, TID) into the root leaf.
// V1 does not support splits, so if there is no free space we return ErrNodeFull.
func (t *Tree) Insert(key KeyType, tid heap.TID) error {
	leaf, page, err := t.getRootLeaf()
	if err != nil {
		return err
	}

	// We want to know whether insert succeeded to mark dirty.
	defer func() {
		_ = t.BP.Unpin(page, err == nil)
	}()

	// Check whether the page has enough free space for another leaf entry.
	// Each entry is fixed LeafEntrySize bytes plus one Slot header.
	need := LeafEntrySize + storage.SlotSize
	if leaf.Page.FreeSpace() < need {
		err = ErrNodeFull
		return err
	}

	err = leaf.AppendEntry(key, tid)
	return err
}

// SearchEqual returns all TIDs with the given key from the root leaf.
func (t *Tree) SearchEqual(key KeyType) ([]heap.TID, error) {
	leaf, page, err := t.getRootLeaf()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = t.BP.Unpin(page, false)
	}()
	return leaf.FindEqual(key)
}
