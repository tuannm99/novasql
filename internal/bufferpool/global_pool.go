package bufferpool

import (
	"errors"
	"sync"

	"github.com/tuannm99/novasql/internal/storage"
)

var (
	DefaultCapacity = 128

	ErrNoFreeFrame = errors.New("bufferpool: no free frame available (all pinned)")
	ErrPagePinned  = errors.New("bufferpool: page is pinned")
)

type Replacer interface {
	RecordAccess(frameID int)
	SetEvictable(frameID int, evictable bool)
	Evict() (frameID int, ok bool)
	Remove(frameID int)
	Size() int
}

type Manager interface {
	GetPage(pageID uint32) (*storage.Page, error)
	Unpin(page *storage.Page, dirty bool) error
	FlushAll() error
}

// ErrUnsupportedFileSet is returned when GlobalPool cannot work with a FileSet implementation.
var ErrUnsupportedFileSet = errors.New("bufferpool: unsupported FileSet (global pool requires LocalFileSet)")

// PageTag uniquely identifies a page in the global pool.
type PageTag struct {
	FSKey  string
	PageID uint32
}

// GlobalPool is a single shared buffer pool for ALL relations (heap/index/ovf).
// It mimics PostgreSQL shared_buffers at a high level.
type GlobalPool struct {
	sm *storage.StorageManager

	mu     sync.Mutex
	frames []*Frame        // len == capacity, nil == free slot
	table  map[PageTag]int // (fsKey,pageID) -> frame index
	repl   Replacer        // replacement policy tracks frame indices [0..cap)
}

// Frame is stored in global frames[].
// NOTE: FS is required to flush/evict correctly.
type Frame struct {
	Tag   PageTag
	FS    storage.LocalFileSet
	Page  *storage.Page
	Dirty bool
	Pin   int32
}

func NewGlobalPool(sm *storage.StorageManager, capacity int) *GlobalPool {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &GlobalPool{
		sm:     sm,
		frames: make([]*Frame, capacity),
		table:  make(map[PageTag]int),
		repl:   newClockAdapter(capacity),
	}
}

// GetPage pins and returns the page (fs,pageID).
func (g *GlobalPool) GetPage(fs storage.FileSet, pageID uint32) (*storage.Page, error) {
	key, lfs, ok := storage.FsKeyOf(fs)
	if !ok {
		return nil, ErrUnsupportedFileSet
	}
	tag := PageTag{FSKey: key, PageID: pageID}

	g.mu.Lock()
	defer g.mu.Unlock()

	// 1) HIT
	if idx, ok := g.table[tag]; ok {
		f := g.frames[idx]
		if f == nil {
			// Inconsistent mapping -> cleanup.
			delete(g.table, tag)
		} else {
			wasZero := (f.Pin == 0)
			f.Pin++

			g.repl.RecordAccess(idx)
			if wasZero {
				g.repl.SetEvictable(idx, false)
			}
			return f.Page, nil
		}
	}

	// 2) Find free slot
	freeIdx := -1
	for i, f := range g.frames {
		if f == nil {
			freeIdx = i
			break
		}
	}
	if freeIdx != -1 {
		page, err := g.sm.LoadPage(lfs, pageID)
		if err != nil {
			return nil, err
		}

		g.frames[freeIdx] = &Frame{
			Tag:   tag,
			FS:    lfs,
			Page:  page,
			Dirty: false,
			Pin:   1,
		}
		g.table[tag] = freeIdx

		g.repl.RecordAccess(freeIdx)
		g.repl.SetEvictable(freeIdx, false)
		return page, nil
	}

	// 3) Evict
	victimIdx, ok := g.repl.Evict()
	if !ok {
		return nil, ErrNoFreeFrame
	}
	victim := g.frames[victimIdx]
	if victim == nil || victim.Pin != 0 {
		// Defensive: replacer should never return nil/pinned victims.
		return nil, ErrNoFreeFrame
	}

	// Flush victim if dirty
	if victim.Dirty {
		if err := g.sm.SavePage(victim.FS, victim.Tag.PageID, *victim.Page); err != nil {
			// Put victim back as evictable if flush fails
			g.repl.RecordAccess(victimIdx)
			g.repl.SetEvictable(victimIdx, true)
			return nil, err
		}
		victim.Dirty = false
	}

	// Load requested page
	newPage, err := g.sm.LoadPage(lfs, pageID)
	if err != nil {
		// Put victim back as evictable
		g.repl.RecordAccess(victimIdx)
		g.repl.SetEvictable(victimIdx, true)
		return nil, err
	}

	// Remove old mapping
	delete(g.table, victim.Tag)

	// Reuse victim frame
	victim.Tag = tag
	victim.FS = lfs
	victim.Page = newPage
	victim.Dirty = false
	victim.Pin = 1

	g.table[tag] = victimIdx
	g.repl.RecordAccess(victimIdx)
	g.repl.SetEvictable(victimIdx, false)

	return newPage, nil
}

// Unpin decreases pin count and marks dirty optionally.
func (g *GlobalPool) Unpin(fs storage.FileSet, page *storage.Page, dirty bool) error {
	if page == nil {
		return nil
	}
	key, _, ok := storage.FsKeyOf(fs)
	if !ok {
		return ErrUnsupportedFileSet
	}
	tag := PageTag{FSKey: key, PageID: page.PageID()}

	g.mu.Lock()
	defer g.mu.Unlock()

	idx, ok := g.table[tag]
	if !ok {
		return nil
	}
	f := g.frames[idx]
	if f == nil {
		delete(g.table, tag)
		return nil
	}

	if dirty {
		f.Dirty = true
	}
	if f.Pin > 0 {
		f.Pin--
		if f.Pin == 0 {
			g.repl.SetEvictable(idx, true)
		}
	}
	return nil
}

// FlushAll flushes all dirty pages in the global pool.
func (g *GlobalPool) FlushAll() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, f := range g.frames {
		if f == nil || !f.Dirty {
			continue
		}
		if err := g.sm.SavePage(f.FS, f.Tag.PageID, *f.Page); err != nil {
			return err
		}
		f.Dirty = false
	}
	return nil
}

// FlushFileSet flushes dirty pages belonging to a single relation (FileSet).
func (g *GlobalPool) FlushFileSet(fs storage.FileSet) error {
	key, _, ok := storage.FsKeyOf(fs)
	if !ok {
		return ErrUnsupportedFileSet
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	for _, f := range g.frames {
		if f == nil || !f.Dirty {
			continue
		}
		if f.Tag.FSKey != key {
			continue
		}
		if err := g.sm.SavePage(f.FS, f.Tag.PageID, *f.Page); err != nil {
			return err
		}
		f.Dirty = false
	}
	return nil
}

// DropFileSet removes ALL pages of a relation from the global pool.
//
// IMPORTANT: This must be called before deleting/renaming underlying files.
// If any page is pinned, ErrPagePinned is returned.
func (g *GlobalPool) DropFileSet(fs storage.FileSet) error {
	key, _, ok := storage.FsKeyOf(fs)
	if !ok {
		return ErrUnsupportedFileSet
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// First pass: detect pinned
	for _, f := range g.frames {
		if f == nil {
			continue
		}
		if f.Tag.FSKey == key && f.Pin != 0 {
			return ErrPagePinned
		}
	}

	// Second pass: flush + remove
	for i, f := range g.frames {
		if f == nil {
			continue
		}
		if f.Tag.FSKey != key {
			continue
		}

		if f.Dirty {
			if err := g.sm.SavePage(f.FS, f.Tag.PageID, *f.Page); err != nil {
				return err
			}
		}

		delete(g.table, f.Tag)
		g.frames[i] = nil
		g.repl.Remove(i)
	}
	return nil
}
