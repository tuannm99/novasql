package bufferpool

import (
	"errors"
	"sync"

	"github.com/tuannm99/novasql/internal/storage"
)

var (
	DefaultCapacity = 128

	ErrNoFreeFrame = errors.New("bufferpool: no free frame available (all pinned)")
)

// Manager is a simple buffer pool interface for table-level usage.
type Manager interface {
	// GetPage returns a page from the buffer pool (pin count is increased).
	GetPage(pageID uint32) (*storage.Page, error)

	// Unpin decreases pin count and marks the page dirty if needed.
	Unpin(page *storage.Page, dirty bool) error

	// FlushAll flushes all dirty pages to disk.
	FlushAll() error
}

// TODO: from now pool is per table, each table will have its own bufferpol, we should make it shared global memory
// Frame holds a single page and its metadata inside the buffer pool.
type Frame struct {
	PageID uint32
	Page   *storage.Page
	Dirty  bool
	Pin    int32
}

var _ Manager = (*Pool)(nil)

// Pool is a simple fixed-size buffer pool bound to one FileSet.
type Pool struct {
	sm *storage.StorageManager
	fs storage.FileSet

	mu        sync.Mutex
	frames    []*Frame       // fixed-size slice (capacity = max frames)
	pageTable map[uint32]int // PageID -> index in frames
	capacity  int
}

func NewPool(sm *storage.StorageManager, fs storage.FileSet, capacity int) *Pool {
	if capacity <= 0 {
		capacity = 16 // default small capacity
	}
	return &Pool{
		sm:        sm,
		fs:        fs,
		frames:    make([]*Frame, 0, capacity),
		pageTable: make(map[uint32]int),
		capacity:  capacity,
	}
}

// GetPage returns a page from buffer pool and increases its pin count.
// If the page does not exist in memory, it will be loaded from disk.
func (p *Pool) GetPage(pageID uint32) (*storage.Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 1) Page already in buffer
	if idx, ok := p.pageTable[pageID]; ok {
		f := p.frames[idx]
		f.Pin++
		return f.Page, nil
	}

	// 2) Buffer has free slot -> append new frame
	if len(p.frames) < p.capacity {
		page, err := p.sm.LoadPage(p.fs, pageID)
		if err != nil {
			return nil, err
		}
		f := &Frame{
			PageID: pageID,
			Page:   page,
			Dirty:  false,
			Pin:    1,
		}
		p.frames = append(p.frames, f)
		p.pageTable[pageID] = len(p.frames) - 1
		return page, nil
	}

	// 3) Buffer is full -> find a victim frame with Pin == 0
	victimIdx := -1
	for i, f := range p.frames {
		if f.Pin == 0 {
			victimIdx = i
			break
		}
	}
	if victimIdx == -1 {
		return nil, ErrNoFreeFrame
	}

	victim := p.frames[victimIdx]

	// Flush victim if dirty
	if victim.Dirty {
		if err := p.sm.SavePage(p.fs, victim.PageID, *victim.Page); err != nil {
			return nil, err
		}
		victim.Dirty = false
	}

	// Remove old mapping
	delete(p.pageTable, victim.PageID)

	// Load new page into the victim frame
	page, err := p.sm.LoadPage(p.fs, pageID)
	if err != nil {
		return nil, err
	}

	victim.PageID = pageID
	victim.Page = page
	victim.Dirty = false
	victim.Pin = 1

	p.pageTable[pageID] = victimIdx

	return page, nil
}

// Unpin decreases the pin count of a page and marks it dirty if needed.
func (p *Pool) Unpin(page *storage.Page, dirty bool) error {
	if page == nil {
		return nil
	}

	pageID := page.PageID()

	p.mu.Lock()
	defer p.mu.Unlock()

	idx, ok := p.pageTable[pageID]
	if !ok {
		// Page is not managed by this pool; ignore silently.
		return nil
	}

	f := p.frames[idx]
	if dirty {
		f.Dirty = true
	}
	if f.Pin > 0 {
		f.Pin--
	}
	return nil
}

// FlushAll flushes all dirty frames to disk.
func (p *Pool) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, f := range p.frames {
		if f == nil || !f.Dirty {
			continue
		}
		if err := p.sm.SavePage(p.fs, f.PageID, *f.Page); err != nil {
			return err
		}
		f.Dirty = false
	}
	return nil
}
