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

type Frame struct {
	PageID uint32
	Page   *storage.Page
	Dirty  bool
	Pin    int32
}

var _ Manager = (*Pool)(nil)

type Pool struct {
	sm *storage.StorageManager
	fs storage.FileSet

	mu        sync.Mutex
	frames    []*Frame       // len == capacity, nil == free slot
	pageTable map[uint32]int // PageID -> frame index

	replacementPolicy Replacer
}

func NewPool(sm *storage.StorageManager, fs storage.FileSet, capacity int) *Pool {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Pool{
		sm:                sm,
		fs:                fs,
		frames:            make([]*Frame, capacity),
		pageTable:         make(map[uint32]int),
		replacementPolicy: newClockAdapter(capacity),
	}
}

func (p *Pool) GetPage(pageID uint32) (*storage.Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 1) HIT
	if idx, ok := p.pageTable[pageID]; ok {
		f := p.frames[idx]
		if f == nil {
			// Inconsistent: mapping exists but frame is nil -> cleanup
			delete(p.pageTable, pageID)
		} else {
			wasZero := (f.Pin == 0)
			f.Pin++

			p.replacementPolicy.RecordAccess(idx)
			if wasZero {
				p.replacementPolicy.SetEvictable(idx, false)
			}
			return f.Page, nil
		}
	}

	// 2) Find free slot
	freeIdx := -1
	for i, f := range p.frames {
		if f == nil {
			freeIdx = i
			break
		}
	}
	if freeIdx != -1 {
		page, err := p.sm.LoadPage(p.fs, pageID)
		if err != nil {
			return nil, err
		}

		p.frames[freeIdx] = &Frame{
			PageID: pageID,
			Page:   page,
			Dirty:  false,
			Pin:    1,
		}
		p.pageTable[pageID] = freeIdx

		p.replacementPolicy.RecordAccess(freeIdx)
		p.replacementPolicy.SetEvictable(freeIdx, false)

		return page, nil
	}

	// 3) Evict
	victimIdx, ok := p.replacementPolicy.Evict()
	if !ok {
		return nil, ErrNoFreeFrame
	}

	victim := p.frames[victimIdx]
	if victim == nil || victim.Pin != 0 {
		// Defensive: replacer should not return nil/pinned victims.
		if victim != nil && victim.Pin == 0 {
			p.replacementPolicy.RecordAccess(victimIdx)
			p.replacementPolicy.SetEvictable(victimIdx, true)
		}
		return nil, ErrNoFreeFrame
	}

	if victim.Dirty {
		if err := p.sm.SavePage(p.fs, victim.PageID, *victim.Page); err != nil {
			// Put victim back as evictable
			p.replacementPolicy.RecordAccess(victimIdx)
			p.replacementPolicy.SetEvictable(victimIdx, true)
			return nil, err
		}
		victim.Dirty = false
	}

	newPage, err := p.sm.LoadPage(p.fs, pageID)
	if err != nil {
		// Put victim back as evictable
		p.replacementPolicy.RecordAccess(victimIdx)
		p.replacementPolicy.SetEvictable(victimIdx, true)
		return nil, err
	}

	delete(p.pageTable, victim.PageID)

	victim.PageID = pageID
	victim.Page = newPage
	victim.Dirty = false
	victim.Pin = 1

	p.pageTable[pageID] = victimIdx

	p.replacementPolicy.RecordAccess(victimIdx)
	p.replacementPolicy.SetEvictable(victimIdx, false)

	return newPage, nil
}

func (p *Pool) Unpin(page *storage.Page, dirty bool) error {
	if page == nil {
		return nil
	}

	pageID := page.PageID()

	p.mu.Lock()
	defer p.mu.Unlock()

	idx, ok := p.pageTable[pageID]
	if !ok {
		return nil
	}

	f := p.frames[idx]
	if f == nil {
		return nil
	}

	if dirty {
		f.Dirty = true
	}

	if f.Pin > 0 {
		f.Pin--
		if f.Pin == 0 {
			p.replacementPolicy.SetEvictable(idx, true)
		}
	}

	return nil
}

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

func (p *Pool) DeletePageFromBuffer(pageID uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	idx, ok := p.pageTable[pageID]
	if !ok {
		return nil
	}

	f := p.frames[idx]
	if f == nil {
		delete(p.pageTable, pageID)
		p.replacementPolicy.Remove(idx)
		return nil
	}

	if f.Pin != 0 {
		return ErrPagePinned
	}

	if f.Dirty {
		if err := p.sm.SavePage(p.fs, f.PageID, *f.Page); err != nil {
			return err
		}
		f.Dirty = false
	}

	p.frames[idx] = nil
	delete(p.pageTable, pageID)
	p.replacementPolicy.Remove(idx)
	return nil
}
