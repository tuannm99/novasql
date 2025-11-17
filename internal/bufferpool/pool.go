package bufferpool

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/tuannm99/novasql/internal/storage"
)

var (
	logDebugPrefix  = "bufferpool: "
	DefaultCapacity = 128

	// ErrNoFreeFrame is returned when no unpinned frame is available for replacement.
	ErrNoFreeFrame = errors.New("bufferpool: no free frame available (all pinned)")

	// ErrPagePinned is returned when trying to evict/delete a pinned page.
	ErrPagePinned = errors.New("bufferpool: page is pinned")
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

// Frame holds a single page and its metadata inside the buffer pool.
type Frame struct {
	PageID uint32
	Page   *storage.Page
	Dirty  bool
	Pin    int32

	// Ref is the CLOCK reference bit.
	// CLOCK is an approximate LRU algorithm:
	//   - When a page is accessed, Ref is set to true.
	//   - When searching for a victim, frames with Ref == true are given
	//     a "second chance" (Ref is cleared and the hand moves on).
	//   - A frame with Pin == 0 and Ref == false can be evicted.
	Ref bool
}

var _ Manager = (*Pool)(nil)

// Pool is a simple fixed-size buffer pool bound to one FileSet.
// We use a CLOCK replacement policy to choose victim frames when the pool is full.
type Pool struct {
	sm *storage.StorageManager
	fs storage.FileSet

	mu        sync.Mutex
	frames    []*Frame       // fixed-size slice, len == capacity, nil == free slot
	pageTable map[uint32]int // PageID -> index in frames
	capacity  int

	// clockHand is the current position of the CLOCK "hand".
	// It moves circularly over frames to choose a victim frame.
	clockHand int
}

// NewPool creates a new buffer pool with the given capacity.
// If capacity <= 0, a small default capacity is used.
func NewPool(sm *storage.StorageManager, fs storage.FileSet, capacity int) *Pool {
	if capacity <= 0 {
		capacity = 16 // default small capacity
	}
	return &Pool{
		sm:        sm,
		fs:        fs,
		frames:    make([]*Frame, capacity), // all nil initially
		pageTable: make(map[uint32]int),
		capacity:  capacity,
		clockHand: 0,
	}
}

// GetPage returns a page from buffer pool and increases its pin count.
// If the page does not exist in memory, it will be loaded from disk.
// Replacement policy for a full pool uses the CLOCK algorithm.
func (p *Pool) GetPage(pageID uint32) (*storage.Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	slog.Debug(logDebugPrefix+"GetPage called", "pageID", pageID)

	// 1) Page already in buffer
	if idx, ok := p.pageTable[pageID]; ok {
		f := p.frames[idx]
		if f == nil {
			// Inconsistent state, should not happen.
			slog.Error(logDebugPrefix+"pageTable points to nil frame",
				"pageID", pageID,
				"frameIdx", idx)
			delete(p.pageTable, pageID)
		} else {
			f.Pin++
			f.Ref = true // mark as recently used for CLOCK
			slog.Debug(logDebugPrefix+"found page in buffer",
				"pageID", pageID,
				"frameIdx", idx,
				"framePin", f.Pin)
			return f.Page, nil
		}
	}

	// 2) Try to find a free slot (nil frame) first
	freeIdx := -1
	for i, f := range p.frames {
		if f == nil {
			freeIdx = i
			break
		}
	}

	if freeIdx != -1 {
		slog.Debug(logDebugPrefix+"using free frame slot", "pageID", pageID, "frameIdx", freeIdx)

		page, err := p.sm.LoadPage(p.fs, pageID)
		if err != nil {
			return nil, err
		}
		f := &Frame{
			PageID: pageID,
			Page:   page,
			Dirty:  false,
			Pin:    1,
			Ref:    true, // newly loaded page is considered recently used
		}
		p.frames[freeIdx] = f
		p.pageTable[pageID] = freeIdx

		slog.Debug(logDebugPrefix+"created new frame",
			"pageID", pageID,
			"frameIdx", freeIdx,
			"framePin", f.Pin)
		return page, nil
	}

	// 3) Buffer is full -> use CLOCK to find a victim frame with Pin == 0
	slog.Debug(logDebugPrefix + "buffer full, CLOCK selecting victim frame")
	victimIdx, err := p.pickVictimLocked()
	if err != nil {
		// Either all frames are pinned or some internal inconsistency
		return nil, err
	}

	victim := p.frames[victimIdx]
	slog.Debug(logDebugPrefix+"selected victim frame",
		"victimPageID", victim.PageID,
		"frameIdx", victimIdx,
		"dirty", victim.Dirty)

	// Flush victim if dirty
	if victim.Dirty {
		slog.Debug(logDebugPrefix+"flushing dirty victim page",
			"victimPageID", victim.PageID)
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
	victim.Ref = true // recently used

	p.pageTable[pageID] = victimIdx

	slog.Debug(logDebugPrefix+"reused victim frame for new page",
		"pageID", pageID,
		"frameIdx", victimIdx,
		"framePin", victim.Pin)

	return page, nil
}

// pickVictimLocked chooses a victim frame using the CLOCK algorithm.
//
// CLOCK algorithm summary:
//   - Frames have a Ref bit (reference bit).
//   - When a page is accessed, Ref is set to true.
//   - When the buffer is full, the "clock hand" sweeps over frames:
//   - If frame is pinned (Pin > 0) or nil: skip it.
//   - If frame.Ref == true: clear Ref to false and skip (second chance).
//   - If frame.Ref == false and Pin == 0: select as victim.
//   - If after a full sweep no unpinned frame is found, ErrNoFreeFrame is returned.
//
// NOTE: The caller must hold p.mu.
func (p *Pool) pickVictimLocked() (int, error) {
	n := p.capacity
	if n == 0 {
		return -1, ErrNoFreeFrame
	}

	scanned := 0

	for scanned < 2*n { // 2 full sweeps upper bound to avoid infinite loops
		idx := p.clockHand
		f := p.frames[idx]

		if f != nil && f.Pin == 0 {
			if !f.Ref {
				// Found a frame that is unpinned and not recently used.
				// This is our victim.
				p.clockHand = (p.clockHand + 1) % n
				return idx, nil
			}
			// Give a second chance: clear Ref and move on.
			f.Ref = false
		}

		// Move clock hand to the next frame.
		p.clockHand = (p.clockHand + 1) % n
		scanned++
	}

	// No suitable victim found: either all frames pinned or some frames are all recently used.
	slog.Debug(logDebugPrefix + "CLOCK could not find a victim (all pinned or busy)")
	return -1, ErrNoFreeFrame
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
		slog.Debug(logDebugPrefix+"Unpin ignored, page not in pool", "pageID", pageID)
		return nil
	}

	f := p.frames[idx]
	if f == nil {
		slog.Error(logDebugPrefix+"Unpin found nil frame",
			"pageID", pageID,
			"frameIdx", idx)
		return nil
	}

	if dirty {
		f.Dirty = true
	}

	if f.Pin > 0 {
		f.Pin--
	}

	// Note: We do NOT set Ref here. Ref is set on GetPage (access).
	// Some implementations set Ref on Unpin as well, but we keep it simple.

	slog.Debug(logDebugPrefix+"Unpin",
		"pageID", pageID,
		"frameIdx", idx,
		"dirty", f.Dirty,
		"newPin", f.Pin)

	return nil
}

// FlushAll flushes all dirty frames to disk.
func (p *Pool) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	slog.Debug(logDebugPrefix + "FlushAll started")

	for idx, f := range p.frames {
		if f == nil || !f.Dirty {
			continue
		}
		slog.Debug(logDebugPrefix+"flushing frame",
			"pageID", f.PageID,
			"frameIdx", idx)
		if err := p.sm.SavePage(p.fs, f.PageID, *f.Page); err != nil {
			return err
		}
		f.Dirty = false
	}

	slog.Debug(logDebugPrefix + "FlushAll completed")
	return nil
}

// DeletePageFromBuffer removes a page from the buffer pool (buffer only, not disk).
// It will fail if the page is currently pinned.
//
// This is useful when the storage layer decides that a page is no longer valid
// (e.g. dropped table, truncated file, free-list management, etc.)
func (p *Pool) DeletePageFromBuffer(pageID uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	idx, ok := p.pageTable[pageID]
	if !ok {
		// Not in pool, nothing to do.
		slog.Debug(logDebugPrefix+"DeletePageFromBuffer: page not in pool", "pageID", pageID)
		return nil
	}

	f := p.frames[idx]
	if f == nil {
		slog.Debug(logDebugPrefix+"DeletePageFromBuffer: nil frame, cleaning mapping only",
			"pageID", pageID,
			"frameIdx", idx)
		delete(p.pageTable, pageID)
		return nil
	}

	if f.Pin != 0 {
		slog.Debug(logDebugPrefix+"DeletePageFromBuffer: page is pinned",
			"pageID", pageID,
			"frameIdx", idx,
			"pin", f.Pin)
		return ErrPagePinned
	}

	// Optionally flush if dirty. Depending on your design, you may skip this
	// if the page is logically deleted at a higher layer.
	if f.Dirty {
		slog.Debug(logDebugPrefix+"DeletePageFromBuffer: flushing dirty page before remove",
			"pageID", pageID)
		if err := p.sm.SavePage(p.fs, f.PageID, *f.Page); err != nil {
			return err
		}
	}

	slog.Debug(logDebugPrefix+"DeletePageFromBuffer: freeing frame",
		"pageID", pageID,
		"frameIdx", idx)

	p.frames[idx] = nil
	delete(p.pageTable, pageID)
	return nil
}
