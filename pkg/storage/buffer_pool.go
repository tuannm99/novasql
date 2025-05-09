package storage

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// EvictionPolicy defines strategies for buffer pool eviction
type EvictionPolicy int

const (
	LRUPolicy EvictionPolicy = iota
	ClockPolicy
	LFUPolicy
)

// BufferDescriptor represents a buffer's metadata
type BufferDescriptor struct {
	pageID     uint32
	dirty      bool
	pinCount   int
	lastAccess time.Time
	data       []byte
	referenced bool // For Clock algorithm
	useCount   int  // For LFU algorithm
}

// BufferPoolStats tracks buffer pool statistics
type BufferPoolStats struct {
	Hits       int64
	Misses     int64
	Evictions  int64
	DiskReads  int64
	DiskWrites int64
	LastReset  time.Time
}

// PageBufferPool manages a pool of buffers for database pages
type PageBufferPool struct {
	buffers        map[uint32]*list.Element // Maps pageID to buffer position
	lruList        *list.List               // For LRU eviction policy
	freeList       *list.List               // List of free buffer slots
	bufferSize     int                      // Size of each buffer (page)
	maxBuffers     int                      // Maximum number of buffers
	storageManager *StorageManager          // Storage manager for I/O
	policy         EvictionPolicy           // Eviction policy
	clockHand      *list.Element            // For Clock algorithm
	mu             sync.RWMutex             // Mutex for thread safety
	stats          *BufferPoolStats         // Statistics
}

// NewPageBufferPool creates a new buffer pool
func NewPageBufferPool(bufferSize, maxBuffers int, sm *StorageManager, policy EvictionPolicy) *PageBufferPool {
	pool := &PageBufferPool{
		buffers:        make(map[uint32]*list.Element),
		lruList:        list.New(),
		freeList:       list.New(),
		bufferSize:     bufferSize,
		maxBuffers:     maxBuffers,
		storageManager: sm,
		policy:         policy,
		stats: &BufferPoolStats{
			LastReset: time.Now(),
		},
	}

	// Initialize free buffer slots
	for i := 0; i < maxBuffers; i++ {
		desc := &BufferDescriptor{
			data: make([]byte, bufferSize),
		}
		pool.freeList.PushBack(desc)
	}

	return pool
}

// AddPage adds a new page to the buffer pool
// This is specifically for pages that are newly created and don't yet exist on disk
func (bp *PageBufferPool) AddPage(page *Page) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page already exists
	if _, exists := bp.buffers[page.ID]; exists {
		return NewStorageError("add page", fmt.Errorf("page %d already exists in buffer pool", page.ID))
	}

	// Allocate buffer
	var bufDesc *BufferDescriptor
	if bp.freeList.Len() > 0 {
		elem := bp.freeList.Front()
		bufDesc = elem.Value.(*BufferDescriptor)
		bp.freeList.Remove(elem)
	} else {
		var err error
		bufDesc, err = bp.evictPage()
		if err != nil {
			return err
		}
		bp.stats.Evictions++
	}

	// Serialize page data
	pageData, err := page.Serialize()
	if err != nil {
		bp.freeList.PushBack(bufDesc)
		return NewStorageError("serialize page", err)
	}

	// Update buffer descriptor
	bufDesc.pageID = page.ID
	bufDesc.dirty = true // New pages are always dirty
	bufDesc.pinCount = 1
	bufDesc.lastAccess = time.Now()
	bufDesc.referenced = true
	bufDesc.useCount = 1
	copy(bufDesc.data, pageData)

	// Add to appropriate list based on policy
	var elem *list.Element
	if bp.policy == LRUPolicy {
		elem = bp.lruList.PushFront(bufDesc)
	} else {
		elem = bp.lruList.PushBack(bufDesc)
		if bp.clockHand == nil {
			bp.clockHand = elem
		}
	}

	bp.buffers[page.ID] = elem
	return nil
}

// GetPage retrieves a page from the buffer pool
func (bp *PageBufferPool) GetPage(pageID uint32) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page is already in memory
	if elem, found := bp.buffers[pageID]; found {
		bp.stats.Hits++
		desc := elem.Value.(*BufferDescriptor)
		desc.pinCount++
		desc.lastAccess = time.Now()
		desc.referenced = true
		desc.useCount++

		// Update position in LRU list if using LRU policy
		if bp.policy == LRUPolicy {
			bp.lruList.MoveToFront(elem)
		}

		page := &Page{ID: pageID}
		if err := page.Deserialize(desc.data); err != nil {
			return nil, NewStorageError("deserialize page", err)
		}
		return page, nil
	}

	bp.stats.Misses++
	bp.stats.DiskReads++

	// Allocate buffer
	var bufDesc *BufferDescriptor
	if bp.freeList.Len() > 0 {
		elem := bp.freeList.Front()
		bufDesc = elem.Value.(*BufferDescriptor)
		bp.freeList.Remove(elem)
	} else {
		var err error
		bufDesc, err = bp.evictPage()
		if err != nil {
			return nil, err
		}
		bp.stats.Evictions++
	}

	// Load page from storage
	pageData, err := bp.storageManager.ReadPage(pageID)
	if err != nil {
		bp.freeList.PushBack(bufDesc)
		return nil, NewStorageError("read page", err)
	}

	// Update buffer descriptor
	bufDesc.pageID = pageID
	bufDesc.dirty = false
	bufDesc.pinCount = 1
	bufDesc.lastAccess = time.Now()
	bufDesc.referenced = true
	bufDesc.useCount = 1
	copy(bufDesc.data, pageData)

	// Add to appropriate list based on policy
	var elem *list.Element
	if bp.policy == LRUPolicy {
		elem = bp.lruList.PushFront(bufDesc)
	} else {
		elem = bp.lruList.PushBack(bufDesc)
		if bp.clockHand == nil {
			bp.clockHand = elem
		}
	}

	bp.buffers[pageID] = elem

	// Deserialize into a Page object
	page := &Page{ID: pageID}
	if err := page.Deserialize(pageData); err != nil {
		return nil, NewStorageError("deserialize page", err)
	}

	return page, nil
}

// ReleasePage decreases the pin count and marks the page as dirty if modified
func (bp *PageBufferPool) ReleasePage(pageID uint32, dirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	elem, found := bp.buffers[pageID]
	if !found {
		return NewStorageError("release page", fmt.Errorf("page %d not in buffer pool", pageID))
	}

	desc := elem.Value.(*BufferDescriptor)
	if desc.pinCount <= 0 {
		return NewStorageError("release page", fmt.Errorf("page %d is not pinned", pageID))
	}

	desc.pinCount--
	if dirty {
		desc.dirty = true
	}

	return nil
}

// FlushPage writes a specific page back to disk if it's dirty
func (bp *PageBufferPool) FlushPage(pageID uint32) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	elem, found := bp.buffers[pageID]
	if !found {
		return nil // Page not in buffer pool, nothing to flush
	}

	desc := elem.Value.(*BufferDescriptor)
	if desc.dirty {
		bp.stats.DiskWrites++
		if err := bp.storageManager.WritePage(pageID, desc.data); err != nil {
			return NewStorageError("flush page", err)
		}
		desc.dirty = false
	}

	return nil
}

// FlushAllPages writes all dirty pages back to disk
func (bp *PageBufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for pageID, elem := range bp.buffers {
		desc := elem.Value.(*BufferDescriptor)
		if desc.dirty {
			bp.stats.DiskWrites++
			if err := bp.storageManager.WritePage(pageID, desc.data); err != nil {
				return NewStorageError("flush all pages", err)
			}
			desc.dirty = false
		}
	}

	return nil
}

// GetStats returns the current buffer pool statistics
func (bp *PageBufferPool) GetStats() *BufferPoolStats {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.stats
}

// ResetStats resets the buffer pool statistics
func (bp *PageBufferPool) ResetStats() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.stats = &BufferPoolStats{
		LastReset: time.Now(),
	}
}

// evictPage selects a page for eviction based on the chosen policy
func (bp *PageBufferPool) evictPage() (*BufferDescriptor, error) {
	if bp.lruList.Len() == 0 {
		return nil, NewStorageError("evict page", ErrBufferPoolFull)
	}

	switch bp.policy {
	case LRUPolicy:
		return bp.evictLRU()
	case ClockPolicy:
		return bp.evictClock()
	case LFUPolicy:
		return bp.evictLFU()
	default:
		return bp.evictLRU() // Default to LRU
	}
}

// evictLRU implements the Least Recently Used eviction policy
func (bp *PageBufferPool) evictLRU() (*BufferDescriptor, error) {
	for e := bp.lruList.Back(); e != nil; e = e.Prev() {
		desc := e.Value.(*BufferDescriptor)
		if desc.pinCount == 0 {
			if desc.dirty {
				bp.stats.DiskWrites++
				if err := bp.storageManager.WritePage(desc.pageID, desc.data); err != nil {
					return nil, NewStorageError("evict LRU", err)
				}
			}
			bp.lruList.Remove(e)
			delete(bp.buffers, desc.pageID)
			desc.pageID = 0
			desc.dirty = false
			desc.pinCount = 0
			desc.referenced = false
			desc.useCount = 0
			return desc, nil
		}
	}
	return nil, NewStorageError("evict LRU", ErrNoFreeBuffer)
}

// evictClock implements the Clock (second-chance) eviction algorithm
func (bp *PageBufferPool) evictClock() (*BufferDescriptor, error) {
	if bp.clockHand == nil {
		if bp.lruList.Len() == 0 {
			return nil, NewStorageError("evict clock", ErrBufferPoolFull)
		}
		bp.clockHand = bp.lruList.Front()
	}

	startHand := bp.clockHand
	for {
		desc := bp.clockHand.Value.(*BufferDescriptor)
		if desc.pinCount == 0 {
			if !desc.referenced {
				if desc.dirty {
					bp.stats.DiskWrites++
					if err := bp.storageManager.WritePage(desc.pageID, desc.data); err != nil {
						return nil, NewStorageError("evict clock", err)
					}
				}
				delete(bp.buffers, desc.pageID)
				nextHand := bp.clockHand.Next()
				if nextHand == nil {
					nextHand = bp.lruList.Front()
				}
				bp.lruList.Remove(bp.clockHand)
				bp.clockHand = nextHand
				desc.pageID = 0
				desc.dirty = false
				desc.pinCount = 0
				desc.referenced = false
				desc.useCount = 0
				return desc, nil
			}
			desc.referenced = false
		}
		bp.clockHand = bp.clockHand.Next()
		if bp.clockHand == nil {
			bp.clockHand = bp.lruList.Front()
		}
		if bp.clockHand == startHand {
			return nil, NewStorageError("evict clock", ErrNoFreeBuffer)
		}
	}
}

// evictLFU implements the Least Frequently Used eviction policy
func (bp *PageBufferPool) evictLFU() (*BufferDescriptor, error) {
	var minUseCount int = -1
	var victimElem *list.Element

	for e := bp.lruList.Front(); e != nil; e = e.Next() {
		desc := e.Value.(*BufferDescriptor)
		if desc.pinCount == 0 && (minUseCount == -1 || desc.useCount < minUseCount) {
			minUseCount = desc.useCount
			victimElem = e
		}
	}

	if victimElem == nil {
		return nil, NewStorageError("evict LFU", ErrNoFreeBuffer)
	}

	desc := victimElem.Value.(*BufferDescriptor)
	if desc.dirty {
		bp.stats.DiskWrites++
		if err := bp.storageManager.WritePage(desc.pageID, desc.data); err != nil {
			return nil, NewStorageError("evict LFU", err)
		}
	}
	bp.lruList.Remove(victimElem)
	delete(bp.buffers, desc.pageID)
	desc.pageID = 0
	desc.dirty = false
	desc.pinCount = 0
	desc.referenced = false
	desc.useCount = 0
	return desc, nil
}
