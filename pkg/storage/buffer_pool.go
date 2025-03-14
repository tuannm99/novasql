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
	// For Clock algorithm
	referenced bool
	// For LFU algorithm
	useCount int
}

// BufferPool manages a pool of buffers for database pages
type BufferPool struct {
	buffers        map[uint32]*list.Element // Maps pageID to buffer position
	lruList        *list.List               // For LRU eviction policy
	freeList       *list.List               // List of free buffer slots
	bufferSize     int                      // Size of each buffer (page)
	maxBuffers     int                      // Maximum number of buffers
	storageManager *StorageManager          // Storage manager for I/O
	policy         EvictionPolicy           // Eviction policy
	clockHand      *list.Element            // For Clock algorithm
	mu             sync.Mutex               // Mutex for thread safety
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(bufferSize, maxBuffers int, sm *StorageManager, policy EvictionPolicy) *BufferPool {
	pool := &BufferPool{
		buffers:        make(map[uint32]*list.Element),
		lruList:        list.New(),
		freeList:       list.New(),
		bufferSize:     bufferSize,
		maxBuffers:     maxBuffers,
		storageManager: sm,
		policy:         policy,
	}

	// Initialize free buffer slots
	for i := 0; i < maxBuffers; i++ {
		desc := &BufferDescriptor{
			pageID:     0,
			dirty:      false,
			pinCount:   0,
			lastAccess: time.Now(),
			data:       make([]byte, bufferSize),
			referenced: false,
			useCount:   0,
		}
		pool.freeList.PushBack(desc)
	}

	return pool
}

// GetPage retrieves a page from the buffer pool
func (bp *BufferPool) GetPage(pageID uint32) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page is already in memory
	if elem, found := bp.buffers[pageID]; found {
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
			return nil, err
		}
		return page, nil
	}

	// Allocate buffer
	var bufDesc *BufferDescriptor
	if bp.freeList.Len() > 0 {
		elem := bp.freeList.Front()
		bufDesc = elem.Value.(*BufferDescriptor)
		bp.freeList.Remove(elem)
	} else {
		// Evict a page based on policy
		var err error
		bufDesc, err = bp.evictPage()
		if err != nil {
			return nil, err
		}
	}

	// Load page from storage
	pageData, err := bp.storageManager.ReadPage(pageID)
	if err != nil {
		bp.freeList.PushBack(bufDesc) // Return buffer if read fails
		return nil, err
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
		return nil, err
	}

	return page, nil
}

// evictPage selects a page for eviction based on the chosen policy
func (bp *BufferPool) evictPage() (*BufferDescriptor, error) {
	if bp.lruList.Len() == 0 {
		return nil, NewStorageError(ErrCodeBufferPoolFull, "All pages are pinned, cannot evict", nil)
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
func (bp *BufferPool) evictLRU() (*BufferDescriptor, error) {
	// Start from the back of the LRU list
	for e := bp.lruList.Back(); e != nil; e = e.Prev() {
		desc := e.Value.(*BufferDescriptor)
		if desc.pinCount == 0 {
			// Write to disk if dirty
			if desc.dirty {
				if err := bp.storageManager.WritePage(desc.pageID, desc.data); err != nil {
					return nil, err
				}
				desc.dirty = false
			}

			// Remove from map and list
			delete(bp.buffers, desc.pageID)
			bp.lruList.Remove(e)

			// Reset descriptor
			desc.pageID = 0
			desc.pinCount = 0
			desc.referenced = false
			desc.useCount = 0

			return desc, nil
		}
	}

	return nil, NewStorageError(ErrCodeBufferPoolFull, "All pages are pinned, cannot evict", nil)
}

// evictClock implements the Clock (second-chance) eviction algorithm
func (bp *BufferPool) evictClock() (*BufferDescriptor, error) {
	if bp.clockHand == nil {
		if bp.lruList.Len() == 0 {
			return nil, NewStorageError(ErrCodeBufferPoolFull, "Buffer pool is empty", nil)
		}
		bp.clockHand = bp.lruList.Front()
	}

	// Do multiple passes until we find a victim
	startingPoint := bp.clockHand
	for {
		desc := bp.clockHand.Value.(*BufferDescriptor)

		// Move the clock hand
		if bp.clockHand.Next() == nil {
			bp.clockHand = bp.lruList.Front()
		} else {
			bp.clockHand = bp.clockHand.Next()
		}

		// Skip pinned pages
		if desc.pinCount > 0 {
			continue
		}

		// If referenced, give a second chance
		if desc.referenced {
			desc.referenced = false
			continue
		}

		// This page is our victim
		if desc.dirty {
			if err := bp.storageManager.WritePage(desc.pageID, desc.data); err != nil {
				return nil, err
			}
			desc.dirty = false
		}

		// Remove from map
		delete(bp.buffers, desc.pageID)

		// Save the element to remove later
		victimElem := bp.clockHand.Prev()
		if victimElem == nil {
			victimElem = bp.lruList.Back()
		}

		// Reset descriptor
		desc.pageID = 0
		desc.pinCount = 0
		desc.referenced = false
		desc.useCount = 0

		// Remove from list
		bp.lruList.Remove(victimElem)

		return desc, nil

		// If we made a full loop and found no victims
		if bp.clockHand == startingPoint {
			return nil, NewStorageError(ErrCodeBufferPoolFull, "All pages are pinned, cannot evict", nil)
		}
	}
}

// evictLFU implements the Least Frequently Used eviction policy
func (bp *BufferPool) evictLFU() (*BufferDescriptor, error) {
	var minUseCount int = -1
	var victimElem *list.Element

	// Find the least frequently used unpinned page
	for e := bp.lruList.Front(); e != nil; e = e.Next() {
		desc := e.Value.(*BufferDescriptor)
		if desc.pinCount == 0 && (minUseCount == -1 || desc.useCount < minUseCount) {
			minUseCount = desc.useCount
			victimElem = e
		}
	}

	if victimElem == nil {
		return nil, NewStorageError(ErrCodeBufferPoolFull, "All pages are pinned, cannot evict", nil)
	}

	desc := victimElem.Value.(*BufferDescriptor)

	// Write to disk if dirty
	if desc.dirty {
		if err := bp.storageManager.WritePage(desc.pageID, desc.data); err != nil {
			return nil, err
		}
		desc.dirty = false
	}

	// Remove from map and list
	delete(bp.buffers, desc.pageID)
	bp.lruList.Remove(victimElem)

	// Reset descriptor
	desc.pageID = 0
	desc.pinCount = 0
	desc.referenced = false
	desc.useCount = 0

	return desc, nil
}

// ReleasePage decreases the pin count and marks the page as dirty if modified
func (bp *BufferPool) ReleasePage(pageID uint32, dirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	elem, found := bp.buffers[pageID]
	if !found {
		return NewStorageError(ErrCodeInvalidOperation, fmt.Sprintf("Page %d not in buffer pool", pageID), nil)
	}

	desc := elem.Value.(*BufferDescriptor)
	if desc.pinCount <= 0 {
		return NewStorageError(ErrCodeInvalidOperation, fmt.Sprintf("Page %d is not pinned", pageID), nil)
	}

	desc.pinCount--
	if dirty {
		desc.dirty = true
	}

	return nil
}

// FlushPage writes a specific page back to disk if it's dirty
func (bp *BufferPool) FlushPage(pageID uint32) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	elem, found := bp.buffers[pageID]
	if !found {
		return nil // Page not in buffer pool, nothing to flush
	}

	desc := elem.Value.(*BufferDescriptor)
	if desc.dirty {
		if err := bp.storageManager.WritePage(pageID, desc.data); err != nil {
			return err
		}
		desc.dirty = false
	}

	return nil
}

// FlushAllPages writes all dirty pages back to disk
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for pageID, elem := range bp.buffers {
		desc := elem.Value.(*BufferDescriptor)
		if desc.dirty {
			if err := bp.storageManager.WritePage(pageID, desc.data); err != nil {
				return err
			}
			desc.dirty = false
		}
	}

	return nil
}
