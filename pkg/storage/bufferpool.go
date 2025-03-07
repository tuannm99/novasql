package storage

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/tuannm99/novasql/pkg/cache"
)

// BufferDescriptor represents a buffer's metadata
type BufferDescriptor struct {
	pageID     uint32
	dirty      bool
	pinCount   int
	lastAccess time.Time
	data       []byte
}

// BufferPool manages a pool of buffers for database pages
type BufferPool struct {
	buffers        map[uint32]*list.Element // Maps pageID to LRU position
	lruManager     *cache.LRUManager        // LRU manager for eviction
	freeList       *list.List               // List of free buffer slots
	bufferSize     int                      // Size of each buffer (page)
	maxBuffers     int                      // Maximum number of buffers
	storageManager *StorageManager          // Storage manager for I/O
	mu             sync.Mutex               // Mutex for thread safety
}

// NewBufferPool creates a new buffer pool with an LRU manager
func NewBufferPool(bufferSize, maxBuffers int, sm *StorageManager) *BufferPool {
	pool := &BufferPool{
		buffers:        make(map[uint32]*list.Element),
		lruManager:     cache.NewLRUManager(),
		freeList:       list.New(),
		bufferSize:     bufferSize,
		maxBuffers:     maxBuffers,
		storageManager: sm,
	}

	// Initialize free buffer slots
	for i := 0; i < maxBuffers; i++ {
		desc := &BufferDescriptor{
			pageID:     0,
			dirty:      false,
			pinCount:   0,
			lastAccess: time.Now(),
			data:       make([]byte, bufferSize),
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
		bp.lruManager.MoveToFront(elem) // Mark as recently used
		desc := elem.Value.(*BufferDescriptor)
		desc.pinCount++
		desc.lastAccess = time.Now()

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
		// Evict least recently used page
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
	copy(bufDesc.data, pageData)

	// Add to LRU and map
	elem := bp.lruManager.PushFront(bufDesc)
	bp.buffers[pageID] = elem

	// Deserialize into a Page object
	page := &Page{ID: pageID}
	if err := page.Deserialize(pageData); err != nil {
		return nil, err
	}

	return page, nil
}

// evictPage selects the least recently used page for eviction
func (bp *BufferPool) evictPage() (*BufferDescriptor, error) {
	if bp.lruManager.Len() == 0 {
		return nil, fmt.Errorf("all pages are pinned, cannot evict")
	}

	// Evict from back of the LRU list
	for {
		elem := bp.lruManager.Back()
		if elem == nil {
			return nil, fmt.Errorf("all pages are pinned, cannot evict")
		}

		desc := elem.Value.(*BufferDescriptor)

		// Skip pinned pages
		if desc.pinCount > 0 {
			bp.lruManager.MoveToFront(elem) // Reconsider later
			continue
		}

		// Write to disk if dirty before eviction
		if desc.dirty {
			if err := bp.storageManager.WritePage(desc.pageID, desc.data); err != nil {
				return nil, err
			}
			desc.dirty = false
		}

		// Remove from buffers and LRU list
		delete(bp.buffers, desc.pageID)
		bp.lruManager.Remove(elem)

		// Reset descriptor
		desc.pageID = 0
		desc.dirty = false
		desc.pinCount = 0

		return desc, nil
	}
}

// ReleasePage decreases the pin count and marks the page as dirty if modified
func (bp *BufferPool) ReleasePage(pageID uint32, dirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	elem, found := bp.buffers[pageID]
	if !found {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	desc := elem.Value.(*BufferDescriptor)
	if desc.pinCount <= 0 {
		return fmt.Errorf("page %d is not pinned", pageID)
	}

	desc.pinCount--
	if dirty {
		desc.dirty = true
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
