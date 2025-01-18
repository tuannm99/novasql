package storage

import (
	"errors"
	"fmt"
	"sync"
)

// BufferManager manages the buffer pool
type BufferManager struct {
	pool    map[int]*Page
	maxSize int
	mutex   sync.Mutex
	// Channel for evicting pages when the buffer pool is full
	pageEvict chan int
}

// NewBufferManager creates a new BufferManager
func NewBufferManager(maxSize int) *BufferManager {
	return &BufferManager{
		pool:      make(map[int]*Page),
		maxSize:   maxSize,
		pageEvict: make(chan int, maxSize),
	}
}

// GetPage retrieves a page by its ID, loading it from "disk" if necessary
func (bm *BufferManager) GetPage(pageID int) (*Page, error) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if page, exists := bm.pool[pageID]; exists {
		return page, nil
	}

	// Simulate loading from disk
	page, err := bm.loadPageFromDisk(pageID)
	if err != nil {
		return nil, err
	}

	bm.pool[pageID] = page
	bm.pageEvict <- pageID
	if len(bm.pool) > bm.maxSize {
		bm.evictPage()
	}

	return page, nil
}

// loadPageFromDisk simulates loading a page from disk
func (bm *BufferManager) loadPageFromDisk(pageID int) (*Page, error) {
	fmt.Println("Loading page from disk:", pageID)
	return &Page{ID: pageID}, nil
}

// evictPage evicts the least recently added page
func (bm *BufferManager) evictPage() {
	select {
	case pageID := <-bm.pageEvict:
		fmt.Println("Evicting page:", pageID)
		delete(bm.pool, pageID)
	default:
		fmt.Println("No pages to evict")
	}
}

// MarkDirty marks a page as modified
func (bm *BufferManager) MarkDirty(pageID int) error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if page, exists := bm.pool[pageID]; exists {
		fmt.Println(page.ID)
		return nil
	}

	return errors.New("page not found in buffer pool")
}
