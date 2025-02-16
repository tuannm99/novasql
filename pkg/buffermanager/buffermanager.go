// buffermanager manage pool of in-memeory pages, which are loaded from disk for cached,
// that means we need to make sure that both page in memory and disk are consistants,
// locking strategy?
// invalidate the page in pool?
package buffermanager

import (
	// "errors"
	// "fmt"
	"sync"

	"github.com/tuannm99/novasql/pkg/storage"
)

type PageTable struct {
	metadata struct {
		dirtyFlag    bool
		pinCounter   int
		// trackingInfo interface{} // don't know what is included yet
	}
	isPin bool

	// we call lock when developing an application that work with a database
	// for example page lock, tuple lock, table lock
	// latch it just a lock but in database terminology/mechanism
	latch sync.Mutex
}

type BufferPool struct {
	frame       *storage.Page
	isDirectory bool
}

// BufferManager manages the buffer pool
type BufferManager struct {
	framePool map[int]*storage.Page

	maxSize   int
	mutex     sync.Mutex
	pageTable PageTable
	// Channel for evicting pages when the buffer pool is full
	pageEvict chan int
}

// NewBufferManager creates a new BufferManager
func NewBufferManager(maxSize int) *BufferManager {
	return &BufferManager{
		framePool: make(map[int]*storage.Page),
		maxSize:   maxSize,
		pageEvict: make(chan int, maxSize),
	}
}

// // GetPage retrieves a page by its ID, loading it from "disk" if necessary
// func (bm *BufferManager) GetPage(pageID int) (*Page, error) {
// 	bm.mutex.Lock()
// 	defer bm.mutex.Unlock()
//
// 	if page, exists := bm.framePool[pageID]; exists {
// 		return page, nil
// 	}
//
// 	// Simulate loading from disk
// 	page, err := bm.loadPageFromDisk(pageID)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	bm.framePool[pageID] = page
// 	bm.pageEvict <- pageID
// 	if len(bm.framePool) > bm.maxSize {
// 		bm.evictPage()
// 	}
//
// 	return page, nil
// }

// // loadPageFromDisk simulates loading a page from disk
// func (bm *BufferManager) loadPageFromDisk(pageID int) (*Page, error) {
// 	fmt.Println("Loading page from disk:", pageID)
// 	return &Page{ID: pageID}, nil
// }
//
// // evictPage evicts the least recently added page
// func (bm *BufferManager) evictPage() {
// 	select {
// 	case pageID := <-bm.pageEvict:
// 		fmt.Println("Evicting page:", pageID)
// 		delete(bm.framePool, pageID)
// 	default:
// 		fmt.Println("No pages to evict")
// 	}
// }
//
// // MarkDirty marks a page as modified
// func (bm *BufferManager) MarkDirty(pageID int) error {
// 	bm.mutex.Lock()
// 	defer bm.mutex.Unlock()
//
// 	if page, exists := bm.framePool[pageID]; exists {
// 		fmt.Println(page.ID)
// 		return nil
// 	}
//
// 	return errors.New("page not found in buffer pool")
// }
