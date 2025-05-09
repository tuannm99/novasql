package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// StorageManager manages database files and storage space
type StorageManager struct {
	dir           string              // Base directory for database files
	pageCount     uint32              // Total number of pages
	segmentFiles  map[uint32]*os.File // Cache of open segment file handles
	segmentMutex  sync.RWMutex        // Mutex for segment file access
	freeLists     map[uint32][]uint32 // Free page lists per page type
	freeListMutex sync.RWMutex        // Mutex for free list access
	maxPageID     uint32              // Highest page ID allocated
	metadataFile  string              // Path to database metadata file
	bufferPool    *PageBufferPool     // Buffer pool for caching pages
}

// NewStorageManager initializes storage in a directory
func NewStorageManager(dir string) (*StorageManager, error) {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(dir, FileMode0755); err != nil {
		return nil, NewStorageError("create directory", err)
	}

	sm := &StorageManager{
		dir:          dir,
		pageCount:    0,
		segmentFiles: make(map[uint32]*os.File),
		freeLists:    make(map[uint32][]uint32),
		maxPageID:    0,
		metadataFile: filepath.Join(dir, "metadata"),
	}

	// Initialize free lists for different page types
	for i := uint32(1); i <= uint32(Overflow); i++ {
		sm.freeLists[i] = []uint32{}
	}

	// Load metadata if it exists
	if err := sm.loadMetadata(); err != nil {
		if !os.IsNotExist(err) {
			return nil, NewStorageError("load metadata", err)
		}
		// Create initial metadata if file doesn't exist
		if err := sm.saveMetadata(); err != nil {
			return nil, NewStorageError("save initial metadata", err)
		}
	}

	// Initialize buffer pool with the storage manager
	sm.bufferPool = NewPageBufferPool(PageSize, 1000, sm, LRUPolicy)

	return sm, nil
}

// GetSegmentPath returns the file path of a segment based on the page number
func (sm *StorageManager) GetSegmentPath(pageID uint32) string {
	segmentID := pageID / (SegmentSize / PageSize)
	return filepath.Join(sm.dir, fmt.Sprintf("segment_%d", segmentID))
}

// getSegmentFile returns an open file handle for the segment
func (sm *StorageManager) getSegmentFile(pageID uint32) (*os.File, error) {
	segmentID := pageID / (SegmentSize / PageSize)

	sm.segmentMutex.RLock()
	file, exists := sm.segmentFiles[segmentID]
	sm.segmentMutex.RUnlock()

	if exists && file != nil {
		return file, nil
	}

	// Need to open the file
	sm.segmentMutex.Lock()
	defer sm.segmentMutex.Unlock()

	// Check again inside the write lock
	if file, exists = sm.segmentFiles[segmentID]; exists && file != nil {
		return file, nil
	}

	segmentPath := sm.GetSegmentPath(pageID)
	file, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE, FileMode0664)
	if err != nil {
		return nil, NewStorageError("open segment file", err)
	}

	sm.segmentFiles[segmentID] = file
	return file, nil
}

// WritePage writes a page to its corresponding segment file
func (sm *StorageManager) WritePage(pageID uint32, data []byte) error {
	if len(data) != PageSize {
		return NewStorageError("write page", fmt.Errorf("page must be exactly %d bytes", PageSize))
	}

	file, err := sm.getSegmentFile(pageID)
	if err != nil {
		return err
	}

	offset := int64((pageID % (SegmentSize / PageSize)) * PageSize)

	// Seek and write atomically to prevent race conditions
	sm.segmentMutex.Lock()
	defer sm.segmentMutex.Unlock()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return NewStorageError("seek", err)
	}

	_, err = file.Write(data)
	if err != nil {
		return NewStorageError("write", err)
	}

	// Update page count if necessary
	if pageID >= sm.pageCount {
		sm.pageCount = pageID + 1
		sm.maxPageID = pageID
		err = sm.saveMetadata()
		if err != nil {
			return NewStorageError("update metadata", err)
		}
	}

	return nil
}

// ReadPage reads a page from disk
func (sm *StorageManager) ReadPage(pageID uint32) ([]byte, error) {
	file, err := sm.getSegmentFile(pageID)
	if err != nil {
		return nil, err
	}

	offset := int64((pageID % (SegmentSize / PageSize)) * PageSize)
	data := make([]byte, PageSize)

	sm.segmentMutex.RLock()
	defer sm.segmentMutex.RUnlock()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, NewStorageError("seek", err)
	}

	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		return nil, NewStorageError("read", err)
	}

	if n != PageSize && pageID < sm.pageCount {
		return nil, NewStorageError("read page", fmt.Errorf("incomplete page read: got %d bytes, expected %d", n, PageSize))
	}

	return data, nil
}

// LoadPage loads a page from disk into memory
func (sm *StorageManager) LoadPage(pageID uint32) (*Page, error) {
	return sm.bufferPool.GetPage(pageID)
}

// SavePage writes a page to disk
func (sm *StorageManager) SavePage(page *Page) error {
	return sm.bufferPool.ReleasePage(page.ID, page.dirty)
}

// AllocatePage allocates a new page of the specified type
func (sm *StorageManager) AllocatePage(pageType PageType) (*Page, error) {
	sm.freeListMutex.Lock()
	defer sm.freeListMutex.Unlock()

	// Check if we have a free page of the requested type
	freeList, exists := sm.freeLists[uint32(pageType)]
	if exists && len(freeList) > 0 {
		// Reuse a page from the free list
		pageID := freeList[len(freeList)-1]
		sm.freeLists[uint32(pageType)] = freeList[:len(freeList)-1]

		// Save metadata to persist free list changes
		if err := sm.saveMetadata(); err != nil {
			return nil, NewStorageError("save metadata", err)
		}

		// Create a new page with the allocated ID
		return NewPage(sm, pageType, pageID)
	}

	// No free pages, allocate a new one
	newPageID := sm.maxPageID + 1
	sm.maxPageID = newPageID
	sm.pageCount = newPageID + 1

	// Save metadata to persist changes
	if err := sm.saveMetadata(); err != nil {
		return nil, NewStorageError("save metadata", err)
	}

	// Create a new page with the allocated ID
	return NewPage(sm, pageType, newPageID)
}

// FreePage marks a page as free for future reuse
func (sm *StorageManager) FreePage(pageID uint32, pageType PageType) error {
	sm.freeListMutex.Lock()
	defer sm.freeListMutex.Unlock()

	// Add page to the appropriate free list
	sm.freeLists[uint32(pageType)] = append(sm.freeLists[uint32(pageType)], pageID)

	// Save metadata to persist free list changes
	return sm.saveMetadata()
}

// loadMetadata loads database metadata from disk
func (sm *StorageManager) loadMetadata() error {
	file, err := os.Open(sm.metadataFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read basic metadata
	var maxPageID, pageCount, numFreeLists uint32
	if err := binary.Read(file, binary.LittleEndian, &maxPageID); err != nil {
		return NewStorageError("read maxPageID", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &pageCount); err != nil {
		return NewStorageError("read pageCount", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &numFreeLists); err != nil {
		return NewStorageError("read numFreeLists", err)
	}

	sm.maxPageID = maxPageID
	sm.pageCount = pageCount

	// Read free lists
	for i := uint32(0); i < numFreeLists; i++ {
		var pageType, count uint32
		if err := binary.Read(file, binary.LittleEndian, &pageType); err != nil {
			return NewStorageError("read pageType", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &count); err != nil {
			return NewStorageError("read free list count", err)
		}

		freeList := make([]uint32, count)
		for j := uint32(0); j < count; j++ {
			var pageID uint32
			if err := binary.Read(file, binary.LittleEndian, &pageID); err != nil {
				return NewStorageError("read free page ID", err)
			}
			freeList[j] = pageID
		}

		sm.freeLists[pageType] = freeList
	}

	return nil
}

// saveMetadata saves database metadata to disk
func (sm *StorageManager) saveMetadata() error {
	file, err := os.OpenFile(sm.metadataFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, FileMode0664)
	if err != nil {
		return NewStorageError("open metadata file", err)
	}
	defer file.Close()

	// Write basic metadata
	if err := binary.Write(file, binary.LittleEndian, sm.maxPageID); err != nil {
		return NewStorageError("write maxPageID", err)
	}
	if err := binary.Write(file, binary.LittleEndian, sm.pageCount); err != nil {
		return NewStorageError("write pageCount", err)
	}

	// Write free lists
	numFreeLists := uint32(len(sm.freeLists))
	if err := binary.Write(file, binary.LittleEndian, numFreeLists); err != nil {
		return NewStorageError("write numFreeLists", err)
	}

	for pageType, freeList := range sm.freeLists {
		if err := binary.Write(file, binary.LittleEndian, pageType); err != nil {
			return NewStorageError("write pageType", err)
		}

		count := uint32(len(freeList))
		if err := binary.Write(file, binary.LittleEndian, count); err != nil {
			return NewStorageError("write free list count", err)
		}

		for _, pageID := range freeList {
			if err := binary.Write(file, binary.LittleEndian, pageID); err != nil {
				return NewStorageError("write free page ID", err)
			}
		}
	}

	return nil
}

// Close closes all open segment files
func (sm *StorageManager) Close() error {
	// Flush the buffer pool
	if err := sm.bufferPool.FlushAllPages(); err != nil {
		return err
	}

	sm.segmentMutex.Lock()
	defer sm.segmentMutex.Unlock()

	for _, file := range sm.segmentFiles {
		if err := file.Close(); err != nil {
			return NewStorageError("close segment file", err)
		}
	}

	// Clear segment file cache
	sm.segmentFiles = make(map[uint32]*os.File)

	return sm.saveMetadata()
}

// GetPageCount returns the total number of pages
func (sm *StorageManager) GetPageCount() uint32 {
	return sm.pageCount
}

// GetMaxPageID returns the highest page ID allocated
func (sm *StorageManager) GetMaxPageID() uint32 {
	return sm.maxPageID
}

// FlushSegment ensures all pages in a segment are written to disk
func (sm *StorageManager) FlushSegment(segmentID uint32) error {
	sm.segmentMutex.RLock()
	file, exists := sm.segmentFiles[segmentID]
	sm.segmentMutex.RUnlock()

	if !exists || file == nil {
		return nil // Nothing to flush
	}

	sm.segmentMutex.Lock()
	defer sm.segmentMutex.Unlock()

	return file.Sync()
}

// FlushAll ensures all segments are written to disk
func (sm *StorageManager) FlushAll() error {
	// First flush the buffer pool
	if err := sm.bufferPool.FlushAllPages(); err != nil {
		return err
	}

	sm.segmentMutex.RLock()
	defer sm.segmentMutex.RUnlock()

	for _, file := range sm.segmentFiles {
		if err := file.Sync(); err != nil {
			return NewStorageError("sync segment file", err)
		}
	}

	return nil
}
