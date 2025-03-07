package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	// We split database to multiple Segment, each Segment contain multiple Pages
	// 1GB per Segment -> 1 Segment can get up to 1Gb / 8Kb = 131072 pages
	SegmentSize  = 1 * 1024 * 1024 * 1024
	FileMode0644 = 0644 // rw-r--r--
	FileMode0664 = 0664 // rw-rw-r--
	FileMode0755 = 0755 // rwxr-xr-x
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
}

// NewStorageManager initializes storage in a directory
func NewStorageManager(dir string) (*StorageManager, error) {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(dir, FileMode0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %v", err)
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
			return nil, fmt.Errorf("failed to load metadata: %v", err)
		}
		// Create initial metadata if file doesn't exist
		if err := sm.saveMetadata(); err != nil {
			return nil, fmt.Errorf("failed to save initial metadata: %v", err)
		}
	}

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
		return nil, fmt.Errorf("failed to open segment file: %v", err)
	}

	sm.segmentFiles[segmentID] = file
	return file, nil
}

// WritePage writes a page to its corresponding segment file
func (sm *StorageManager) WritePage(pageID uint32, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("page must be exactly %d bytes", PageSize)
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
		return fmt.Errorf("seek failed: %v", err)
	}

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("write failed: %v", err)
	}

	// Update page count if necessary
	if pageID >= sm.pageCount {
		sm.pageCount = pageID + 1
		sm.maxPageID = pageID
		err = sm.saveMetadata()
		if err != nil {
			return fmt.Errorf("failed to update metadata: %v", err)
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
		return nil, fmt.Errorf("seek failed: %v", err)
	}

	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read failed: %v", err)
	}

	if n != PageSize && pageID < sm.pageCount {
		return nil, fmt.Errorf("incomplete page read: got %d bytes, expected %d", n, PageSize)
	}

	return data, nil
}

// LoadPage loads a page from disk into memory
func (sm *StorageManager) LoadPage(pageID uint32) (*Page, error) {
	data, err := sm.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	page := &Page{
		ID:    pageID,
		Data:  make([]byte, PageSize-DefaultPageHeaderSize),
		dirty: false,
	}

	err = page.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize page: %v", err)
	}

	return page, nil
}

// SavePage writes a page to disk
func (sm *StorageManager) SavePage(page *Page) error {
	if !page.dirty {
		return nil // Skip if page hasn't changed
	}

	data, err := page.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize page: %v", err)
	}

	err = sm.WritePage(page.ID, data)
	if err != nil {
		return fmt.Errorf("failed to write page: %v", err)
	}

	page.dirty = false
	return nil
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
			return nil, fmt.Errorf("failed to save metadata: %v", err)
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

		return nil, fmt.Errorf("failed to save metadata: %v", err)
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
		return fmt.Errorf("failed to read maxPageID: %v", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &pageCount); err != nil {
		return fmt.Errorf("failed to read pageCount: %v", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &numFreeLists); err != nil {
		return fmt.Errorf("failed to read numFreeLists: %v", err)
	}

	sm.maxPageID = maxPageID
	sm.pageCount = pageCount

	// Read free lists
	for i := uint32(0); i < numFreeLists; i++ {
		var pageType, count uint32
		if err := binary.Read(file, binary.LittleEndian, &pageType); err != nil {
			return fmt.Errorf("failed to read pageType: %v", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &count); err != nil {
			return fmt.Errorf("failed to read free list count: %v", err)
		}

		freeList := make([]uint32, count)
		for j := uint32(0); j < count; j++ {
			var pageID uint32
			if err := binary.Read(file, binary.LittleEndian, &pageID); err != nil {
				return fmt.Errorf("failed to read free page ID: %v", err)
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
		return fmt.Errorf("failed to open metadata file: %v", err)
	}
	defer file.Close()

	// Write basic metadata
	if err := binary.Write(file, binary.LittleEndian, sm.maxPageID); err != nil {
		return fmt.Errorf("failed to write maxPageID: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, sm.pageCount); err != nil {
		return fmt.Errorf("failed to write pageCount: %v", err)
	}

	// Write free lists
	numFreeLists := uint32(len(sm.freeLists))
	if err := binary.Write(file, binary.LittleEndian, numFreeLists); err != nil {
		return fmt.Errorf("failed to write numFreeLists: %v", err)
	}

	for pageType, freeList := range sm.freeLists {
		if err := binary.Write(file, binary.LittleEndian, pageType); err != nil {
			return fmt.Errorf("failed to write pageType: %v", err)
		}

		count := uint32(len(freeList))
		if err := binary.Write(file, binary.LittleEndian, count); err != nil {
			return fmt.Errorf("failed to write free list count: %v", err)
		}

		for _, pageID := range freeList {
			if err := binary.Write(file, binary.LittleEndian, pageID); err != nil {
				return fmt.Errorf("failed to write free page ID: %v", err)
			}
		}
	}

	return nil
}

// Close closes all open segment files
func (sm *StorageManager) Close() error {
	sm.segmentMutex.Lock()
	defer sm.segmentMutex.Unlock()

	for _, file := range sm.segmentFiles {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close segment file: %v", err)
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
	sm.segmentMutex.RLock()
	defer sm.segmentMutex.RUnlock()

	for _, file := range sm.segmentFiles {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync segment file: %v", err)
		}
	}

	return nil
}
