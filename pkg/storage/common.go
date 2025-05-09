package storage

import "fmt"

const (
	OneB  = 1
	OneKB = 1024
	OneMB = OneKB * 1024
	OneGB = OneMB * 1024
)

const (
	// 8KB page size, similar to PostgreSQL
	PageSize   = OneKB * 8
	CanCompact = 0x01
)

const (
	// We split database to multiple Segment, each Segment contain multiple Pages
	// 1GB per Segment -> 1 Segment can get up to 1Gb / 8Kb = 131072 pages
	SegmentSize  = 1 * 1024 * 1024 * 1024
	FileMode0644 = 0o644 // rw-r--r--
	FileMode0664 = 0o664 // rw-rw-r--
	FileMode0755 = 0o755 // rwxr-xr-x
)

// PageType enum
type PageType uint8

const (
	Slotted PageType = iota + 1
	Directory
	BTree
	Overflow
)

// Common errors
var (
	ErrPageNotFound     = fmt.Errorf("page not found")
	ErrPageFull         = fmt.Errorf("page is full")
	ErrPageCorrupted    = fmt.Errorf("page is corrupted")
	ErrBufferPoolFull   = fmt.Errorf("buffer pool is full")
	ErrStorageIO        = fmt.Errorf("storage I/O error")
	ErrInvalidOperation = fmt.Errorf("invalid operation")
	ErrNoFreeBuffer     = fmt.Errorf("no free buffer available")
)

// StorageError represents an error in the storage system
type StorageError struct {
	Op  string // Operation that failed
	Err error  // The underlying error
}

func (e *StorageError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Op, e.Err)
	}
	return e.Op
}

func (e *StorageError) Unwrap() error {
	return e.Err
}

// NewStorageError creates a new storage error
func NewStorageError(op string, err error) *StorageError {
	return &StorageError{
		Op:  op,
		Err: err,
	}
}
