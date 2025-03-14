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

// Error codes
const (
	ErrCodePageNotFound = iota + 1000
	ErrCodePageFull
	ErrCodePageCorrupted
	ErrCodeBufferPoolFull
	ErrCodeStorageIO
	ErrCodeInvalidOperation
)

// StorageError represents an error in the storage system
type StorageError struct {
	Code    int
	Message string
	Err     error
}

func (e *StorageError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *StorageError) Unwrap() error {
	return e.Err
}

// NewStorageError creates a new storage error
func NewStorageError(code int, message string, err error) *StorageError {
	return &StorageError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}
