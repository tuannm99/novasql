package locking

// used for pin/unpin pages
// when a page was unpin we can flush page from memory to the disk

import (
	"fmt"
	"sync/atomic"
)

type RefCount struct {
	count int32
}

func NewRefCount() *RefCount {
	return &RefCount{count: 1}
}

func (r *RefCount) Inc() {
	atomic.AddInt32(&r.count, 1)
}

func (r *RefCount) Dec() bool {
	newCount := atomic.AddInt32(&r.count, -1)
	if newCount < 0 {
		panic("refcount dropped below zero")
	}
	return newCount == 0
}

func (r *RefCount) Get() int32 {
	return atomic.LoadInt32(&r.count)
}

func (r *RefCount) String() string {
	return fmt.Sprintf("RefCount: %d", r.Get())
}
