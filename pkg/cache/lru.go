package cache

import (
	"container/list"
	"sync"
)

type LRUManager struct {
	lruList *list.List
	mu      sync.Mutex
}

func NewLRUManager() *LRUManager {
	return &LRUManager{
		lruList: list.New(),
	}
}

func (l *LRUManager) MoveToFront(elem *list.Element) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lruList.MoveToFront(elem)
}

func (l *LRUManager) Remove(elem *list.Element) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lruList.Remove(elem)
}

func (l *LRUManager) PushFront(value interface{}) *list.Element {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lruList.PushFront(value)
}

func (l *LRUManager) Back() *list.Element {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lruList.Back()
}

func (l *LRUManager) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lruList.Len()
}
