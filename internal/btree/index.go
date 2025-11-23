package btree

import "github.com/tuannm99/novasql/internal/heap"

// Index is a minimal interface BTree should satisfy to be used by planner/executor.
type Index interface {
	Insert(key KeyType, tid heap.TID) error
	SearchEqual(key KeyType) ([]heap.TID, error)
	RangeScan(minKey, maxKey KeyType) ([]heap.TID, error)
}
