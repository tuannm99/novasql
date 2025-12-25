package btree

import "fmt"

// ErrOutOfOrderInsert was originally used at the leaf level to enforce that
// keys are inserted in non-decreasing order. The V2 implementation enforces
// monotonic inserts at the Tree level instead, so leaves are free to accept
// keys in any order.
var ErrOutOfOrderInsert = fmt.Errorf("btree: keys must be inserted in non-decreasing order")
