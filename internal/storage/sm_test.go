package storage_test
//
// import (
// 	"testing"
//
// 	"github.com/stretchr/testify/assert"
// 	"github.com/tuannm99/novasql/internal/storage/common"
// )
//
// func TestManager(t *testing.T) {
// 	// tạo SM + FileSet cho quan hệ "12345" trong thư mục "data/base/16384"
// 	sm := NewStorageManager(common.Embedded, "data")
// 	fs := LocalFileSet{Dir: "data/base/16384", Base: "12345"}
//
// 	// Load page
// 	pg, err := sm.LoadPage(fs, 0)
// 	if err != nil { /* ... */
// 	}
// 	if pg.IsUninitialized() {
// 		pg.init(0)
// 	}
//
// 	// Insert
// 	slot, ok := pg.InsertTuple([]byte("hello=world"))
// 	if !ok {
// 		// chuyển page khác
// 	}
//
// 	// Read
// 	data, ok := pg.ReadTuple(slot)
//
// 	// Save
// 	_ = sm.SavePage(fs, 0, pg)
// }
