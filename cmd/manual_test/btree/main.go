package main

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/tuannm99/novasql/internal/btree"
	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/engine"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

func main() {
	dataDir := filepath.Join("data", "test", "btree_db")

	db := engine.NewDatabase(dataDir)
	defer db.Close()

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: false},
			{Name: "name", Type: record.ColText, Nullable: false},
			{Name: "active", Type: record.ColBool, Nullable: false},
		},
	}

	tbl, err := db.CreateTable("users", schema)
	if err != nil {
		log.Fatalf("CreateTable: %v", err)
	}

	// Index file
	idxFS := storage.LocalFileSet{
		Dir:  filepath.Join(dataDir, "indexes"),
		Base: "users_id_idx",
	}
	idxBP := bufferpool.NewPool(db.SM, idxFS, bufferpool.DefaultCapacity)
	idx := btree.NewTree(db.SM, idxFS, idxBP)

	// Insert rows + index
	for i := 1; i <= 10; i++ {
		tid, err := tbl.Insert([]any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0,
		})
		if err != nil {
			log.Fatalf("Insert: %v", err)
		}
		if err := idx.Insert(int64(i), tid); err != nil {
			log.Fatalf("Index.Insert: %v", err)
		}
	}

	_ = tbl.BP.FlushAll()
	_ = idxBP.FlushAll()

	fmt.Println("Search id=7 via index:")
	tids, err := idx.SearchEqual(7)
	if err != nil {
		log.Fatalf("SearchEqual: %v", err)
	}
	for _, tid := range tids {
		row, err := tbl.Get(tid)
		if err != nil {
			log.Fatalf("Get: %v", err)
		}
		fmt.Printf("TID=%+v row=%#v\n", tid, row)
	}
}
