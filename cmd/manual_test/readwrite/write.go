package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/tuannm99/novasql/internal/engine"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
)

func main() {
	// Data directory for this manual test.
	dataDir := filepath.Join("data/test", "manual_db")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	db := engine.NewDatabase(dataDir)
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("db close error: %v", err)
		}
	}()

	// Define a simple schema: (id INT64, name TEXT, active BOOL)
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: false},
			{Name: "name", Type: record.ColText, Nullable: false},
			{Name: "active", Type: record.ColBool, Nullable: false},
		},
	}

	// Create a table "users".
	tbl, err := db.CreateTable("users", schema)
	if err != nil {
		log.Fatalf("CreateTable: %v", err)
	}

	fmt.Println("Inserting rows...")
	var ids []heap.TID
	for i := 1; i <= 10; i++ {
		row := []any{
			int64(i),
			fmt.Sprintf("user-%d", i),
			i%2 == 0, // active = true for even IDs
		}
		tid, err := tbl.Insert(row)
		if err != nil {
			log.Fatalf("Insert row %d: %v", i, err)
		}
		ids = append(ids, tid)
	}

	// Do a couple of updates.
	fmt.Println("Updating some rows...")
	if len(ids) >= 2 {
		// Update first row's name
		err = tbl.Update(ids[0], []any{
			int64(1),
			"user-1-updated",
			true,
		})
		if err != nil {
			log.Fatalf("Update row 1: %v", err)
		}

		// Update second row to inactive
		err = tbl.Update(ids[1], []any{
			int64(2),
			"user-2",
			false,
		})
		if err != nil {
			log.Fatalf("Update row 2: %v", err)
		}
	}

	// Delete one row.
	fmt.Println("Deleting row with id=5...")
	if len(ids) >= 5 {
		if err := tbl.Delete(ids[4]); err != nil {
			log.Fatalf("Delete row 5: %v", err)
		}
	}

	fmt.Println("Scan after CRUD (in writer process):")
	err = tbl.Scan(func(id heap.TID, row []any) error {
		fmt.Printf("TID=%+v row=%#v\n", id, row)
		return nil
	})
	if err != nil {
		log.Fatalf("Scan: %v", err)
	}

	// Ensure all dirty pages are persisted to disk.
	if err := tbl.Flush(); err != nil {
		log.Fatalf("Flush: %v", err)
	}

	fmt.Println("Writer finished. Now you can run the reader script.")
}
