package main

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/tuannm99/novasql/internal/engine"
	"github.com/tuannm99/novasql/internal/heap"
)

func main() {
	dataDir := filepath.Join("data/test", "manual_db")

	db := engine.NewDatabase(dataDir)
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("db close error: %v", err)
		}
	}()

	// Open existing table "users" using engine meta (schema from JSON).
	tbl, err := db.OpenTable("users")
	if err != nil {
		log.Fatalf("OpenTable: %v", err)
	}

	fmt.Println("Scanning users from disk (reader process):")
	err = tbl.Scan(func(id heap.TID, row []any) error {
		fmt.Printf("TID=%+v row=%#v\n", id, row)
		return nil
	})
	if err != nil {
		log.Fatalf("Scan: %v", err)
	}
}
