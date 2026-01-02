package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/heap"
)

func main() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))

	dataDir := filepath.Join("data/test", "manual_db")

	db := novasql.NewDatabase(dataDir)
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

	fmt.Println("Try Get on old TID (PageID=0, Slot=0):")
	oldID := heap.TID{PageID: 0, Slot: 0}
	row, err := tbl.Get(oldID)
	if err != nil {
		log.Printf("Get old TID error: %v", err)
	} else {
		fmt.Printf("Old TID=%+v row=%#v\n", oldID, row)
	}
}
