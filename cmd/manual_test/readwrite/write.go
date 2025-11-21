package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
)

func main() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))

	// Data directory for this manual test.
	dataDir := filepath.Join("data/test", "manual_db")
	_ = os.RemoveAll(dataDir)

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	db := novasql.NewDatabase(dataDir)
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

	fmt.Println("Inserting normal rows...")
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

	// Each "Tuan" = 4 bytes; 7000 * 4  > 8KB page payload.
	largeStr := strings.Repeat("Tuan", 7000)
	slog.Info("building large row",
		"len_large_str", len(largeStr),
	)

	// Insert a big row that must go through OverflowManager.
	largeTID, err := tbl.Insert([]any{
		int64(11),
		largeStr,
		true,
	})
	if err != nil {
		log.Fatalf("Insert large row: %v", err)
	}
	slog.Info("inserted large row",
		"tid", largeTID,
		"len_large_str", len(largeStr),
	)

	fmt.Println("Scan after CRUD (in writer process):")
	err = tbl.Scan(func(id heap.TID, row []any) error {
		// Defensive: avoid panic if row has unexpected layout.
		var (
			idVal     any
			nameVal   any
			activeVal any
		)

		if len(row) > 0 {
			idVal = row[0]
		}
		if len(row) > 1 {
			nameVal = row[1]
		}
		if len(row) > 2 {
			activeVal = row[2]
		}

		// Try to cast safely
		var (
			idInt        int64
			idOK         bool
			nameStr      string
			nameIsString bool
			nameLen      int
			preview      string
			activeBool   bool
			activeOK     bool
		)

		if idVal != nil {
			idInt, idOK = idVal.(int64)
			slog.Info("idInt", idInt)
		}
		if nameVal != nil {
			if s, ok := nameVal.(string); ok {
				nameIsString = true
				nameStr = s
				nameLen = len(s)
				preview = nameStr
				if len(preview) > 50 {
					preview = preview[:50] + "..."
				}
			}
		}
		if activeVal != nil {
			activeBool, activeOK = activeVal.(bool)
			slog.Info("activeBool", activeBool)
		}

		// Log raw row nếu có gì bất thường để debug overflow
		if !idOK || !nameIsString || !activeOK {
			slog.Warn("unexpected row layout",
				"tid", id,
				"row", row,
				"id_ok", idOK,
				"name_is_string", nameIsString,
				"active_ok", activeOK,
			)
		}

		fmt.Printf("TID=%+v id=%v name_len=%d name_preview=%q active=%v\n",
			id,
			idVal,
			nameLen,
			preview,
			activeVal,
		)

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
