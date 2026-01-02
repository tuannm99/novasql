package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/btree"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
)

func main() {
	// Nice logs
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	dataDir := "./data/test/schema"
	must(os.MkdirAll(filepath.Join(dataDir, "tables"), 0o755))

	slog.Info("=== MANUAL TEST START ===", "dataDir", dataDir)

	// 1) Open DB
	db := novasql.NewDatabase(dataDir)
	defer func() { _ = db.Close() }()

	// 2) Create schema
	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: false},
			{Name: "name", Type: record.ColText, Nullable: false},
			{Name: "active", Type: record.ColBool, Nullable: false},
		},
	}

	// Clean old run (best-effort)
	_ = db.DropTable("users")

	// 3) Create table
	tbl, err := db.CreateTable("users", schema)
	must(err)
	defer func() { _ = tbl.Close() }()

	// 4) Create index (no backfill by design)
	tree, err := db.CreateBTreeIndex("users", "idx_users_id", "id")
	must(err)
	defer func() { _ = tree.Close() }()

	// ------------------------------------------------------------
	// CASE A: Insert heap + insert index, then SearchEqual
	// ------------------------------------------------------------
	slog.Info("=== CASE A: insert + index search ===")
	tids := make(map[int64]heap.TID)

	for i := int64(1); i <= 10; i++ {
		tid, err := tbl.Insert([]any{i, fmt.Sprintf("user-%d", i), i%2 == 0})
		must(err)
		tids[i] = tid

		// IMPORTANT: currently executor/planner chưa sync tự động -> manual insert index
		must(tree.Insert(btree.KeyType(i), tid))
	}

	// Flush all dirty pages (global pool)
	must(db.FlushAllPools())

	// Search key 7
	gotTIDs, err := tree.SearchEqual(7)
	must(err)
	mustf(len(gotTIDs) == 1, "expected 1 tid for key=7, got=%d", len(gotTIDs))

	row, err := tbl.Get(gotTIDs[0])
	must(err)
	slog.Info("SearchEqual(7) -> row", "tid", gotTIDs[0], "row", row)

	// ------------------------------------------------------------
	// CASE B: Update row (name dài) để test redirect nhưng TID vẫn dùng được
	// ------------------------------------------------------------
	slog.Info("=== CASE B: update redirect, index should still point to old TID ===")
	tid7 := tids[7]

	longName := "user-7-updated-and-very-very-very-long-to-trigger-redirect-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	must(tbl.Update(tid7, []any{int64(7), longName, true}))
	must(db.FlushAllPools())

	// Index vẫn trả về tid7 (vì không đổi key)
	gotTIDs, err = tree.SearchEqual(7)
	must(err)
	mustf(len(gotTIDs) == 1, "expected 1 tid for key=7 after update, got=%d", len(gotTIDs))

	row, err = tbl.Get(gotTIDs[0])
	must(err)
	slog.Info("After update, SearchEqual(7) -> row", "tid", gotTIDs[0], "name", row[1])

	// ------------------------------------------------------------
	// CASE C: Delete heap row nhưng KHÔNG xoá index entry -> show inconsistency
	// ------------------------------------------------------------
	slog.Info("=== CASE C: delete heap only (intentionally), index becomes dangling ===")
	tid5 := tids[5]
	must(tbl.Delete(tid5))
	must(db.FlushAllPools())

	gotTIDs, err = tree.SearchEqual(5)
	must(err)
	slog.Info("SearchEqual(5) after heap delete", "numTIDs", len(gotTIDs), "tids", gotTIDs)

	if len(gotTIDs) > 0 {
		_, err = tbl.Get(gotTIDs[0])
		if err == nil {
			slog.Error("Expected tbl.Get to fail for deleted row but it succeeded (check delete semantics)")
		} else {
			// Depending on your Page.ReadTuple behavior this might be ErrBadSlot or something else
			slog.Info("As expected: index returns dangling TID after heap delete",
				"tid", gotTIDs[0],
				"err", err,
			)
		}
	}

	// ------------------------------------------------------------
	// CASE D: Close + reopen DB, then OpenTable + OpenBTreeIndex and re-check
	// ------------------------------------------------------------
	slog.Info("=== CASE D: reopen and re-check persisted data/index meta ===")

	// Close handles
	must(tbl.Close())
	must(tree.Close())
	must(db.Close())

	// Reopen
	db2 := novasql.NewDatabase(dataDir)
	defer func() { _ = db2.Close() }()

	tbl2, err := db2.OpenTable("users")
	must(err)
	defer func() { _ = tbl2.Close() }()

	tree2, err := db2.OpenBTreeIndex("users", "idx_users_id")
	must(err)
	defer func() { _ = tree2.Close() }()

	// Key 7 should still be readable with updated long name
	gotTIDs, err = tree2.SearchEqual(7)
	must(err)
	mustf(len(gotTIDs) == 1, "expected 1 tid for key=7 after reopen, got=%d", len(gotTIDs))

	row, err = tbl2.Get(gotTIDs[0])
	must(err)
	slog.Info("After reopen, SearchEqual(7) -> row", "tid", gotTIDs[0], "name", row[1])

	// Key 5: index still returns tid, but heap row is deleted (dangling)
	gotTIDs, err = tree2.SearchEqual(5)
	must(err)
	if len(gotTIDs) > 0 {
		_, err = tbl2.Get(gotTIDs[0])
		if err != nil {
			slog.Info("After reopen: still dangling index TID for deleted row (expected until executor sync exists)",
				"tid", gotTIDs[0],
				"err", err,
			)
		}
	}

	slog.Info("=== MANUAL TEST DONE ===")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func mustf(ok bool, format string, args ...any) {
	if !ok {
		panic(fmt.Sprintf(format, args...))
	}
}
