package main

import (
	"log/slog"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/record"
)

func main() {
	db := novasql.NewDatabase("./data/test/schema")

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: false},
			{Name: "name", Type: record.ColText, Nullable: false},
		},
	}

	tbl, _ := db.CreateTable("users", schema)
	tid, _ := tbl.Insert([]any{int64(1), "Tuan"})
	row, _ := tbl.Get(tid)
	slog.Info("tbl", "row", row)

	tid, _ = tbl.Insert([]any{int64(2), "Tuan2"})
	row, _ = tbl.Get(tid)
	slog.Info("tbl", "row", row)
}
