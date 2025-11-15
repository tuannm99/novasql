package main

import (
	"fmt"

	"github.com/tuannm99/novasql/internal/engine"
	"github.com/tuannm99/novasql/internal/storage"
)

func main() {
	db := engine.NewDatabase("./basedir")

	schema := storage.Schema{
		Cols: []storage.Column{
			{Name: "id", Type: storage.ColInt64, Nullable: false},
			{Name: "name", Type: storage.ColText, Nullable: false},
		},
	}

	tbl, _ := db.CreateTable("users", schema)
	tid, _ := tbl.Insert([]any{int64(1), "Tuan"})
	row, _ := tbl.Get(tid)
	fmt.Println("row:", row)
}
