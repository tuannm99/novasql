package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/sql/executor"
)

func main() {
	dataDir := filepath.Join("data", "test", "manual_db_sql")
	_ = os.RemoveAll(dataDir)

	db := novasql.NewDatabase(dataDir)
	defer db.Close()

	exec := executor.NewExecutor(db)

	rs, err := exec.ExecSQL(`CREATE TABLE users_sql (id INT, name TEXT)`)
	if err != nil {
		log.Fatalf("create table: %v", err)
	}
	fmt.Printf("❤❤❤ tuannm: [main.go][22][rs]: %+v\n", rs)

	rs, err = exec.ExecSQL(`INSERT INTO users_sql VALUES (1, 'user-1')`)
	if err != nil {
		log.Fatalf("insert: %v", err)
	}
	fmt.Printf("❤❤❤ tuannm: [main.go][22][rs]: %+v\n", rs)

	res, err := exec.ExecSQL(`SELECT * FROM users_sql`)
	if err != nil {
		log.Fatalf("select: %v", err)
	}

	fmt.Println("Columns:", res.Columns)
	for _, row := range res.Rows {
		fmt.Println("Row:", row)
	}
}
