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

	_, err := exec.ExecSQL(`CREATE TABLE users_sql (id INT, name TEXT)`)
	if err != nil {
		log.Fatalf("create table: %v", err)
	}

	_, err = exec.ExecSQL(`INSERT INTO users_sql VALUES (1, 'user-1')`)
	if err != nil {
		log.Fatalf("insert: %v", err)
	}

	res, err := exec.ExecSQL(`SELECT * FROM users_sql`)
	if err != nil {
		log.Fatalf("select: %v", err)
	}

	fmt.Println("Columns:", res.Columns)
	for _, row := range res.Rows {
		fmt.Println("Row:", row)
	}
}
