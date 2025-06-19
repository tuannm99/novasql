package main

import (
	"flag"
	"log"
	"os"

	"github.com/tuannm99/novasql/internal/storage"
)

func main() {
	
	workDir := flag.String("data-dir", "./data", "Working directory for database files")
	flag.Parse()

	
	if err := os.MkdirAll(*workDir, storage.FileMode0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	select {}
}
