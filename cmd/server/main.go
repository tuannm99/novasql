package main

import (
	"flag"
	"log"
	"os"

	"github.com/tuannm99/novasql/internal/storage"
)

func main() {
	// Parse command line arguments
	workDir := flag.String("data-dir", "./data", "Working directory for database files")
	flag.Parse()

	// Create directory if it doesn't exist
	if err := os.MkdirAll(*workDir, storage.FileMode0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	select {}
}
