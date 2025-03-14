package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tuannm99/novasql/pkg/database"
	"github.com/tuannm99/novasql/pkg/storage"
)

func main() {
	// Parse command line arguments
	workDir := flag.String("data-dir", "./data", "Working directory for database files")
	flag.Parse()

	// Create directory if it doesn't exist
	if err := os.MkdirAll(*workDir, storage.FileMode0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize the database
	db, err := database.New(*workDir)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("Shutting down...")
		db.Close()
		os.Exit(0)
	}()

	fmt.Printf("NovaSQL started with data directory: %s\n", *workDir)
	// TODO: Here we would add server code (e.g., REST API, TCP server for SQL)

	// For now, just keep the server running
	select {}
}
