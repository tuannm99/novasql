package main

import (
	"flag"
	"log"
	"os"

	"github.com/tuannm99/novasql/internal"
	"github.com/tuannm99/novasql/internal/storage/common"
)

func main() {
	cfg, err := internal.LoadConfig("novasql.yaml")
	if err != nil {
		panic(err)
	}

	workDir := flag.String("data-dir", "./data", "Working directory for database files")
	flag.Parse()

	if err := os.MkdirAll(*workDir, common.FileMode0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	storageMode, err := common.GetStorageMode(cfg.Storage.Mode)
	if err != nil {
		panic(err)
	}

	_, err = internal.NewDatabase(*workDir, storageMode)
	if err != nil {
		panic(err)
	}

	select {}
}
