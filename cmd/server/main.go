package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/tuannm99/novasql/internal"
	"github.com/tuannm99/novasql/internal/storage"
	"github.com/tuannm99/novasql/server/novasqlwire"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "novasql.yaml", "Path to novasql yaml config")
	flag.Parse()

	cfg, err := internal.LoadConfig(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	addr := os.Getenv("NOVASQL_ADDR")
	if addr == "" {
		// Use config port by default
		port := cfg.Server.Port
		if port == 0 {
			port = 6543
		}
		addr = fmt.Sprintf("127.0.0.1:%d", port)
	}

	workdir := cfg.Storage.Workdir
	if workdir == "" {
		workdir = "./data"
	}

	if err := os.MkdirAll(workdir, storage.FileMode0755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	sc := novasqlwire.ServerConfig{
		Addr:    addr,
		Workdir: workdir,
		CfgPath: cfgPath,
	}

	if err := novasqlwire.Run(sc); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
