package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal"
	"github.com/tuannm99/novasql/internal/sql/executor"
	"github.com/tuannm99/novasql/internal/storage"
	"github.com/tuannm99/novasql/server/novasqlwire"
)

type serverConfig struct {
	addr    string
	workdir string
	cfgPath string
}

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

	sc := serverConfig{
		addr:    addr,
		workdir: workdir,
		cfgPath: cfgPath,
	}

	if err := run(sc); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func run(sc serverConfig) error {
	ln, err := net.Listen("tcp", sc.addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer func() { _ = ln.Close() }()

	log.Printf("novasql tcp server listening on %s (workdir=%s)", sc.addr, sc.workdir)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(ctx, conn, sc.workdir)
	}
}

func handleConn(ctx context.Context, conn net.Conn, workdir string) {
	defer func() { _ = conn.Close() }()

	// No global deadline; you can set per-request deadline if needed.
	_ = conn.SetDeadline(time.Time{})

	executor, cleanup := newSessionExecutor(workdir)
	defer func() { _ = cleanup() }()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var req novasqlwire.ExecuteRequest
		if err := novasqlwire.ReadFrame(conn, &req); err != nil {
			// Client closed or bad frame.
			return
		}

		res, err := executor.ExecSQL(req.SQL)
		if err != nil {
			_ = novasqlwire.WriteFrame(conn, novasqlwire.ExecuteResponse{
				ID:    req.ID,
				Error: err.Error(),
			})
			continue
		}

		_ = novasqlwire.WriteFrame(conn, novasqlwire.ExecuteResponse{
			ID:     req.ID,
			Result: res,
		})
	}
}

// newSessionExecutor returns a fresh DB per connection so USE <db> is session-scoped.
func newSessionExecutor(workdir string) (*executor.Executor, func() error) {
	db := novasql.NewDatabase(workdir)
	ex := executor.NewExecutor(db)
	cleanup := func() error { return db.Close() }
	return ex, cleanup
}
