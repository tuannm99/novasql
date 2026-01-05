package novasqlwire

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/sql/executor"
)

type ServerConfig struct {
	Addr    string
	Workdir string
	CfgPath string
}

func Run(sc ServerConfig) error {
	ln, err := net.Listen("tcp", sc.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer func() { _ = ln.Close() }()

	log.Printf("novasql tcp server listening on %s (workdir=%s)", sc.Addr, sc.Workdir)

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
		go handleConn(ctx, conn, sc.Workdir)
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

		var req ExecuteRequest
		if err := ReadFrame(conn, &req); err != nil {
			// Client closed or bad frame.
			return
		}

		res, err := executor.ExecSQL(req.SQL)
		if err != nil {
			_ = WriteFrame(conn, ExecuteResponse{
				ID:    req.ID,
				Error: err.Error(),
			})
			continue
		}

		_ = WriteFrame(conn, ExecuteResponse{
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
