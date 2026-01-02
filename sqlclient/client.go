package sqlclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tuannm99/novasql/internal/sql/executor"
	"github.com/tuannm99/novasql/server/novasqlwire"
)

// Client is a simple synchronous client.
// It locks send/recv so you can call Exec concurrently but they'll serialize.
// Later you can upgrade to async with a reader goroutine + pending map.
type Client struct {
	conn net.Conn
	mu   sync.Mutex
	id   atomic.Uint64

	// Optional per-request timeout (0 = no timeout).
	rwTimeout time.Duration
}

func Dial(addr string, timeout time.Duration) (*Client, error) {
	d := net.Dialer{Timeout: timeout}
	c, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: c}, nil
}

func DialContext(ctx context.Context, addr string, timeout time.Duration) (*Client, error) {
	d := net.Dialer{Timeout: timeout}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: c}, nil
}

// SetRWTimeout sets a per-Exec read/write deadline.
// Useful to avoid hanging forever if server dies.
func (c *Client) SetRWTimeout(d time.Duration) {
	if c == nil {
		return
	}
	c.rwTimeout = d
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Exec(sql string) (*executor.Result, error) {
	return c.ExecContext(context.Background(), sql)
}

func (c *Client) ExecContext(ctx context.Context, sql string) (*executor.Result, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("sqlclient: nil client")
	}

	reqID := c.id.Add(1)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Apply deadline if configured or context has deadline.
	if err := c.applyDeadline(ctx); err != nil {
		return nil, err
	}
	defer func() {
		// Clear deadline after request so idle connection doesn't expire.
		_ = c.conn.SetDeadline(time.Time{})
	}()

	req := novasqlwire.ExecuteRequest{ID: reqID, SQL: sql}
	if err := novasqlwire.WriteFrame(c.conn, req); err != nil {
		return nil, err
	}

	var resp novasqlwire.ExecuteResponse
	if err := novasqlwire.ReadFrame(c.conn, &resp); err != nil {
		return nil, err
	}

	if resp.ID != reqID {
		return nil, fmt.Errorf("sqlclient: response id mismatch: got=%d want=%d", resp.ID, reqID)
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp.Result, nil
}

func (c *Client) applyDeadline(ctx context.Context) error {
	// Prefer context deadline if present; otherwise use rwTimeout.
	if dl, ok := ctx.Deadline(); ok {
		return c.conn.SetDeadline(dl)
	}
	if c.rwTimeout > 0 {
		return c.conn.SetDeadline(time.Now().Add(c.rwTimeout))
	}
	return nil
}
