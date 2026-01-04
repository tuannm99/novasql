package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chzyer/readline"
	"github.com/tuannm99/novasql/internal/sql/executor"
	sqlwire "github.com/tuannm99/novasql/server/novasqlwire"
)

// ---- TCP client (sync) ----

type Client struct {
	conn net.Conn
	mu   sync.Mutex
	id   atomic.Uint64
}

func Dial(addr string, timeout time.Duration) (*Client, error) {
	d := net.Dialer{Timeout: timeout}
	c, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: c}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Exec(sql string) (*executor.Result, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("sqlclient: nil client")
	}

	reqID := c.id.Add(1)

	c.mu.Lock()
	defer c.mu.Unlock()

	req := sqlwire.ExecuteRequest{ID: reqID, SQL: sql}
	if err := sqlwire.WriteFrame(c.conn, req); err != nil {
		return nil, err
	}

	var resp sqlwire.ExecuteResponse
	if err := sqlwire.ReadFrame(c.conn, &resp); err != nil {
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

// ---- History (own file) ----

type History struct {
	path  string
	lines []string
}

func NewHistory(path string) *History {
	return &History{path: path}
}

func (h *History) Load(max int) error {
	if h.path == "" {
		return nil
	}
	f, err := os.Open(h.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		s := strings.TrimSpace(sc.Text())
		if s == "" {
			continue
		}
		h.lines = append(h.lines, s)
		if max > 0 && len(h.lines) > max {
			h.lines = h.lines[len(h.lines)-max:]
		}
	}
	return sc.Err()
}

func (h *History) Append(stmt string) error {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" || h.path == "" {
		return nil
	}

	// store single-line; collapse whitespace/newlines
	stmt = compactOneLine(stmt)

	// ensure dir exists
	if err := os.MkdirAll(filepath.Dir(h.path), 0o755); err != nil {
		return err
	}

	f, err := os.OpenFile(h.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := fmt.Fprintln(f, stmt); err != nil {
		return err
	}
	h.lines = append(h.lines, stmt)
	return nil
}

func (h *History) Print(last int) {
	if last <= 0 || last > len(h.lines) {
		last = len(h.lines)
	}
	start := len(h.lines) - last
	if start < 0 {
		start = 0
	}
	for i := start; i < len(h.lines); i++ {
		fmt.Printf("%5d  %s\n", i+1, h.lines[i])
	}
}

func compactOneLine(s string) string {
	// replace newlines/tabs with spaces, then collapse multiple spaces
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.TrimSpace(s)

	var b strings.Builder
	b.Grow(len(s))
	space := false
	for _, r := range s {
		if r == ' ' {
			if !space {
				b.WriteByte(' ')
				space = true
			}
			continue
		}
		space = false
		b.WriteRune(r)
	}
	return b.String()
}

// ---- REPL helpers ----

// statementComplete checks if we have a terminating ';' outside single quotes.
func statementComplete(buf string) bool {
	inQuote := false
	escaped := false

	for _, r := range buf {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if r == '\'' {
			inQuote = !inQuote
			continue
		}
		if r == ';' && !inQuote {
			return true
		}
	}
	return false
}

func normalizeStmt(buf string) string {
	// keep original semicolon requirement as parser wants
	// but trim leading/trailing whitespace
	return strings.TrimSpace(buf)
}

func isMetaCommand(line string) bool {
	line = strings.TrimSpace(line)
	return strings.HasPrefix(line, "\\") ||
		line == "quit" || line == "exit"
}

func printResult(res *executor.Result) {
	if len(res.Columns) == 0 {
		// DDL/DML
		fmt.Printf("OK (%d affected)\n", res.AffectedRows)
		return
	}

	cols := res.Columns
	rows := res.Rows

	// 1) compute widths
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	for _, row := range rows {
		for i := range cols {
			var s string
			if i < len(row) && row[i] != nil {
				s = fmt.Sprintf("%v", row[i])
			} else {
				s = "NULL"
			}
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
	}

	// helper to print a row
	printRow := func(values []string) {
		for i := range cols {
			if i > 0 {
				fmt.Print(" | ")
			}
			fmt.Print(padRight(values[i], widths[i]))
		}
		fmt.Println()
	}

	// 2) header
	hdr := make([]string, len(cols))
	copy(hdr, cols)
	printRow(hdr)

	// 3) separator ----+----
	for i := range cols {
		if i > 0 {
			fmt.Print("-+-")
		}
		fmt.Print(strings.Repeat("-", widths[i]))
	}
	fmt.Println()

	// 4) rows
	for _, row := range rows {
		out := make([]string, len(cols))
		for i := range cols {
			if i < len(row) && row[i] != nil {
				out[i] = fmt.Sprintf("%v", row[i])
			} else {
				out[i] = "NULL"
			}
		}
		printRow(out)
	}

	fmt.Printf("(%d rows)\n", res.AffectedRows)
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}

func defaultHistoryPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".novasql_history"
	}
	return filepath.Join(home, ".novasql_history")
}

func main() {
	var (
		addr       = flag.String("addr", "127.0.0.1:8866", "server address")
		timeout    = flag.Duration("timeout", 3*time.Second, "dial timeout")
		histPath   = flag.String("history", defaultHistoryPath(), "history file path")
		histMax    = flag.Int("history-max", 2000, "max history lines loaded into memory")
		oneShotSQL = flag.String("c", "", "execute one SQL and exit (must end with ';')")
	)
	flag.Parse()

	cli, err := Dial(*addr, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = cli.Close() }()

	// one-shot mode
	if strings.TrimSpace(*oneShotSQL) != "" {
		res, err := cli.Exec(*oneShotSQL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		printResult(res)
		return
	}

	h := NewHistory(*histPath)
	_ = h.Load(*histMax)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "novasql> ",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "readline: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = rl.Close() }()

	// preload history into readline (so ↑ works immediately)
	for _, line := range h.lines {
		_ = rl.SaveHistory(line) // add to in-memory history
	}

	var buf strings.Builder

	fmt.Printf("connected to %s\n", *addr)
	fmt.Println("type \\help for help")

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			// Ctrl+C clears current buffer
			if buf.Len() > 0 {
				buf.Reset()
				rl.SetPrompt("novasql> ")
				continue
			}
			fmt.Println("^C")
			continue
		}
		if err != nil {
			// EOF
			fmt.Println()
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// meta commands
		if isMetaCommand(line) {
			switch line {
			case "\\q", "quit", "exit":
				return
			case "\\help":
				fmt.Println(`meta commands:
  \q | quit | exit       quit
  \history               print history
  \help                  show help

sql:
  end statement with ';' (parser requires it)
  multiline is supported (CLI will wait until ';')`)
			case "\\history":
				h.Print(50)
			default:
				fmt.Printf("unknown command: %s\n", line)
			}
			continue
		}

		// accumulate sql
		if buf.Len() > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(line)

		if !statementComplete(buf.String()) {
			rl.SetPrompt("...> ")
			continue
		}

		stmt := normalizeStmt(buf.String())
		buf.Reset()
		rl.SetPrompt("novasql> ")

		// persist history by executed statement
		_ = h.Append(stmt)
		_ = rl.SaveHistory(compactOneLine(stmt))

		res, err := cli.Exec(stmt)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		printResult(res)
	}
}
