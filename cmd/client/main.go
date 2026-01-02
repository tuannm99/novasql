package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tuannm99/novasql/internal/sql/executor"
	"github.com/tuannm99/novasql/sqlclient"
)

func main() {
	var (
		addr    = flag.String("addr", envOr("NOVASQL_ADDR", "127.0.0.1:8866"), "server address host:port")
		timeout = flag.Duration("timeout", 5*time.Second, "dial timeout")
		execSQL = flag.String("e", "", "execute one statement and exit")
		file    = flag.String("f", "", "execute statements from file and exit")
		noRepl  = flag.Bool("no-repl", false, "do not start interactive repl (use with -e or -f)")
		autoSem = flag.Bool("auto-semicolon", true, "auto append ';' when missing (client-side convenience)")
	)
	flag.Parse()

	c, err := sqlclient.Dial(*addr, *timeout)
	if err != nil {
		fatalf("dial %s: %v", *addr, err)
	}
	defer func() { _ = c.Close() }()

	// -e
	if strings.TrimSpace(*execSQL) != "" {
		stmt := strings.TrimSpace(*execSQL)
		if *autoSem {
			stmt = ensureSemicolon(stmt)
		}
		if err := runOne(c, stmt); err != nil {
			fatalf("%v", err)
		}
		return
	}

	// -f
	if strings.TrimSpace(*file) != "" {
		b, err := os.ReadFile(*file)
		if err != nil {
			fatalf("read file: %v", err)
		}
		sqls := splitStatements(string(b))
		for _, s := range sqls {
			stmt := strings.TrimSpace(s)
			if stmt == "" {
				continue
			}
			if *autoSem {
				stmt = ensureSemicolon(stmt)
			}
			if err := runOne(c, stmt); err != nil {
				fatalf("%v", err)
			}
		}
		return
	}

	if *noRepl {
		return
	}

	// Interactive REPL
	fmt.Printf("novasql> connected to %s\n", *addr)
	fmt.Println("Type SQL ending with ';'. Commands: \\q, \\help")
	repl(c, *autoSem)
}

func repl(c *sqlclient.Client, autoSemicolon bool) {
	in := bufio.NewReader(os.Stdin)
	var buf strings.Builder

	for {
		if buf.Len() == 0 {
			fmt.Print("novasql> ")
		} else {
			fmt.Print("...> ")
		}

		line, err := in.ReadString('\n')
		if err != nil {
			fmt.Println()
			return
		}
		line = strings.TrimRight(line, "\r\n")

		trim := strings.TrimSpace(line)
		if buf.Len() == 0 && strings.HasPrefix(trim, `\`) {
			switch trim {
			case `\q`, `\quit`, `\exit`:
				return
			case `\help`:
				fmt.Println("Commands:")
				fmt.Println("  \\q | \\quit | \\exit  Quit")
				fmt.Println("  \\help              Show this help")
				continue
			default:
				fmt.Printf("unknown command: %s\n", trim)
				continue
			}
		}

		buf.WriteString(line)
		buf.WriteString("\n")

		// If user ended statement
		if strings.Contains(line, ";") {
			stmt := strings.TrimSpace(buf.String())

			// take first statement until first ';' for now (simple)
			one, rest := takeFirstStatement(stmt)
			if strings.TrimSpace(one) != "" {
				if autoSemicolon {
					one = ensureSemicolon(one)
				}
				if err := runOne(c, one); err != nil {
					fmt.Printf("error: %v\n", err)
				}
			}

			buf.Reset()
			if strings.TrimSpace(rest) != "" {
				// if user pasted multiple statements, execute the rest as batch
				more := splitStatements(rest)
				for _, s := range more {
					s = strings.TrimSpace(s)
					if s == "" {
						continue
					}
					if autoSemicolon {
						s = ensureSemicolon(s)
					}
					if err := runOne(c, s); err != nil {
						fmt.Printf("error: %v\n", err)
						break
					}
				}
			}
		}
	}
}

func runOne(c *sqlclient.Client, sql string) error {
	res, err := c.Exec(sql)
	if err != nil {
		return fmt.Errorf("%s -> %w", oneLine(sql), err)
	}
	printResult(res)
	return nil
}

func printResult(res *executor.Result) {
	// DML
	if len(res.Columns) == 0 {
		fmt.Printf("OK (%d rows)\n", res.AffectedRows)
		return
	}

	// SELECT
	rows := res.Rows
	cols := res.Columns

	// compute widths
	w := make([]int, len(cols))
	for i := range cols {
		w[i] = len(cols[i])
	}
	for _, r := range rows {
		for i := range cols {
			s := fmtCell(cellAt(r, i))
			if len(s) > w[i] {
				w[i] = len(s)
			}
		}
	}

	sep := func() {
		fmt.Print("+")
		for i := range cols {
			fmt.Print(strings.Repeat("-", w[i]+2))
			fmt.Print("+")
		}
		fmt.Println()
	}

	sep()
	fmt.Print("|")
	for i := range cols {
		fmt.Printf(" %-*s |", w[i], cols[i])
	}
	fmt.Println()
	sep()

	for _, r := range rows {
		fmt.Print("|")
		for i := range cols {
			fmt.Printf(" %-*s |", w[i], fmtCell(cellAt(r, i)))
		}
		fmt.Println()
	}
	sep()
	fmt.Printf("(%d rows)\n", len(rows))
}

func cellAt(row []any, i int) any {
	if i < 0 || i >= len(row) {
		return nil
	}
	return row[i]
}

func fmtCell(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func ensureSemicolon(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	if strings.HasSuffix(s, ";") {
		return s
	}
	return s + ";"
}

// splitStatements: split by ';' (naive, no quote escaping)
func splitStatements(s string) []string {
	parts := strings.Split(s, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p+";")
	}
	return out
}

// takeFirstStatement returns (firstStmtIncludingSemicolon, restAfterFirst)
func takeFirstStatement(s string) (string, string) {
	idx := strings.Index(s, ";")
	if idx < 0 {
		return s, ""
	}
	first := strings.TrimSpace(s[:idx+1])
	rest := strings.TrimSpace(s[idx+1:])
	return first, rest
}

func oneLine(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return s
}

func envOr(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
