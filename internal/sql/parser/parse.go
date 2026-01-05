package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// parseIdent validates an identifier (db/table/column name) for phase 1.
// Rules (simple):
//   - must be exactly one token (no spaces)
//   - first char: letter or '_'
//   - rest: letter/digit/'_'
func parseIdent(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("missing identifier")
	}

	parts := strings.Fields(s)
	if len(parts) != 1 {
		return "", fmt.Errorf("invalid identifier %q", s)
	}
	id := parts[0]

	for i, r := range id {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return "", fmt.Errorf("invalid identifier %q", id)
			}
			continue
		}

		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return "", fmt.Errorf("invalid identifier %q", id)
		}
	}

	return id, nil
}

// Parse parses a single SQL statement into an AST.
// Policy: statement MUST end with ';'
func Parse(sql string) (Statement, error) {
	s := strings.TrimSpace(sql)
	if s == "" {
		return nil, fmt.Errorf("empty statement")
	}

	// Require ';' at the end (after trimming spaces/newlines)
	if !strings.HasSuffix(s, ";") {
		return nil, fmt.Errorf("missing ';' terminator")
	}

	// Strip the trailing ';' and trim again
	s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	if s == "" {
		return nil, fmt.Errorf("empty statement")
	}

	up := strings.ToUpper(s)

	switch {
	// database
	case strings.HasPrefix(up, "CREATE DATABASE"):
		return parseCreateDatabase(s)
	case strings.HasPrefix(up, "DROP DATABASE"):
		return parseDropDatabase(s)
	case strings.HasPrefix(up, "USE "):
		return parseUseDatabase(s)

	// table
	case strings.HasPrefix(up, "CREATE TABLE"):
		return parseCreateTable(s)
	case strings.HasPrefix(up, "DROP TABLE"):
		return parseDropTable(s)

	case strings.HasPrefix(up, "INSERT INTO"):
		return parseInsert(s)
	case strings.HasPrefix(up, "SELECT"):
		return parseSelect(s)
	case strings.HasPrefix(up, "UPDATE"):
		return parseUpdate(s)
	case strings.HasPrefix(up, "DELETE FROM"):
		return parseDelete(s)

	default:
		return nil, fmt.Errorf("unsupported statement: %q", sql)
	}
}

func parseCreateDatabase(sql string) (Statement, error) {
	rest := strings.TrimSpace(sql[len("CREATE DATABASE"):])
	name, err := parseIdent(rest)
	if err != nil {
		return nil, fmt.Errorf("invalid CREATE DATABASE syntax: %w", err)
	}
	return &CreateDatabaseStmt{Name: name}, nil
}

func parseDropDatabase(sql string) (Statement, error) {
	rest := strings.TrimSpace(sql[len("DROP DATABASE"):])
	name, err := parseIdent(rest)
	if err != nil {
		return nil, fmt.Errorf("invalid DROP DATABASE syntax: %w", err)
	}
	return &DropDatabaseStmt{Name: name}, nil
}

func parseUseDatabase(sql string) (Statement, error) {
	rest := strings.TrimSpace(sql[len("USE "):])
	name, err := parseIdent(rest)
	if err != nil {
		return nil, fmt.Errorf("invalid USE syntax: %w", err)
	}
	return &UseDatabaseStmt{Name: name}, nil
}

func parseCreateTable(sql string) (Statement, error) {
	// Very naive: "CREATE TABLE users (id INT, name TEXT, active BOOL)"
	withoutPrefix := strings.TrimSpace(sql[len("CREATE TABLE"):])
	parts := strings.SplitN(withoutPrefix, "(", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	tableName, err := parseIdent(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax: %w", err)
	}

	defPart := strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")
	defPart = strings.TrimSpace(defPart)
	if defPart == "" {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax: empty column list")
	}

	colDefs := strings.Split(defPart, ",")
	var cols []ColumnDef
	for _, def := range colDefs {
		def = strings.TrimSpace(def)
		toks := strings.Fields(def)
		if len(toks) < 2 {
			return nil, fmt.Errorf("invalid column def: %q", def)
		}

		colName, err := parseIdent(toks[0])
		if err != nil {
			return nil, fmt.Errorf("invalid column name: %w", err)
		}

		cols = append(cols, ColumnDef{
			Name: colName,
			Type: strings.ToUpper(toks[1]),
		})
	}

	return &CreateTableStmt{
		TableName: tableName,
		Columns:   cols,
	}, nil
}

func parseDropTable(sql string) (Statement, error) {
	rest := strings.TrimSpace(sql[len("DROP TABLE"):])
	name, err := parseIdent(rest)
	if err != nil {
		return nil, fmt.Errorf("invalid DROP TABLE syntax: %w", err)
	}
	return &DropTableStmt{TableName: name}, nil
}

func parseInsert(sql string) (Statement, error) {
	// "INSERT INTO users VALUES (1, 'abc', true, null)"
	rest := strings.TrimSpace(sql[len("INSERT INTO"):])

	// Case-insensitive VALUES using splitKeyword.
	tablePart, valPart := splitKeyword(rest, "VALUES")
	if strings.TrimSpace(valPart) == "" {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}

	tableName, err := parseIdent(tablePart)
	if err != nil {
		return nil, fmt.Errorf("invalid INSERT syntax: %w", err)
	}

	valPart = strings.TrimSpace(valPart)
	if !strings.HasPrefix(valPart, "(") || !strings.HasSuffix(valPart, ")") {
		return nil, fmt.Errorf("invalid INSERT values syntax")
	}
	valPart = strings.TrimSpace(valPart[1 : len(valPart)-1])

	rawVals := splitComma(valPart)
	var exprs []Expr
	for _, rv := range rawVals {
		lit, err := parseLiteral(strings.TrimSpace(rv))
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, &LiteralExpr{Value: lit})
	}

	return &InsertStmt{
		TableName: tableName,
		Values:    exprs,
	}, nil
}

func parseSelect(sql string) (Statement, error) {
	// "SELECT * FROM users [WHERE col = literal]"
	up := strings.ToUpper(sql)

	if !strings.HasPrefix(up, "SELECT * FROM ") {
		return nil, fmt.Errorf("only SELECT * FROM <table> supported for now")
	}

	rest := strings.TrimSpace(sql[len("SELECT * FROM "):])
	tablePart, wherePart := splitKeyword(rest, "WHERE")

	tableName, err := parseIdent(tablePart)
	if err != nil {
		return nil, fmt.Errorf("invalid SELECT syntax: %w", err)
	}

	var w *WhereEq
	if strings.TrimSpace(wherePart) != "" {
		we, err := parseWhereEq(wherePart)
		if err != nil {
			return nil, err
		}
		w = we
	}

	return &SelectStmt{TableName: tableName, Where: w}, nil
}

func parseUpdate(sql string) (Statement, error) {
	// "UPDATE t SET a=1, b='x' [WHERE id=1]"
	rest := strings.TrimSpace(sql[len("UPDATE"):])
	tablePart, afterTable := splitKeyword(rest, "SET")

	tableName, err := parseIdent(tablePart)
	if err != nil {
		return nil, fmt.Errorf("invalid UPDATE syntax: %w", err)
	}

	setPart, wherePart := splitKeyword(afterTable, "WHERE")
	setPart = strings.TrimSpace(setPart)
	if setPart == "" {
		return nil, fmt.Errorf("invalid UPDATE syntax: missing SET")
	}

	assignStrs := splitComma(setPart)
	assigns := make([]Assignment, 0, len(assignStrs))
	for _, a := range assignStrs {
		a = strings.TrimSpace(a)
		kv := strings.SplitN(a, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid assignment: %q", a)
		}

		col, err := parseIdent(kv[0])
		if err != nil {
			return nil, fmt.Errorf("invalid assignment column: %w", err)
		}

		valRaw := strings.TrimSpace(kv[1])
		lit, err := parseLiteral(valRaw)
		if err != nil {
			return nil, err
		}

		assigns = append(assigns, Assignment{
			Column: col,
			Value:  &LiteralExpr{Value: lit},
		})
	}

	var w *WhereEq
	if strings.TrimSpace(wherePart) != "" {
		we, err := parseWhereEq(wherePart)
		if err != nil {
			return nil, err
		}
		w = we
	}

	return &UpdateStmt{
		TableName:   tableName,
		Assignments: assigns,
		Where:       w,
	}, nil
}

func parseDelete(sql string) (Statement, error) {
	// "DELETE FROM t [WHERE col=literal]"
	rest := strings.TrimSpace(sql[len("DELETE FROM"):])
	tablePart, wherePart := splitKeyword(rest, "WHERE")

	tableName, err := parseIdent(tablePart)
	if err != nil {
		return nil, fmt.Errorf("invalid DELETE syntax: %w", err)
	}

	var w *WhereEq
	if strings.TrimSpace(wherePart) != "" {
		we, err := parseWhereEq(wherePart)
		if err != nil {
			return nil, err
		}
		w = we
	}

	return &DeleteStmt{TableName: tableName, Where: w}, nil
}

func parseWhereEq(s string) (*WhereEq, error) {
	// very naive: "col = literal"
	s = strings.TrimSpace(s)
	kv := strings.SplitN(s, "=", 2)
	if len(kv) != 2 {
		return nil, fmt.Errorf("only WHERE <col> = <literal> supported")
	}

	col, err := parseIdent(kv[0])
	if err != nil {
		return nil, fmt.Errorf("invalid WHERE column: %w", err)
	}

	valRaw := strings.TrimSpace(kv[1])
	lit, err := parseLiteral(valRaw)
	if err != nil {
		return nil, err
	}

	return &WhereEq{
		Column: col,
		Value:  &LiteralExpr{Value: lit},
	}, nil
}

func parseLiteral(rv string) (any, error) {
	up := strings.ToUpper(rv)

	// NULL
	if up == "NULL" {
		return nil, nil
	}

	// BOOL
	if up == "TRUE" {
		return true, nil
	}
	if up == "FALSE" {
		return false, nil
	}

	// STRING (single quotes)
	if len(rv) >= 2 && rv[0] == '\'' && rv[len(rv)-1] == '\'' {
		// NOTE: minimal; no escape support yet
		return rv[1 : len(rv)-1], nil
	}

	// INT64
	if i, err := strconv.ParseInt(rv, 10, 64); err == nil {
		return i, nil
	}

	return nil, fmt.Errorf("unsupported literal: %q", rv)
}

// splitKeyword splits "X <keyword> Y" case-insensitively.
// returns (X, Y). If keyword not present => (s, "").
//
// NOTE (phase 1 limitation):
//   - requires spaces around keyword (" WHERE ").
func splitKeyword(s, keyword string) (string, string) {
	up := strings.ToUpper(s)
	k := " " + strings.ToUpper(keyword) + " "
	idx := strings.Index(up, k)
	if idx < 0 {
		return s, ""
	}
	left := strings.TrimSpace(s[:idx])
	right := strings.TrimSpace(s[idx+len(k):])
	return left, right
}

// splitComma splits a comma-separated list, ignoring commas inside quotes (simple version).
func splitComma(s string) []string {
	parts := []string{}
	cur := strings.Builder{}
	inQuote := false
	for _, r := range s {
		switch r {
		case '\'':
			inQuote = !inQuote
			cur.WriteRune(r)
		case ',':
			if inQuote {
				cur.WriteRune(r)
			} else {
				parts = append(parts, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}
