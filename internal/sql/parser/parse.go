package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse parses a single SQL statement into an AST.
func Parse(sql string) (Statement, error) {
	s := strings.TrimSpace(sql)
	up := strings.ToUpper(s)

	switch {
	case strings.HasPrefix(up, "CREATE TABLE"):
		return parseCreateTable(s)
	case strings.HasPrefix(up, "INSERT INTO"):
		return parseInsert(s)
	case strings.HasPrefix(up, "SELECT"):
		return parseSelect(s)
	default:
		return nil, fmt.Errorf("unsupported statement: %q", sql)
	}
}

func parseCreateTable(sql string) (Statement, error) {
	// Very naive: "CREATE TABLE users (id INT, name TEXT)"
	// up := strings.ToUpper(sql)
	withoutPrefix := strings.TrimSpace(sql[len("CREATE TABLE"):])
	parts := strings.SplitN(withoutPrefix, "(", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}
	tableName := strings.TrimSpace(parts[0])
	defPart := strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")

	colDefs := strings.Split(defPart, ",")
	var cols []ColumnDef
	for _, def := range colDefs {
		def = strings.TrimSpace(def)
		toks := strings.Fields(def)
		if len(toks) < 2 {
			return nil, fmt.Errorf("invalid column def: %q", def)
		}
		cols = append(cols, ColumnDef{
			Name: toks[0],
			Type: strings.ToUpper(toks[1]),
		})
	}
	return &CreateTableStmt{
		TableName: tableName,
		Columns:   cols,
	}, nil
}

func parseInsert(sql string) (Statement, error) {
	// "INSERT INTO users VALUES (1, 'abc')"
	// up := strings.ToUpper(sql)
	rest := strings.TrimSpace(sql[len("INSERT INTO"):])
	parts := strings.SplitN(rest, "VALUES", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}
	tableName := strings.TrimSpace(parts[0])
	valPart := strings.TrimSpace(parts[1])
	if !strings.HasPrefix(valPart, "(") || !strings.HasSuffix(valPart, ")") {
		return nil, fmt.Errorf("invalid INSERT values syntax")
	}
	valPart = strings.TrimSpace(valPart[1 : len(valPart)-1])

	rawVals := splitComma(valPart)
	var exprs []Expr
	for _, rv := range rawVals {
		rv = strings.TrimSpace(rv)
		// naive literal parse: number or quoted string
		if strings.HasPrefix(rv, "'") && strings.HasSuffix(rv, "'") {
			exprs = append(exprs, &LiteralExpr{Value: strings.Trim(rv, "'")})
		} else if i, err := strconv.Atoi(rv); err == nil {
			exprs = append(exprs, &LiteralExpr{Value: i})
		} else {
			return nil, fmt.Errorf("unsupported literal: %q", rv)
		}
	}
	return &InsertStmt{
		TableName: tableName,
		Values:    exprs,
	}, nil
}

func parseSelect(sql string) (Statement, error) {
	// "SELECT * FROM users"
	up := strings.ToUpper(sql)
	if !strings.HasPrefix(up, "SELECT * FROM ") {
		return nil, fmt.Errorf("only SELECT * FROM <table> supported for now")
	}
	tableName := strings.TrimSpace(sql[len("SELECT * FROM "):])
	return &SelectStmt{TableName: tableName}, nil
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
