package parser

// Statement is the root interface for all SQL statements.
type Statement interface {
	stmtNode()
}

// ----- CREATE TABLE -----
type ColumnDef struct {
	Name string
	Type string // "INT", "TEXT" (simple for now)
	// TODO: nullable, default, primary key, ...
}

type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (*CreateTableStmt) stmtNode() {}

// ----- INSERT -----
type InsertStmt struct {
	TableName string
	Values    []Expr // only constant expr for now
}

func (*InsertStmt) stmtNode() {}

// ----- SELECT -----
type SelectStmt struct {
	TableName string
	// TODO: later: projection list, WHERE, LIMIT, ORDER BY...
}

func (*SelectStmt) stmtNode() {}

// ----- Expressions -----
type Expr interface {
	exprNode()
}

type LiteralExpr struct {
	Value any
}

func (*LiteralExpr) exprNode() {}
