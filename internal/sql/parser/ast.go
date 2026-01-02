package parser

// Statement is the root interface for all SQL statements.
type Statement interface {
	stmtNode()
}

// ----- CREATE DATABASE / DROP DATABASE / USE -----

type CreateDatabaseStmt struct {
	Name string
}

func (*CreateDatabaseStmt) stmtNode() {}

type DropDatabaseStmt struct {
	Name string
}

func (*DropDatabaseStmt) stmtNode() {}

// UseDatabaseStmt is "USE <db>" (select database).
type UseDatabaseStmt struct {
	Name string
}

func (*UseDatabaseStmt) stmtNode() {}

// ----- CREATE TABLE / DROP TABLE -----

type ColumnDef struct {
	Name string
	Type string // "INT", "TEXT", "BOOL"
	// TODO: nullable, default, primary key, ...
}

type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (*CreateTableStmt) stmtNode() {}

type DropTableStmt struct {
	TableName string
}

func (*DropTableStmt) stmtNode() {}

// ----- INSERT -----

type InsertStmt struct {
	TableName string
	Values    []Expr // only constant expr for now
}

func (*InsertStmt) stmtNode() {}

// ----- SELECT -----

type SelectStmt struct {
	TableName string
	Where     *WhereEq // optional
}

func (*SelectStmt) stmtNode() {}

// ----- UPDATE -----

type Assignment struct {
	Column string
	Value  Expr // literal only
}

type UpdateStmt struct {
	TableName   string
	Assignments []Assignment
	Where       *WhereEq // optional
}

func (*UpdateStmt) stmtNode() {}

// ----- DELETE -----

type DeleteStmt struct {
	TableName string
	Where     *WhereEq // optional
}

func (*DeleteStmt) stmtNode() {}

// ----- WHERE (phase 1: only col = literal) -----

type WhereEq struct {
	Column string
	Value  Expr // literal only
}

// ----- Expressions -----

type Expr interface {
	exprNode()
}

type LiteralExpr struct {
	Value any
}

func (*LiteralExpr) exprNode() {}
