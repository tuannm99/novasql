package planner

import (
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/sql/parser"
)

// Plan is the interface for executable plans.
type Plan interface {
	planNode()
}

// ----- DB plans -----

type CreateDatabasePlan struct{ Name string }

func (*CreateDatabasePlan) planNode() {}

type DropDatabasePlan struct{ Name string }

func (*DropDatabasePlan) planNode() {}

type UseDatabasePlan struct{ Name string }

func (*UseDatabasePlan) planNode() {}

// ----- Table plans -----

type CreateTablePlan struct {
	TableName string
	Schema    record.Schema
}

func (*CreateTablePlan) planNode() {}

type DropTablePlan struct {
	TableName string
}

func (*DropTablePlan) planNode() {}

// ----- DML plans -----

type InsertPlan struct {
	TableName string
	Values    []parser.Expr
}

func (*InsertPlan) planNode() {}

type WhereEq struct {
	Column string
	Value  any // already coerced
}

type SeqScanPlan struct {
	TableName string
	Where     *WhereEq
}

func (*SeqScanPlan) planNode() {}

type IndexLookupPlan struct {
	TableName     string
	IndexFileBase string
	Column        string
	Key           int64
	Where         *WhereEq // safety re-check
}

func (*IndexLookupPlan) planNode() {}

type Assignment struct {
	Column string
	Value  any // already coerced
}

type UpdatePlan struct {
	TableName string
	Assigns   []Assignment
	Where     *WhereEq
}

func (*UpdatePlan) planNode() {}

type DeletePlan struct {
	TableName string
	Where     *WhereEq
}

func (*DeletePlan) planNode() {}
