package planner

import (
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/sql/parser"
)

// Plan is the interface for executable plans.
type Plan interface {
	planNode()
}

// ----- Plan nodes -----

type CreateTablePlan struct {
	TableName string
	Schema    record.Schema
}

func (*CreateTablePlan) planNode() {}

type InsertPlan struct {
	TableName string
	Values    []parser.Expr // evaluated at execution
}

func (*InsertPlan) planNode() {}

type SeqScanPlan struct {
	TableName string
	// TODO: projection, filter, ...
}

func (*SeqScanPlan) planNode() {}
