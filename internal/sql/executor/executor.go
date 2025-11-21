package executor

import (
	"fmt"

	engine "github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/sql/parser"
	"github.com/tuannm99/novasql/internal/sql/planner"
)

// Executor executes a plan against a Database.
type Executor struct {
	DB *engine.Database
}

func NewExecutor(db *engine.Database) *Executor {
	return &Executor{DB: db}
}

// ExecSQL is the top-level entry: SQL string -> Result.
func (e *Executor) ExecSQL(sql string) (*Result, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	plan, err := planner.BuildPlan(stmt, e.DB)
	if err != nil {
		return nil, err
	}
	return e.execPlan(plan)
}

func (e *Executor) execPlan(p planner.Plan) (*Result, error) {
	switch plan := p.(type) {
	case *planner.CreateTablePlan:
		return e.execCreateTable(plan)
	case *planner.InsertPlan:
		return e.execInsert(plan)
	case *planner.SeqScanPlan:
		return e.execSeqScan(plan)
	default:
		return nil, fmt.Errorf("executor: unsupported plan type %T", p)
	}
}

func (e *Executor) execCreateTable(p *planner.CreateTablePlan) (*Result, error) {
	_, err := e.DB.CreateTable(p.TableName, p.Schema)
	if err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 0}, nil
}

func (e *Executor) execInsert(p *planner.InsertPlan) (*Result, error) {
	tbl, err := e.DB.OpenTable(p.TableName)
	if err != nil {
		return nil, err
	}

	// Evaluate literal expressions only (phase 1).
	values := make([]any, len(p.Values))
	for i, expr := range p.Values {
		lit, ok := expr.(*parser.LiteralExpr)
		if !ok {
			return nil, fmt.Errorf("executor: only literal expressions supported in INSERT")
		}
		values[i] = lit.Value
	}

	_, err = tbl.Insert(values)
	if err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execSeqScan(p *planner.SeqScanPlan) (*Result, error) {
	tbl, err := e.DB.OpenTable(p.TableName)
	if err != nil {
		return nil, err
	}

	res := &Result{}
	// Fill columns from schema
	for _, col := range tbl.Schema.Cols {
		res.Columns = append(res.Columns, col.Name)
	}

	err = tbl.Scan(func(id heap.TID, row []any) error {
		// NOTE: copy row if you plan to reuse the slice inside heap.Page
		// For now, assume tbl.Scan gives a fresh []any.
		res.Rows = append(res.Rows, row)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
