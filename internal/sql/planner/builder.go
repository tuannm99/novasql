package planner

import (
	"fmt"
	"strings"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/sql/parser"
)

// BuildPlan builds a physical plan from an AST Statement.
// It may need access to catalog/schema via engine.Database.
func BuildPlan(stmt parser.Statement, db *novasql.Database) (Plan, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return buildCreateTablePlan(s)
	case *parser.InsertStmt:
		return buildInsertPlan(s)
	case *parser.SelectStmt:
		return buildSelectPlan(s)
	default:
		return nil, fmt.Errorf("planner: unsupported statement type %T", stmt)
	}
}

func buildCreateTablePlan(s *parser.CreateTableStmt) (Plan, error) {
	var cols []record.Column
	for _, c := range s.Columns {
		colType, err := mapSQLType(c.Type)
		if err != nil {
			return nil, err
		}
		cols = append(cols, record.Column{
			Name:     c.Name,
			Type:     colType,
			Nullable: true, // default
		})
	}
	return &CreateTablePlan{
		TableName: s.TableName,
		Schema:    record.Schema{Cols: cols},
	}, nil
}

func buildInsertPlan(s *parser.InsertStmt) (Plan, error) {
	return &InsertPlan{
		TableName: s.TableName,
		Values:    s.Values,
	}, nil
}

func buildSelectPlan(s *parser.SelectStmt) (Plan, error) {
	return &SeqScanPlan{
		TableName: s.TableName,
	}, nil
}

func mapSQLType(t string) (record.ColumnType, error) {
	switch strings.ToUpper(t) {
	case "INT", "INTEGER":
		return record.ColInt64, nil
	case "TEXT":
		return record.ColText, nil
	default:
		return 0, fmt.Errorf("unsupported column type: %s", t)
	}
}
