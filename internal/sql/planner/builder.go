package planner

import (
	"fmt"
	"strings"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/sql/parser"
)

func BuildPlan(stmt parser.Statement, db *novasql.Database) (Plan, error) {
	switch s := stmt.(type) {
	case *parser.CreateDatabaseStmt:
		return &CreateDatabasePlan{Name: s.Name}, nil
	case *parser.DropDatabaseStmt:
		return &DropDatabasePlan{Name: s.Name}, nil
	case *parser.UseDatabaseStmt:
		return &UseDatabasePlan{Name: s.Name}, nil

	case *parser.CreateTableStmt:
		return buildCreateTablePlan(s)
	case *parser.DropTableStmt:
		return &DropTablePlan{TableName: s.TableName}, nil

	case *parser.InsertStmt:
		return &InsertPlan{TableName: s.TableName, Values: s.Values}, nil

	case *parser.SelectStmt:
		return buildSelectPlan(s, db)

	case *parser.UpdateStmt:
		return buildUpdatePlan(s, db)

	case *parser.DeleteStmt:
		return buildDeletePlan(s, db)

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

func buildSelectPlan(s *parser.SelectStmt, db *novasql.Database) (Plan, error) {
	// Bind schema to coerce WHERE and choose index if possible
	tbl, err := db.OpenTable(s.TableName)
	if err != nil {
		return nil, err
	}

	var where *WhereEq
	if s.Where != nil {
		w, err := bindWhereEq(tbl.Schema, s.Where)
		if err != nil {
			return nil, err
		}
		where = w
	}

	// Optional: if WHERE is "col=int64" and there's btree index on that column => IndexLookupPlan
	if where != nil {
		if key, ok := where.Value.(int64); ok {
			base, ok := findBTreeIndexBaseByColumn(db, s.TableName, where.Column)
			if ok && base != "" {
				return &IndexLookupPlan{
					TableName:     s.TableName,
					IndexFileBase: base,
					Column:        where.Column,
					Key:           key,
					Where:         where,
				}, nil
			}
		}
	}

	return &SeqScanPlan{TableName: s.TableName, Where: where}, nil
}

func buildUpdatePlan(s *parser.UpdateStmt, db *novasql.Database) (Plan, error) {
	tbl, err := db.OpenTable(s.TableName)
	if err != nil {
		return nil, err
	}

	assigns := make([]Assignment, 0, len(s.Assignments))
	for _, a := range s.Assignments {
		lit, ok := a.Value.(*parser.LiteralExpr)
		if !ok {
			return nil, fmt.Errorf("planner: only literal supported in UPDATE SET")
		}
		v, err := coerceLiteralToColumn(tbl.Schema, a.Column, lit.Value)
		if err != nil {
			return nil, err
		}
		assigns = append(assigns, Assignment{
			Column: a.Column,
			Value:  v,
		})
	}

	var where *WhereEq
	if s.Where != nil {
		w, err := bindWhereEq(tbl.Schema, s.Where)
		if err != nil {
			return nil, err
		}
		where = w
	}

	return &UpdatePlan{
		TableName: s.TableName,
		Assigns:   assigns,
		Where:     where,
	}, nil
}

func buildDeletePlan(s *parser.DeleteStmt, db *novasql.Database) (Plan, error) {
	tbl, err := db.OpenTable(s.TableName)
	if err != nil {
		return nil, err
	}

	var where *WhereEq
	if s.Where != nil {
		w, err := bindWhereEq(tbl.Schema, s.Where)
		if err != nil {
			return nil, err
		}
		where = w
	}
	return &DeletePlan{TableName: s.TableName, Where: where}, nil
}

func bindWhereEq(schema record.Schema, w *parser.WhereEq) (*WhereEq, error) {
	lit, ok := w.Value.(*parser.LiteralExpr)
	if !ok {
		return nil, fmt.Errorf("planner: only literal supported in WHERE")
	}
	v, err := coerceLiteralToColumn(schema, w.Column, lit.Value)
	if err != nil {
		return nil, err
	}
	return &WhereEq{Column: w.Column, Value: v}, nil
}

func coerceLiteralToColumn(schema record.Schema, colName string, v any) (any, error) {
	pos := -1
	var col record.Column
	for i := range schema.Cols {
		if schema.Cols[i].Name == colName {
			pos = i
			col = schema.Cols[i]
			break
		}
	}
	if pos < 0 {
		return nil, fmt.Errorf("planner: unknown column: %s", colName)
	}

	if v == nil {
		if !col.Nullable {
			return nil, fmt.Errorf("planner: column %s is NOT NULL", colName)
		}
		return nil, nil
	}

	switch col.Type {
	case record.ColInt64:
		switch x := v.(type) {
		case int64:
			return x, nil
		case int:
			return int64(x), nil
		case int32:
			return int64(x), nil
		default:
			return nil, fmt.Errorf("planner: column %s expects INT64, got %T", colName, v)
		}
	case record.ColText:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("planner: column %s expects TEXT, got %T", colName, v)
		}
		return s, nil
	case record.ColBool:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("planner: column %s expects BOOL, got %T", colName, v)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("planner: unsupported column type: %s", colName)
	}
}

func mapSQLType(t string) (record.ColumnType, error) {
	switch strings.ToUpper(t) {
	case "INT", "INTEGER":
		return record.ColInt64, nil
	case "TEXT":
		return record.ColText, nil
	case "BOOL", "BOOLEAN":
		return record.ColBool, nil
	default:
		return 0, fmt.Errorf("unsupported column type: %s", t)
	}
}

// findBTreeIndexBaseByColumn tries to locate a btree index for (table, column).
// NOTE: uses dynamic json decoding to avoid tight coupling to IndexMeta fields.
func findBTreeIndexBaseByColumn(db *novasql.Database, table, col string) (string, bool) {
	metas, err := db.ListTables()
	if err != nil {
		return "", false
	}

	var tm *novasql.TableMeta
	for _, m := range metas {
		if m != nil && m.Name == table {
			tm = m
			break
		}
	}
	if tm == nil {
		return "", false
	}

	for _, im := range tm.Indexes {
		if im.Kind != novasql.IndexKindBTree {
			continue
		}
		if im.KeyColumn != col {
			continue
		}

		if im.FileBase != "" {
			return im.FileBase, true
		}
		if im.Name != "" {
			return table + "__idx__" + im.Name, true
		}
		return "", false
	}

	return "", false
}
