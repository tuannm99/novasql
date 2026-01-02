package planner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/sql/parser"
)

func TestBuildPlan_CreateDropUseDB_NoDBNeeded(t *testing.T) {
	// Create DB
	{
		p, err := BuildPlan(&parser.CreateDatabaseStmt{Name: "db1"}, nil)
		require.NoError(t, err)
		plan, ok := p.(*CreateDatabasePlan)
		require.True(t, ok)
		require.Equal(t, "db1", plan.Name)
	}

	// Drop DB
	{
		p, err := BuildPlan(&parser.DropDatabaseStmt{Name: "db2"}, nil)
		require.NoError(t, err)
		plan, ok := p.(*DropDatabasePlan)
		require.True(t, ok)
		require.Equal(t, "db2", plan.Name)
	}

	// Use DB
	{
		p, err := BuildPlan(&parser.UseDatabaseStmt{Name: "db3"}, nil)
		require.NoError(t, err)
		plan, ok := p.(*UseDatabasePlan)
		require.True(t, ok)
		require.Equal(t, "db3", plan.Name)
	}
}

func TestBuildPlan_CreateDropTable_NoDBNeeded(t *testing.T) {
	// Create table
	{
		stmt := &parser.CreateTableStmt{
			TableName: "users",
			Columns: []parser.ColumnDef{
				{Name: "id", Type: "INT"},
				{Name: "name", Type: "TEXT"},
				{Name: "active", Type: "BOOL"},
			},
		}

		p, err := BuildPlan(stmt, nil)
		require.NoError(t, err)

		plan, ok := p.(*CreateTablePlan)
		require.True(t, ok)
		require.Equal(t, "users", plan.TableName)

		require.Len(t, plan.Schema.Cols, 3)
		require.Equal(t, "id", plan.Schema.Cols[0].Name)
		require.Equal(t, record.ColInt64, plan.Schema.Cols[0].Type)
		require.True(t, plan.Schema.Cols[0].Nullable)

		require.Equal(t, "name", plan.Schema.Cols[1].Name)
		require.Equal(t, record.ColText, plan.Schema.Cols[1].Type)

		require.Equal(t, "active", plan.Schema.Cols[2].Name)
		require.Equal(t, record.ColBool, plan.Schema.Cols[2].Type)
	}

	// Drop table
	{
		p, err := BuildPlan(&parser.DropTableStmt{TableName: "users"}, nil)
		require.NoError(t, err)

		plan, ok := p.(*DropTablePlan)
		require.True(t, ok)
		require.Equal(t, "users", plan.TableName)
	}
}

func TestBuildPlan_Insert_NoDBNeeded(t *testing.T) {
	stmt := &parser.InsertStmt{
		TableName: "users",
		Values: []parser.Expr{
			&parser.LiteralExpr{Value: int64(1)},
			&parser.LiteralExpr{Value: "a"},
		},
	}

	p, err := BuildPlan(stmt, nil)
	require.NoError(t, err)

	plan, ok := p.(*InsertPlan)
	require.True(t, ok)
	require.Equal(t, "users", plan.TableName)
	require.Len(t, plan.Values, 2)
}

func TestMapSQLType(t *testing.T) {
	t.Run("int_variants", func(t *testing.T) {
		got, err := mapSQLType("INT")
		require.NoError(t, err)
		require.Equal(t, record.ColInt64, got)

		got, err = mapSQLType("integer")
		require.NoError(t, err)
		require.Equal(t, record.ColInt64, got)
	})

	t.Run("text", func(t *testing.T) {
		got, err := mapSQLType("TeXt")
		require.NoError(t, err)
		require.Equal(t, record.ColText, got)
	})

	t.Run("bool_variants", func(t *testing.T) {
		got, err := mapSQLType("BOOL")
		require.NoError(t, err)
		require.Equal(t, record.ColBool, got)

		got, err = mapSQLType("boolean")
		require.NoError(t, err)
		require.Equal(t, record.ColBool, got)
	})

	t.Run("unsupported", func(t *testing.T) {
		_, err := mapSQLType("FLOAT")
		require.Error(t, err)
	})
}

func TestCoerceLiteralToColumn(t *testing.T) {
	schema := record.Schema{Cols: []record.Column{
		{Name: "id", Type: record.ColInt64, Nullable: false},
		{Name: "name", Type: record.ColText, Nullable: true},
		{Name: "active", Type: record.ColBool, Nullable: true},
	}}

	t.Run("unknown_column", func(t *testing.T) {
		_, err := coerceLiteralToColumn(schema, "nope", int64(1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown column")
	})

	t.Run("null_not_nullable", func(t *testing.T) {
		_, err := coerceLiteralToColumn(schema, "id", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "NOT NULL")
	})

	t.Run("null_nullable_ok", func(t *testing.T) {
		got, err := coerceLiteralToColumn(schema, "name", nil)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("int64_ok", func(t *testing.T) {
		got, err := coerceLiteralToColumn(schema, "id", int64(7))
		require.NoError(t, err)
		require.Equal(t, int64(7), got)
	})

	t.Run("int_and_int32_are_coerced", func(t *testing.T) {
		got, err := coerceLiteralToColumn(schema, "id", int(7))
		require.NoError(t, err)
		require.Equal(t, int64(7), got)

		got, err = coerceLiteralToColumn(schema, "id", int32(8))
		require.NoError(t, err)
		require.Equal(t, int64(8), got)
	})

	t.Run("int_type_mismatch", func(t *testing.T) {
		_, err := coerceLiteralToColumn(schema, "id", "x")
		require.Error(t, err)
	})

	t.Run("text_ok", func(t *testing.T) {
		got, err := coerceLiteralToColumn(schema, "name", "abc")
		require.NoError(t, err)
		require.Equal(t, "abc", got)
	})

	t.Run("text_type_mismatch", func(t *testing.T) {
		_, err := coerceLiteralToColumn(schema, "name", int64(1))
		require.Error(t, err)
	})

	t.Run("bool_ok", func(t *testing.T) {
		got, err := coerceLiteralToColumn(schema, "active", true)
		require.NoError(t, err)
		require.Equal(t, true, got)
	})

	t.Run("bool_type_mismatch", func(t *testing.T) {
		_, err := coerceLiteralToColumn(schema, "active", "true")
		require.Error(t, err)
	})
}

func TestBindWhereEq(t *testing.T) {
	schema := record.Schema{Cols: []record.Column{
		{Name: "id", Type: record.ColInt64, Nullable: false},
		{Name: "name", Type: record.ColText, Nullable: true},
	}}

	t.Run("ok_literal_is_coerced", func(t *testing.T) {
		w, err := bindWhereEq(schema, &parser.WhereEq{
			Column: "id",
			Value:  &parser.LiteralExpr{Value: int64(1)},
		})
		require.NoError(t, err)
		require.Equal(t, "id", w.Column)
		require.Equal(t, int64(1), w.Value)
	})

	t.Run("unknown_column", func(t *testing.T) {
		_, err := bindWhereEq(schema, &parser.WhereEq{
			Column: "nope",
			Value:  &parser.LiteralExpr{Value: int64(1)},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown column")
	})
}

func TestBuildCreateTablePlan(t *testing.T) {
	stmt := &parser.CreateTableStmt{
		TableName: "t",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
			{Name: "ok", Type: "BOOL"},
		},
	}
	p, err := buildCreateTablePlan(stmt)
	require.NoError(t, err)

	plan, ok := p.(*CreateTablePlan)
	require.True(t, ok)
	require.Equal(t, "t", plan.TableName)
	require.Len(t, plan.Schema.Cols, 3)
	require.Equal(t, record.ColInt64, plan.Schema.Cols[0].Type)
	require.Equal(t, record.ColText, plan.Schema.Cols[1].Type)
	require.Equal(t, record.ColBool, plan.Schema.Cols[2].Type)
}

func TestBuildCreateTablePlan_UnsupportedType(t *testing.T) {
	stmt := &parser.CreateTableStmt{
		TableName: "t",
		Columns: []parser.ColumnDef{
			{Name: "x", Type: "FLOAT"},
		},
	}
	_, err := buildCreateTablePlan(stmt)
	require.Error(t, err)
}
