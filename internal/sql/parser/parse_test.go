package parser

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_RequireSemicolon(t *testing.T) {
	_, err := Parse("SELECT * FROM users")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing ';'")
}

func TestParse_CreateDatabase(t *testing.T) {
	stmt, err := Parse("CREATE DATABASE testdb;")
	require.NoError(t, err)

	s, ok := stmt.(*CreateDatabaseStmt)
	require.True(t, ok, "want *CreateDatabaseStmt, got %T", stmt)
	assert.Equal(t, "testdb", s.Name)
}

func TestParse_CreateDatabase_RejectExtraTokens(t *testing.T) {
	_, err := Parse("CREATE DATABASE testdb ok;")
	require.Error(t, err)
}

func TestParse_CreateDatabase_Invalid(t *testing.T) {
	_, err := Parse("CREATE DATABASE   ;")
	require.Error(t, err)
}

func TestParse_DropDatabase(t *testing.T) {
	stmt, err := Parse("DROP DATABASE testdb;")
	require.NoError(t, err)

	s, ok := stmt.(*DropDatabaseStmt)
	require.True(t, ok, "want *DropDatabaseStmt, got %T", stmt)
	assert.Equal(t, "testdb", s.Name)
}

func TestParse_UseDatabase(t *testing.T) {
	stmt, err := Parse("USE testdb;")
	require.NoError(t, err)

	s, ok := stmt.(*UseDatabaseStmt)
	require.True(t, ok, "want *UseDatabaseStmt, got %T", stmt)
	assert.Equal(t, "testdb", s.Name)
}

func TestParse_UseDatabase_InvalidIdent(t *testing.T) {
	_, err := Parse("USE 123abc;")
	require.Error(t, err)
}

func TestParse_CreateTable(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT, name TEXT, active BOOL);")
	require.NoError(t, err)

	s, ok := stmt.(*CreateTableStmt)
	require.True(t, ok, "want *CreateTableStmt, got %T", stmt)

	require.Equal(t, "users", s.TableName)
	require.Len(t, s.Columns, 3)

	assert.Equal(t, ColumnDef{Name: "id", Type: "INT"}, s.Columns[0])
	assert.Equal(t, ColumnDef{Name: "name", Type: "TEXT"}, s.Columns[1])
	assert.Equal(t, ColumnDef{Name: "active", Type: "BOOL"}, s.Columns[2])
}

func TestParse_CreateTable_Invalid(t *testing.T) {
	_, err := Parse("CREATE TABLE users id INT, name TEXT;")
	require.Error(t, err)

	// empty col list (must include ';' because parser requires terminator)
	_, err = Parse("CREATE TABLE users ();")
	require.Error(t, err)
}

func TestParse_CreateTable_InvalidTableName(t *testing.T) {
	_, err := Parse("CREATE TABLE users ok (id INT);")
	require.Error(t, err)
}

func TestParse_CreateTable_InvalidColumnName(t *testing.T) {
	_, err := Parse("CREATE TABLE users (1id INT);")
	require.Error(t, err)
}

func TestParse_DropTable(t *testing.T) {
	stmt, err := Parse("DROP TABLE users;")
	require.NoError(t, err)

	s, ok := stmt.(*DropTableStmt)
	require.True(t, ok, "want *DropTableStmt, got %T", stmt)
	assert.Equal(t, "users", s.TableName)
}

func TestParse_Insert(t *testing.T) {
	stmt, err := Parse("INSERT INTO users VALUES (1, 'abc', true, NULL);")
	require.NoError(t, err)

	s, ok := stmt.(*InsertStmt)
	require.True(t, ok, "want *InsertStmt, got %T", stmt)

	assert.Equal(t, "users", s.TableName)
	require.Len(t, s.Values, 4)

	want := []any{int64(1), "abc", true, nil}
	for i := range want {
		lit, ok := s.Values[i].(*LiteralExpr)
		require.True(t, ok, "value[%d]: want *LiteralExpr, got %T", i, s.Values[i])
		assert.True(t, reflect.DeepEqual(lit.Value, want[i]),
			"value[%d]: want %#v got %#v", i, want[i], lit.Value)
	}
}

func TestParse_Insert_SplitCommaInsideQuotes(t *testing.T) {
	stmt, err := Parse("INSERT INTO t VALUES ('a,b', 2);")
	require.NoError(t, err)

	s, ok := stmt.(*InsertStmt)
	require.True(t, ok, "want *InsertStmt, got %T", stmt)
	require.Len(t, s.Values, 2)

	v0 := s.Values[0].(*LiteralExpr).Value
	v1 := s.Values[1].(*LiteralExpr).Value

	assert.Equal(t, "a,b", v0)
	assert.Equal(t, int64(2), v1)
}

func TestParse_Insert_LowercaseValues_ShouldPass(t *testing.T) {
	stmt, err := Parse("INSERT INTO users values (1);")
	require.NoError(t, err)

	s, ok := stmt.(*InsertStmt)
	require.True(t, ok, "want *InsertStmt, got %T", stmt)
	require.Len(t, s.Values, 1)
	assert.Equal(t, "users", s.TableName)
	assert.Equal(t, int64(1), s.Values[0].(*LiteralExpr).Value)
}

func TestParse_Insert_RejectExtraTokensInTableName(t *testing.T) {
	_, err := Parse("INSERT INTO users ok VALUES (1);")
	require.Error(t, err)
}

func TestParse_Select_NoWhere(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users;")
	require.NoError(t, err)

	s, ok := stmt.(*SelectStmt)
	require.True(t, ok, "want *SelectStmt, got %T", stmt)

	assert.Equal(t, "users", s.TableName)
	assert.Nil(t, s.Where)
}

func TestParse_Select_WithWhere(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users WHERE id = 10;")
	require.NoError(t, err)

	s, ok := stmt.(*SelectStmt)
	require.True(t, ok, "want *SelectStmt, got %T", stmt)

	require.NotNil(t, s.Where)
	assert.Equal(t, "id", s.Where.Column)

	lit, ok := s.Where.Value.(*LiteralExpr)
	require.True(t, ok, "want *LiteralExpr, got %T", s.Where.Value)
	assert.Equal(t, int64(10), lit.Value)
}

func TestParse_Select_InvalidWhereColumn(t *testing.T) {
	_, err := Parse("SELECT * FROM users WHERE 1id = 10;")
	require.Error(t, err)
}

func TestParse_Update(t *testing.T) {
	stmt, err := Parse("UPDATE users SET name='x', active=false WHERE id=1;")
	require.NoError(t, err)

	s, ok := stmt.(*UpdateStmt)
	require.True(t, ok, "want *UpdateStmt, got %T", stmt)

	assert.Equal(t, "users", s.TableName)
	require.Len(t, s.Assignments, 2)

	assert.Equal(t, "name", s.Assignments[0].Column)
	assert.Equal(t, "active", s.Assignments[1].Column)

	require.NotNil(t, s.Where)
	assert.Equal(t, "id", s.Where.Column)
}

func TestParse_Update_InvalidMissingSet(t *testing.T) {
	_, err := Parse("UPDATE users WHERE id=1;")
	require.Error(t, err)
}

func TestParse_Update_InvalidAssignColumn(t *testing.T) {
	_, err := Parse("UPDATE users SET 1name='x' WHERE id=1;")
	require.Error(t, err)
}

func TestParse_Delete(t *testing.T) {
	stmt, err := Parse("DELETE FROM users WHERE id = 1;")
	require.NoError(t, err)

	s, ok := stmt.(*DeleteStmt)
	require.True(t, ok, "want *DeleteStmt, got %T", stmt)

	assert.Equal(t, "users", s.TableName)
	require.NotNil(t, s.Where)
	assert.Equal(t, "id", s.Where.Column)
}

func TestParse_Unsupported(t *testing.T) {
	_, err := Parse("ALTER TABLE t ADD COLUMN x INT;")
	require.Error(t, err)
}

func TestParseLiteral(t *testing.T) {
	cases := []struct {
		in   string
		want any
		ok   bool
	}{
		{"NULL", nil, true},
		{"null", nil, true},
		{"TRUE", true, true},
		{"false", false, true},
		{"'abc'", "abc", true},
		{"123", int64(123), true},
		{"-7", int64(-7), true},
		{"'a,b'", "a,b", true},
		{"1.2", nil, false}, // unsupported
		{"abc", nil, false}, // unsupported
		{"'unterminated", nil, false},
	}

	for _, tc := range cases {
		got, err := parseLiteral(tc.in)
		if tc.ok {
			require.NoError(t, err, "parseLiteral(%q)", tc.in)
			assert.True(t, reflect.DeepEqual(got, tc.want),
				"parseLiteral(%q): want %#v got %#v", tc.in, tc.want, got)
		} else {
			require.Error(t, err, "parseLiteral(%q)", tc.in)
		}
	}
}

func TestSplitComma(t *testing.T) {
	in := "1,'a,b',true,NULL,'x'"
	got := splitComma(in)
	want := []string{"1", "'a,b'", "true", "NULL", "'x'"}
	assert.Equal(t, want, got)
}

func TestSplitKeyword(t *testing.T) {
	left, right := splitKeyword("users WHERE id=1", "WHERE")
	assert.Equal(t, "users", left)
	assert.Equal(t, "id=1", right)

	left, right = splitKeyword("users", "WHERE")
	assert.Equal(t, "users", left)
	assert.Empty(t, right)

	// limitation: requires spaces around keyword.
	left, right = splitKeyword("users WHEREid=1", "WHERE")
	assert.Equal(t, "users WHEREid=1", left)
	assert.Empty(t, right)
}
