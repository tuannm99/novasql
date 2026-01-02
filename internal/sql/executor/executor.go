package executor

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/tuannm99/novasql"
	"github.com/tuannm99/novasql/internal/btree"
	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/heap"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/sql/parser"
	"github.com/tuannm99/novasql/internal/sql/planner"
	"github.com/tuannm99/novasql/internal/storage"
)

// Result is the generic query result returned to the caller.
type Result struct {
	Columns []string
	Rows    [][]any

	// For DML:
	AffectedRows int64
}

// executorDB is a small seam for unit-testing Executor without a real DB.
type executorDB interface {
	CreateDatabase(name string) error
	DropDatabase(name string) (any, error)
	SelectDatabase(name string) (any, error)

	CreateTable(table string, schema record.Schema) (any, error)
	DropTable(table string) error
	OpenTable(table string) (*heap.Table, error)

	ListTables() ([]*novasql.TableMeta, error)

	TableDir() string
	BufferView(fs storage.FileSet) bufferpool.Manager
	StorageManager() *storage.StorageManager
}

// realDB adapts *novasql.Database to executorDB.
type realDB struct {
	db *novasql.Database
}

func (r realDB) CreateDatabase(name string) error { return r.db.CreateDatabase(name) }
func (r realDB) DropDatabase(name string) (any, error) {
	return r.db.DropDatabase(name)
}

func (r realDB) SelectDatabase(name string) (any, error) {
	return r.db.SelectDatabase(name)
}

func (r realDB) CreateTable(table string, schema record.Schema) (any, error) {
	return r.db.CreateTable(table, schema)
}
func (r realDB) DropTable(table string) error { return r.db.DropTable(table) }
func (r realDB) OpenTable(table string) (*heap.Table, error) {
	return r.db.OpenTable(table)
}
func (r realDB) ListTables() ([]*novasql.TableMeta, error) { return r.db.ListTables() }
func (r realDB) TableDir() string                          { return r.db.TableDir() }
func (r realDB) BufferView(fs storage.FileSet) bufferpool.Manager {
	return r.db.BufferView(fs)
}
func (r realDB) StorageManager() *storage.StorageManager { return r.db.SM }

// Executor executes a plan against a Database.
type Executor struct {
	DB executorDB

	// raw is the real database used by planner.BuildPlan (it currently expects *novasql.Database).
	// This keeps production path simple while still allowing executorDB to be mocked in unit tests.
	raw *novasql.Database

	// for unit-test: inject btree insert behavior
	btreeInsertFn func(im novasql.IndexMeta, key int64, tid heap.TID) error
}

func NewExecutor(db *novasql.Database) *Executor {
	ex := &Executor{
		DB:  realDB{db: db},
		raw: db,
	}
	ex.btreeInsertFn = ex.btreeInsert
	return ex
}

// NewExecutorForTest allows injecting a fake executorDB while still supplying a real *novasql.Database
// (or a lightweight in-memory one) for planner.BuildPlan if needed.
func NewExecutorForTest(db executorDB, raw *novasql.Database) *Executor {
	ex := &Executor{
		DB:  db,
		raw: raw,
	}
	// default to real implementation unless test overrides
	ex.btreeInsertFn = ex.btreeInsert
	return ex
}

// ExecSQL is the top-level entry: SQL string -> Result.
func (e *Executor) ExecSQL(sql string) (*Result, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	if e.raw == nil {
		return nil, fmt.Errorf("executor: raw database is nil (planner requires *novasql.Database)")
	}

	plan, err := planner.BuildPlan(stmt, e.raw)
	if err != nil {
		return nil, err
	}
	return e.execPlan(plan)
}

func (e *Executor) execPlan(p planner.Plan) (*Result, error) {
	switch plan := p.(type) {
	case *planner.CreateDatabasePlan:
		return e.execCreateDatabase(plan)
	case *planner.DropDatabasePlan:
		return e.execDropDatabase(plan)
	case *planner.UseDatabasePlan:
		return e.execUseDatabase(plan)

	case *planner.CreateTablePlan:
		return e.execCreateTable(plan)
	case *planner.DropTablePlan:
		return e.execDropTable(plan)

	case *planner.InsertPlan:
		return e.execInsert(plan)

	case *planner.IndexLookupPlan:
		return e.execIndexLookup(plan)
	case *planner.SeqScanPlan:
		return e.execSeqScan(plan)

	case *planner.UpdatePlan:
		return e.execUpdate(plan)
	case *planner.DeletePlan:
		return e.execDelete(plan)

	default:
		return nil, fmt.Errorf("executor: unsupported plan type %T", p)
	}
}

func (e *Executor) execCreateDatabase(p *planner.CreateDatabasePlan) (*Result, error) {
	if err := e.DB.CreateDatabase(p.Name); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 0}, nil
}

func (e *Executor) execDropDatabase(p *planner.DropDatabasePlan) (*Result, error) {
	if _, err := e.DB.DropDatabase(p.Name); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 0}, nil
}

func (e *Executor) execUseDatabase(p *planner.UseDatabasePlan) (*Result, error) {
	if _, err := e.DB.SelectDatabase(p.Name); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 0}, nil
}

func (e *Executor) execCreateTable(p *planner.CreateTablePlan) (*Result, error) {
	_, err := e.DB.CreateTable(p.TableName, p.Schema)
	if err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 0}, nil
}

func (e *Executor) execDropTable(p *planner.DropTablePlan) (*Result, error) {
	if err := e.DB.DropTable(p.TableName); err != nil {
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
	raw := make([]any, len(p.Values))
	for i, expr := range p.Values {
		lit, ok := expr.(*parser.LiteralExpr)
		if !ok {
			return nil, fmt.Errorf("executor: only literal expressions supported in INSERT")
		}
		raw[i] = lit.Value
	}

	// Normalize int -> int64 (strict type checks follow schema).
	values, err := coerceInsertValues(tbl.Schema, raw)
	if err != nil {
		return nil, err
	}

	tid, err := tbl.Insert(values)
	if err != nil {
		return nil, err
	}

	// Maintain btree indexes on INSERT (only int64 key columns for now).
	if err := e.syncBTreeIndexesOnInsert(p.TableName, tbl.Schema, values, tid); err != nil {
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
	for _, col := range tbl.Schema.Cols {
		res.Columns = append(res.Columns, col.Name)
	}

	err = tbl.Scan(func(id heap.TID, row []any) error {
		if p.Where != nil {
			ok, err := matchWhere(tbl.Schema, p.Where, row)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}

		// avoid slice aliasing
		cp := make([]any, len(row))
		copy(cp, row)
		res.Rows = append(res.Rows, cp)
		return nil
	})
	if err != nil {
		return nil, err
	}

	res.AffectedRows = int64(len(res.Rows))
	return res, nil
}

func (e *Executor) execIndexLookup(p *planner.IndexLookupPlan) (*Result, error) {
	tbl, err := e.DB.OpenTable(p.TableName)
	if err != nil {
		return nil, err
	}

	idxFS := storage.LocalFileSet{
		Dir:  e.DB.TableDir(),
		Base: p.IndexFileBase,
	}
	idxBP := e.DB.BufferView(idxFS)

	tree, err := btree.OpenTree(e.DB.StorageManager(), idxFS, idxBP)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tree.Close() }()

	tids, err := tree.SearchEqual(p.Key)
	if err != nil {
		return nil, err
	}

	res := &Result{}
	for _, col := range tbl.Schema.Cols {
		res.Columns = append(res.Columns, col.Name)
	}

	for _, tid := range tids {
		row, err := tbl.Get(tid)
		if err != nil {
			// stale/dangling index entry: ignore
			continue
		}
		// SAFETY: re-check predicate to avoid returning wrong row if index stale after UPDATE
		if p.Where != nil {
			ok, err := matchWhere(tbl.Schema, p.Where, row)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		cp := make([]any, len(row))
		copy(cp, row)
		res.Rows = append(res.Rows, cp)
	}

	res.AffectedRows = int64(len(res.Rows))
	return res, nil
}

func (e *Executor) execUpdate(p *planner.UpdatePlan) (*Result, error) {
	tbl, err := e.DB.OpenTable(p.TableName)
	if err != nil {
		return nil, err
	}

	var affected int64

	// Scan and update tuples in heap.
	err = tbl.Scan(func(id heap.TID, row []any) error {
		if p.Where != nil {
			ok, err := matchWhere(tbl.Schema, p.Where, row)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}

		newRow := make([]any, len(row))
		copy(newRow, row)

		// Apply assignments by column name
		for _, a := range p.Assigns {
			pos := colPos(tbl.Schema, a.Column)
			if pos < 0 {
				return fmt.Errorf("executor: unknown column in UPDATE: %s", a.Column)
			}
			newRow[pos] = a.Value
		}

		if err := tbl.Update(id, newRow); err != nil {
			return err
		}

		// Optional: insert new index entry for updated indexed key (old stays stale).
		// Correctness is preserved because index lookup re-checks WHERE against heap row.
		if err := e.syncBTreeIndexesOnUpdateMaybeInsert(p.TableName, tbl.Schema, newRow, id, p.Assigns); err != nil {
			return err
		}

		affected++
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Result{AffectedRows: affected}, nil
}

func (e *Executor) execDelete(p *planner.DeletePlan) (*Result, error) {
	tbl, err := e.DB.OpenTable(p.TableName)
	if err != nil {
		return nil, err
	}

	var affected int64

	err = tbl.Scan(func(id heap.TID, row []any) error {
		if p.Where != nil {
			ok, err := matchWhere(tbl.Schema, p.Where, row)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}

		if err := tbl.Delete(id); err != nil {
			return err
		}
		// NOTE: index delete not implemented yet -> index entries become stale.
		// Correctness: IndexLookupPlan filters by heap.Get + matchWhere.
		affected++
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Result{AffectedRows: affected}, nil
}

func colPos(schema record.Schema, name string) int {
	for i := range schema.Cols {
		if schema.Cols[i].Name == name {
			return i
		}
	}
	return -1
}

func matchWhere(schema record.Schema, w *planner.WhereEq, row []any) (bool, error) {
	pos := colPos(schema, w.Column)
	if pos < 0 {
		return false, fmt.Errorf("executor: unknown column in WHERE: %s", w.Column)
	}
	got := row[pos]
	want := w.Value

	// NULL handling
	if got == nil || want == nil {
		return got == nil && want == nil, nil
	}

	switch schema.Cols[pos].Type {
	case record.ColInt64:
		g, ok1 := got.(int64)
		wv, ok2 := want.(int64)
		if !ok1 || !ok2 {
			return false, fmt.Errorf("executor: WHERE type mismatch on %s", w.Column)
		}
		return g == wv, nil
	case record.ColText:
		g, ok1 := got.(string)
		wv, ok2 := want.(string)
		if !ok1 || !ok2 {
			return false, fmt.Errorf("executor: WHERE type mismatch on %s", w.Column)
		}
		return g == wv, nil
	case record.ColBool:
		g, ok1 := got.(bool)
		wv, ok2 := want.(bool)
		if !ok1 || !ok2 {
			return false, fmt.Errorf("executor: WHERE type mismatch on %s", w.Column)
		}
		return g == wv, nil
	default:
		return false, fmt.Errorf("executor: unsupported WHERE type on %s", w.Column)
	}
}

func coerceInsertValues(schema record.Schema, raw []any) ([]any, error) {
	if len(raw) != len(schema.Cols) {
		return nil, fmt.Errorf("executor: insert values count %d != schema %d", len(raw), len(schema.Cols))
	}
	out := make([]any, len(raw))
	for i := range raw {
		v := raw[i]
		col := schema.Cols[i]
		if v == nil {
			if !col.Nullable {
				return nil, fmt.Errorf("executor: column %s is NOT NULL", col.Name)
			}
			out[i] = nil
			continue
		}
		switch col.Type {
		case record.ColInt64:
			switch x := v.(type) {
			case int64:
				out[i] = x
			case int:
				out[i] = int64(x)
			case int32:
				out[i] = int64(x)
			default:
				return nil, fmt.Errorf("executor: column %s expects INT64, got %T", col.Name, v)
			}
		case record.ColText:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("executor: column %s expects TEXT, got %T", col.Name, v)
			}
			out[i] = s
		case record.ColBool:
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("executor: column %s expects BOOL, got %T", col.Name, v)
			}
			out[i] = b
		default:
			return nil, fmt.Errorf("executor: unsupported column type %v", col.Type)
		}
	}
	return out, nil
}

// syncBTreeIndexesOnInsert inserts (key, tid) into all BTree indexes of the table.
// V1 constraints:
//   - Only indexes with KeyColumn == schema int64 column are maintained.
//   - If btree enforces out-of-order constraint, we best-effort skip with a warning.
func (e *Executor) syncBTreeIndexesOnInsert(
	tableName string,
	schema record.Schema,
	values []any,
	tid heap.TID,
) error {
	idxs, err := e.listBTreeIndexes(tableName)
	if err != nil {
		return err
	}
	if len(idxs) == 0 {
		return nil
	}

	insertFn := e.btreeInsertFn
	if insertFn == nil {
		insertFn = e.btreeInsert
	}

	for _, im := range idxs {
		col := im.KeyColumn
		pos := colPos(schema, col)
		if pos < 0 {
			// Index meta references unknown column; skip but keep running.
			slog.Warn("executor: btree index refers to unknown column",
				"table", tableName, "index", im.Name, "col", col)
			continue
		}
		if schema.Cols[pos].Type != record.ColInt64 {
			// V1: only int64 keys supported.
			continue
		}

		// NULL key policy: skip index entry for NULL.
		if values[pos] == nil {
			continue
		}

		k, ok := values[pos].(int64)
		if !ok {
			// should not happen after coerceInsertValues
			return fmt.Errorf(
				"executor: btree index key is not int64: table=%s col=%s got=%T",
				tableName,
				col,
				values[pos],
			)
		}

		if err := insertFn(im, k, tid); err != nil {
			// If btree rejects out-of-order inserts, skip as best-effort (index becomes incomplete).
			if errors.Is(err, btree.ErrOutOfOrderInsert) {
				slog.Warn(
					"executor: btree out-of-order insert skipped (index may be incomplete)",
					"table", tableName,
					"index", im.Name,
					"col", col,
					"key", k,
					"tidPage", tid.PageID,
					"tidSlot", tid.Slot,
				)
				continue
			}
			return err
		}
	}
	return nil
}

// syncBTreeIndexesOnUpdateMaybeInsert best-effort inserts new entries when an indexed column is updated.
// NOTE: This does NOT delete old entries, so indexes can become stale/bloated.
func (e *Executor) syncBTreeIndexesOnUpdateMaybeInsert(
	tableName string,
	schema record.Schema,
	newRow []any,
	tid heap.TID,
	assigns []planner.Assignment,
) error {
	// Minimal safe behavior: do nothing for now.
	return nil
}

// ---- helpers ----

func (e *Executor) listBTreeIndexes(tableName string) ([]novasql.IndexMeta, error) {
	metas, err := e.DB.ListTables()
	if err != nil {
		return nil, err
	}
	var tm *novasql.TableMeta
	for _, m := range metas {
		if m != nil && m.Name == tableName {
			tm = m
			break
		}
	}
	if tm == nil {
		return nil, fmt.Errorf("executor: table meta not found: %s", tableName)
	}

	out := make([]novasql.IndexMeta, 0, len(tm.Indexes))
	for _, im := range tm.Indexes {
		if im.Kind != novasql.IndexKindBTree {
			continue
		}
		out = append(out, im)
	}
	return out, nil
}

func (e *Executor) btreeInsert(im novasql.IndexMeta, key int64, tid heap.TID) error {
	base := im.FileBase
	if base == "" {
		return fmt.Errorf("executor: btree index missing file base (index=%s)", im.Name)
	}

	idxFS := storage.LocalFileSet{
		Dir:  e.DB.TableDir(),
		Base: base,
	}
	idxBP := e.DB.BufferView(idxFS)

	tree, err := btree.OpenTree(e.DB.StorageManager(), idxFS, idxBP)
	if err != nil {
		return err
	}
	defer func() { _ = tree.Close() }()

	return tree.Insert(key, tid)
}
