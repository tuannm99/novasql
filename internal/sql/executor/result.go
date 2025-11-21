package executor

// Result is the generic query result returned to the caller.
type Result struct {
	Columns []string
	Rows    [][]any

	// For DML:
	AffectedRows int64
}
