package novasqlwire

import "github.com/tuannm99/novasql/internal/sql/executor"

// ExecuteRequest is a single SQL command request.
type ExecuteRequest struct {
	ID  uint64 `json:"id"`
	SQL string `json:"sql"`
}

// ExecuteResponse is the response for a request ID.
type ExecuteResponse struct {
	ID     uint64           `json:"id"`
	Result *executor.Result `json:"result,omitempty"`
	Error  string           `json:"error,omitempty"`
}
