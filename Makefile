
test:
	go test ./internal... ./pkg/... -cover -v -coverprofile=coverage.out
	go tool cover -html=coverage.out

