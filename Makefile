
test:
	go test ./internal/... -cover -v -coverprofile=coverage.out
	go tool cover -html=coverage.out

