build:
	go mod download
	go install ./...

unit_test:
	go mod download
	go test ./... -v -short

integration:
	go mod download
	go test ./... -v

run: 
	go mod download
	go run ./cmd/benchmark/main.go -file ./db/query_params.csv
debug: 
	go mod download
	go run ./cmd/benchmark/main.go -file ./db/query_params.csv