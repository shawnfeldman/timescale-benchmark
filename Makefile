build:
	go mod download
	go install ./...

test:
	go mod download
	go test ./... -v

integration:
	go mod download
	go test ./... -v

run: 
	go mod download
	go run ./cmd/benchmark/main.go -file ./db/query_params.csv