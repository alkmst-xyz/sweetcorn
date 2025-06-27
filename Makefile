.PHONY: build
build:
	@echo "Building..."
	@go build -o sweetcorn cmd/sweetcorn/main.go
	
.PHONY: test
test:
	@echo "Running tests..."
	@go test ./...
