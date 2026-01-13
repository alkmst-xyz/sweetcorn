# sweetcorn Makefile

# Build configuration
BUILD_DIR := ./build
BIN_DIR := $(BUILD_DIR)/bin
COVERAGE_DIR := $(BUILD_DIR)/coverage

DIR_DEMO := ./examples/demo

###############################################################################
# Default Target
###############################################################################

.PHONY: help
help:
	@echo "sweetcorn Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make [target]"

###############################################################################
# Setup
###############################################################################

.PHONY: dev-setup
dev-setup: ## Setup development environment
	@echo "[INFO] Setting up development environment..."
	@mkdir -p $(BIN_DIR) $(COVERAGE_DIR)

.PHONY: deps
deps: ## Download and tidy dependencies
	@echo "[INFO] Downloading dependencies..."
	go mod download
	go mod tidy

.PHONY: deps-update
deps-update: ## Update all dependencies to latest versions
	@echo "[INFO] Updating dependencies to latest versions..."
	go get -u -t ./...
	go mod tidy

.PHONY: dev-tools
dev-tools: ## Install development tools
	@echo "[INFO] Installing development tools..."
	@go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest

## Install UI dependencies.
.PHONY: install-ui-deps
install-ui-deps:
	@echo "[INFO] Installing UI dependencies..."
	@pnpm install

###############################################################################
# Build
###############################################################################

# Build UI
.PHONY: build-ui
build-ui:
	@echo "[INFO] Building sweetcorn UI..."
	@pnpm --filter sweetcorn-ui run build
	@echo "[INFO] Copying web assets..."
	@cp -r ./packages/sweetcorn-ui/build ./internal/web/build

# Build sweetcorn
.PHONY: build
build: dev-setup build-ui
	@echo "[INFO] Building sweetcorn..."
	go build -o $(BIN_DIR) .

# Build demo
.PHONY: build-demo
build-demo: dev-setup
	@echo "[INFO] Building demo..."
	cd $(DIR_DEMO) && go build -o demo .

###############################################################################
# Run
###############################################################################

# Build and run sweetcorn
.PHONY: run
run: build
	@echo "[INFO] Starting sweetcorn..."
	$(BIN_DIR)/sweetcorn

# Build and run demo
.PHONY: run-demo
run-demo: build-demo
	@echo "[INFO] Starting demo..."	
	OTEL_RESOURCE_ATTRIBUTES="service.name=dice,service.version=0.1.0" ./examples/demo/demo

###############################################################################
# Testing
###############################################################################

.PHONY: test
test: dev-setup ## Run all tests
	@echo "[INFO] Running tests..."
	go test -v ./...

.PHONY: test-coverage
test-coverage: dev-setup ## Run all tests with coverage
	@echo "[INFO] Running tests with coverage..."
	go test -v -coverprofile $(COVERAGE_DIR)/coverage.out ./...
	go tool cover -html $(COVERAGE_DIR)/coverage.out -o $(COVERAGE_DIR)/coverage.html

###############################################################################
# Linting and Code Quality
###############################################################################

.PHONY: fmt
fmt: ## Format code
	@echo "[INFO] Formatting code..."
	go fmt ./...

.PHONY: vet
vet: ## Run go vet
	@echo "[INFO] Running go vet..."
	go vet ./...

.PHONY: mod-verify
mod-verify: ## Verify module dependencies
	@echo "[INFO] Verifying module dependencies..."
	go mod verify

###############################################################################
# CI/CD
###############################################################################

.PHONY: ci
ci: | install-ui-deps build-ui deps vet mod-verify test build ## Run CI pipeline ('|' operator imposes ordering)
	@echo "[INFO] CI pipeline completed"

###############################################################################
# Cleanup
###############################################################################

.PHONY: clean
clean: ## Clean build artifacts
	@echo "[INFO] Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)

###############################################################################
# Development
###############################################################################

# Run sweetcorn
.PHONY: dev-sweetcorn
dev-sweetcorn:
	@echo "[INFO] Running sweetcorn..."
	go run .

# Run demo
.PHONY: dev-demo
dev-demo:
	@echo "[INFO] Starting demo..."	
	cd $(DIR_DEMO) && OTEL_RESOURCE_ATTRIBUTES="service.name=dice,service.version=0.1.0" go run .

# Run the frontend server in development mode.
.PHONY: dev-ui
dev-ui:
	@echo "[INFO] Running frontend..."
	@fnm use && pnpm --filter sweetcorn-ui run dev
