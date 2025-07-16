# sweetcorn Makefile

# Project information
PROJECT_NAME := sweetcorn

# Build configuration
BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin
COVERAGE_DIR := $(BUILD_DIR)/coverage

# Binary names
SWEETCORN_BIN := $(BIN_DIR)/sweetcorn

###############################################################################
# Default Target
###############################################################################

.PHONY: help
help:
	@echo "$(PROJECT_NAME) Makefile"
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
	@go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@v0.128.0

## Install UI dependencies.
.PHONY: install-ui-deps
install-ui-deps:
	@echo "[INFO] Installing UI dependencies..."
	@cd web && pnpm install

###############################################################################
# Building
###############################################################################

.PHONY: build
build: dev-setup ## Build sweetcorn binary
	@echo "[INFO] Building sweetcorn..."
	go build \
		-o $(SWEETCORN_BIN) \
		cmd/sweetcorn/main.go

# Build UI
.PHONY: build-ui
build-ui:
	@echo "[INFO] Building sweetcorn UI..."
	@cd web && pnpm build

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
ci: | deps vet mod-verify test build ## Run CI pipeline ('|' operator imposes ordering)
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

.PHONY: run
run: build ## Run the server
	@echo "[INFO] Starting sweetcorn..."
	$(SWEETCORN_BIN)

# Run the frontend server in development mode.
.PHONY: run-ui
run-ui:
	@echo "[INFO] Running frontend..."
	@cd web && fnm use && pnpm dev