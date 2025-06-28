# sweetcorn Makefile

# Project information
PROJECT_NAME := sweetcorn

# Build configuration
BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin

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
	@mkdir -p $(BIN_DIR)

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
	@echo Installing development tools...
	@go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@v0.128.0

###############################################################################
# Building
###############################################################################

.PHONY: build
build: dev-setup ## Build sweetcorn binary
	@echo "[INFO] Building sweetcorn..."
	go build \
		-o $(SWEETCORN_BIN) \
		cmd/sweetcorn/main.go

###############################################################################
# Testing
###############################################################################
	
.PHONY: test
test: dev-setup
	@echo "[INFO] Running tests..."
	go test -v ./...

# =============================================================================
# Linting and Code Quality
# =============================================================================

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

# =============================================================================
# CI/CD
# =============================================================================

.PHONY: ci
ci: | deps vet mod-verify test build ## Run CI pipeline ('|' operator imposes ordering)
	@echo "[INFO] CI pipeline completed"

# =============================================================================
# Cleanup
# =============================================================================

.PHONY: clean
clean: ## Clean build artifacts
	@echo "[INFO] Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)