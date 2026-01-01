# sweetcorn Makefile

# Build configuration
BUILD_DIR := ./build
BIN_DIR := $(BUILD_DIR)/bin
COVERAGE_DIR := $(BUILD_DIR)/coverage

DIR_SWEETCORN := ./src/sweetcorn
DIR_DEMO := ./src/demo

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
	cd $(DIR_SWEETCORN) && go mod download
	cd $(DIR_SWEETCORN) && go mod tidy

.PHONY: deps-update
deps-update: ## Update all dependencies to latest versions
	@echo "[INFO] Updating dependencies to latest versions..."
	cd $(DIR_SWEETCORN) && go get -u -t ./...
	cd $(DIR_SWEETCORN) && go mod tidy

.PHONY: dev-tools
dev-tools: ## Install development tools
	@echo "[INFO] Installing development tools..."
	@go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest

## Install UI dependencies.
.PHONY: install-ui-deps
install-ui-deps:
	@echo "[INFO] Installing UI dependencies..."
	@fnm use && pnpm install

###############################################################################
# Build
###############################################################################

# Build UI
.PHONY: build-ui
build-ui:
	@echo "[INFO] Building sweetcorn UI..."
	@fnm use && pnpm --filter sweetcorn-ui run build
	@echo "[INFO] Copying web assets..."
	@cp -r ./src/sweetcorn-ui/build ./src/sweetcorn/internal/web/build

# Build sweetcorn
.PHONY: build
build: dev-setup build-ui
	@echo "[INFO] Building sweetcorn..."
	go build -o $(BIN_DIR) $(DIR_SWEETCORN)

# Build demo
.PHONY: build-demo
build-demo: dev-setup
	@echo "[INFO] Building demo..."
	go build -o $(BIN_DIR) $(DIR_DEMO)

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
	OTEL_RESOURCE_ATTRIBUTES="service.name=dice,service.version=0.1.0" $(BIN_DIR)/demo

###############################################################################
# Testing
###############################################################################

.PHONY: test
test: dev-setup ## Run all tests
	@echo "[INFO] Running tests..."
	cd $(DIR_SWEETCORN) && go test -v ./...

.PHONY: test-coverage
test-coverage: dev-setup ## Run all tests with coverage
	@echo "[INFO] Running tests with coverage..."
	cd $(DIR_SWEETCORN) && go test -v -coverprofile ../../$(COVERAGE_DIR)/coverage.out ./...
	cd $(DIR_SWEETCORN) && go tool cover -html ../../$(COVERAGE_DIR)/coverage.out -o ../../$(COVERAGE_DIR)/coverage.html

###############################################################################
# Linting and Code Quality
###############################################################################

.PHONY: fmt
fmt: ## Format code
	@echo "[INFO] Formatting code..."
	cd $(DIR_SWEETCORN) && go fmt ./...

.PHONY: vet
vet: ## Run go vet
	@echo "[INFO] Running go vet..."
	cd $(DIR_SWEETCORN) && go vet ./...

.PHONY: mod-verify
mod-verify: ## Verify module dependencies
	@echo "[INFO] Verifying module dependencies..."
	cd $(DIR_SWEETCORN) && go mod verify

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
	go run ./src/sweetcorn

# Run demo
.PHONY: dev-demo
dev-demo:
	@echo "[INFO] Starting demo..."	
	OTEL_RESOURCE_ATTRIBUTES="service.name=dice,service.version=0.1.0" go run ./src/sweetcorn

# Run the frontend server in development mode.
.PHONY: dev-ui
dev-ui:
	@echo "[INFO] Running frontend..."
	@fnm use && pnpm --filter sweetcorn-ui run dev
