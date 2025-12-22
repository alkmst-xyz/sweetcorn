# Makefile

.PHONY: build
build:
	@echo "[INFO] Building demo..."
	@go build -o demo .

.PHONY: run
run: build
	@echo "[INFO] Staring demo application..."
	OTEL_RESOURCE_ATTRIBUTES="service.name=dice,service.version=0.1.0" ./demo