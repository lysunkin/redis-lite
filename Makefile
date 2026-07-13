# ---------------------------------------------------------------------------
# redis-lite Makefile
# ---------------------------------------------------------------------------

BINARY     := redis-lite
BUILD_DIR  := bin
GO         := go
GOLINT     := golangci-lint

# All Go packages in the module
PKGS := ./...

.PHONY: all build test lint run install-lint clean help

## all: build + test + lint (default target)
all: build test lint

## build: compile the binary into ./bin/redis-lite
build:
	@mkdir -p $(BUILD_DIR)
	$(GO) build -o $(BUILD_DIR)/$(BINARY) .

## test: run all tests (no watch mode; use -v for verbose output)
test:
	$(GO) test $(PKGS) -race -count=1

## test-v: same as test but with verbose output
test-v:
	$(GO) test $(PKGS) -race -count=1 -v

## lint: run golangci-lint
lint:
	$(GOLINT) run $(PKGS)

## install-lint: install golangci-lint via Homebrew (macOS)
install-lint:
	brew install golangci-lint

## run: build and start the server
run: build
	./$(BUILD_DIR)/$(BINARY)

## clean: remove compiled artifacts
clean:
	@rm -rf $(BUILD_DIR)

## help: list all available targets
help:
	@grep -E '^##' Makefile | sed 's/## /  /'
