BINARY = mako
VERSION = 0.1.0
LDFLAGS = -ldflags "-X github.com/Stefen-Taime/mako/internal/cli.Version=$(VERSION)"

.PHONY: build test lint clean install

build: ## Build binary
	go build $(LDFLAGS) -o bin/$(BINARY) .

install: ## Install to GOPATH/bin
	go install $(LDFLAGS) .

test: ## Run tests
	go test -v -count=1 ./...

bench: ## Run benchmarks
	go test -bench=. -benchmem ./...

lint: ## Run linters
	golangci-lint run ./...

clean: ## Clean build artifacts
	rm -rf bin/ .mako/

# ── Examples ─────────────────────────
.PHONY: example-validate example-generate example-dry-run

example-validate: build ## Validate example pipelines
	./bin/$(BINARY) validate examples/simple/pipeline.yaml
	./bin/$(BINARY) validate examples/advanced/pipeline.yaml

example-generate: build ## Generate K8s + Terraform for example
	@echo "=== Kubernetes ==="
	./bin/$(BINARY) generate examples/simple/pipeline.yaml --k8s
	@echo ""
	@echo "=== Terraform ==="
	./bin/$(BINARY) generate examples/simple/pipeline.yaml --tf

example-dry-run: build ## Dry-run with fixture data
	cat test/fixtures/events.jsonl | ./bin/$(BINARY) dry-run examples/simple/pipeline.yaml

# ── Docker ───────────────────────────
.PHONY: docker docker-runner

docker: ## Build CLI Docker image
	docker build -t mako:$(VERSION) -f Dockerfile .

docker-runner: ## Build runner Docker image
	docker build -t mako-runner:$(VERSION) -f Dockerfile.runner .

# ── Release ──────────────────────────
.PHONY: release

release: test ## Cross-compile for release
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY)-linux-amd64 .
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o bin/$(BINARY)-darwin-arm64 .
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY)-darwin-amd64 .
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY)-windows-amd64.exe .

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
