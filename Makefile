BUILD_PATH := cmd/jocko/jocko
DOCKER_TAG := latest

all: test

deps:
	@go mod download
	@go mod tidy

vet:
	@go vet ./...

build:
	@go build -o $(BUILD_PATH) cmd/jocko/main.go

release:
	@which goreleaser 2>/dev/null || go install github.com/goreleaser/goreleaser@latest
	@goreleaser

clean:
	@rm -rf dist

build-docker:
	@docker build -t travisjeffery/jocko:$(DOCKER_TAG) .

generate:
	@go generate

test:
	@go test -v ./...

test-race:
	@go test -v -race -p=1 ./...

.PHONY: test-race test build-docker clean release build deps vet all
