BUILD_PATH := cmd/quafka/quafka
DOCKER_TAG := latest

all: test

deps:
	@go mod download
	@go mod tidy

vet:
	@go vet ./...

lint:
	@golangci-lint run

build:
	@go build -o $(BUILD_PATH) cmd/quafka/main.go

release:
	@which goreleaser 2>/dev/null || go install github.com/goreleaser/goreleaser@latest
	@goreleaser

clean:
	@rm -rf dist

build-docker:
	@docker build -t bodaay/quafka:$(DOCKER_TAG) .

generate:
	@go generate

test:
	@go test -v ./...

test-race:
	@go test -v -race -p=1 ./...

.PHONY: test-race test build-docker clean release build deps vet lint all
