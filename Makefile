.PHONY: deps fmt build

LOCALS := $(shell find . -name "*.go")

.EXPORT_ALL_VARIABLES:
GO111MODULE := on

all: deps fmt build

deps:
	go get ./...

fmt:
	gofmt -w $(LOCALS)
	go vet ./...

build:
	go build -i -o bin/qcat ./cmd/qcat/