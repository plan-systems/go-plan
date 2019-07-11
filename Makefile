MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -euc
.DEFAULT_GOAL := build

.PHONY: *

protobufs:
	./build-protobufs --protos ../plan-protobufs/pkg --dest .

setup: protobufs
	go get ./...

build: build/pdi build/plan build/repo build/ski build/tools
build/%:
	go build ./$*

test: test/pdi test/plan test/repo test/ski test/tools
test/%:
	go test -v ./$*
