MAKEFLAGS += --warn-undefined-variables
SHELL = /bin/bash -o nounset -o errexit -o pipefail
.DEFAULT_GOAL = build

.PHONY: *

# ----------------------------------------
# dev/test

protobufs:
	./build-protobufs.sh --protos ../plan-protobufs/pkg --dest .

check: fmt lint test

fmt:
	go fmt ./...

lint:
	go vet ./...

test:
	go test -v ./...

SUBPACKAGES := build/client build/pdi build/plan build/repo build/ski build/tools

build: $(SUBPACKAGES)
build/%:
	go build ./$*

# ----------------------------------------
# multi-repo workspace

# uncomments 'replace' flags in go.mod so that we can use a locally
# cloned version of plan repo dependencies for development. marks
# changes to the go.mod and go.sum files as temporarily ignored by
# git as well.
hack:
	sed -i'' 's~^// replace~replace~' go.mod
	git update-index --assume-unchanged go.mod
	git update-index --assume-unchanged go.sum

# comments-out 'replace' flags in go.mod to undo the work above
unhack:
	sed -i'' 's~^replace~// replace~' go.mod
	git update-index --no-assume-unchanged go.mod
	git update-index --no-assume-unchanged go.sum
