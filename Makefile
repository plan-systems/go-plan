MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -euc
.DEFAULT_GOAL := build
GOPATH := $(abspath $(shell pwd)/../../../..)

help:
	@echo -e "\033[32m"
	@echo "This project uses glide to manage its dependencies. Download the"
	@echo "latest version from https://github.com/Masterminds/glide/releases"
	@echo "Targets in this Makefile will set the GOPATH appropriately if the"
	@echo "repository is within ./src/github.com/plan-systems/plan-core"
	@echo "Otherwise... good luck."
	@echo "GOPATH=$(GOPATH)"

# ----------------------------------------
# working environment

GLIDE_ERR := "You need glide installed to set up this project. Download the latest version from https://github.com/Masterminds/glide/releases"
check-glide:
	@command -v glide || { echo $(GLIDE_ERR) ; exit 1;}

setup: check-glide glide.lock

glide.lock: glide.yaml
	mkdir -p vendor
	GOPATH=$(GOPATH) glide up

build: build/ski build/pnode glide.lock

build/ski:
	GOPATH=$(GOPATH) go build -o ski ./cmd/ski/main.go

build/pnode:
	GOPATH=$(GOPATH) go build -o pnode ./cmd/pnode/main.go


# ----------------------------------------
# demo

run:
	GOPATH=$(GOPATH) go run main.go

# set a single package to test by passing the PKG variable
PKG ?=
ifeq ($(PKG), )
PKG := ./...
else
PKG := github.com/plan-systems/plan-core/$(PKG)
endif

test:
	GOPATH=$(GOPATH) go test -v $(PKG)
