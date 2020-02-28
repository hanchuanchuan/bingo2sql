PROJECT=bingo2sql
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

GO        := GO111MODULE=on go
GOBUILD   := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)

VERSION := $(shell git describe --tags --dirty)

# 指定部分单元测试跳过
ifeq ("$(SHORT)", "1")
	GOTEST    := CGO_ENABLED=1 $(GO) test -p 3 -short
else
	GOTEST    := CGO_ENABLED=1 $(GO) test -p 3
endif

OVERALLS  := CGO_ENABLED=1 GO111MODULE=on overalls
GOVERALLS := goveralls

ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"
PACKAGE_LIST  := go list ./...| grep -vE "vendor"
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/hanchuanchuan/$(PROJECT)/||'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go" | grep -vE "vendor")

GOFAIL_ENABLE  := $$(find $$PWD/ -type d | grep -vE "(\.git|vendor)" | xargs gofail enable)
GOFAIL_DISABLE := $$(find $$PWD/ -type d | grep -vE "(\.git|vendor)" | xargs gofail disable)

# LDFLAGS += -X "github.com/hanchuanchuan/$(PROJECT)//mysql.TiDBReleaseVersion=$(shell git describe --tags --dirty)"
# LDFLAGS += -X "github.com/hanchuanchuan/$(PROJECT)//util/printer.TiDBBuildTS=$(shell date '+%Y-%m-%d %H:%M:%S')"
# LDFLAGS += -X "github.com/hanchuanchuan/$(PROJECT)//util/printer.TiDBGitHash=$(shell git rev-parse HEAD)"
# LDFLAGS += -X "github.com/hanchuanchuan/$(PROJECT)//util/printer.TiDBGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
# LDFLAGS += -X "github.com/hanchuanchuan/$(PROJECT)//util/printer.GoVersion=$(shell go version)"

CHECK_LDFLAGS += $(LDFLAGS)

.PHONY: all build update clean test gotest server check

default: server buildsucc

server-admin-check: server_check buildsucc

buildsucc:
	@echo Build TiDB Server successfully!

all: server


build:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -ldflags '-s -w $(LDFLAGS)' -o bin/$(PROJECT) cmd/bingo2sql.go

# The retool tools.json is setup from hack/retool-install.sh
check-setup:
	@which retool >/dev/null 2>&1 || go get github.com/twitchtv/retool
	@retool sync

check: check-setup fmt lint vet

# These need to be fixed before they can be ran regularly
check-fail: goword check-static check-slow

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

goword:
	retool do goword $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

check-static:
	@ # vet and fmt have problems with vendor when ran through metalinter
	CGO_ENABLED=0 retool do gometalinter.v2 --disable-all --deadline 120s \
	  --enable misspell \
	  --enable megacheck \
	  --enable ineffassign \
	  $$($(PACKAGE_DIRECTORIES))

check-slow:
	CGO_ENABLED=0 retool do gometalinter.v2 --disable-all \
	  --enable errcheck \
	  $$($(PACKAGE_DIRECTORIES))
	CGO_ENABLED=0 retool do gosec $$($(PACKAGE_DIRECTORIES))

lint:
	@echo "linting"
	@CGO_ENABLED=0 retool do revive -formatter friendly -config revive.toml $(PACKAGES)

vet:
	@echo "vet"
	@go vet -all -shadow $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

clean:
	$(GO) clean -i ./...
	rm -rf *.out

test: gotest explaintest

explaintest: server
	@cd cmd/explaintest && ./run-tests.sh -s ../../bin/$(PROJECT)

gotest:
	$(GO) get github.com/etcd-io/gofail@v0.0.0-20180808172546-51ce9a71510a
	@$(GOFAIL_ENABLE)
ifeq ("$(TRAVIS_COVERAGE)", "1")
	@echo "Running in TRAVIS_COVERAGE mode."
	@export log_level=error; \
	go get github.com/go-playground/overalls
	# go get github.com/mattn/goveralls
	# $(OVERALLS) -project=github.com/hanchuanchuan/$(PROJECT)/ -covermode=count -ignore='.git,vendor,cmd,docs,LICENSES' || { $(GOFAIL_DISABLE); exit 1; }
	# $(GOVERALLS) -service=travis-ci -coverprofile=overalls.coverprofile || { $(GOFAIL_DISABLE); exit 1; }

	$(OVERALLS) -project=github.com/hanchuanchuan/$(PROJECT)/ -covermode=count -ignore='.git,vendor,cmd,docs,LICENSES' -concurrency=1 -- -short || { $(GOFAIL_DISABLE); exit 1; }
else
	@echo "Running in native mode."
	@export log_level=error; \
	$(GOTEST) -timeout 30m -cover $(PACKAGES) || { $(GOFAIL_DISABLE); exit 1; }
endif
	@$(GOFAIL_DISABLE)

race:
	$(GO) get github.com/etcd-io/gofail@v0.0.0-20180808172546-51ce9a71510a
	@$(GOFAIL_ENABLE)
	@export log_level=debug; \
	$(GOTEST) -timeout 30m -race $(PACKAGES) || { $(GOFAIL_DISABLE); exit 1; }
	@$(GOFAIL_DISABLE)

leak:
	$(GO) get github.com/etcd-io/gofail@v0.0.0-20180808172546-51ce9a71510a
	@$(GOFAIL_ENABLE)
	@export log_level=debug; \
	$(GOTEST) -tags leak $(PACKAGES) || { $(GOFAIL_DISABLE); exit 1; }
	@$(GOFAIL_DISABLE)

tikv_integration_test:
	$(GO) get github.com/etcd-io/gofail@v0.0.0-20180808172546-51ce9a71510a
	@$(GOFAIL_ENABLE)
	$(GOTEST) ./store/tikv/. -with-tikv=true || { $(GOFAIL_DISABLE); exit 1; }
	@$(GOFAIL_DISABLE)

RACE_FLAG =
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GOBUILD   = GOPATH=$(GOPATH) CGO_ENABLED=1 $(GO) build
endif


server:
	$(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS)' -o bin/$(PROJECT) cmd/bingo2sql.go

server_check:
	$(GOBUILD) $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o bin/$(PROJECT) cmd/bingo2sql.go


update:
	which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
ifdef PKG
	dep ensure -add ${PKG}
else
	dep ensure -update
endif
	@echo "removing test files"
	dep prune
	bash ./hack/clean_vendor.sh



gofail-enable:
# Converting gofail failpoints...
	@$(GOFAIL_ENABLE)

gofail-disable:
# Restoring gofail failpoints...
	@$(GOFAIL_DISABLE)

upload-coverage: SHELL:=/bin/bash
upload-coverage:
ifeq ("$(TRAVIS_COVERAGE)", "1")
	mv overalls.coverprofile coverage.txt
	bash <(curl -s https://codecov.io/bash)
endif


# 	windows无法build,github.com/outbrain/golib有引用syslog.Writer,其在windows未实现.
.PHONY: release
release:
	@echo "$(CGREEN)Cross platform building for release ...$(CEND)"
	@mkdir -p release
	# @for GOOS in linux; do
	@for GOOS in windows darwin linux; do \
		echo "Building $${GOOS}-$${GOARCH} ..."; \
		GOOS=$${GOOS} GOARCH=amd64 $(GOBUILD) -ldflags '-s -w $(LDFLAGS)'  -o bin/$(PROJECT) cmd/bingo2sql.go; \
		cd bin; \
		tar -czf $(PROJECT)-$${GOOS}-${VERSION}.tar.gz $(PROJECT); \
		rm -f $(PROJECT); \
		cd ..; \
	done

docker:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -ldflags '-s -w $(LDFLAGS)' -o bin/$(PROJECT) cmd/bingo2sql.go
	v1=$(shell git tag | awk -F'-' '{print $1}' |tail -1) && docker build -t hanchuanchuan/$(PROJECT)/:$${v1} . \
	&& docker tag hanchuanchuan/$(PROJECT)/:$${v1} hanchuanchuan/$(PROJECT)/:latest

docker-push:
	v1=$(shell git tag|tail -1) && docker push hanchuanchuan/$(PROJECT)/:$${v1} \
	&& docker push hanchuanchuan/$(PROJECT)/:latest
