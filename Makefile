TARGET = ./cc-energy-manager
VERSION = 0.0.1
GIT_HASH := $(shell git rev-parse --short HEAD || echo 'development')
CURRENT_TIME = $(shell date +"%Y-%m-%d:T%H:%M:%S")
LD_FLAGS = '-s -X main.date=${CURRENT_TIME} -X main.version=${VERSION} -X main.commit=${GIT_HASH}'

EXECUTABLES = go
K := $(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),some string,$(error "No $(exec) in PATH")))

.PHONY: clean distclean test tags $(TARGET)

.NOTPARALLEL:

$(TARGET):
	$(info ===>  BUILD cc-energy-manager)
	@go build -ldflags=${LD_FLAGS} ./cmd/cc-energy-manager

clean:
	$(info ===>  CLEAN)
	@go clean
	@rm -f $(TARGET)

distclean:
	@$(MAKE) clean
	$(info ===>  DISTCLEAN)

test:
	$(info ===>  TESTING)
	@go clean -testcache
	@go build ./...
	@go vet ./...
	@go test ./...

tags:
	$(info ===>  TAGS)
	@ctags -R
