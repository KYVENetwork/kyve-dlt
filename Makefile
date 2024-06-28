COMMIT := $(shell git log -1 --format='%H')
VERSION := 0.2 # $(shell echo $(shell git describe --tags) | sed 's/^v//')

ldflags = -X main.AppName=dlt \
		  -X main.Version=$(VERSION) \
		  -X main.Commit=$(COMMIT)

BUILD_FLAGS := -ldflags '$(ldflags)' -trimpath

.PHONY: build format lint release

all: format lint build

###############################################################################
###                                  Build                                  ###
###############################################################################

build:
	@echo "🤖 Building KYVE-DLT ..."
	@go build $(BUILD_FLAGS) -o "$(PWD)/build/" ./cmd/dlt
	@echo "✅ Completed build!"

###############################################################################
###                          Formatting & Linting                           ###
###############################################################################

format:
	@echo "🤖 Running formatter..."
	@gofmt -l -w .
	@echo "✅ Completed formatting!"

lint:
	@echo "🤖 Running linter..."
	@golangci-lint run --timeout=10m
	@echo "✅ Completed linting!"

release:
	@echo "🤖 Creating KYVE DLT releases..."
	@rm -rf release
	@mkdir -p release

	@GOOS=darwin GOARCH=arm64 go build $(BUILD_FLAGS) ./cmd/dlt
	@tar -czf release/dlt_darwin_arm64.tar.gz dlt
	@shasum -a 256 release/dlt_darwin_arm64.tar.gz >> release/release_checksum

	@GOOS=linux GOARCH=arm64 go build $(BUILD_FLAGS) ./cmd/dlt
	@tar -czf release/dlt_linux_arm64.tar.gz dlt
	@shasum -a 256 release/dlt_linux_arm64.tar.gz >> release/release_checksum

	@rm dlt
	@echo "✅ Completed release creation!"