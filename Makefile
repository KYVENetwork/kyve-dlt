COMMIT := $(shell git log -1 --format='%H')
VERSION := 1.0.0 # $(shell echo $(shell git describe --tags) | sed 's/^v//')

ldflags = -X github.com/KYVENetwork/KYVE-DLT/cmd/dlt/commands.Version=$(VERSION) \
		  -X github.com/KYVENetwork/KYVE-DLT/cmd/dlt/commands.Commit=$(COMMIT) \
		  -s \
		  -w

BUILD_FLAGS := -ldflags '$(ldflags)' -trimpath -buildvcs=false

.PHONY: build format lint release

all: format lint build

###############################################################################
###                                  Build                                  ###
###############################################################################

build:
	@echo "🤖 Building KYVE-DLT ..."
	@CGO_ENABLED=0 go build $(BUILD_FLAGS) -o "$(PWD)/build/" ./cmd/dlt
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

	@GOOS=linux GOARCH=amd64 go build $(BUILD_FLAGS) ./cmd/dlt
	@tar -czf release/dlt_linux_amd64.tar.gz dlt
	@shasum -a 256 release/dlt_linux_amd64.tar.gz >> release/release_checksum

	@rm dlt
	@echo "✅ Completed release creation!"