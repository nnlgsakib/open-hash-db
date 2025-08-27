.PHONY: all build clean

# Define the output directory and application name
BUILD_DIR := bin
APP_NAME := openhash$(if $(filter Windows_NT,$(OS)),.exe,)
BUILD_PATH := $(BUILD_DIR)/$(APP_NAME)

# Detect OS and set platform-specific commands
ifeq ($(OS),Windows_NT)
    RM_DIR := rmdir /s /q
    MKDIR_P := mkdir
    SHELL := cmd.exe
else
    RM_DIR := rm -rf
    MKDIR_P := mkdir -p
    SHELL := /bin/sh
endif

# Detect GOOS and GOARCH if not set
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

# Get Git metadata with fallbacks for robustness
ifeq ($(OS),Windows_NT)
    GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>nul || echo unknown)
    GIT_AUTHOR := $(shell git config user.name 2>nul || echo unknown)
    GIT_VERSION := $(shell echo $(GIT_BRANCH) | findstr /R "v[0-9]*\.[0-9]*\.[0-9]*" 2>nul | for /f "tokens=1 delims=_" %%i in ('more') do @echo %%i)
    ifeq ($(GIT_VERSION),)
        GIT_VERSION := v0.0.0-dev
    endif
else
    GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    GIT_AUTHOR := $(shell git config user.name 2>/dev/null || echo "unknown")
    GIT_VERSION := $(shell echo $(GIT_BRANCH) | grep -o "v[0-9]*\.[0-9]*\.[0-9]*" 2>/dev/null || echo "v0.0.0-dev")
endif

# Default target
all: build

# Build target depends on clean for a fresh build
build: clean $(BUILD_PATH)

$(BUILD_PATH):
	@echo Building $(APP_NAME) for $(GOOS)/$(GOARCH)...
	$(MKDIR_P) "$(BUILD_DIR)"
ifeq ($(OS),Windows_NT)
	set "GOOS=$(GOOS)" && set "GOARCH=$(GOARCH)" && go build -o "$(BUILD_PATH)" -ldflags="-X 'openhashdb/version.Version=$(GIT_VERSION)' -X 'openhashdb/version.Author=$(GIT_AUTHOR)' -X 'openhashdb/version.Branch=$(GIT_BRANCH)'" ./cmd/...
else
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o "$(BUILD_PATH)" -ldflags="-X 'openhashdb/version.Version=$(GIT_VERSION)' -X 'openhashdb/version.Author=$(GIT_AUTHOR)' -X 'openhashdb/version.Branch=$(GIT_BRANCH)'" ./cmd/...
endif
	@echo Build complete: $(BUILD_PATH)

clean:
	@echo Cleaning build artifacts...
ifeq ($(OS),Windows_NT)
	@if exist "$(BUILD_DIR)" $(RM_DIR) "$(BUILD_DIR)"
else
	@if [ -d "$(BUILD_DIR)" ]; then $(RM_DIR) "$(BUILD_DIR)"; fi
endif
	@echo Clean complete.

.PHONY: proto
proto:
	@echo Generating protobuf Go files into protobuf/pb
	cd protobuf && protoc -I=. --go_out=paths=source_relative:pb *.proto
	@echo Proto generation complete.
