TARGET := armv7-unknown-linux-gnueabihf
PROFILE := release
BIN_NAME := client
BINARY_PATH := $(abspath target/$(TARGET)/$(PROFILE)/$(BIN_NAME))

.PHONY: all build print-binary-path clean

all: build

build:
	cross build --release --target $(TARGET)
	@printf 'Binary path: %s\n' "$(BINARY_PATH)"

print-binary-path:
	@printf '%s\n' "$(BINARY_PATH)"

clean:
	cargo clean
