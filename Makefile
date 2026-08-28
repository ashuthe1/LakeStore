# LakeStore — build, test, and run everything from here.
#
#   make engine        build the C++ storage engine (required before Go builds)
#   make build         build lakestored + lakectl into ./bin
#   make test          run the full test matrix (C++ engine, raft, cgo, cluster)
#   make bench         run storage engine microbenchmarks
#   make cluster-up    start a local 3-node cluster (data under ./data)
#   make cluster-down  stop it
#   make clean         remove build artifacts, binaries, and local cluster data

NPROC := $(shell sysctl -n hw.ncpu 2>/dev/null || nproc)
ENGINE_BUILD := storage-engine/build

.PHONY: all engine engine-test engine-bench build test go-test bench \
        cluster-up cluster-down clean proto

all: engine build

# --- C++ storage engine ---

engine:
	cmake -S storage-engine -B $(ENGINE_BUILD) -DCMAKE_BUILD_TYPE=Release
	cmake --build $(ENGINE_BUILD) -j$(NPROC)

engine-test: engine
	cd $(ENGINE_BUILD) && ./lakestore_test

engine-bench: engine
	cd $(ENGINE_BUILD) && ./lakestore_bench

# --- Go: raft, server, client ---

build: engine
	go build -o bin/lakestored ./cmd/lakestored
	go build -o bin/lakectl ./cmd/lakectl

go-test: engine
	go test ./raft/ -race -timeout 300s
	go test ./engine/ ./client/ -timeout 120s
	go test ./server/ -timeout 300s

test: engine-test go-test
	@echo "--------------------------------------"
	@echo "All test suites passed."

bench: engine-bench

# Regenerate protobuf/gRPC stubs (needs protoc, protoc-gen-go, protoc-gen-go-grpc)
proto:
	PATH=$$PATH:$$HOME/go/bin protoc \
	  --go_out=api/pb --go_opt=paths=source_relative \
	  --go-grpc_out=api/pb --go-grpc_opt=paths=source_relative \
	  --proto_path=api api/lakestore.proto

# --- Local cluster ---

cluster-up: build
	./scripts/run-cluster.sh

cluster-down:
	./scripts/stop-cluster.sh

clean:
	rm -rf $(ENGINE_BUILD) storage-engine/build-asan storage-engine/build-tsan
	rm -rf bin data
	go clean ./...
