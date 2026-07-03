Lakestore 
Plan 
— Phase 1: LSM-Tree Storage Engine
— Phase 2: Raft
— Phase 3: Connecting the 2 layers


## 1. Core Components Implemented

### 1.1 MemTable & SkipList
- **Lock-Free Reads / Serialized Writes:** The core memory structure is a custom SkipList. It allows fast, lock-free concurrent reads while writes are serialized under a mutex to prevent circular pointer corruption.
- **Custom Memory Allocation:** Uses placement-new and byte-aligned memory allocation for SkipList nodes to maximize CPU cache locality and prevent memory fragmentation.
- **Internal Keys:** User keys are wrapped into `InternalKey` structures containing the user string, a 64-bit Sequence Number, and a 1-byte Operation Type (Put/Delete). This enables Multi-Version Concurrency Control (MVCC) and point-in-time reads.

### 1.2 Write-Ahead Log (WAL)
- **Binary Persistence:** Before any data is placed in RAM, it is sequentially appended to a `.log` file on disk.
- **Custom Encoding:** Operations are tightly packed using Varint-32 encoding for lengths to save disk space and maximize I/O throughput.
- **Batching:** Multiple operations can be grouped into a `WriteBatch` with a single sequence number block and atomic disk flush.

### 1.3 Concurrency & Thread Safety
- **State Mutex:** A central `std::mutex` and `std::condition_variable` govern the engine state.
- **Immutable Rotation:** When the active MemTable exceeds its size threshold (64MB), it is rotated to a read-only `Immutable MemTable` (`imm_`) while a background thread is signaled to flush it to disk. 

### 1.4 Binary Parsers & Utilities
- **Slice:** A lightweight pointer+length struct used to pass string data around without triggering deep copies.
- **Coding Helpers:** Safe `GetVarint32` and `PutVarint32` functions with strict bounds-checking to prevent segmentation faults when reading corrupted data.

### 1.5 Testing & Tooling
- **Google Test Suite:** 20 comprehensive unit tests covering CRUD ops, MVCC, durability, recovery, thread-safety, and iterators.
- **Google Benchmark:** Integrated micro-benchmarking suite to measure raw throughput of Sequential Writes, Random Writes, and Random Reads.

---

## 2. Data Flow & Lifecycle

### 2.1 The Write Path (Memory)
When a user calls `db->Put("key", "value")`:
1. **Batch Creation:** The operation is added to a `WriteBatch`.
2. **Lock Acquisition:** The engine acquires the main `mutex_`. If an immutable MemTable is currently stalling the system waiting for a disk flush, the thread waits.
3. **Sequence Assignment:** The engine calculates the number of operations in the batch and fetches the next available 64-bit Sequence Number.
4. **WAL Append:** The batch payload (`[Seq][OpCount][Type][KeyLen][Key][ValLen][Val]`) is written to the `.log` file. (If `sync=true`, a physical `fdatasync` is called).
5. **MemTable Insertion:** The payload is parsed, and each operation is inserted into the SkipList with its unique sequence number.
6. **Threshold Check:** If the MemTable exceeds 64MB, it is frozen, a new one is allocated, and a background compaction is scheduled.
7. **Unlock:** The mutex is released, and `Status::OK()` is returned.

### 2.2 The Read Path
When a user calls `db->Get("key")`:
1. The engine constructs a lookup key containing the user's key and the *maximum possible sequence number* (to ensure the latest version is read).
2. It queries the active **MemTable**.
3. If not found, it queries the **Immutable MemTable** (if one exists).
4. *(Phase 2 scope)* If not found, it checks the Bloom Filters of on-disk **SSTables** (Level 0 downwards) and reads the data block if a match is found.

### 2.3 Deletions (Tombstones)
Deletions are handled as "Writes". Calling `db->Delete("key")` inserts a new entry into the MemTable with the `kTypeDeletion` flag (a Tombstone). During a read, if the engine encounters a Tombstone for a key before finding the actual value, it immediately returns `NotFound`.

### 2.4 System Crash & Recovery
If the system crashes (or restarts normally):
1. `DBImpl::Open` is called.
2. The `MANIFEST` file is read to determine the current database state and valid file numbers.
3. A new, empty MemTable is allocated.
4. The system scans the directory for `.log` files, sorts them by ID, and sequentially reads the binary payloads.
5. The `DecodeAndInsert` pipeline replays every single operation back into the MemTable, perfectly restoring the state of the database to the exact millisecond before the crash.

---

## 3. Current Benchmarks
<img width="1210" height="321" alt="LakeStore-benchmark" src="https://github.com/user-attachments/assets/6fcbbbfa-0959-4d7f-8a9c-f8236b9cd047" />


--

## 4. Executing/testing:

```sh
mkdir -p build
cd build

# Configure the project for maximum performance (Release mode)
cmake -DCMAKE_BUILD_TYPE=Release ..

# Compile on macOS:
make -j$(sysctl -n hw.ncpu)

# OR Compile on Linux:
make -j$(nproc)

./lakestore_main

./lakestore_test

./lakestore_bench

```