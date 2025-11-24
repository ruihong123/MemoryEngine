# MVCC Storage Benchmark for SELCC

## Overview

This benchmark compares three different multi-version concurrency control (MVCC) storage strategies in the SELCC distributed storage engine:

### Storage Strategies

1. **Dedicated Delta Section (Type 1)**
   - Current MVOCC implementation
   - Stores undo logs in a dedicated delta section (separate from data pages)
   - Minimal interference with data page caching
   - Low cache invalidation overhead

2. **Version Chain (Type 2)**
   - PostgreSQL-style approach
   - Each update creates a new tuple at a new address
   - Forms a version chain from newest to oldest
   - Index must be updated to point to the latest version
   - High space overhead but straightforward version traversal

3. **Delta in SELCC Pages (Type 3)**
   - Stores undo segments in regular SELCC-managed pages
   - Every read/write to delta pages triggers SELCC invalidation messages
   - Tests worst-case scenario for cache coherence overhead
   - High invalidation message count

## Workload

The benchmark simulates a workload with:
- **Write threads**: Continuously create new versions of tuples
- **Read threads**: Access data with old snapshots (snapshot_ts < commit_ts - 10000)

This workload stresses the multi-version storage system by:
- Creating many versions quickly (write threads)
- Requiring access to old versions (read threads with lagging snapshots)

## Building

```bash
cd /home/ruihong/Projects/MemoryEngine
mkdir -p cmake-build-release
cd cmake-build-release
cmake .. -DCMAKE_BUILD_TYPE=Release
make mvcc_storage_bench
```

## Running the Benchmark

### Single Run

Run one storage strategy:

```bash
./scripts/mvcc_benchmark_single.sh <storage_type> [options]
```

**Storage Types:**
- `1` - Dedicated Delta Section
- `2` - Version Chain (PostgreSQL-like)
- `3` - Delta in SELCC Pages

**Options:**
- `--writers N` - Number of writer threads (default: 4)
- `--readers N` - Number of reader threads (default: 2)
- `--tuples N` - Number of tuples (default: 100000)
- `--duration N` - Benchmark duration in seconds (default: 30)
- `--node_id N` - Node ID (default: 0)
- `--tcp_port N` - TCP port (default: 19843)
- `--cache_size N` - Cache size in GB (default: 2)

**Example:**
```bash
./scripts/mvcc_benchmark_single.sh 1 --writers 8 --readers 4 --duration 60
```

### Compare All Strategies

Run all three strategies sequentially:

```bash
./scripts/mvcc_benchmark.sh [options]
```

This will run all three storage strategies with the same configuration and save results to a timestamped file.

**Example:**
```bash
./scripts/mvcc_benchmark.sh --writers 8 --readers 4 --duration 30
```

### Direct Execution

You can also run the benchmark binary directly:

```bash
./cmake-build-release/mvcc_storage_bench \
    --storage_type 1 \
    --writers 4 \
    --readers 2 \
    --num_tuples 100000 \
    --duration 30 \
    --node_id 0 \
    --tcp_port 19843 \
    --cache_size 2
```

## Output Metrics

The benchmark reports:

### Throughput
- Total write operations
- Total read operations  
- Write throughput (ops/sec)
- Read throughput (ops/sec)
- Total throughput (ops/sec)

### Latency
- Average write latency (nanoseconds and microseconds)
- Average read latency (nanoseconds and microseconds)

### Storage-Specific Metrics
- **Version Chain**: Average version chain traversals per read
- **Delta Section/Delta in Page**: Number of delta applications

### Cache Statistics
- Cache invalidations (total)
- Cache hits (valid)
- Invalidation rate (percentage)

## Expected Results

### Dedicated Delta Section (Type 1)
- **Pros**: Low cache invalidation rate, good read/write throughput
- **Cons**: Requires separate delta section management

### Version Chain (Type 2)
- **Pros**: Simple version management, no delta application needed
- **Cons**: High space overhead, index update overhead, longer version chain traversals for old snapshots

### Delta in SELCC Pages (Type 3)
- **Pros**: No separate delta section needed
- **Cons**: **Very high cache invalidation rate**, severe performance degradation due to SELCC invalidation messages

## Key Observations

The benchmark is designed to demonstrate:

1. **Cache Invalidation Impact**: Type 3 should show significantly higher invalidation rates compared to Types 1 and 2

2. **Version Access Cost**: Type 2 will show increasing cost for accessing old versions (version chain traversal), while Type 1 has more predictable delta application cost

3. **Space vs. Time Tradeoff**: Type 2 uses more space (new tuple for each version) but may have better locality for recent versions. Type 1 uses less space but requires delta application.

4. **Scalability**: As the number of writer threads increases, Type 3's performance should degrade more rapidly due to SELCC invalidation contention

## Tuning Parameters

### For High Contention Testing:
```bash
--writers 16 --readers 8 --num_tuples 10000
```
Small tuple count with many threads creates high contention.

### For Long Version Chain Testing:
```bash
--writers 8 --readers 2 --duration 120
```
More writers and longer duration create longer version chains.

### For Cache Pressure Testing:
```bash
--num_tuples 1000000 --cache_size 1
```
Large tuple count with small cache increases cache misses.

## Multi-Node Execution

For distributed testing, start memory nodes first, then compute nodes:

**Memory Node:**
```bash
./build/memory_server --node_id 0 --tcp_port 19843
```

**Compute Node:**
```bash
./scripts/mvcc_benchmark_single.sh 1 --node_id 2 --tcp_port 19843
```

## Troubleshooting

### Build Errors
- Make sure all dependencies are installed
- Check that CMake version is 3.10+
- Verify that ibverbs, boost, and libmemcached are available

### Runtime Errors
- Ensure memory nodes are running before starting compute nodes
- Check that ports are not already in use
- Verify sufficient memory is available for the cache size

### Low Performance
- Check CPU affinity with `htop` - threads should be bound to separate cores
- Monitor network bandwidth if using distributed setup
- Increase cache size if cache hit rate is too low

## Code Structure

- `test/benchmark_mvcc_storage.cpp` - Main benchmark implementation
- `scripts/mvcc_benchmark.sh` - Script to run all strategies
- `scripts/mvcc_benchmark_single.sh` - Script to run single strategy

## Future Improvements

Potential enhancements to the benchmark:

1. **Workload Patterns**: Add support for Zipfian distribution, read-heavy vs. write-heavy workloads
2. **Garbage Collection**: Implement and measure GC overhead for each strategy
3. **Mixed Snapshots**: Test with varying snapshot lags
4. **Index Updates**: Measure index update overhead for Version Chain strategy
5. **Network Metrics**: Collect RDMA operation counts and bandwidth usage

