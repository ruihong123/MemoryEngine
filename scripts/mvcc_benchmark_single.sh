#!/bin/bash

# Single run of MVCC Storage Benchmark
# Usage: ./mvcc_benchmark_single.sh <storage_type> [options]

if [ $# -lt 1 ]; then
    echo "Usage: $0 <storage_type> [--writers N] [--readers N] [--tuples N] [--duration N] [--node_id N] [--tcp_port N] [--cache_size N]"
    echo ""
    echo "Storage Types:"
    echo "  1 - Dedicated Delta Section (Current MVOCC)"
    echo "  2 - Version Chain (PostgreSQL-like)"
    echo "  3 - Delta in SELCC Pages"
    exit 1
fi

STORAGE_TYPE=$1
shift

# Default parameters
WRITERS=4
READERS=2
NUM_TUPLES=100000
DURATION=30
NODE_ID=0
TCP_PORT=19843
CACHE_SIZE=2
BUILD_DIR="cmake-build-release"

# Parse remaining arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --writers)
            WRITERS="$2"
            shift 2
            ;;
        --readers)
            READERS="$2"
            shift 2
            ;;
        --tuples)
            NUM_TUPLES="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --node_id)
            NODE_ID="$2"
            shift 2
            ;;
        --tcp_port)
            TCP_PORT="$2"
            shift 2
            ;;
        --cache_size)
            CACHE_SIZE="$2"
            shift 2
            ;;
        --build_dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

BENCHMARK_BIN="${BUILD_DIR}/mvcc_storage_bench"

# Check if benchmark binary exists
if [ ! -f "$BENCHMARK_BIN" ]; then
    echo "Error: Benchmark binary not found at $BENCHMARK_BIN"
    echo "Please build the project first:"
    echo "  cd $BUILD_DIR && make mvcc_storage_bench"
    exit 1
fi

# Run benchmark
$BENCHMARK_BIN \
    --storage_type $STORAGE_TYPE \
    --writers $WRITERS \
    --readers $READERS \
    --num_tuples $NUM_TUPLES \
    --duration $DURATION \
    --node_id $NODE_ID \
    --tcp_port $TCP_PORT \
    --cache_size $CACHE_SIZE

