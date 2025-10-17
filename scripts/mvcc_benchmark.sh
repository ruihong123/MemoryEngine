#!/bin/bash

# MVCC Storage Benchmark Runner
# Tests all three storage strategies with different configurations

# Default parameters
WRITERS=4
READERS=2
NUM_TUPLES=100000
DURATION=30
NODE_ID=0
TCP_PORT=19843
CACHE_SIZE=2
BUILD_DIR="cmake-build-release"

# Parse command line arguments
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
            echo "Usage: $0 [--writers N] [--readers N] [--tuples N] [--duration N] [--node_id N] [--tcp_port N] [--cache_size N] [--build_dir DIR]"
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

# Results file
RESULTS_FILE="mvcc_benchmark_results_$(date +%Y%m%d_%H%M%S).txt"

echo "========================================" | tee -a $RESULTS_FILE
echo "MVCC Storage Benchmark - All Strategies" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE
echo "Configuration:" | tee -a $RESULTS_FILE
echo "  Writers: $WRITERS" | tee -a $RESULTS_FILE
echo "  Readers: $READERS" | tee -a $RESULTS_FILE
echo "  Tuples: $NUM_TUPLES" | tee -a $RESULTS_FILE
echo "  Duration: $DURATION seconds" | tee -a $RESULTS_FILE
echo "  Cache Size: $CACHE_SIZE GB" | tee -a $RESULTS_FILE
echo "  Node ID: $NODE_ID" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE
echo "" | tee -a $RESULTS_FILE

# Strategy 1: Dedicated Delta Section
echo "========================================" | tee -a $RESULTS_FILE
echo "Strategy 1: Dedicated Delta Section" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE
$BENCHMARK_BIN \
    --storage_type 1 \
    --writers $WRITERS \
    --readers $READERS \
    --num_tuples $NUM_TUPLES \
    --duration $DURATION \
    --node_id $NODE_ID \
    --tcp_port $TCP_PORT \
    --cache_size $CACHE_SIZE 2>&1 | tee -a $RESULTS_FILE

echo "" | tee -a $RESULTS_FILE
sleep 5

# Strategy 2: Version Chain (PostgreSQL-like)
echo "========================================" | tee -a $RESULTS_FILE
echo "Strategy 2: Version Chain (PostgreSQL)" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE
$BENCHMARK_BIN \
    --storage_type 2 \
    --writers $WRITERS \
    --readers $READERS \
    --num_tuples $NUM_TUPLES \
    --duration $DURATION \
    --node_id $NODE_ID \
    --tcp_port $TCP_PORT \
    --cache_size $CACHE_SIZE 2>&1 | tee -a $RESULTS_FILE

echo "" | tee -a $RESULTS_FILE
sleep 5

# Strategy 3: Delta in SELCC Pages
echo "========================================" | tee -a $RESULTS_FILE
echo "Strategy 3: Delta in SELCC Pages" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE
$BENCHMARK_BIN \
    --storage_type 3 \
    --writers $WRITERS \
    --readers $READERS \
    --num_tuples $NUM_TUPLES \
    --duration $DURATION \
    --node_ID $NODE_ID \
    --tcp_port $TCP_PORT \
    --cache_size $CACHE_SIZE 2>&1 | tee -a $RESULTS_FILE

echo "" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE
echo "Benchmark Complete!" | tee -a $RESULTS_FILE
echo "Results saved to: $RESULTS_FILE" | tee -a $RESULTS_FILE
echo "========================================" | tee -a $RESULTS_FILE

