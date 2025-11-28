# Usage Guide for run_benchmarks_default.sh

## Overview
This script runs TPCC, TATP, and SmallBank benchmarks with default query ratios. You can run all three benchmarks or specify a single one.

## Basic Usage

### Run All Three Benchmarks (Default Configuration)
```bash
./run_benchmarks_default.sh
```
- Runs TPCC, TATP, and SmallBank sequentially
- Hot table scanner (long-running scan queries): **DISABLED** by default
- File logging: **ENABLED** by default (outputs to log files)

### Run a Single Benchmark
```bash
./run_benchmarks_default.sh tpcc
./run_benchmarks_default.sh tatp
./run_benchmarks_default.sh smallbank
```

## Command Line Options

### Control Hot Table Scanner (Long-Running Scan Queries)
The hot table scanner enables long-running scan queries that run concurrently with transactions.

**Enable hot table scanner:**
```bash
./run_benchmarks_default.sh --hot
./run_benchmarks_default.sh tpcc --hot
```

**Disable hot table scanner (default):**
```bash
./run_benchmarks_default.sh --no-hot
./run_benchmarks_default.sh tpcc --no-hot
```

### Control File Logging

**Disable file logging (output to /dev/null):**
```bash
./run_benchmarks_default.sh --no-log
./run_benchmarks_default.sh tpcc --no-log
```

**Note:** By default, file logging is enabled and outputs are written to:
- `${output_dir}/tpcc_default.log`
- `${output_dir}/tatp_default.log`
- `${output_dir}/smallbank_default.log`

## Complete Examples

### Run All Benchmarks WITHOUT Hot Table Scanner and WITHOUT Logging
```bash
./run_benchmarks_default.sh --no-hot --no-log
```

### Run All Benchmarks WITH Hot Table Scanner and WITHOUT Logging
```bash
./run_benchmarks_default.sh --hot --no-log
```

### Run All Benchmarks in Both Configurations
To run all three benchmarks with and without hot table scanner (both without logging):

```bash
# Run all benchmarks WITHOUT hot table scanner
./run_benchmarks_default.sh --no-hot --no-log

# Run all benchmarks WITH hot table scanner
./run_benchmarks_default.sh --hot --no-log
```

### Run TPCC Only (Both Configurations)
```bash
# Without hot table scanner
./run_benchmarks_default.sh tpcc --no-hot --no-log

# With hot table scanner
./run_benchmarks_default.sh tpcc --hot --no-log
```

### Run TATP Only (Both Configurations)
```bash
# Without hot table scanner
./run_benchmarks_default.sh tatp --no-hot --no-log

# With hot table scanner
./run_benchmarks_default.sh tatp --hot --no-log
```

### Run SmallBank Only (Both Configurations)
```bash
# Without hot table scanner
./run_benchmarks_default.sh smallbank --no-hot --no-log

# With hot table scanner
./run_benchmarks_default.sh smallbank --hot --no-log
```

## Understanding the Options

1. **Hot Table Scanner (`--hot` / `--no-hot`)**: 
   - Controls whether long-running scan queries run concurrently with transactions
   - Default: **disabled** (`--no-hot`)
   - This is the feature you asked about for "with and without long running scan query"

2. **File Logging (`--no-log`)**:
   - When disabled, all output is redirected to `/dev/null`
   - When enabled (default), outputs are saved to log files in `${output_dir}/`
   - Note: Application-level redo logging is disabled by default (controlled by `-log` flag, which is not added by this script)

3. **Benchmark Selection**:
   - If no benchmark is specified, all three run sequentially
   - Specify `tpcc`, `tatp`, or `smallbank` to run only that benchmark

## Configuration

The script uses these default parameters:
- Threads: 8
- Warehouses (TPCC): 8
- Transactions: 1,000,000
- Cache memory: 8 GB
- Remote memory: 55 GB per node
- Port: Random port starting from 13000

These are hardcoded in the script but can be modified if needed.

## Output Locations

By default, log files are written to:
- `${output_dir}` (default: `/users/Ruihong/MemoryEngine/scripts/data`)

Log files:
- `tpcc_default.log`
- `tatp_default.log`
- `smallbank_default.log`

When `--no-log` is used, no files are created (output goes to `/dev/null`).

