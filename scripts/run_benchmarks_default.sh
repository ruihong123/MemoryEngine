#!/bin/bash
# Testing script to run TPCC, TATP, and SmallBank benchmarks with default query ratios
# Usage: ./run_benchmarks_default.sh [benchmark_name] [--hot] [--no-hot] [--both] [--rec]
#   If benchmark_name is specified, runs only that benchmark (tpcc|tatp|smallbank)
#   If not specified, runs all three benchmarks sequentially
#   --hot: Enable hot table scanner / long-running scan queries (default: disabled)
#   --no-hot: Disable hot table scanner (explicitly, this is the default)
#   --both: Run each benchmark both with and without hot scanner (runs twice)
#   --rec_memory: Enable memory node failure recovery test (uses connection_cloudlab_2replicas.conf and enables file logging, default: disabled)
#   --rec_compute: Enable compute node failure recovery test (uses connection_cloudlab_2replicas.conf and enables file logging, default: disabled)
#          NOTE: Failure recovery is currently only supported for TPCC benchmark

set -o nounset
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/..

# Configuration files
conf_file_2replicas=$bin/../connection_cloudlab_2replicas.conf
conf_file_noreplica=$bin/../connection_cloudlab_noreplica.conf
conf_file=$bin/../connection_replication.conf
memcached_conf_file_all=$bin/../memcached_cloudlab_servers.conf
memcached_conf_file=$bin/../memcached_ip.conf

# Directory for log files
output_dir="/users/Ruihong/MemoryEngine/scripts/data"
core_dump_dir="/mnt/core_dump"

# Working environment
proj_dir="/users/Ruihong/MemoryEngine"
bin_dir="${proj_dir}/debug"
ssh_opts="-o StrictHostKeyChecking=no"

# Memory and port configuration
cache_mem_size=8 # 8 GB Local memory size
remote_mem_size=55 # 55 GB Remote memory size per node
port=$((13000+RANDOM%1000))

# Default benchmark parameters
default_threads=8 # default 8
default_warehouses=16 # default 256
default_dist_ratio=100

# Benchmark-specific transaction counts (based on 8GB cache warmup estimation)
# See CACHE_WARMUP_ESTIMATION.md for detailed rationale
# TPC-C: Larger records (~6.5KB/txn), better locality -> fewer txns needed
tpcc_txns=200000 #default 2000000
# TATP: Small records (~120B/txn), high cardinality (40M subscribers) -> more txns needed
tatp_txns=50000000 #default 50000000
# SmallBank: Small records (~120B/txn), very high cardinality (200M accounts) -> more txns needed
smallbank_txns=35000000 #default 35000000

# Hot table scanner configuration (can be overridden via command line)
enable_hot_table_scanner=false
run_both_modes=false

# File logging configuration (automatically enabled when --rec is used)
enable_file_logging=false

# Failure recovery test configuration (can be overridden via command line)
enable_failure_recovery=false
failure_recovery_type=0  # 0 for memory node failure, 1 for compute node failure

# Default TPC-C query ratios (standard TPC-C mix)
# Frequency weights: Delivery=1, Payment=10, NewOrder=10, OrderStatus=1, StockLevel=1
# These normalize to approximately: Payment: 43.5%, NewOrder: 43.5%, OrderStatus: 4.3%, Delivery: 4.3%, StockLevel: 4.3%
# Note: Ratios will be set conditionally in setup_tpcc() based on failure recovery mode
TPCC_DELIVERY=1
TPCC_PAYMENT=10
TPCC_NEW_ORDER=10
TPCC_ORDER_STATUS=1
TPCC_STOCK_LEVEL=1

# Initialize node arrays (will be populated in setup_config)
declare -a compute_nodes
declare -a memory_nodes
declare -a all_nodes
master_host=""

echo "Benchmark Testing Script - Default Query Ratios"
echo "================================================"

# Function to setup configuration files
setup_config() {
  # Create output directory if it doesn't exist
  if [ ! -d "$output_dir" ]; then
    echo "Creating output directory: $output_dir"
    mkdir -p "$output_dir"
  fi

  # Select config file based on failure recovery mode
  if [ "$enable_failure_recovery" = true ]; then
    conf_file_source="$conf_file_2replicas"
    echo "Using 2-replica configuration (connection_cloudlab_2replicas.conf) for failure recovery test"
  else
    conf_file_source="$conf_file_noreplica"
    echo "Using no-replica configuration (connection_cloudlab_noreplica.conf)"
  fi

  # Create the working config file from the source config file
  compute_line_all=$(grep -v '^#' "$conf_file_source" | grep -v '^$' | sed -n '1p')
  memory_line_all=$(grep -v '^#' "$conf_file_source" | grep -v '^$' | sed -n '2p')
  
  # Create the working config file
  echo "$compute_line_all" > "$conf_file"
  echo "$memory_line_all" >> "$conf_file"
  
  # Copy all replication configuration lines (lines 3+) from conf_file_source
  grep -v '^#' "$conf_file_source" | grep -v '^$' | tail -n +3 >> "$conf_file"

  # Copy memcached configuration
  cp "$memcached_conf_file_all" "$memcached_conf_file"
  echo "Copied memcached config from $memcached_conf_file_all to $memcached_conf_file"

  # Parse the working config file
  compute_line=$(grep -v '^#' $conf_file | grep -v '^$' | sed -n '1p')
  memory_line=$(grep -v '^#' $conf_file | grep -v '^$' | sed -n '2p')
  read -r -a compute_nodes <<< "$compute_line"
  read -r -a memory_nodes <<< "$memory_line"
  compute_num=${#compute_nodes[@]}
  memory_num=${#memory_nodes[@]}
  
  echo "Memory nodes: ${memory_nodes[@]}"
  echo "Compute nodes: ${compute_nodes[@]}"
  
  master_host=${compute_nodes[0]}

  # Combine all nodes for parallel operations (declare as global array)
  all_nodes=("${memory_nodes[@]}" "${compute_nodes[@]}")
  
  # Create output directory and rsync config files to all nodes in parallel
  for node in "${all_nodes[@]}"
  do
    {
      ssh ${ssh_opts} ${node} "mkdir -p ${output_dir}"
      echo "Rsync config files to $node"
      rsync -vz $proj_dir/connection_replication.conf $proj_dir/memcached_ip.conf ${node}:$proj_dir/
    } &
  done
  
  # Wait for all parallel operations to complete
  wait

  read -r -a memcached_node <<< $(head -n 1 $memcached_conf_file)
  echo "Restarting memcached on ${memcached_node[0]}"
  ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"
}

# Function to cleanup processes
cleanup() {
  # Check if all_nodes array is initialized and has elements
  if [ -z "${all_nodes:-}" ] || [ ${#all_nodes[@]} -eq 0 ]; then
    echo "No nodes configured for cleanup"
    return
  fi
  
  echo "Cleaning up processes..."
  for node in "${all_nodes[@]}"
  do
    ssh ${ssh_opts} ${node} "pkill -f memory_server; pkill -f memory_server_tpcc; pkill -f memory_server_term; pkill -f tpcc; pkill -f tatp; pkill -f smallbank" 2>/dev/null || true
  done
  sleep 2
}

# Function to run TPCC benchmark
run_tpcc() {
  local hot_scanner_enabled=$1
  local suffix=""
  if [ "$hot_scanner_enabled" = true ]; then
    suffix="_hot"
    echo ""
    echo "========================================="
    echo "Running TPCC Benchmark (WITH hot scanner)"
    echo "========================================="
  else
    suffix="_nohot"
    echo ""
    echo "========================================="
    echo "Running TPCC Benchmark (WITHOUT hot scanner)"
    echo "========================================="
  fi
  # Set query ratios based on failure recovery mode
  if [ "$enable_failure_recovery" = true ]; then
    # Failure recovery test ratios: Delivery=1, Payment=10, NewOrder=10, OrderStatus=40, StockLevel=40
    TPCC_DELIVERY=1
    TPCC_PAYMENT=10
    TPCC_NEW_ORDER=10
    TPCC_ORDER_STATUS=40
    TPCC_STOCK_LEVEL=40
    echo "Query Ratios (Failure Recovery mix): NewOrder=${TPCC_NEW_ORDER}%, Payment=${TPCC_PAYMENT}%, OrderStatus=${TPCC_ORDER_STATUS}%, Delivery=${TPCC_DELIVERY}%, StockLevel=${TPCC_STOCK_LEVEL}%"
    enable_file_logging=true
    suffix="${suffix}_fail"
  else
    # Standard TPC-C mix: Delivery=1, Payment=10, NewOrder=10, OrderStatus=1, StockLevel=1
    TPCC_DELIVERY=1
    TPCC_PAYMENT=10
    TPCC_NEW_ORDER=10
    TPCC_ORDER_STATUS=1
    TPCC_STOCK_LEVEL=1
    echo "Query Ratios (Standard TPC-C mix): NewOrder=${TPCC_NEW_ORDER}%, Payment=${TPCC_PAYMENT}%, OrderStatus=${TPCC_ORDER_STATUS}%, Delivery=${TPCC_DELIVERY}%, StockLevel=${TPCC_STOCK_LEVEL}%"
  fi
  echo "Transaction count: ${tpcc_txns} (optimized for 8GB cache warmup)"
  
  if [ "$enable_file_logging" = true ]; then
    output_file="${output_dir}/tpcc_default${suffix}.log"
  else
    output_file="/dev/null"
  fi
  benchmark_args="-p$port -sf${default_warehouses} -sf1 -c${default_threads} -rde${TPCC_DELIVERY} -rpa${TPCC_PAYMENT} -rne${TPCC_NEW_ORDER} -ror${TPCC_ORDER_STATUS} -rst${TPCC_STOCK_LEVEL} -t${tpcc_txns} -f${conf_file} -lat"
  
  # Add cache size if not specified
  if [[ " ${benchmark_args} " != *" -cs"* ]]; then
    benchmark_args="${benchmark_args} -cs${cache_mem_size}"
  fi
  
  # Add hot table scanner flag if enabled
  if [ "$hot_scanner_enabled" = true ]; then
    benchmark_args="${benchmark_args} -hot"
    echo "Hot table scanner: ENABLED"
  else
    echo "Hot table scanner: DISABLED"
  fi
  
  # Add failure recovery flag if enabled
  if [ "$enable_failure_recovery" = true ]; then
    if [ "$failure_recovery_type" = "1" ]; then
      benchmark_args="${benchmark_args} -rec_compute"
      echo "Failure recovery test: ENABLED (compute node failure)"
    else
      benchmark_args="${benchmark_args} -rec_memory"
      echo "Failure recovery test: ENABLED (memory node failure)"
    fi
  else
    echo "Failure recovery test: DISABLED"
  fi
  
  # Restart memcached before starting benchmark
  read -r -a memcached_node <<< $(head -n 1 $memcached_conf_file)
  echo "Restarting memcached on ${memcached_node[0]} before benchmark run"
  ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"
  
  # Start memory servers for TPCC
  for ((i=0;i<${#memory_nodes[@]};i++)); do
    memory=${memory_nodes[$i]}
    # Create separate log file for each memory server to capture errors
    memory_log_file="${output_dir}/memory_server_${memory}_${suffix}.log"
    script_memory="ulimit -c unlimited && cd ${bin_dir} && ./memory_server_tpcc $port $(($remote_mem_size)) $((2*$i +1)) > ${memory_log_file} 2>&1"
    echo "Starting memory server on $memory (logs: ${memory_log_file})"
    # Enable coredump on memory nodes
    ssh ${ssh_opts} ${memory} "echo '$core_dump_dir/core$memory' | sudo tee /proc/sys/kernel/core_pattern"
    echo "[DEBUG] $memory: ssh ${ssh_opts} ${memory} \"$script_memory\""
    ssh ${ssh_opts} ${memory} "$script_memory" &
    sleep 1
  done
  
  sleep 2
  
  # Start compute nodes
  script_compute="cd ${bin_dir} && ./tpcc ${benchmark_args} -d${default_dist_ratio}"
  
  # Start master node
  echo "Starting master node on $master_host"
  ssh ${ssh_opts} ${master_host} "echo '$core_dump_dir/core$master_host' | sudo tee /proc/sys/kernel/core_pattern"
  echo "[DEBUG] $master_host: ssh ${ssh_opts} ${master_host} \"ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}\""
  ssh ${ssh_opts} ${master_host} "ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}" &
  
  # Start worker nodes
  for ((i=1;i<${#compute_nodes[@]};i++)); do
    compute=${compute_nodes[$i]}
    echo "Starting worker node on $compute"
    ssh ${ssh_opts} ${compute} "echo '$core_dump_dir/core$compute' | sudo tee /proc/sys/kernel/core_pattern"
    echo "[DEBUG] $compute: ssh ${ssh_opts} ${compute} \"ulimit -S -c unlimited && $script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}\""
    ssh ${ssh_opts} ${compute} "ulimit -S -c unlimited && $script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}" &
  done
  
  wait
  sleep 3
  
  echo "TPCC benchmark completed. Results in: ${output_file}"
  
  # Wait for log replay if enabled
  echo "Waiting for log replay to complete..."
  sleep 5
}

# Function to run TATP benchmark
run_tatp() {
  local hot_scanner_enabled=$1
  
  # Check if failure recovery is enabled (not supported for TATP)
  if [ "$enable_failure_recovery" = true ]; then
    echo ""
    echo "========================================="
    echo "ERROR: Failure recovery test is not ready for TATP benchmark"
    echo "========================================="
    echo "Failure recovery is currently only supported for TPCC benchmark."
    echo "Please run TATP without the --rec flag."
    exit 1
  fi
  
  local suffix=""
  if [ "$hot_scanner_enabled" = true ]; then
    suffix="_hot"
    echo ""
    echo "========================================="
    echo "Running TATP Benchmark (WITH hot scanner)"
    echo "========================================="
  else
    suffix="_nohot"
    echo ""
    echo "========================================="
    echo "Running TATP Benchmark (WITHOUT hot scanner)"
    echo "========================================="
  fi
  echo "Query Ratios (hardcoded in code): GetSubscriberData=35%, GetNewDestination=10%, GetAccessData=35%, UpdateSubscriberData=2%, UpdateLocation=14%, InsertCallForwarding=2%, DeleteCallForwarding=2%"
  echo "Transaction count: ${tatp_txns} (optimized for 8GB cache warmup)"
  
  if [ "$enable_file_logging" = true ]; then
    output_file="${output_dir}/tatp_default${suffix}.log"
  else
    output_file="/dev/null"
  fi
  benchmark_args="-p$port -sf100000 -c${default_threads} -t${tatp_txns} -f${conf_file} -lat"
  
  # Add cache size if not specified
  if [[ " ${benchmark_args} " != *" -cs"* ]]; then
    benchmark_args="${benchmark_args} -cs${cache_mem_size}"
  fi
  
  # Add hot table scanner flag if enabled
  if [ "$hot_scanner_enabled" = true ]; then
    benchmark_args="${benchmark_args} -hot"
    echo "Hot table scanner: ENABLED"
  else
    echo "Hot table scanner: DISABLED"
  fi
  
  # Restart memcached before starting benchmark
  read -r -a memcached_node <<< $(head -n 1 $memcached_conf_file)
  echo "Restarting memcached on ${memcached_node[0]} before benchmark run"
  ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"
  
  # Start memory servers for TATP (using memory_server_tpcc for transaction benchmarks)
  for ((i=0;i<${#memory_nodes[@]};i++)); do
    memory=${memory_nodes[$i]}
    script_memory="ulimit -c unlimited && cd ${bin_dir} && ./memory_server_tpcc $port $(($remote_mem_size)) $((2*$i +1)) > ${output_file} 2>&1"
    echo "Starting memory server on $memory"
    # Enable coredump on memory nodes
    ssh ${ssh_opts} ${memory} "echo '$core_dump_dir/core$memory' | sudo tee /proc/sys/kernel/core_pattern"
    echo "[DEBUG] $memory: ssh ${ssh_opts} ${memory} \"$script_memory\""
    ssh ${ssh_opts} ${memory} "$script_memory" &
    sleep 1
  done
  
  sleep 2
  
  # Start compute nodes
  script_compute="cd ${bin_dir} && ./tatp ${benchmark_args}"
  
  # Start master node
  echo "Starting master node on $master_host"
  ssh ${ssh_opts} ${master_host} "echo '$core_dump_dir/core$master_host' | sudo tee /proc/sys/kernel/core_pattern"
  echo "[DEBUG] $master_host: ssh ${ssh_opts} ${master_host} \"ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}\""
  ssh ${ssh_opts} ${master_host} "ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}" &
  
  # Start worker nodes
  for ((i=1;i<${#compute_nodes[@]};i++)); do
    compute=${compute_nodes[$i]}
    echo "Starting worker node on $compute"
    ssh ${ssh_opts} ${compute} "echo '$core_dump_dir/core$compute' | sudo tee /proc/sys/kernel/core_pattern"
    echo "[DEBUG] $compute: ssh ${ssh_opts} ${compute} \"ulimit -S -c unlimited && $script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}\""
    ssh ${ssh_opts} ${compute} "ulimit -S -c unlimited && $script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}" &
  done
  
  wait
  sleep 3
  
  echo "TATP benchmark completed. Results in: ${output_file}"
  
  # Wait for log replay if enabled
  echo "Waiting for log replay to complete..."
  sleep 5
}

# Function to run SmallBank benchmark
run_smallbank() {
  local hot_scanner_enabled=$1
  
  # Check if failure recovery is enabled (not supported for SmallBank)
  if [ "$enable_failure_recovery" = true ]; then
    echo ""
    echo "========================================="
    echo "ERROR: Failure recovery test is not ready for SmallBank benchmark"
    echo "========================================="
    echo "Failure recovery is currently only supported for TPCC benchmark."
    echo "Please run SmallBank without the --rec flag."
    exit 1
  fi
  
  local suffix=""
  if [ "$hot_scanner_enabled" = true ]; then
    suffix="_hot"
    echo ""
    echo "========================================="
    echo "Running SmallBank Benchmark (WITH hot scanner)"
    echo "========================================="
  else
    suffix="_nohot"
    echo ""
    echo "========================================="
    echo "Running SmallBank Benchmark (WITHOUT hot scanner)"
    echo "========================================="
  fi
  echo "Query Ratios (hardcoded in code): Amalgamate=15%, Balance=15%, DepositChecking=15%, SendPayment=25%, TransactSavings=15%, WriteCheck=15%"
  echo "Transaction count: ${smallbank_txns} (optimized for 8GB cache warmup)"
  
  if [ "$enable_file_logging" = true ]; then
    output_file="${output_dir}/smallbank_default${suffix}.log"
  else
    output_file="/dev/null"
  fi
  benchmark_args="-p$port -sf100000 -c${default_threads} -t${smallbank_txns} -f${conf_file} -lat"
  
  # Add cache size if not specified
  if [[ " ${benchmark_args} " != *" -cs"* ]]; then
    benchmark_args="${benchmark_args} -cs${cache_mem_size}"
  fi
  
  # Add hot table scanner flag if enabled
  if [ "$hot_scanner_enabled" = true ]; then
    benchmark_args="${benchmark_args} -hot"
    echo "Hot table scanner: ENABLED"
  else
    echo "Hot table scanner: DISABLED"
  fi

  # Restart memcached before starting benchmark
  read -r -a memcached_node <<< $(head -n 1 $memcached_conf_file)
  echo "Restarting memcached on ${memcached_node[0]} before benchmark run"
  ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"
  # Start memory servers for SmallBank (using memory_server_tpcc for transaction benchmarks)
  for ((i=0;i<${#memory_nodes[@]};i++)); do
    memory=${memory_nodes[$i]}
    script_memory="ulimit -c unlimited && cd ${bin_dir} && ./memory_server_tpcc $port $(($remote_mem_size)) $((2*$i +1)) > ${output_file} 2>&1"
    echo "Starting memory server on $memory"
    # Enable coredump on memory nodes
    ssh ${ssh_opts} ${memory} "echo '$core_dump_dir/core$memory' | sudo tee /proc/sys/kernel/core_pattern"
    echo "[DEBUG] $memory: ssh ${ssh_opts} ${memory} \"$script_memory\""
    ssh ${ssh_opts} ${memory} "$script_memory" &
    sleep 1
  done
  
  sleep 2
  
  # Start compute nodes
  script_compute="cd ${bin_dir} && ./smallbank ${benchmark_args}"
  
  # Start master node
  echo "Starting master node on $master_host"
  ssh ${ssh_opts} ${master_host} "echo '$core_dump_dir/core$master_host' | sudo tee /proc/sys/kernel/core_pattern"
  echo "[DEBUG] $master_host: ssh ${ssh_opts} ${master_host} \"ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}\""
  ssh ${ssh_opts} ${master_host} "ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}" &
  
  # Start worker nodes
  for ((i=1;i<${#compute_nodes[@]};i++)); do
    compute=${compute_nodes[$i]}
    echo "Starting worker node on $compute"
    ssh ${ssh_opts} ${compute} "echo '$core_dump_dir/core$compute' | sudo tee /proc/sys/kernel/core_pattern"
    echo "[DEBUG] $compute: ssh ${ssh_opts} ${compute} \"ulimit -S -c unlimited && $script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}\""
    ssh ${ssh_opts} ${compute} "ulimit -S -c unlimited && $script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}" &
  done
  
  wait
  sleep 3
  
  echo "SmallBank benchmark completed. Results in: ${output_file}"
  
  # Wait for log replay if enabled
  echo "Waiting for log replay to complete..."
  sleep 5
}

# Main execution
main() {
  # Parse command line arguments
  benchmark_name=""
  while [ $# -gt 0 ]; do
    case $1 in
      --hot)
        enable_hot_table_scanner=true
        shift
        ;;
      --no-hot)
        enable_hot_table_scanner=false
        shift
        ;;
      --both)
        run_both_modes=true
        shift
        ;;
      --rec_memory)
        enable_failure_recovery=true
        failure_recovery_type=0  # 0 for memory node failure
        enable_file_logging=true
        shift
        ;;
      --rec_compute)
        enable_failure_recovery=true
        failure_recovery_type=1  # 1 for compute node failure
        enable_file_logging=true
        shift
        ;;
      tpcc|tatp|smallbank)
        if [ -z "$benchmark_name" ]; then
          benchmark_name=$1
        else
          echo "Error: Multiple benchmark names specified. Use only one."
          exit 1
        fi
        shift
        ;;
      *)
        echo "Unknown option: $1"
        echo "Usage: $0 [benchmark_name] [--hot] [--no-hot] [--both] [--rec_memory|--rec_compute]"
        echo "  benchmark_name: tpcc, tatp, or smallbank (optional, runs all if not specified)"
        echo "  --hot: Enable hot table scanner (long-running scan queries)"
        echo "  --no-hot: Disable hot table scanner (default)"
        echo "  --both: Run each benchmark both with and without hot scanner (runs twice)"
        echo "  --rec_memory: Enable memory node failure recovery test (uses connection_cloudlab_2replicas.conf and enables file logging)"
        echo "  --rec_compute: Enable compute node failure recovery test (uses connection_cloudlab_2replicas.conf and enables file logging)"
        echo "         NOTE: Failure recovery is currently only supported for TPCC benchmark"
        exit 1
        ;;
    esac
  done
  
  # Check if a specific benchmark was requested
  if [ -n "$benchmark_name" ]; then
    setup_config
    
    if [ "$run_both_modes" = true ]; then
      # Run benchmark both with and without hot scanner
      echo "Running $benchmark_name benchmark in both modes (with and without hot scanner)..."
      case $benchmark_name in
        tpcc)
          run_tpcc false
          # cleanup
          sleep 10
          port=$((port + 100))
          run_tpcc true
          ;;
        tatp)
          run_tatp false
          cleanup
          sleep 10
          port=$((port + 100))
          run_tatp true
          ;;
        smallbank)
          run_smallbank false
          cleanup
          sleep 10
          port=$((port + 100))
          run_smallbank true
          ;;
        *)
          echo "Unknown benchmark: $benchmark_name"
          echo "Valid benchmarks: tpcc, tatp, smallbank"
          exit 1
          ;;
      esac
    else
      # Run benchmark with specified hot scanner setting
      case $benchmark_name in
        tpcc)
          run_tpcc $enable_hot_table_scanner
          ;;
        tatp)
          run_tatp $enable_hot_table_scanner
          ;;
        smallbank)
          run_smallbank $enable_hot_table_scanner
          ;;
        *)
          echo "Unknown benchmark: $benchmark_name"
          echo "Valid benchmarks: tpcc, tatp, smallbank"
          exit 1
          ;;
      esac
    fi
    
    cleanup
  else
    # Run all benchmarks sequentially
    # Check if failure recovery is enabled with all benchmarks (not supported for TATP/SmallBank)
    if [ "$enable_failure_recovery" = true ]; then
      echo ""
      echo "========================================="
      echo "WARNING: Failure recovery test is not ready for TATP and SmallBank benchmarks"
      echo "========================================="
      echo "Failure recovery is currently only supported for TPCC benchmark."
      echo "TATP and SmallBank will be skipped when --rec_memory or --rec_compute is enabled."
      echo ""
      # Only run TPCC when failure recovery is enabled
      run_tpcc $enable_hot_table_scanner
      cleanup
      echo ""
      echo "========================================="
      echo "Benchmark completed (TPCC only with failure recovery)!"
      if [ "$enable_file_logging" = true ]; then
        echo "Results are in: $output_dir"
        local suffix=""
        if [ "$enable_hot_table_scanner" = true ]; then
          suffix="_hot_fail"
        else
          suffix="_nohot_fail"
        fi
        echo "  - TPCC: ${output_dir}/tpcc_default${suffix}.log"
      fi
      echo "========================================="
      return
    fi
    
    setup_config
    
    echo "Running all benchmarks sequentially..."
    if [ "$run_both_modes" = true ]; then
      echo "Mode: Running each benchmark BOTH with and without hot scanner"
    elif [ "$enable_hot_table_scanner" = true ]; then
      echo "Hot table scanner: ENABLED for all benchmarks"
    else
      echo "Hot table scanner: DISABLED for all benchmarks"
    fi
    if [ "$enable_failure_recovery" = true ]; then
      if [ "$failure_recovery_type" = "1" ]; then
        echo "Failure recovery test: ENABLED - COMPUTE NODE FAILURE (using connection_cloudlab_2replicas.conf, file logging enabled)"
      else
        echo "Failure recovery test: ENABLED - MEMORY NODE FAILURE (using connection_cloudlab_2replicas.conf, file logging enabled)"
      fi
    else
      echo "Failure recovery test: DISABLED (using connection_cloudlab_noreplica.conf, file logging disabled)"
    fi
    echo ""
    
    if [ "$run_both_modes" = true ]; then
      # Run each benchmark twice (with and without hot scanner)
      run_tpcc false
      cleanup
      sleep 10
      port=$((port + 100))
      run_tpcc true
      cleanup
      sleep 10
      
      port=$((port + 100))
      run_tatp false
      cleanup
      sleep 10
      port=$((port + 100))
      run_tatp true
      cleanup
      sleep 10
      
      port=$((port + 100))
      run_smallbank false
      cleanup
      sleep 10
      port=$((port + 100))
      run_smallbank true
      cleanup
      
      echo ""
      echo "========================================="
      echo "All benchmarks completed (both modes)!"
      if [ "$enable_file_logging" = true ]; then
        echo "Results are in: $output_dir"
        local suffix_nohot=""
        local suffix_hot=""
        if [ "$enable_failure_recovery" = true ]; then
          suffix_nohot="_nohot_fail"
          suffix_hot="_hot_fail"
        else
          suffix_nohot="_nohot"
          suffix_hot="_hot"
        fi
        echo "  - TPCC (no hot): ${output_dir}/tpcc_default${suffix_nohot}.log"
        echo "  - TPCC (hot): ${output_dir}/tpcc_default${suffix_hot}.log"
        echo "  - TATP (no hot): ${output_dir}/tatp_default${suffix_nohot}.log"
        echo "  - TATP (hot): ${output_dir}/tatp_default${suffix_hot}.log"
        echo "  - SmallBank (no hot): ${output_dir}/smallbank_default${suffix_nohot}.log"
        echo "  - SmallBank (hot): ${output_dir}/smallbank_default${suffix_hot}.log"
      else
        echo "File logging was disabled (output to /dev/null)"
      fi
      echo "========================================="
    else
      # Run each benchmark once with specified setting
      run_tpcc $enable_hot_table_scanner
      cleanup
      sleep 10
      
      # Use different port for next benchmark
      port=$((port + 100))
      run_tatp $enable_hot_table_scanner
      cleanup
      sleep 10
      
      # Use different port for next benchmark
      port=$((port + 100))
      run_smallbank $enable_hot_table_scanner
      cleanup
      
      echo ""
      echo "========================================="
      echo "All benchmarks completed!"
      if [ "$enable_file_logging" = true ]; then
        echo "Results are in: $output_dir"
        local suffix=""
        if [ "$enable_hot_table_scanner" = true ]; then
          suffix="_hot"
        else
          suffix="_nohot"
        fi
        if [ "$enable_failure_recovery" = true ]; then
          suffix="${suffix}_fail"
        fi
        echo "  - TPCC: ${output_dir}/tpcc_default${suffix}.log"
        echo "  - TATP: ${output_dir}/tatp_default${suffix}.log"
        echo "  - SmallBank: ${output_dir}/smallbank_default${suffix}.log"
      else
        echo "File logging was disabled (output to /dev/null)"
      fi
      echo "========================================="
    fi
  fi
}

# Trap to cleanup on exit
trap cleanup EXIT

# Run main function
main "$@"

