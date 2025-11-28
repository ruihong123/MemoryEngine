#!/bin/bash
# Testing script to run TPCC, TATP, and SmallBank benchmarks with default query ratios
# Usage: ./run_benchmarks_default.sh [benchmark_name] [--hot] [--no-hot] [--no-log]
#   If benchmark_name is specified, runs only that benchmark (tpcc|tatp|smallbank)
#   If not specified, runs all three benchmarks sequentially
#   --hot: Enable hot table scanner / long-running scan queries (default: disabled)
#   --no-hot: Disable hot table scanner (explicitly, this is the default)
#   --no-log: Disable file logging (output redirected to /dev/null)

set -o nounset
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/..

# Configuration files
conf_file_all=$bin/../connection_cloudlab_2replicas.conf
conf_file=$bin/../connection_replication.conf
memcached_conf_file_all=$bin/../memcached_cloudlab_servers.conf
memcached_conf_file=$bin/../memcached_ip.conf

# Directory for log files
output_dir="/users/Ruihong/MemoryEngine/scripts/data"
core_dump_dir="/mnt/core_dump"

# Working environment
proj_dir="/users/Ruihong/MemoryEngine"
bin_dir="${proj_dir}/release"
ssh_opts="-o StrictHostKeyChecking=no"

# Memory and port configuration
cache_mem_size=8 # 8 GB Local memory size
remote_mem_size=55 # 55 GB Remote memory size per node
port=$((13000+RANDOM%1000))

# Default benchmark parameters
default_threads=8
default_warehouses=8
default_txns=1000000
default_dist_ratio=100

# Hot table scanner configuration (can be overridden via command line)
enable_hot_table_scanner=false

# File logging configuration (can be overridden via command line)
enable_file_logging=true

# Default TPC-C query ratios (standard TPC-C mix)
# Frequency weights: Delivery=1, Payment=10, NewOrder=10, OrderStatus=1, StockLevel=1
# These normalize to approximately: Payment: 43.5%, NewOrder: 43.5%, OrderStatus: 4.3%, Delivery: 4.3%, StockLevel: 4.3%
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

  # Create the working config file from the source config file
  compute_line_all=$(grep -v '^#' "$conf_file_all" | grep -v '^$' | sed -n '1p')
  memory_line_all=$(grep -v '^#' "$conf_file_all" | grep -v '^$' | sed -n '2p')
  
  # Create the working config file
  echo "$compute_line_all" > "$conf_file"
  echo "$memory_line_all" >> "$conf_file"
  
  # Copy all replication configuration lines (lines 3+) from conf_file_all
  grep -v '^#' "$conf_file_all" | grep -v '^$' | tail -n +3 >> "$conf_file"

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
  if [ ${#all_nodes[@]} -eq 0 ]; then
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
  echo ""
  echo "========================================="
  echo "Running TPCC Benchmark"
  echo "========================================="
  echo "Query Ratios (Standard TPC-C mix): NewOrder=${TPCC_NEW_ORDER}%, Payment=${TPCC_PAYMENT}%, OrderStatus=${TPCC_ORDER_STATUS}%, Delivery=${TPCC_DELIVERY}%, StockLevel=${TPCC_STOCK_LEVEL}%"
  
  if [ "$enable_file_logging" = true ]; then
    output_file="${output_dir}/tpcc_default.log"
  else
    output_file="/dev/null"
  fi
  benchmark_args="-p$port -sf${default_warehouses} -sf1 -c${default_threads} -rde${TPCC_DELIVERY} -rpa${TPCC_PAYMENT} -rne${TPCC_NEW_ORDER} -ror${TPCC_ORDER_STATUS} -rst${TPCC_STOCK_LEVEL} -t${default_txns} -f${conf_file} -lat"
  
  # Add cache size if not specified
  if [[ " ${benchmark_args} " != *" -cs"* ]]; then
    benchmark_args="${benchmark_args} -cs${cache_mem_size}"
  fi
  
  # Add hot table scanner flag if enabled
  if [ "$enable_hot_table_scanner" = true ]; then
    benchmark_args="${benchmark_args} -hot"
    echo "Hot table scanner: ENABLED"
  else
    echo "Hot table scanner: DISABLED"
  fi
  
  # Start memory servers for TPCC
  for ((i=0;i<${#memory_nodes[@]};i++)); do
    memory=${memory_nodes[$i]}
    script_memory="ulimit -c unlimited && cd ${bin_dir} && ./memory_server_tpcc $port $(($remote_mem_size)) $((2*$i +1)) > ${output_file} 2>&1"
    echo "Starting memory server on $memory"
    ssh ${ssh_opts} ${memory} "echo '$core_dump_dir/core$memory' | sudo tee /proc/sys/kernel/core_pattern"
    ssh ${ssh_opts} ${memory} "$script_memory" &
    sleep 1
  done
  
  sleep 2
  
  # Start compute nodes
  script_compute="cd ${bin_dir} && ./tpcc ${benchmark_args} -d${default_dist_ratio}"
  
  # Start master node
  echo "Starting master node on $master_host"
  ssh ${ssh_opts} ${master_host} "echo '$core_dump_dir/core$master_host' | sudo tee /proc/sys/kernel/core_pattern"
  ssh ${ssh_opts} ${master_host} "ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}" &
  
  # Start worker nodes
  for ((i=1;i<${#compute_nodes[@]};i++)); do
    compute=${compute_nodes[$i]}
    echo "Starting worker node on $compute"
    ssh ${ssh_opts} ${compute} "echo '$core_dump_dir/core$compute' | sudo tee /proc/sys/kernel/core_pattern"
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
  echo ""
  echo "========================================="
  echo "Running TATP Benchmark"
  echo "========================================="
  echo "Query Ratios (hardcoded in code): GetSubscriberData=35%, GetNewDestination=10%, GetAccessData=35%, UpdateSubscriberData=2%, UpdateLocation=14%, InsertCallForwarding=2%, DeleteCallForwarding=2%"
  
  if [ "$enable_file_logging" = true ]; then
    output_file="${output_dir}/tatp_default.log"
  else
    output_file="/dev/null"
  fi
  benchmark_args="-p$port -sf100000 -c${default_threads} -t${default_txns} -f${conf_file} -lat"
  
  # Add cache size if not specified
  if [[ " ${benchmark_args} " != *" -cs"* ]]; then
    benchmark_args="${benchmark_args} -cs${cache_mem_size}"
  fi
  
  # Add hot table scanner flag if enabled
  if [ "$enable_hot_table_scanner" = true ]; then
    benchmark_args="${benchmark_args} -hot"
    echo "Hot table scanner: ENABLED"
  else
    echo "Hot table scanner: DISABLED"
  fi
  
  # Start memory servers for TATP (using memory_server_tpcc for transaction benchmarks)
  for ((i=0;i<${#memory_nodes[@]};i++)); do
    memory=${memory_nodes[$i]}
    script_memory="ulimit -c unlimited && cd ${bin_dir} && ./memory_server_tpcc $port $(($remote_mem_size)) $((2*$i +1)) > ${output_file} 2>&1"
    echo "Starting memory server on $memory"
    ssh ${ssh_opts} ${memory} "echo '$core_dump_dir/core$memory' | sudo tee /proc/sys/kernel/core_pattern"
    ssh ${ssh_opts} ${memory} "$script_memory" &
    sleep 1
  done
  
  sleep 2
  
  # Start compute nodes
  script_compute="cd ${bin_dir} && ./tatp ${benchmark_args}"
  
  # Start master node
  echo "Starting master node on $master_host"
  ssh ${ssh_opts} ${master_host} "echo '$core_dump_dir/core$master_host' | sudo tee /proc/sys/kernel/core_pattern"
  ssh ${ssh_opts} ${master_host} "ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}" &
  
  # Start worker nodes
  for ((i=1;i<${#compute_nodes[@]};i++)); do
    compute=${compute_nodes[$i]}
    echo "Starting worker node on $compute"
    ssh ${ssh_opts} ${compute} "echo '$core_dump_dir/core$compute' | sudo tee /proc/sys/kernel/core_pattern"
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
  echo ""
  echo "========================================="
  echo "Running SmallBank Benchmark"
  echo "========================================="
  echo "Query Ratios (hardcoded in code): Amalgamate=15%, Balance=15%, DepositChecking=15%, SendPayment=25%, TransactSavings=15%, WriteCheck=15%"
  
  if [ "$enable_file_logging" = true ]; then
    output_file="${output_dir}/smallbank_default.log"
  else
    output_file="/dev/null"
  fi
  benchmark_args="-p$port -sf100000 -c${default_threads} -t${default_txns} -f${conf_file} -lat"
  
  # Add cache size if not specified
  if [[ " ${benchmark_args} " != *" -cs"* ]]; then
    benchmark_args="${benchmark_args} -cs${cache_mem_size}"
  fi
  
  # Add hot table scanner flag if enabled
  if [ "$enable_hot_table_scanner" = true ]; then
    benchmark_args="${benchmark_args} -hot"
    echo "Hot table scanner: ENABLED"
  else
    echo "Hot table scanner: DISABLED"
  fi
  
  # Start memory servers for SmallBank (using memory_server_tpcc for transaction benchmarks)
  for ((i=0;i<${#memory_nodes[@]};i++)); do
    memory=${memory_nodes[$i]}
    script_memory="ulimit -c unlimited && cd ${bin_dir} && ./memory_server_tpcc $port $(($remote_mem_size)) $((2*$i +1)) > ${output_file} 2>&1"
    echo "Starting memory server on $memory"
    ssh ${ssh_opts} ${memory} "echo '$core_dump_dir/core$memory' | sudo tee /proc/sys/kernel/core_pattern"
    ssh ${ssh_opts} ${memory} "$script_memory" &
    sleep 1
  done
  
  sleep 2
  
  # Start compute nodes
  script_compute="cd ${bin_dir} && ./smallbank ${benchmark_args}"
  
  # Start master node
  echo "Starting master node on $master_host"
  ssh ${ssh_opts} ${master_host} "echo '$core_dump_dir/core$master_host' | sudo tee /proc/sys/kernel/core_pattern"
  ssh ${ssh_opts} ${master_host} "ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 | tee -a ${output_file}" &
  
  # Start worker nodes
  for ((i=1;i<${#compute_nodes[@]};i++)); do
    compute=${compute_nodes[$i]}
    echo "Starting worker node on $compute"
    ssh ${ssh_opts} ${compute} "echo '$core_dump_dir/core$compute' | sudo tee /proc/sys/kernel/core_pattern"
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
      --no-log)
        enable_file_logging=false
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
        echo "Usage: $0 [benchmark_name] [--hot] [--no-hot] [--no-log]"
        echo "  benchmark_name: tpcc, tatp, or smallbank (optional, runs all if not specified)"
        echo "  --hot: Enable hot table scanner (long-running scan queries)"
        echo "  --no-hot: Disable hot table scanner (default)"
        echo "  --no-log: Disable file logging (output to /dev/null)"
        exit 1
        ;;
    esac
  done
  
  # Check if a specific benchmark was requested
  if [ -n "$benchmark_name" ]; then
    setup_config
    
    case $benchmark_name in
      tpcc)
        run_tpcc
        ;;
      tatp)
        run_tatp
        ;;
      smallbank)
        run_smallbank
        ;;
      *)
        echo "Unknown benchmark: $benchmark_name"
        echo "Valid benchmarks: tpcc, tatp, smallbank"
        exit 1
        ;;
    esac
    
    cleanup
  else
    # Run all benchmarks sequentially
    setup_config
    
    echo "Running all benchmarks sequentially..."
    if [ "$enable_hot_table_scanner" = true ]; then
      echo "Hot table scanner: ENABLED for all benchmarks"
    else
      echo "Hot table scanner: DISABLED for all benchmarks"
    fi
    if [ "$enable_file_logging" = true ]; then
      echo "File logging: ENABLED (output to ${output_dir})"
    else
      echo "File logging: DISABLED (output to /dev/null)"
    fi
    echo ""
    
    run_tpcc
    cleanup
    sleep 5
    
    # Use different port for next benchmark
    port=$((port + 100))
    run_tatp
    cleanup
    sleep 5
    
    # Use different port for next benchmark
    port=$((port + 100))
    run_smallbank
    cleanup
    
    echo ""
    echo "========================================="
    echo "All benchmarks completed!"
    if [ "$enable_file_logging" = true ]; then
      echo "Results are in: $output_dir"
      echo "  - TPCC: ${output_dir}/tpcc_default.log"
      echo "  - TATP: ${output_dir}/tatp_default.log"
      echo "  - SmallBank: ${output_dir}/smallbank_default.log"
    else
      echo "File logging was disabled (--no-log flag used)"
    fi
    echo "========================================="
  fi
}

# Trap to cleanup on exit
trap cleanup EXIT

# Run main function
main "$@"

