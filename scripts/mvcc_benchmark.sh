#!/usr/bin/env bash
# MVCC Storage Benchmark Automation Script
# Similar to micro_replicate.sh but for mvcc_storage_bench

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/..
BIN_HOME=$bin/../debug
home_dir="/users/Ruihong/MemoryEngine"

conf_file_all=$bin/../connection_cloudlab_replica.conf
conf_file=$bin/../connection_replication.conf
memcached_conf_file_all=$bin/../memcached_cloudlab_servers.conf
memcached_conf_file=$bin/../memcached_ip.conf

log_file=$bin/log
cache_mem_size=8 # 8 GB Local cache size
mem_region_size=40 # 40 GB Remote memory size per memory server
port=$((10000+RANDOM%1000))

# MVCC Benchmark Parameters (defaults will be set in run_mvcc_benchmark function)
# These are commented out so loop variables can override them
# threads=8
# read_ratio=50
# mixed_workload=1
# writers=4
# readers=4
# storage_type=1  # 1=DeltaSection, 2=VersionChain, 3=DeltaInGCL
# workload_type=0  # 0=uniform, 1=zipfian
# zipfian_theta=0.99
# num_tuples=100000
# snapshot_lag=10000
# warmup_duration=10
# duration=30
result_file=$bin/results/mvcc_storage

run() {
    # Set defaults for any variables not set by the loops
    : ${threads:=8}
    : ${read_ratio:=50}
    : ${mixed_workload:=1}
    : ${writers:=4}
    : ${readers:=4}
    : ${storage_type:=1}
    : ${workload_type:=0}
    : ${zipfian_theta:=0.99}
    : ${num_tuples:=100000}
    : ${snapshot_lag:=10000}
    : ${warmup_duration:=10}
    : ${duration:=30}
    
    echo "========================================="
    echo "Running MVCC Storage Benchmark"
    echo "result_file=$result_file"
    echo "nodes=$node, threads=$threads, read_ratio=$read_ratio"
    echo "storage_type=$storage_type, workload_type=$workload_type"
    echo "num_tuples=$num_tuples, snapshot_lag=$snapshot_lag"
    echo "mixed_workload=$mixed_workload, zipfian_theta=$zipfian_theta"
    echo "duration=$duration"
    echo "========================================="

    # Get compute and memory nodes from config
    compute_line_all=$(grep -v '^#' "$conf_file_all" | grep -v '^$' | sed -n '1p')
    memory_line_all=$(grep -v '^#' "$conf_file_all" | grep -v '^$' | sed -n '2p')
    
    # Create the working config file (limit compute nodes by $node parameter)
    awk -v pos="$node" -F' ' '{
        for (i=1; i<=NF; i++) {
            if (i <= pos) {
                printf("%s", $i)
                if (i < pos) printf(" ")
            }
        }
        print ""
    }' <(echo "$compute_line_all") > "$conf_file"
    
    # Second line: memory nodes - limit to match compute nodes
    # For MVCC benchmark, typically we want memory_nodes >= compute_nodes / 2
    # But to keep it simple, we'll use the same limit as compute nodes
    awk -v pos="$node" -F' ' '{
        for (i=1; i<=NF; i++) {
            if (i <= pos) {
                printf("%s", $i)
                if (i < pos) printf(" ")
            }
        }
        print ""
    }' <(echo "$memory_line_all") >> "$conf_file"
    
    # Copy all replication configuration lines (lines 3+)
    grep -v '^#' "$conf_file_all" | grep -v '^$' | tail -n +3 >> "$conf_file"

    # Copy memcached configuration
    cp "$memcached_conf_file_all" "$memcached_conf_file"
    echo "Copied memcached config from $memcached_conf_file_all to $memcached_conf_file"

    old_IFS=$IFS
    IFS=' '
    compute_line=$(grep -v '^#' $conf_file | grep -v '^$' | sed -n '1p')
    memory_line=$(grep -v '^#' $conf_file | grep -v '^$' | sed -n '2p')
    read -r -a compute_nodes <<< "$compute_line"
    read -r -a memory_nodes <<< "$memory_line"
    compute_num=${#compute_nodes[@]}
    memory_num=${#memory_nodes[@]}
    
    echo "Memory nodes:"
    i=0
    for memory in "${memory_nodes[@]}"
    do
       echo "  [$i] $memory"
       i=$((i+1))
    done
    
    echo "Compute nodes:"
    i=0
    for compute in "${compute_nodes[@]}"
    do
       echo "  [$i] $compute"
       i=$((i+1))
    done

    # Kill any existing processes
    echo "Cleaning up existing processes..."
    j=0
    for compute in "${compute_nodes[@]}"
    do
      ip=`echo $compute | cut -d ' ' -f1`
      ssh -o StrictHostKeyChecking=no $ip "pkill -f mvcc_storage_bench > /dev/null 2>&1 && cd $BIN_HOME"
      j=$((j+1))
    done

    j=0
    for memory in "${memory_nodes[@]}"
    do
      ip=`echo $memory | cut -d ' ' -f1`
      ssh -o StrictHostKeyChecking=no $ip "pkill -f memory_server_term > /dev/null 2>&1 && cd $BIN_HOME"
      j=$((j+1))
    done

    # Sync config files to all nodes
    i=0
    while [ $i -lt $memory_num ]
    do
      echo "Rsync configs to ${memory_nodes[$i]}"
      rsync -vz $home_dir/connection_replication.conf ${memory_nodes[$i]}:$home_dir/connection_replication.conf
      rsync -vz $home_dir/memcached_ip.conf ${memory_nodes[$i]}:$home_dir/memcached_ip.conf
      i=$((i+1))
    done
    
    i=0
    while [ $i -lt $compute_num ]
    do
      echo "Rsync configs to ${compute_nodes[$i]}"
      rsync -vz $home_dir/connection_replication.conf ${compute_nodes[$i]}:$home_dir/connection_replication.conf
      rsync -vz $home_dir/memcached_ip.conf ${compute_nodes[$i]}:$home_dir/memcached_ip.conf
      i=$((i+1))
    done

    echo "Physical compute nodes: $compute_num"
    echo "Physical memory nodes: $memory_num"
    
    # Count logical memory regions for replication
    logical_memory_num=$(grep -v '^#' "$conf_file" | tail -n +3 | grep -v '^$' | wc -l)
    if [ $logical_memory_num -eq 0 ]; then
        logical_memory_num=$memory_num
        echo "No logical memory regions defined, using physical memory nodes: $logical_memory_num"
    else
        echo "Logical memory regions: $logical_memory_num"
    fi
    
    # Restart memcached
    read -r -a memcached_node <<< $(head -n 1 "$memcached_conf_file")
    echo "Restarting memcached on ${memcached_node[0]}"
    ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"

    # Calculate remote memory size per memory server
    if [ $size_grow = 1  ]; then
      mem_region_size=$(($mem_region_size*$compute_num))
    fi
    remote_mem_size=$(($mem_region_size*$replication_factor/$memory_num))

    # Start memory servers
    i=0
    for memory in "${memory_nodes[@]}"
    do
      ip=$memory
      echo ""
      echo "Starting memory server on $memory (node_id=$((2*$i+1)))"
      echo "$BIN_HOME/memory_server_term $port $(($remote_mem_size+2)) $((2*$i+1)) $mem_region_size"
      ssh -o StrictHostKeyChecking=no $ip "ulimit -c 50000000 && cd $BIN_HOME && numactl --physcpubind=4 ./memory_server_term $port $(($remote_mem_size+2)) $((2*$i+1)) $mem_region_size | tee -a $log_file.$ip" &
      sleep 1
      i=$((i+1))
    done
    
    sleep 2
    
    # Start compute nodes with MVCC storage benchmark
    i=0
    for compute in "${compute_nodes[@]}"
    do
      ip=$compute
      is_master=0
      if [ $i = 0 ]; then
        is_master=1
      fi
      
      echo ""
      echo "Starting MVCC benchmark on $compute (node_id=$((2*$i)))"
      
      if [ $mixed_workload = 1 ]; then
        cmd="$BIN_HOME/mvcc_storage_bench \
--node_id $((2*$i)) \
--tcp_port $port \
--threads $threads \
--read_ratio $read_ratio \
--mixed_workload 1 \
--storage_type $storage_type \
--workload_type $workload_type \
--zipfian_theta $zipfian_theta \
--num_tuples $num_tuples \
--snapshot_lag $snapshot_lag \
--warmup_duration $warmup_duration \
--duration $duration \
--cache_size $cache_mem_size"
      else
        cmd="$BIN_HOME/mvcc_storage_bench \
--node_id $((2*$i)) \
--tcp_port $port \
--mixed_workload 0 \
--writers $writers \
--readers $readers \
--storage_type $storage_type \
--workload_type $workload_type \
--zipfian_theta $zipfian_theta \
--num_tuples $num_tuples \
--snapshot_lag $snapshot_lag \
--warmup_duration $warmup_duration \
--duration $duration \
--cache_size $cache_mem_size"
      fi
      
      echo "$cmd"
      ssh -o StrictHostKeyChecking=no $ip "ulimit -c 50000000 && cd $BIN_HOME && $cmd | tee $log_file.$ip" &
      sleep 1
      i=$((i+1))
    done

    wait

    sleep 1
    IFS="$old_IFS"
}

# ==============================================================================
# Single Configurable Test Function
# ==============================================================================
# This function runs MVCC storage benchmarks with configurable parameters.
# Set variables before calling this function to customize the test.
#
# Example usage:
#   result_file=$bin/results/my_test
#   node_range="1 2 4"
#   threads_range="4 8 16"
#   read_ratio_range="50 80 95"
#   storage_type_range="1 2"
#   workload_type_range="0 1"
#   run_mvcc_benchmark
#
# Configurable Parameters:
#   result_file         - Output file path (default: $bin/results/mvcc_storage)
#   node_range          - Number of compute nodes (default: "4")
#   threads_range       - Threads per node (default: "8")
#
#   Workload Mode (choose one):
#   mixed_workload      - 1=mixed read/write, 0=separate writers/readers (default: 1)
#   read_ratio_range    - Read % for mixed mode (default: "50")
#   writers             - Writer threads for separate mode (default: 4)
#   readers             - Reader threads for separate mode (default: 4)
#                         Note: If mixed_workload=1, use read_ratio_range
#                               If mixed_workload=0, use writers/readers
#
#   storage_type_range  - Storage types: 1=Delta, 2=Chain, 3=GCL (default: "1 2 3")
#   workload_type_range - Access pattern: 0=Uniform, 1=Zipfian (default: "0")
#   zipfian_theta_range - Zipfian theta values (default: "0.99")
#   num_tuples          - Number of tuples (default: 100000)
#   snapshot_lag_range  - Snapshot lag values (default: "10000")
#   warmup_duration     - Warmup seconds (default: 10)
#   duration            - Benchmark duration seconds (default: 30)
# ==============================================================================

run_mvcc_benchmark() {
  # Set defaults for any unset parameters (using := syntax)
  : ${result_file:=$bin/results/mvcc_storage}
  : ${node_range:="8"}
  : ${threads_range:="8"}
  
  # Workload mode configuration
  : ${mixed_workload:=1}           # 1=mixed read/write, 0=separate writers/readers
  : ${read_ratio_range:="50"}      # For mixed_workload=1: read percentage
  : ${writers:=1}                  # For mixed_workload=0: number of writer threads
  : ${readers:=7}                  # For mixed_workload=0: number of reader threads
  
  : ${storage_type_range:="1"}
  : ${workload_type_range:="0"}
  : ${zipfian_theta_range:="0.1"}
  : ${num_tuples:=100000}
  : ${snapshot_lag_range:="10000"}
  : ${warmup_duration:=10}
  : ${duration:=10}
  : ${size_grow:=0}
  : ${replication_factor:=1}
  
  echo "========================================="
  echo "    MVCC Storage Benchmark"
  echo "========================================="
  echo "Configuration:"
  echo "  result_file: $result_file"
  echo "  node_range: $node_range"
  echo "  threads_range: $threads_range"
  echo ""
  echo "Workload Mode:"
  if [ "$mixed_workload" -eq 1 ]; then
    echo "  mixed_workload: 1 (mixed read/write)"
    echo "  read_ratio_range: $read_ratio_range"
  else
    echo "  mixed_workload: 0 (separate writers/readers)"
    echo "  writers: $writers"
    echo "  readers: $readers"
  fi
  echo ""
  echo "Other Parameters:"
  echo "  storage_type_range: $storage_type_range"
  echo "  workload_type_range: $workload_type_range"
  echo "  zipfian_theta_range: $zipfian_theta_range"
  echo "  num_tuples: $num_tuples"
  echo "  snapshot_lag_range: $snapshot_lag_range"
  echo "  warmup_duration: $warmup_duration"
  echo "  duration: $duration"
  echo "========================================="
  
  # Nested loops for all parameter combinations
  for storage_type in $storage_type_range
  do
    for node in $node_range
    do
      for threads in $threads_range
      do
        for read_ratio in $read_ratio_range
        do
          for workload_type in $workload_type_range
          do
            for zipfian_theta in $zipfian_theta_range
            do
              for snapshot_lag in $snapshot_lag_range
              do
                run
              done
            done
          done
        done
      done
    done
  done
  
  echo "========================================="
  echo "  Benchmark suite complete!"
  echo "========================================="
}

# ==============================================================================
# Example Configurations (uncomment to use)
# ==============================================================================

# Example 1: Quick test with default settings
run_mvcc_benchmark

# Example 2: Storage strategy comparison
# result_file=$bin/results/storage_comparison
# node_range="4"
# threads_range="8"
# read_ratio_range="50 80 95"
# storage_type_range="1 2 3"
# workload_type_range="0"
# run_mvcc_benchmark

# Example 3: Scalability test
# result_file=$bin/results/scalability
# node_range="1 2 4 8"
# threads_range="1 2 4 8 16"
# read_ratio_range="80"
# storage_type_range="1"
# run_mvcc_benchmark

# Example 4: Zipfian workload test
# result_file=$bin/results/zipfian
# node_range="4"
# threads_range="8"
# read_ratio_range="80"
# storage_type_range="1 2"
# workload_type_range="1"
# zipfian_theta_range="0.0 0.5 0.8 0.9 0.95 0.99"
# run_mvcc_benchmark

# Example 5: Snapshot lag sensitivity
# result_file=$bin/results/snapshot_lag
# node_range="4"
# threads_range="8"
# read_ratio_range="80"
# storage_type_range="1 2"
# snapshot_lag_range="1000 5000 10000 20000 50000"
# run_mvcc_benchmark
