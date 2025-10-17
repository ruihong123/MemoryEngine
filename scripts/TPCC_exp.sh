#!/bin/bash
# Updated script to work with replication-aware config format
# Uses connection_cloudlab_replica.conf as source
# Creates connection_replication.conf as the working config
set -o nounset
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/..
#BIN_HOME=$bin/../release
# With the specified arguments for benchmark setting,
# this script_compute runs tpcc for varied distributed ratios

# specify your hosts_file here
# hosts_file specify a list of host names and port numbers, with the host names in the first column
#Compute_file="../tpcc/compute.txt"
#Memory_file="../tpcc/memory.txt"
conf_file_all=$bin/../connection_cloudlab_replica.conf
conf_file=$bin/../connection_replication.conf
memcached_conf_file_all=$bin/../memcached_cloudlab_servers.conf
memcached_conf_file=$bin/../memcached_ip.conf

# specify your directory for log files
output_dir="/users/Ruihong/MemoryEngine/scripts/data"
core_dump_dir="/mnt/core_dump"
# working environment
proj_dir="/users/Ruihong/MemoryEngine"
bin_dir="${proj_dir}/release"
script_dir="${proj_dir}/database/scripts"
ssh_opts="-o StrictHostKeyChecking=no"

cache_mem_size=8 # 8 gb Local memory size (Currently not working)
remote_mem_size=55 # 8 gb Remote memory size pernode is enough
port=$((13000+RANDOM%1000))

compute_ARGS="$@"

echo "input Arguments: ${compute_ARGS}"
echo "launch..."

launch () {

  # Create output directory if it doesn't exist
  if [ ! -d "$output_dir" ]; then
    echo "Creating output directory: $output_dir"
    mkdir -p "$output_dir"
  fi

  # Create the working config file from the source config file
  # Get compute nodes from conf_file_all (connection_cloudlab_replica.conf) - skip comments and empty lines
  compute_line_all=$(grep -v '^#' "$conf_file_all" | grep -v '^$' | sed -n '1p')
  
  # Get memory nodes from conf_file_all (connection_cloudlab_replica.conf) - skip comments and empty lines
  memory_line_all=$(grep -v '^#' "$conf_file_all" | grep -v '^$' | sed -n '2p')
  
  # Create the working config file
  # First line: compute nodes from conf_file_all (all nodes for TPCC)
  echo "$compute_line_all" > "$conf_file"
  
  # Second line: memory nodes from conf_file_all (unchanged)
  echo "$memory_line_all" >> "$conf_file"
  
  # Copy all replication configuration lines (lines 3+) from conf_file_all - skip comments and empty lines
  grep -v '^#' "$conf_file_all" | grep -v '^$' | tail -n +3 >> "$conf_file"

  # Copy memcached configuration from memcached_cloudlab_servers.conf to memcached_ip.conf
  # This ensures the working memcached config matches the compute nodes being used
  cp "$memcached_conf_file_all" "$memcached_conf_file"
  echo "Copied memcached config from $memcached_conf_file_all to $memcached_conf_file"

  # Parse the working config file
  compute_line=$(grep -v '^#' $conf_file | grep -v '^$' | sed -n '1p')
  memory_line=$(grep -v '^#' $conf_file | grep -v '^$' | sed -n '2p')
  read -r -a compute_nodes <<< "$compute_line"
  read -r -a memory_nodes <<< "$memory_line"
  compute_num=${#compute_nodes[@]}
  memory_num=${#memory_nodes[@]}
  
  echo "memory nodes:"
  for memory in "${memory_nodes[@]}"
  do
     echo $memory
  done
  echo "compute nodes:"
  for compute in "${compute_nodes[@]}"
  do
     echo $compute
  done

  master_host=${compute_nodes[0]}

  # Count logical memory regions for replication (skip comments and empty lines)
  logical_memory_num=$(grep -v '^#' "$conf_file" | tail -n +3 | grep -v '^$' | wc -l)
  if [ $logical_memory_num -eq 0 ]; then
      # Fallback to physical memory nodes if no logical regions defined
      logical_memory_num=$memory_num
      echo "No logical memory regions defined, using physical memory nodes: $logical_memory_num"
  else
      echo "Logical memory regions: $logical_memory_num"
  fi

  echo "Physical compute nodes: $compute_num"
  echo "Physical memory nodes: $memory_num"

  # Combine all nodes for parallel operations
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
  echo "restart memcached on ${memcached_node[0]}"
  ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"
  # rm /proj/purduedb-PG0/logs/core

  dist_ratio=$1
  echo "start tpcc for dist_ratio ${dist_ratio}"
  output_file="${output_dir}/${dist_ratio}_tpcc.log"
  memory_file="${output_dir}/Memory.log"
  for ((i=0;i<${#memory_nodes[@]};i++)); do
        memory=${memory_nodes[$i]}
        script_memory="ulimit -c unlimited && cd ${bin_dir} && ./memory_server_tpcc $port $(($remote_mem_size)) $((2*$i +1)) > ${output_file} 2>&1"
        echo "start worker: ssh ${ssh_opts} ${memory} '$script_memory' &"
        ssh ${ssh_opts} ${memory} "echo '$core_dump_dir/core$memory' | sudo tee /proc/sys/kernel/core_pattern"
        ssh ${ssh_opts} ${memory} " $script_memory" &
        sleep 1
  done
  script_compute="cd ${bin_dir} && ./tpcc ${compute_ARGS} -d${dist_ratio}"
  echo "start master: ssh ${ssh_opts} ${master_host} '$script_compute -sn$master_host  -nid0 | tee -a ${output_file} "
  ssh ${ssh_opts} ${master_host} "echo '$core_dump_dir/core$master_host' | sudo tee /proc/sys/kernel/core_pattern"

  ssh ${ssh_opts} ${master_host} "ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 |tee -a ${output_file}" &
#  sleep 1

  for ((i=1;i<${#compute_nodes[@]};i++)); do
    compute=${compute_nodes[$i]}
    echo "start worker: ssh ${ssh_opts} ${compute} '$script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}' &"
    ssh ${ssh_opts} ${compute} "echo '$core_dump_dir/core$compute' | sudo tee /proc/sys/kernel/core_pattern"
    ssh ${ssh_opts} ${compute} "ulimit -S -c unlimited && $script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}" &
#    sleep 1
  done

  wait
  sleep 3
  echo "done for ${dist_ratio}"
}

run_tpcc () {
#  dist_ratios=(0 10 20 30 40 50 60 70 80 90 100)
  dist_ratios=(100)

  for dist_ratio in ${dist_ratios[@]}; do
    launch ${dist_ratio}
  done
}



vary_query_ratio () {
  #read_ratios=(0 30 50 70 90 100)
  thread_number=(8)
  WarehouseNum=(80 256)
  FREQUENCY_DELIVERY=(100 0 0 0 0 1 33 0 0)
  FREQUENCY_PAYMENT=(0 100 0 0 0 10 33 0 50)
  FREQUENCY_NEW_ORDER=(0 0 100 0 0 10 33 0 50)
  FREQUENCY_ORDER_STATUS=(0 0 0 100 0 1 0 50 0)
  FREQUENCY_STOCK_LEVEL=(0 0 0 0 100 1 0 50 0)
  for ware_num in ${WarehouseNum[@]}; do
    for qr_index in 5; do
      for thread_n in ${thread_number[@]}; do
        compute_ARGS="-p$port -sf$ware_num -sf1 -c$thread_n -rde${FREQUENCY_DELIVERY[$qr_index]} -rpa${FREQUENCY_PAYMENT[$qr_index]} -rne${FREQUENCY_NEW_ORDER[$qr_index]} -ror${FREQUENCY_ORDER_STATUS[$qr_index]} -rst${FREQUENCY_STOCK_LEVEL[$qr_index]} -t4000000 -f${conf_file} -lat"
        run_tpcc
      done
    done
  done
}

vary_temp_locality () {
  #localities=(0 30 50 70 90 100)
  localities=(0 50 100)
  for locality in ${localities[@]}; do
    old_user_args=${compute_ARGS}
    compute_ARGS="${compute_ARGS -l${locality}}"
    run_tpcc
    compute_ARGS=${old_user_args}
  done
}

auto_fill_params () {
  # so that users don't need to specify parameters for themselves
  compute_ARGS="-p$port -sf512 -sf1 -c4 -t200000 -f${conf_file}"
}

auto_fill_params
# run standard tpcc
#run_tpcc
#vary_thread_number
vary_query_ratio
# vary_read_ratios
#vary_temp_locality
