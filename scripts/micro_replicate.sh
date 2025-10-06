#! /usr/bin/env bash
# Updated script to work with replication-aware config format
# Uses connection_db_replica.conf as source (compute nodes limited by $node)
# Creates connection_replication.conf as the working config
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/..
BIN_HOME=$bin/../release
home_dir="/users/Ruihong/MemoryEngine"

conf_file_all=$bin/../connection_cloudlab_replica.conf
conf_file=$bin/../connection_replication.conf
memcached_conf_file_all=$bin/../memcached_cloudlab_servers.conf
memcached_conf_file=$bin/../memcached_ip.conf
# alpha = 1/(1-theta)
#compute_nodes=$bin/compute_nodes
#memory_nodes=$bin/memory_nodes
log_file=$bin/log
cache_mem_size=8 # 8 gb Local memory size
mem_region_size=40 # 48 gb Remote memory size
#master_ip=db3.cs.purdue.edu # make sure this is in accordance with the server whose is_master=1
master_port=12311
port=$((10000+RANDOM%1000))
#compute_num = 0
#memory_num = 0
run() {
    echo "run for result_file=$result_file,
    thread=$thread, zipfian_alpha=$zipfian_alpha, workload=$workload,
    remote_ratio=$remote_ratio, shared_ratio=$shared_ratio,
    read_ratio=$read_ratio, op_type=$op_type,
    space_locality=$space_locality, time_locality=$time_locality"

#    compute_line_all=$(sed -n '1p' conf_file_all)
#    memory_line_all=$(sed -n '2p' conf_file_all)
    # Get compute nodes from conf_file_all (connection_db_replica.conf) - skip comments and empty lines
    compute_line_all=$(grep -v '^#' "$conf_file_all" | grep -v '^$' | sed -n '1p')
    
    # Get memory nodes from conf_file_all (connection_db_replica.conf) - skip comments and empty lines
    memory_line_all=$(grep -v '^#' "$conf_file_all" | grep -v '^$' | sed -n '2p')
    
    # Create the working config file
    # First line: compute nodes from conf_file_all (limited by $node)
    awk -v pos="$node" -F' ' '{
        for (i=1; i<=NF; i++) {
            if (i <= pos) {
                printf("%s", $i)
                if (i < pos) printf(" ")
            }
        }
        print ""
    }' <(echo "$compute_line_all") > "$conf_file"
    
    # Second line: memory nodes from conf_file_all (unchanged)
    echo "$memory_line_all" >> "$conf_file"
    
    # Copy all replication configuration lines (lines 3+) from conf_file_all - skip comments and empty lines
    grep -v '^#' "$conf_file_all" | grep -v '^$' | tail -n +3 >> "$conf_file"

    # Copy memcached configuration from memcached_cloudlab_servers.conf to memcached_ip.conf
    # This ensures the working memcached config matches the compute nodes being used
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
    echo "memory nodes:"
    i=0
    for memory in "${memory_nodes[@]}"
    do
       echo $memory
       i=$((i+1))
    done
    i=0
    echo "compute nodes:"
    for compute in "${compute_nodes[@]}"
    do
       echo $compute
       i=$((i+1))
    done

    j=0
    for compute in "${compute_nodes[@]}"
    do
      ip=`echo $compute | cut -d ' ' -f1`
      ssh -o StrictHostKeyChecking=no $ip "killall micro_bench > /dev/null 2>&1 && cd $BIN_HOME"
      j=$((j+1))
  #		if [ $j = $node ]; then
  #			break;
  #		fi
    done

    j=0
      for memory in "${memory_nodes[@]}"
      do
        ip=`echo $memory | cut -d ' ' -f1`
        ssh -o StrictHostKeyChecking=no $ip "killall memory_server_term > /dev/null 2>&1 && cd $BIN_HOME"
        j=$((j+1))
  #  		if [ $j = $node ]; then
  #  			break;
  #  		fi
      done

    i=0
    while [ $i -lt $memory_num ]
    do
      echo "Rsync the connection_replication.conf to ${memory_nodes[$i]}"
      rsync -vz $home_dir/connection_replication.conf ${memory_nodes[$i]}:$home_dir/connection_replication.conf
      echo "Rsync the memcached_ip.conf to ${memory_nodes[$i]}"
      rsync -vz $home_dir/memcached_ip.conf ${memory_nodes[$i]}:$home_dir/memcached_ip.conf
      i=$((i+1))
    done
    i=0
    while [ $i -lt $compute_num ]
    do
      echo "Rsync the connection_replication.conf to ${compute_nodes[$i]}"
      rsync -vz $home_dir/connection_replication.conf ${compute_nodes[$i]}:$home_dir/connection_replication.conf
      echo "Rsync the memcached_ip.conf to ${compute_nodes[$i]}"
      rsync -vz $home_dir/memcached_ip.conf ${compute_nodes[$i]}:$home_dir/memcached_ip.conf
      i=$((i+1))
    done
    i=0
#    compute_nodes_arr=`cat "$compute_nodes"`
#    memory_nodes_arr=`cat "$memory_nodes"`
#    echo ${#memory_nodes[@]}
#    echo ${#memory_nodes[@]}

#    compute_num=$(wc -l < $compute_nodes)
#    memory_num=$(wc -l < $memory_nodes)
#    compute_num=$((compute_num+1))
#    memory_num=$((memory_num+1))
#    echo `cat $slaves`
    echo "Physical compute nodes: $compute_num"
    echo "Physical memory nodes: $memory_num"
    echo "Memcached config source: $memcached_conf_file_all"
    echo "Memcached config target: $memcached_conf_file"
    
    # Count logical memory regions for replication (skip comments and empty lines)
    logical_memory_num=$(grep -v '^#' "$conf_file" | tail -n +3 | grep -v '^$' | wc -l)
    if [ $logical_memory_num -eq 0 ]; then
        # Fallback to physical memory nodes if no logical regions defined
        logical_memory_num=$memory_num
        echo "No logical memory regions defined, using physical memory nodes: $logical_memory_num"
    else
        echo "Logical memory regions: $logical_memory_num"
    fi
  	read -r -a memcached_node <<< $(head -n 1 "$memcached_conf_file")
  	echo "restart memcached on ${memcached_node[0]}"
    ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"

    if [ $size_grow = 1  ]; then
      mem_region_size=$(($mem_region_size*$compute_num))
    else
      mem_region_size=$mem_region_size
    fi
    remote_mem_size=$(($mem_region_size*$replication_factor/$memory_num))

    for memory in "${memory_nodes[@]}"
        do
          ip=$memory
#            	port=`echo $memory | cut -d ' ' -f2`
          echo ""
          echo "memory = $memory, ip = $ip, port = $port"
          echo "$BIN_HOME/memory_server_term  $port $(($remote_mem_size +2)) $((2*$i +1)) $mem_region_size | tee -a $log_file.$ip"
          ssh -o StrictHostKeyChecking=no $ip	"ulimit -c 50000000 && cd $BIN_HOME && numactl --physcpubind=4 ./memory_server_term  $port $(($remote_mem_size+2)) $((2*$i +1)) $mem_region_size | tee -a $log_file.$ip " &
          sleep 1
          i=$((i+1))
#        	if [ "$i" = "$node" ]; then
#        		break
#        	fi
        done # for slave
    sleep 2
    i=0
    for compute in "${compute_nodes[@]}"
      do
        ip=$compute
        if [ $i = 0 ]; then
          is_master=1
              master_ip=$ip
        else
          is_master=0
        fi
#        if [ $port == $ip ]; then
#          port=12345
#        fi
        echo ""
        echo "compute = $compute, ip = $ip, port = $port"
        # For replication config, we need to count logical memory regions
        # Count the number of logical memory regions (lines 3+ in config, skip comments)
        logical_memory_num=$(grep -v '^#' "$conf_file" | tail -n +3 | grep -v '^$' | wc -l)
        if [ $logical_memory_num -eq 0 ]; then
            # Fallback to physical memory nodes if no logical regions defined
            logical_memory_num=$memory_num
        fi
        
        echo "$BIN_HOME/micro_bench --op_type $op_type --workload $workload --zipfian_alpha $zipfian_alpha --no_thread $thread --shared_ratio $shared_ratio --read_ratio $read_ratio --space_locality $space_locality --time_locality $time_locality --result_file $result_file --this_node_id $((2*$i)) --tcp_port $port --is_master $is_master --cache_size $cache_mem_size --allocated_mem_size $mem_region_size --compute_num $compute_num --memory_num $logical_memory_num | tee -a $log_file.$ip"
        ssh -o StrictHostKeyChecking=no $ip	"ulimit -c 50000000 && cd $BIN_HOME && ./micro_bench --op_type $op_type --workload $workload --zipfian_alpha $zipfian_alpha --no_thread $thread --shared_ratio $shared_ratio --read_ratio $read_ratio --space_locality $space_locality --time_locality $time_locality --result_file $result_file --this_node_id $((2*$i)) --tcp_port $port --is_master $is_master --cache_size $cache_mem_size --allocated_mem_size $mem_region_size --compute_num $compute_num --memory_num $logical_memory_num | tee $log_file.$ip" &
        sleep 1
        i=$((i+1))
  #    	if [ "$i" = "$node" ]; then
  #    		break
  #    	fi
      done # for compute

	  wait


    sleep 1
    IFS="$old_IFS"
}


run_thread_test() {
# thread test
echo "*********************run thread test**********************"
result_file=$bin/results/thread
node_range="8"
thread_range="1 2 3 4 5 6 7 8"
remote_range="0 50 100"
shared_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
read_range="50 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for remote_ratio in $remote_range
do
for op_type in $op_range
do
for read_ratio in $read_range
do
for shared_ratio in $shared_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_remote_test() {
# remote test
echo "**************************run remote test****************************"
result_file=$bin/results/remote_ratio
node_range="8"
thread_range="1"
remote_range="0 10 20 30 40 50 60 70 80 90 100"
shared_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
read_range="0 100" #"0 50 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for op_type in $op_range
do
for read_ratio in $read_range
do
for shared_ratio in $shared_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for remote_ratio in $remote_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_shared_test() {
# shared test
echo "**************************run shared test****************************"
result_file=$bin/results/shared_ratio
node_range="8"
thread_range="1"
remote_range="88"
shared_range="0 10 20 30 40 50 60 70 80 90 100"
read_range="50" #"0 50 70 80 90 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}

run_shared_test_noeviction() {
# shared test
echo "**************************run shared test****************************"
result_file=$bin/results/shared_ratio-noeviction
node_range="8"
thread_range="1"
remote_range="88"
shared_range="0 10 20 30 40 50 60 70 80 90 100"
read_range="50 100" #"0 50 70 80 90 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.5

for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_read_test() {
# read ratio test
echo "**************************run read ratio test****************************"
result_file=$bin/results/read_ratio
node_range="8"
thread_range="1"
remote_range="0 50 100"
shared_range="0"
read_range="0 10 20 30 40 50 60 70 80 90 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for remote_ratio in $remote_range
do
for op_type in $op_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for shared_ratio in $shared_range
do
for read_ratio in $read_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}

run_space_test() {
# space locality test
echo "**************************run space locality test****************************"
result_file=$bin/results/space_locality
node_range="8"
thread_range="1"
remote_range="100"
shared_range="0"
read_range="0 50 100"
space_range="0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for remote_ratio in $remote_range
do
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for shared_ratio in $shared_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_time_test() {
# time locality test
echo "**************************run time locality test****************************"
result_file=$bin/results/time_locality
node_range="8"
thread_range="1"
remote_range="100"
shared_range="0"
read_range="0 50 100"
space_range="0"
time_range="0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for remote_ratio in $remote_range
do
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for shared_ratio in $shared_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_node_test() {
# node test
echo "**************************run node test****************************"
result_file=$bin/results/node
node_range="8"
thread_range="8"
remote_range="100"
shared_range="100"
size_grow=0 # 0 not grow, 1 grow with node number
read_range="0 50 95 100"
space_range="0"
time_range="0"
workload_range="0 1" # 0 uniform, 1 single zipfian, n >1 multispot zipfian.
replication_factors="3"
zipfian_alpha_range="0.99" #make sure workload = 1 if we want to test zipfian.
#
op_range="1" # use 1
#cache_th=0.5
for workload in $workload_range
do
for zipfian_alpha in $zipfian_alpha_range
do
for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
  if [ $shared_ratio != 100 ] && [ $size_grow = 1 ]; then
      exit
  fi
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do

for thread in $thread_range
do
for node in $node_range
do
for replication_factor in $replication_factors
do
  echo $node
#    remote_ratio=`echo "($node-1)*100/$node" | bc`
#    echo $remote_ratio
#    if [[ $node = 1 ]]; then
#        continue;
#    fi
  run
done
done
done
done
done
done
done
done
done
done
done
}

#run_thread_test
#run_read_test
#run_time_test
#run_shared_test
#run_remote_test
#run_space_test
#run_shared_test_noeviction
run_node_test
