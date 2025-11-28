#!/bin/bash
home_dir="/users/Ruihong/MemoryEngine/"
side_dir="/users/Ruihong/motor/"
nmemory="9"
ncompute="9"
nmachines="18"
nshard="9"
numa_node=("0" "1")
port=$((10000+RANDOM%1000))
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/..
BIN_HOME=$bin/../release
core_dump_dir="/mnt/core_dump"
github_repo="https://github.com/ruihong123/MemoryEngine"
gitbranch="reserved_branch1"

cleanup_memory_node() {
  local node="$1"
  ssh ${ssh_opts} "${node}" bash <<EOF
pkill -f micro_bench >/dev/null 2>&1 || true
pkill -f mvcc_storage_bench >/dev/null 2>&1 || true
sudo pkill -f motor_mempool >/dev/null 2>&1 || true
pkill -f memory_server_term >/dev/null 2>&1 || true
pkill -f tpcc >/dev/null 2>&1 || true
pkill -f tatp >/dev/null 2>&1 || true
pkill -f smallbank >/dev/null 2>&1 || true
pkill -f memory_server_tpcc >/dev/null 2>&1 || true
pkill -f memory_server >/dev/null 2>&1 || true
pkill -f btree_bench >/dev/null 2>&1 || true
rm -f ${home_dir}/scripts/log* >/dev/null 2>&1 || true
rm -f ${home_dir}/scripts/results/* >/dev/null 2>&1 || true
rm -f ${home_dir}/scripts/data/* >/dev/null 2>&1 || true
rm -f ${home_dir}/debug/logdump.txt >/dev/null 2>&1 || true
rm -f ${home_dir}/release/logdump.txt >/dev/null 2>&1 || true
rm -f ${core_dump_dir}/core* >/dev/null 2>&1 || true
EOF
}

cleanup_compute_node() {
  local node="$1"
  ssh ${ssh_opts} "${node}" bash <<EOF
rm -f /mnt/core_dump/core* >/dev/null 2>&1 || true
pkill -f motor_mempool >/dev/null 2>&1 || true
pkill -f micro_bench >/dev/null 2>&1 || true
pkill -f mvcc_storage_bench >/dev/null 2>&1 || true
pkill -f memory_server_term >/dev/null 2>&1 || true
pkill -f tpcc >/dev/null 2>&1 || true
pkill -f tatp >/dev/null 2>&1 || true
pkill -f smallbank >/dev/null 2>&1 || true
pkill -f memory_server_tpcc >/dev/null 2>&1 || true
pkill -f memory_server >/dev/null 2>&1 || true
pkill -f btree_bench >/dev/null 2>&1 || true
rm -f ${home_dir}/scripts/log* >/dev/null 2>&1 || true
rm -f ${home_dir}/scripts/results/* >/dev/null 2>&1 || true
rm -f ${home_dir}/scripts/data/* >/dev/null 2>&1 || true
rm -f ${home_dir}/debug/logdump.txt >/dev/null 2>&1 || true
rm -f ${home_dir}/release/logdump.txt >/dev/null 2>&1 || true
rm -f ${core_dump_dir}/core* >/dev/null 2>&1 || true
EOF
}

function run_bench() {
  results_dir="$SRC_HOME/scripts/results"
  if [ -d "$results_dir" ]; then
    echo "Cleaning results directory at $results_dir"
    find "$results_dir" -mindepth 1 -delete
  fi
  communication_port=()
#	memory_port=()
	memory_server=()
  memory_shard=()

#	compute_port=()
	compute_server=()
	compute_shard=()
#	machines=()
	i=0
  n=0
  while [ $n -lt $nmemory ]
  do
    memory_server+=("node-$i")
    i=$((i+1))
    n=$((n+1))
  done
  n=0
  i=$((nmachines-1))
  while [ $n -lt $ncompute ]
  do

    compute_server+=("node-$i")
    i=$((i-1))
    n=$((n+1))
  done
  echo "here are the sets up"
  echo $?
  echo compute servers are ${compute_server[@]}
  echo memoryserver is ${memory_server[@]}
#  echo ${machines[@]}
  n=0
  while [ $n -lt $nshard ]
  do
    communication_port+=("$((port+n))")
    n=$((n+1))
  done
  n=0
  while [ $n -lt $nshard ]
  do
    # if [[ $i == "2" ]]; then
    # 	i=$((i-1))
    # 	continue
    # fi
    compute_shard+=(${compute_server[$n%$ncompute]})
    memory_shard+=(${memory_server[$n%$nmemory]})
    n=$((n+1))
  done
  echo compute shards are ${compute_shard[@]}
  echo memory shards are ${memory_shard[@]}
  echo communication ports are ${communication_port[@]}
#  test for download and compile the codes
  n=0
#  while [ $n -lt $nshard ]
#  do
#    echo "Set up the ${compute_shard[n]}"
#    ssh -o StrictHostKeyChecking=no ${compute_shard[n]}  "sudo apt-get install -y libnuma-dev htop" &
##    ssh -o StrictHostKeyChecking=no ${compute_shard[n]} "screen -d -m pwd && cd /users/Ruihong/TimberSaw/build && git checkout $gitbranch && git pull &&  cmake -DCMAKE_BUILD_TYPE=Release .. && make db_bench Server -j 32 > /dev/null && sudo apt install numactl -y " &
##    screen -d -m pwd && cd /users/Ruihong && git clone --recurse-submodules $github_repo && cd MemoryEngine/ && mkdir release &&  cd release && cmake -DCMAKE_BUILD_TYPE=Release .. && sudo apt install numactl -y &&screen -d -m pwd && cd /users/Ruihong && git clone --recurse-submodules $github_repo && cd MemoryEngine/ && mkdir release &&  cd release && cmake -DCMAKE_BUILD_TYPE=Release .. && sudo apt install numactl -y &&
#    echo "Set up the ${memory_shard[n]}"
#    ssh -o StrictHostKeyChecking=no ${memory_shard[n]}  "sudo apt-get install -y libnuma-dev htop" &
##    ssh -o StrictHostKeyChecking=no ${memory_shard[n]} "screen -d -m pwd && cd /users/Ruihong/TimberSaw/build && git checkout $gitbranch && git pull &&  cmake -DCMAKE_BUILD_TYPE=Release .. && make db_bench Server -j 32 > /dev/null && sudo apt install numactl -y" &
#    n=$((n+1))
#    sleep 1
#  done
  for node in ${memory_shard[@]}
  do
    echo "Rsync the $node rsync -a $home_dir $node:$home_dir"
#    ssh -o StrictHostKeyChecking=no $node "sudo apt-get install -y libnuma-dev numactl htop libmemcached-dev libboost-all-dev" &

#    ssh -o StrictHostKeyChecking=no $node  "sudo umount /mnt/core_dump & rm /mnt/core_dump/core*"

    
#    ssh -o StrictHostKeyChecking=no $node "killall micro_bench memory_server_term > /dev/null 2>&1"
#    ssh -o StrictHostKeyChecking=no $node "sudo apt install libtbb-dev -y" &
    # ssh -o StrictHostKeyChecking=no $node "pkill -f memory_server_tpcc" &
    #  ssh -o StrictHostKeyChecking=no $node "pkill -f tpcc" &
    cleanup_memory_node "${node}"
    rsync -a "$home_dir" "$node:$home_dir" &
    rsync -a "$side_dir" "$node:$side_dir" &
#    ssh ${ssh_opts} $node "sudo mkdir /mnt/core_dump && sudo mkfs.ext4 /dev/sda4 && sudo mount /dev/sda4 /mnt/core_dump"

    ssh ${ssh_opts} $node "echo '$core_dump_dir/core$node.%p' | sudo tee /proc/sys/kernel/core_pattern ;  sudo chown -R Ruihong:purduedb-PG0 /mnt/core_dump" &
#    ssh -o StrictHostKeyChecking=no $node  "sudo mount /dev/sda4 /mnt/core_dump" &

#    ssh -o StrictHostKeyChecking=no $node "sudo /etc/init.d/openibd restart" &
#    ssh -o StrictHostKeyChecking=no $node "sudo mst start" &
#    ssh -o StrictHostKeyChecking=no $node "echo '/proj/purduedb-PG0/logs/core$node' | sudo tee /proc/sys/kernel/core_pattern"
  done
  for node in ${compute_shard[@]}
  do
    echo "Rsync the $node rsync -a $home_dir $node:$home_dir"
#    ssh -o StrictHostKeyChecking=no $node "sudo apt-get install -y libnuma-dev numactl htop libmemcached-dev libboost-all-dev" &
#    ssh -o StrictHostKeyChecking=no $node  "sudo umount /mnt/core_dump & rm /mnt/core_dump/core*"
    #  ssh -o StrictHostKeyChecking=no $node "pkill -f memory_server_tpcc" &
    #  ssh -o StrictHostKeyChecking=no $node "pkill -f tpcc" &
    cleanup_compute_node "${node}"
    rsync -a "$home_dir" "$node:$home_dir" &
    rsync -a "$side_dir" "$node:$side_dir" &
#    ssh ${ssh_opts} $node "sudo mkdir /mnt/core_dump && sudo mkfs.ext4 /dev/sda4 && sudo mount /dev/sda4 /mnt/core_dump"

    ssh ${ssh_opts} $node "echo '$core_dump_dir/core$node.%p' | sudo tee /proc/sys/kernel/core_pattern ; sudo chown -R Ruihong:purduedb-PG0 /mnt/core_dump" &

#    ssh -o StrictHostKeyChecking=no $node  "sudo mount /dev/sda4 /mnt/core_dump" &

#    ssh -o StrictHostKeyChecking=no $node "sudo /etc/init.d/openibd restart" &
#    ssh -o StrictHostKeyChecking=no $node "sudo mst start" &
#    ssh -o StrictHostKeyChecking=no $node "echo '/proj/purduedb-PG0/logs/core$node' | sudo tee /proc/sys/kernel/core_pattern"


  done
  echo "All rsync operations completed."
  
  read -r -a memcached_node <<< $(head -n 1 $SRC_HOME/memcached_ip.conf)
  echo "restart memcached on ${memcached_node[0]}"
  ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"


#  systemctl status opensmd.service
#    sudo /etc/init.d/openibd restart
#    sudo mst start


	}
	run_bench