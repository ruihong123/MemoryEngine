#!/bin/bash
home_dir="/users/Ruihong/MemoryEngine"
nmemory="16"
ncompute="16"
nmachines="32"
nshard="16"
numa_node=("0" "1")
port=$((10000+RANDOM%1000))
github_repo="https://github.com/ruihong123/MemoryEngine"
gitbranch="reserved_branch1"
function run_bench() {
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



  while [ $n -lt $nshard ]
  do
    echo "Set up the ${compute_shard[$n]}"
#   flame graph: sudo apt install linux-tools-4.15.0-121-generic linux-cloud-tools-4.15.0-121-generic
    ssh -o StrictHostKeyChecking=no ${compute_shard[$n]}  "git clone https://github.com/google/cityhash && cd cityhash/ && ./configure && make all check CXXFLAGS='-g -O3' && sudo make install && cd .. && sudo apt-get update && sudo apt-get install -y libnuma-dev numactl htop libmemcached-dev memcached libboost-all-dev libtbb-dev linux-tools-common" &
    ssh -o StrictHostKeyChecking=no ${compute_shard[$n]}  "sudo mkdir /mnt/core_dump ; sudo mkfs.ext4 /dev/sda4 ; sudo mount /dev/sda4 /mnt/core_dump ; sudo chown -R Ruihong:purduedb-PG0 /mnt/core_dump" &

#    ssh -o StrictHostKeyChecking=no ${compute_shard[n]} "screen -d -m pwd && cd /users/Ruihong/TimberSaw/build && git checkout $gitbranch && git pull &&  cmake -DCMAKE_BUILD_TYPE=Release .. && make db_bench Server -j 32 > /dev/null && sudo apt install numactl -y " &
#    screen -d -m pwd && cd /users/Ruihong && git clone --recurse-submodules $github_repo && cd MemoryEngine/ && mkdir release &&  cd release && cmake -DCMAKE_BUILD_TYPE=Release .. && sudo apt install numactl -y &&screen -d -m pwd && cd /users/Ruihong && git clone --recurse-submodules $github_repo && cd MemoryEngine/ && mkdir release &&  cd release && cmake -DCMAKE_BUILD_TYPE=Release .. && sudo apt install numactl -y &&
    echo "Set up the ${memory_shard[$n]}"
    ssh -o StrictHostKeyChecking=no ${memory_shard[$n]}  "git clone https://github.com/google/cityhash && cd cityhash/ && ./configure && make all check CXXFLAGS='-g -O3' && sudo make install && cd .. && sudo apt-get update && sudo apt-get install -y libnuma-dev numactl htop libmemcached-dev memcached libboost-all-dev libtbb-dev linux-tools-common" &
    ssh -o StrictHostKeyChecking=no ${memory_shard[$n]}  "sudo mkdir /mnt/core_dump ; sudo mkfs.ext4 /dev/sda4 ; sudo mount /dev/sda4 /mnt/core_dump ; sudo chown -R Ruihong:purduedb-PG0 /mnt/core_dump" &
#   git clone https://github.com/google/cityhash &&
#    ssh -o StrictHostKeyChecking=no ${memory_shard[n]} "screen -d -m pwd && cd /users/Ruihong/TimberSaw/build && git checkout $gitbranch && git pull &&  cmake -DCMAKE_BUILD_TYPE=Release .. && make db_bench Server -j 32 > /dev/null && sudo apt install numactl -y" &
    n=$((n+1))
    sleep 1
  done
  for node in ${memory_shard[@]}
  do
    echo "Rsync the $node"
    rsync -a ~/MemoryEngine $node:/users/Ruihong/
  done
  for node in ${compute_shard[@]}
  do
    echo "Rsync the $node"
    rsync -a ~/MemoryEngine $node:/users/Ruihong/
  done
	}
	run_bench