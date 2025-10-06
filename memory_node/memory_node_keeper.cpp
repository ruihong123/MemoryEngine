//
// Created by ruihong on 7/29/21.
//
#include "memory_node/memory_node_keeper.h"

//#include "db/filename.h"
//#include "db/table_cache.h"
#include <cassert>
#include <fstream>
#include <list>
#include <sstream>
#include <limits>
#include <cctype>

//#include "table/table_builder_bams.h"
//#include "table/table_builder_memoryside.h"
#define _mm_clflush(addr) \
asm volatile("clflush %0" : "+m" (*(volatile char *)(addr)))
namespace DSMEngine {

std::shared_ptr<RDMA_Manager> Memory_Node_Keeper::rdma_mg = std::shared_ptr<RDMA_Manager>();
DSMEngine::Memory_Node_Keeper::Memory_Node_Keeper(bool use_sub_compaction, uint32_t tcp_port, int pr_s,
                                                  DSMEngine::config_t &config)
    : pr_size(pr_s),
      usesubcompaction(use_sub_compaction)

{
    //check whether the node id is correct.
    assert(config.node_id%2 == 1);
    //  size_t write_block_size = 4*1024*1024;
    //  size_t read_block_size = 4*1024;
//    size_t table_size = 16*1024;
    size_t table_size = kLeafPageSize;
    rdma_mg = std::make_shared<RDMA_Manager>(config, table_size); //set memory server node id as 1.
    Compactor_pool_.SetBackgroundThreads(0);

    // Set up the connection information.
    std::string connection_conf;
    size_t pos = 0;
    std::ifstream myfile;
    myfile.open (config_file_name, std::ios_base::in);
    std::string space_delimiter = " ";

    std::getline(myfile,connection_conf );
    uint8_t i = 0;
    uint8_t id;
    while ((pos = connection_conf.find(space_delimiter)) != std::string::npos) {
      id = 2*i;
      rdma_mg->compute_nodes.insert({id, connection_conf.substr(0, pos)});
      connection_conf.erase(0, pos + space_delimiter.length());
      i++;
    }
    rdma_mg->compute_nodes.insert({2*i, connection_conf});
    assert((rdma_mg->node_id - 1)/2 <  rdma_mg->GetComputeNodeNum());
    i = 0;
    std::getline(myfile,connection_conf );
    while ((pos = connection_conf.find(space_delimiter)) != std::string::npos) {
      id = 2*i +1;
      rdma_mg->memory_nodes.insert({id, connection_conf.substr(0, pos)});
      connection_conf.erase(0, pos + space_delimiter.length());
      i++;
    }
    rdma_mg->memory_nodes.insert({2*i + 1, connection_conf});
    
    // Parse optional replication/size lines (v2) - same as compute side
    auto parse_size = [&](const std::string& tok) -> uint64_t {
        // Returns bytes; throws on malformed input
        auto s = tok; for (auto& c: s) c = std::tolower(c);
        uint64_t mul = 1ULL;
        if ((s.length() >= 2 && s.substr(s.length()-2) == "kb") || (s.length() >= 1 && s.back() == 'k')) { mul = 1024ULL;  s.erase(s.back()=='b'?s.length()-2:s.length()-1); }
        else if ((s.length() >= 2 && s.substr(s.length()-2) == "mb") || (s.length() >= 1 && s.back() == 'm')) { mul = 1024ULL*1024;  s.erase(s.back()=='b'?s.length()-2:s.length()-1); }
        else if ((s.length() >= 2 && s.substr(s.length()-2) == "gb") || (s.length() >= 1 && s.back() == 'g')) { mul = 1024ULL*1024*1024;  s.erase(s.back()=='b'?s.length()-2:s.length()-1); }
        else if ((s.length() >= 2 && s.substr(s.length()-2) == "tb") || (s.length() >= 1 && s.back() == 't')) { mul = 1024ULL*1024*1024*1024; s.erase(s.back()=='b'?s.length()-2:s.length()-1); }
        // else: plain bytes
        uint64_t v = 0;
        try { v = std::stoull(s); } catch (...) { throw std::invalid_argument("bad size: "+tok); }
        uint64_t bytes = v * mul;
        assert(bytes != 2147483648);
        if (bytes == 0) {
            throw std::out_of_range("size is zero: "+tok);
        }
        return bytes;
    };

    std::string line;
    // Prepare ordered vector to resolve memory indices → physical ids
    std::vector<uint16_t> mem_phys_ids; mem_phys_ids.reserve(rdma_mg->memory_nodes.size());
    for (const auto& kv : rdma_mg->memory_nodes) mem_phys_ids.push_back(kv.first); // ascending odd ids

    while (std::getline(myfile, line)) {
        // skip comments/empties
        auto p = line.find_first_not_of(" \t\r\n");
        if (p == std::string::npos || line[p] == '#') continue;

        std::istringstream iss(line);
        int lid_int; std::string size_tok;
        if (!(iss >> lid_int)) continue;
        uint16_t logical_id = static_cast<uint16_t>(lid_int);

        uint64_t region_bytes = 0;
        // size is optional; if absent, region_bytes stays 0 (unspecified)
        std::streampos after_id = iss.tellg();
        if (iss >> size_tok) {
            // Look ahead: if token is purely digits or ends with unit → treat as size; else revert
            bool is_size = std::isdigit(size_tok[0]);
            for (auto c: size_tok) if (std::isalpha(c)) { is_size = true; break; }
            if (!is_size) { iss.seekg(after_id); size_tok.clear(); }
        }
        if (!size_tok.empty()) {
            try { region_bytes = parse_size(size_tok); } catch (const std::exception& e) {
                fprintf(stderr, "config: invalid size for logical %u: %s\n", logical_id, e.what());
                continue;
            }
        }

        std::vector<PhysicalRegion> physical_regions;
        int mem_idx;
        while (iss >> mem_idx) {
            if (mem_idx < 0 || static_cast<size_t>(mem_idx) >= mem_phys_ids.size()) continue;
            physical_regions.push_back({mem_phys_ids[mem_idx], 0, 0}); // base_ptr will be synchronized later via memcached
        }
        if (physical_regions.empty()) {
            // Identity fallback if no replica list provided
            if (rdma_mg->memory_nodes.count(logical_id)) physical_regions.push_back({logical_id, 0});
        }
        if (!physical_regions.empty()) {
            rdma_mg->logical_groups[logical_id] = LogicalGroup{std::move(physical_regions), region_bytes};
        }
    }

    // Backward-compat: if no logical groups parsed, build identity groups
    if (rdma_mg->logical_groups.empty()) {
        for (const auto& kv : rdma_mg->memory_nodes) {
            rdma_mg->logical_groups[kv.first] = LogicalGroup{{{kv.first, 0}}, /*bytes=*/0};
        }
    }

    myfile.close();
    
    // Memcached connection is now handled by RDMA_Manager
  }

  Memory_Node_Keeper::~Memory_Node_Keeper() {
//    delete opts->filter_policy;
    // Memcached disconnection is now handled by RDMA_Manager
  }


  void Memory_Node_Keeper::SetBackgroundThreads(int num, ThreadPoolType type) {
    Compactor_pool_.SetBackgroundThreads(num);
  }



  void Memory_Node_Keeper::server_communication_thread(std::string client_ip,
                                                 int socket_fd) {
    printf("A new shared memory thread start\n");
    printf("checkpoint1");
    char temp_receive[4*sizeof(ibv_mr)];
    char temp_send[4*sizeof(ibv_mr)] = "Q";
    int rc = 0;
    uint16_t compute_node_id;
    rdma_mg->ConnectQPThroughSocket(client_ip, socket_fd, compute_node_id);

    printf("The connected compute node's id is %d\n", compute_node_id);
    rdma_mg->res->sock_map.insert({compute_node_id, socket_fd});
    //TODO: use Local_Memory_Allocation to bulk allocate, and assign within this function.
//    ibv_mr send_mr[32] = {};
//    for(int i = 0; i<32; i++){
//      rdma_mg->Allocate_Local_RDMA_Slot(send_mr[i], "message");
//    }

//    char* send_buff;
//    if (!rdma_mg_->Local_Memory_Register(&send_buff, &send_mr, 1000, std::string())) {
//      fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
//    }
//    int buffer_number = 32;
    ibv_mr recv_mr[RECEIVE_OUTSTANDING_SIZE] = {};
    for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++){
      rdma_mg->Allocate_Local_RDMA_Slot(recv_mr[i], Message);
    }



//    char* recv_buff;
//    if (!rdma_mg_->Local_Memory_Register(&recv_buff, &recv_mr, 1000, std::string())) {
//      fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
//    }
    //  post_receive<int>(recv_mr, client_ip);
    for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++) {
      rdma_mg->post_receive<RDMA_Request>(&recv_mr[i], compute_node_id, client_ip);
    }
//    rdma_mg_->post_receive(recv_mr, client_ip, sizeof(Computing_to_memory_msg));
    // sync after send & recv buffer creation and receive request posting.
    rdma_mg->local_mem_regions.reserve(100);
    if(!rdma_mg->pre_allocated_flag)
    {
      std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
        if(!rdma_mg->pre_allocated_flag)
        {
          rdma_mg->Preregister_Memory(pr_size);
          rdma_mg->pre_allocated_flag = true;
          
          // Broadcast metadata (base pointers and rkeys) for all logical regions this memory node hosts
          for (const auto& [logical_id, group] : rdma_mg->logical_groups) {
              for (const auto& phys_reg : group.physical_regions) {
                  if (phys_reg.phys_id == rdma_mg->node_id) {
                      broadcastReplicaMetadata(logical_id, phys_reg.phys_id, phys_reg.base_ptr, phys_reg.rkey);
                  }
              }
          }
        }
    }
      ibv_mr* mr_data = rdma_mg->preregistered_region;
      assert(mr_data->length >= (uint64_t)pr_size*1024ull*1024*1024);
      memcpy(temp_send, mr_data, sizeof(ibv_mr));

      rdma_mg->global_lock_table = rdma_mg->create_lock_table();
      memcpy(temp_send + sizeof(ibv_mr), rdma_mg->global_lock_table, sizeof(ibv_mr));
    //TODO: ALLocate space for the lock table
        // If this is the node 0 then it need to broad cast the index table mr.
      if (rdma_mg->node_id == 1){
          rdma_mg->global_index_table = rdma_mg->create_index_table();
          memcpy(temp_send+ 2*sizeof(ibv_mr), rdma_mg->global_index_table, sizeof(ibv_mr));
          rdma_mg->timestamp_oracle = rdma_mg->create_timestamp_oracle();
          memcpy(temp_send+ 3*sizeof(ibv_mr), rdma_mg->timestamp_oracle, sizeof(ibv_mr));
          printf("timestamp oracle sent to node%u is %p\n", compute_node_id, rdma_mg->timestamp_oracle->addr);
          fflush(stdout);

      }


    if (rdma_mg->sock_sync_data(socket_fd, 4*sizeof(ibv_mr), temp_send,
                       temp_receive)) /* just send a dummy char back and forth */
      {
      fprintf(stderr, "sync error after QPs are were moved to RTS\n");
      rc = 1;
      }
//      shutdown(socket_fd, 2);
//    close(socket_fd);
    //  post_send<int>(res->mr_send, client_ip);

    rdma_mg->memory_connection_counter.fetch_add(1);
//    std::thread* thread_sync;
    if (rdma_mg->memory_connection_counter.load() == rdma_mg->compute_nodes.size()
        && rdma_mg->node_id == 1){
      std::thread thread_sync(&RDMA_Manager::sync_with_computes_Mside, rdma_mg.get());
      //Need to be detached.
      thread_sync.detach();
    }
    //  if(poll_completion(wc, 2, client_ip))
    //    printf("The main qp not create correctly");
    //  else
    //    printf("The main qp not create correctly");
    // Computing node and share memory connection succeed.
    // Now is the communication through rdma.


    //  receive_msg_buf = (computing_to_memory_msg*)recv_buff;
    //  receive_msg_buf->command = ntohl(receive_msg_buf->command);
    //  receive_msg_buf->content.qp_config.qp_num = ntohl(receive_msg_buf->content.qp_config.qp_num);
    //  receive_msg_buf->content.qp_config.lid = ntohs(receive_msg_buf->content.qp_config.lid);
    //  ibv_wc wc[3] = {};
      ibv_wc wc[3] = {};
    // TODO: implement a heart beat mechanism.
    int buffer_position = 0;
    int miss_poll_counter = 0;
    while (!rdma_mg->exit_flag) {
      //      printf("The main thread is running\n");
      //      fflush(stdout
//      rdma_mg->poll_completion(wc, 1, client_ip, false, compute_node_id);
      if (rdma_mg->try_poll_completions(wc, 1, client_ip, false, compute_node_id) == 0){
        // exponetial back off to save cpu cycles.
        if(++miss_poll_counter < 256){
          continue;
        }
        if(++miss_poll_counter < 512){
          usleep(16);

          continue ;
        }
        if(++miss_poll_counter < 1024){
          usleep(256);

          continue;
        }else{
          usleep(1024);
          continue;
        }
      }
      miss_poll_counter = 0;
      if(wc[0].wc_flags & IBV_WC_WITH_IMM){
        wc[0].imm_data;// use this to find the correct condition variable.
        cv_temp.notify_all();
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],
                                            compute_node_id,
                                            "main");

        // increase the buffer index
        if (buffer_position == RECEIVE_OUTSTANDING_SIZE - 1 ){
          buffer_position = 0;
        } else{
          buffer_position++;
        }
        continue;
      }
      RDMA_Request* receive_msg_buf = new RDMA_Request();
      *receive_msg_buf = *(RDMA_Request*)recv_mr[buffer_position].addr;
//      memcpy(receive_msg_buf, recv_mr[buffer_position].addr, sizeof(RDMA_Request));

      // copy the pointer of receive buf to a new place because
      // it is the same with send buff pointer.
      if (receive_msg_buf->command == create_mr_128MB_) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],
                                            compute_node_id,
                                            client_ip);

          create_mr_1GB_handler(receive_msg_buf, client_ip, compute_node_id);
//        rdma_mg_->post_send<ibv_mr>(send_mr,client_ip);  // note here should be the mr point to the send buffer.
//        rdma_mg_->poll_completion(wc, 1, client_ip, true);
      } else if (receive_msg_buf->command == create_mr_any_) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],
                                            compute_node_id,
                                            client_ip);
          assert(false);
//        create_mr_any_handler(receive_msg_buf, client_ip, compute_node_id);
      } else if (receive_msg_buf->command == create_qp_) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],
                                            compute_node_id,
                                            client_ip);
        create_qp_handler(receive_msg_buf, client_ip, compute_node_id);
        //        rdma_mg_->post_send<registered_qp_config>(send_mr, client_ip);
//        rdma_mg_->poll_completion(wc, 1, client_ip, true);


      } else if (receive_msg_buf->command == sync_option) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],
                                            compute_node_id,
                                            client_ip);
        sync_option_handler(receive_msg_buf, client_ip, compute_node_id);
      } else if (receive_msg_buf->command == put_qp_info) {
//          printf("Put QP information for %u\n",receive_msg_buf->content.qp_config_xcompute.node_id_pairs);
          rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],compute_node_id,client_ip);
          std::unique_lock<std::shared_mutex> l(qp_info_mtx);
          uint32_t target_node_id_pair = receive_msg_buf->content.qp_config_xcompute.node_id_pairs;
          qp_info_map_xcompute.insert({target_node_id_pair,receive_msg_buf->content.qp_config_xcompute});
          if (qp_info_map_xcompute.size() == rdma_mg->GetComputeNodeNum()*(rdma_mg->GetComputeNodeNum()-1)){
              ready_for_get.store(true);
          }
      } else if (receive_msg_buf->command == get_qp_info) {
          rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],compute_node_id,client_ip);
          Get_qp_info_handler(receive_msg_buf, client_ip, compute_node_id);
      } else if (receive_msg_buf->command == qp_reset_) {// depracated functions
        //THis should not be called because the recevei mr will be reset and the buffer
        // counter will be reset as 0
          assert(false);
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],
                                            compute_node_id,
                                            client_ip);
        qp_reset_handler(receive_msg_buf, client_ip, socket_fd,
                         compute_node_id);
        DEBUG_PRINT("QP has been reconnect from the memory node side\n");
        //TODO: Pause all the background tasks because the remote qp is not ready.
        // stop sending back messasges. The compute node may not reconnect its qp yet!
      } else if (receive_msg_buf->command == heart_beat) {
          //TODO: heartbeat for failure recovery.

      } else if (receive_msg_buf->command == broadcast_root) {
          rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position],
                                              compute_node_id,
                                              client_ip);
//          rdma_mg->broadcast_new_root(receive_msg_buf, client_ip, compute_node_id);
          //        rdma_mg_->post_send<registered_qp_config>(send_mr, client_ip);
//        rdma_mg_->poll_completion(wc, 1, client_ip, true);

      } else {
        printf("corrupt message from client. %d\n", receive_msg_buf->command);
        assert(false);
        break;
      }
      // increase the buffer index
      if (buffer_position == RECEIVE_OUTSTANDING_SIZE - 1 ){
        buffer_position = 0;
      } else{
        buffer_position++;
      }
    }
//    assert(false);
  }
  void Memory_Node_Keeper::Server_to_Client_Communication() {
  if (rdma_mg->resources_create()) {
    fprintf(stderr, "failed to create resources\n");
  }
  int rc;
  if (rdma_mg->rdma_config.gid_idx >= 0) {
    printf("checkpoint0");
    rc = ibv_query_gid(rdma_mg->res->ib_ctx, rdma_mg->rdma_config.ib_port,
                       rdma_mg->rdma_config.gid_idx,
                       &(rdma_mg->res->my_gid));
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_mg->rdma_config.ib_port, rdma_mg->rdma_config.gid_idx);

      return;
    }
  } else
    memset(&(rdma_mg->res->my_gid), 0, sizeof rdma_mg->res->my_gid);
  server_sock_connect(rdma_mg->rdma_config.server_name,
                      rdma_mg->rdma_config.tcp_port);
}
// connection code for server side, will get prepared for multiple connection
// on the same port.
int Memory_Node_Keeper::server_sock_connect(const char* servername, int port) {
  struct addrinfo* resolved_addr = NULL;
  struct addrinfo* iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  struct sockaddr address;
  socklen_t len = sizeof(struct sockaddr);
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }

  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    int option = 1;
    setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&option,sizeof(int));
    if (sockfd >= 0) {
      /* Server mode. Set up listening socket an accept a connection */
      listenfd = sockfd;
      sockfd = -1;
      if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
        goto sock_connect_exit;
      listen(listenfd, 20);
      while (!exit_all_threads_) {

        sockfd = accept(listenfd, &address, &len);
        std::string client_id =
            std::string(
                inet_ntoa(((struct sockaddr_in*)(&address))->sin_addr)) +
                    std::to_string(((struct sockaddr_in*)(&address))->sin_port);
        // Client id must be composed of ip address and port number.
        std::cout << "connection built up from" << client_id << std::endl;
        std::cout << "connection family is " << address.sa_family << std::endl;
        if (sockfd < 0) {
          fprintf(stderr, "Connection accept error, erron: %d\n", errno);
          break;
        }
        main_comm_threads.emplace_back(
            [this](std::string client_ip, int socketfd) {
              this->server_communication_thread(client_ip, socketfd);
              },
              std::string(address.sa_data), sockfd);
        // No need to detach, because the main_comm_threads will not be destroyed.
        main_comm_threads.back().detach();
      }
      usleep(1000);
    }
  }
  sock_connect_exit:

  if (listenfd) close(listenfd);
  if (resolved_addr) freeaddrinfo(resolved_addr);
  if (sockfd < 0) {
    if (servername)
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
    else {
      perror("server accept");
      fprintf(stderr, "accept() failed\n");
    }
  }
  return sockfd;
}
  void Memory_Node_Keeper::JoinAllThreads(bool wait_for_jobs_to_complete) {
    Compactor_pool_.JoinThreads(wait_for_jobs_to_complete);
  }
  void Memory_Node_Keeper::create_mr_1GB_handler(RDMA_Request* request,
                                                 std::string& client_ip,
                                                 uint8_t target_node_id) {
    DEBUG_PRINT("Create new mr\n");
//  std::cout << "create memory region command receive for" << client_ip
//  << std::endl;
  //TODO: consider the edianess of the RDMA request and reply.
  ibv_mr send_mr;
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
  RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;

  ibv_mr* mr;
  char* buff;
  {
    std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
    assert(request->content.mr_request.mem_size == define::Alloc_Granu); // Preallocation requrie memory is in chunk of 128MB
    
    // Extract logical region ID from the request
    uint16_t logical_region_id = request->content.mr_request.target_region_id;
    // printf("Memory node %u: Processing MR request for logical region %u\n", 
    //        rdma_mg->node_id, logical_region_id);
    
    try {
        if (!rdma_mg->Local_Memory_Register(&buff, &mr, request->content.mr_request.mem_size,
                                            Regular_Page, logical_region_id)) {
            fprintf(stderr, "Memory registering failed for logical region %u by size of 0x%x\n",
                    logical_region_id, static_cast<unsigned>(request->content.mr_request.mem_size));
            // Send error reply instead of asserting
            send_pointer->received = false;
            send_pointer->content.mr = {}; // Empty MR to indicate failure
            
            rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr,
                                sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
            rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
            delete request;
            return;
        }
    } catch (const std::runtime_error& e) {
        lck.unlock();
        fprintf(stderr, "Exception in memory registration: %s\n", e.what());
        // Send error reply for exception case
        send_pointer->received = true;
        send_pointer->content.mr = {}; // Empty MR to indicate failure
        send_pointer->content.mr.addr = (void*)1; // Set addr to 1 to mark error
        
        rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr,
                            sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
        rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
        delete request;
        return;
    }
  }

  send_pointer->content.mr = *mr;
  assert(send_pointer->content.mr.length == define::Alloc_Granu);
  send_pointer->received = true;
  // printf("Node %u: Writing MR to client %s at position %p\n", rdma_mg->node_id, client_ip.c_str(), request->buffer);
  // fflush(stdout);
  rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr,
                      sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
  delete request;
  }


//    void Memory_Node_Keeper::create_mr_any_handler(RDMA_Request* request,
//                                                   std::string& client_ip,
//                                                   uint8_t target_node_id) {
//        DEBUG_PRINT("Create new mr\n");
////  std::cout << "create memory region command receive for" << client_ip
////  << std::endl;
//        //TODO: consider the edianess of the RDMA request and reply.
//        ibv_mr send_mr;
//        rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
//        RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
//
//        ibv_mr* mr;
//        char* buff;
//        {
//            std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
//            assert(request->content.mem_size = 1024*1024*1024); // Preallocation requrie memory is 1GB
//            if (!rdma_mg->Local_Memory_Register(&buff, &mr, request->content.mem_size,
//                                                Regular_Page)) {
//                fprintf(stderr, "memory registering failed by size of 0x%x\n",
//                        static_cast<unsigned>(request->content.mem_size));
//                assert(false);
//
//            }
//        }
//
//        send_pointer->content.mr = *mr;
//        send_pointer->received = true;
//
//        rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr,
//                            sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
//        rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
//        delete request;
//    }
  // the client ip can by any string differnt from read_local write_local_flush
  // and write_local_compact
  void Memory_Node_Keeper::create_qp_handler(RDMA_Request* request,
                                             std::string& client_ip,
                                             uint8_t target_node_id) {
    int rc;
    DEBUG_PRINT("Create new qp\n");
  assert(request->buffer != nullptr);
  assert(request->rkey != 0);
  char gid_str[17];
  memset(gid_str, 0, 17);
  memcpy(gid_str, request->content.qp_config.gid, 16);

  // create a unique id for the connection from the compute node
  std::string new_qp_id =
      std::string(gid_str) +
      std::to_string(request->content.qp_config.lid) +
      std::to_string(request->content.qp_config.qp_num);

  std::cout << "create query pair command receive for" << client_ip
  << std::endl;
  fprintf(stdout, "Remote QP number=0x%x\n",
          request->content.qp_config.qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n",
          request->content.qp_config.lid);
  ibv_mr send_mr;
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
  RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
  ibv_qp* qp = rdma_mg->create_qp_Mside(false, new_qp_id);
  if (rdma_mg->rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(rdma_mg->res->ib_ctx, rdma_mg->rdma_config.ib_port,
                       rdma_mg->rdma_config.gid_idx, &(rdma_mg->res->my_gid));
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_mg->rdma_config.ib_port, rdma_mg->rdma_config.gid_idx);
      return;
    }
  } else
    memset(&(rdma_mg->res->my_gid), 0, sizeof(rdma_mg->res->my_gid));
  /* exchange using TCP sockets info required to connect QPs */
  send_pointer->content.qp_config.qp_num =
      rdma_mg->qp_map_Mside[new_qp_id]->qp_num;
  send_pointer->content.qp_config.lid = rdma_mg->res->port_attr.lid;
  memcpy(send_pointer->content.qp_config.gid, &(rdma_mg->res->my_gid), 16);
  send_pointer->received = true;
  Registered_qp_config* remote_con_data = new Registered_qp_config(request->content.qp_config);
  {
      std::unique_lock<std::shared_mutex> l1(rdma_mg->qp_cq_map_mutex);
      //keep the remote qp information
      rdma_mg->qp_main_connection_info_Mside.insert({new_qp_id, remote_con_data});
  }

  rdma_mg->connect_qp_Mside(qp, new_qp_id);

  rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr,
                      sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
  delete request;
  }

  // THis funciton is deprecated now
  void Memory_Node_Keeper::qp_reset_handler(RDMA_Request* request,
                                            std::string& client_ip,
                                            int socket_fd,
                                            uint8_t target_node_id) {
    ibv_mr send_mr;
    char temp_receive[2];
    char temp_send[] = "Q";
//    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
//    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    //reset the qp state.
    ibv_qp* qp = rdma_mg->qp_map_Mside.at(client_ip);
    printf("qp number before reset is %d\n", qp->qp_num);


    rdma_mg->modify_qp_to_reset(qp);
    rdma_mg->connect_qp(qp, client_ip, 0);
    printf("qp number after reset is %d\n", qp->qp_num);
    //NOte: This is not correct because we did not recycle the receive mr, so we also
    //  need to repost the receive wr and start from mr #1
//    send_pointer->received = true;
//    rdma_mg->RDMA_Write(request.reply_buffer, request.rkey,
//                        &send_mr, sizeof(RDMA_Reply),client_ip, IBV_SEND_SIGNALED,1);
    rdma_mg->sock_sync_data(socket_fd, 1, temp_send,
                            temp_receive);
    delete request;
//    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
  }
  void Memory_Node_Keeper::sync_option_handler(RDMA_Request* request,
                                               std::string& client_ip,
                                               uint8_t target_node_id) {
    DEBUG_PRINT("SYNC option \n");
    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    send_pointer->content.ive = {};
    ibv_mr edit_recv_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(edit_recv_mr, BigPage);
    send_pointer->buffer = edit_recv_mr.addr;
    send_pointer->rkey = edit_recv_mr.rkey;
    assert(request->content.ive.buffer_size < edit_recv_mr.length);
    send_pointer->received = true;
    //TODO: how to check whether the version edit message is ready, we need to know the size of the
    // version edit in the first REQUEST from compute node.

    // we need buffer_size - 1 to poll the last byte of the buffer.
    volatile char* polling_byte = (char*)edit_recv_mr.addr + request->content.ive.buffer_size - 1;
    memset((void*)polling_byte, 0, 1);
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr,
                        sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1, target_node_id);

    while (*(unsigned char*)polling_byte == 0){
      _mm_clflush(polling_byte);
      asm volatile ("sfence\n" : : );
      asm volatile ("lfence\n" : : );
      asm volatile ("mfence\n" : : );
      std::fprintf(stderr, "Polling sync option handler\n");
      std::fflush(stderr);
    }
    *opts = *static_cast<Options*>(edit_recv_mr.addr);
    opts->ShardInfo = nullptr;
    opts->env = nullptr;
//    opts->filter_policy = new InternalFilterPolicy(NewBloomFilterPolicy(opts->bloom_bits));
//    opts->comparator = &internal_comparator_;
    Compactor_pool_.SetBackgroundThreads(opts->max_background_compactions);
    printf("Option sync finished\n");
    delete request;
  }

    void Memory_Node_Keeper::Get_qp_info_handler(RDMA_Request *request, std::string &client_ip, uint8_t target_node_id) {
        DEBUG_PRINT("handling get qp info \n");
        int rc;

        ibv_mr* send_mr = rdma_mg->Get_local_send_message_mr();
        RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr->addr;
//        std::unique_lock<std::mutex> l(qp_info_mtx);
        while(ready_for_get.load() == false);
        std::shared_lock<std::shared_mutex> l(qp_info_mtx);
        auto target_qp_info = qp_info_map_xcompute.at(request->content.target_id_pair);
        assert(request->buffer != nullptr);
        assert(request->rkey != 0);

        /* exchange using TCP sockets info required to connect QPs */
        send_pointer->content.qp_config_xcompute = target_qp_info;
//        send_pointer->content.qp_config.lid = rdma_mg->res->port_attr.lid;
//        memcpy(send_pointer->content.qp_config.gid, &(rdma_mg->res->my_gid), 16);
        send_pointer->received = true;
        Registered_qp_config* remote_con_data = new Registered_qp_config(request->content.qp_config);


        rdma_mg->RDMA_Write(request->buffer, request->rkey, send_mr,
                            sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
//        rdma_mg->Deallocate_Local_RDMA_Slot(send_mr->addr, Message);
        delete request;
    }



}

// Memcached helper methods implementation - now uses RDMA_Manager

void DSMEngine::Memory_Node_Keeper::broadcastReplicaMetadata(uint16_t logical_id, uint16_t physical_id, uint64_t base_ptr, uint32_t rkey) {
    // Use RDMA_Manager's memcached interface
    if (!rdma_mg) {
        fprintf(stderr, "Memory_Node_Keeper: RDMA_Manager not available for memcached operation\n");
        return;
    }
    
    // Create key: "metadata_logical_<logical_id>_physical_<physical_id>"
    char key[100];
    snprintf(key, sizeof(key), "metadata_logical_%u_physical_%u", logical_id, physical_id);
    
    // Create value: combined format "base_ptr:rkey"
    char value[64];
    snprintf(value, sizeof(value), "%lu:%u", base_ptr, rkey);
    
    // Use RDMA_Manager's memcached interface
    rdma_mg->memcachedSet(key, strlen(key), value, strlen(value));
    printf("Broadcasted metadata: logical_id=%u, physical_id=%u, base_ptr=%lu, rkey=%u\n", 
           logical_id, physical_id, base_ptr, rkey);
}


