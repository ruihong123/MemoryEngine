#include <fstream>
#include "rdma.h"
#include <cstdint>
#include "Tools/env.h"
#include "Common.h"
#include "storage/page.h"
#include "HugePageAlloc.h"
#include "include/cache.h"
//#include "port/port_posix.h"
//#include "DSMEngine/env.h"
#ifdef RDMAPROCESSANALYSIS
extern int TimePrintCounter[MAX_APP_THREAD];
extern bool Show_Me_The_Print;
#endif
uint64_t cache_invalidation[MAX_APP_THREAD] = {0};
#ifdef GETANALYSIS
std::atomic<uint64_t> PrereadTotal = 0;
std::atomic<uint64_t> Prereadcounter = 0;
std::atomic<uint64_t> PostreadTotal = 0;
std::atomic<uint64_t> Postreadcounter = 0;
std::atomic<uint64_t> MemcopyTotal = 0;
std::atomic<uint64_t> Memcopycounter = 0;
std::atomic<uint64_t> NextStepTotal = 0;
std::atomic<uint64_t> NextStepcounter = 0;
std::atomic<uint64_t> WholeopTotal = 0;
std::atomic<uint64_t> Wholeopcounter = 0;
#endif
#define _mm_clflush(addr) \
asm volatile("clflush %0" : "+m" (*(volatile char *)(addr)))
namespace DSMEngine {
uint16_t RDMA_Manager::node_id = 0;
#ifdef PROCESSANALYSIS
std::atomic<uint64_t> RDMA_Manager::RDMAReadTimeElapseSum = 0;
std::atomic<uint64_t> RDMA_Manager::ReadCount = 0;
#endif
#define INVALIDATION_INTERVAL 5
#define DELTASECTIONSIZE 10*1024*1024
//TODO: This should be moved to some other classes which is strongly related to btree or storage engine.
thread_local GlobalAddress path_stack[define::kMaxLevelOfTree];

//#ifdef GETANALYSIS
//std::atomic<uint64_t> RDMA_Manager::RDMAFindmrElapseSum = 0;
//std::atomic<uint64_t> RDMA_Manager::RDMAMemoryAllocElapseSum = 0;
//std::atomic<uint64_t> RDMA_Manager::ReadCount1 = 0;
//#endif

//#ifndef NDEBUG
    thread_local int RDMA_Manager::thread_id = 0;
    thread_local int RDMA_Manager::qp_inc_ticket = 0;
    thread_local uint64_t RDMA_Manager::round_robin_cur = 0;

//#endif

//uint64_t cache_hit_valid[MAX_APP_THREAD][8];
//#define R_SIZE 32
void UnrefHandle_rdma(void* ptr) { delete static_cast<std::string*>(ptr); }
void UnrefHandle_qp(void* ptr) {
  if (ptr == nullptr) return;
  if (ibv_destroy_qp(static_cast<ibv_qp*>(ptr))) {
    fprintf(stderr, "Thread local qp failed to destroy QP\n");
  } else {
//    printf("thread local qp destroy successfully!");
  }
}
void UnrefHandle_cq(void* ptr) {
  if (ptr == nullptr) return;
  if (ibv_destroy_cq(static_cast<ibv_cq*>(ptr))) {
    fprintf(stderr, "Thread local cq failed to destroy QP\n");
  } else {
//    printf("thread local cq destroy successfully!");
  }
}
void Destroy_mr(void* ptr) {
  if (ptr == nullptr) return;
  ibv_dereg_mr((ibv_mr*)ptr);
//  delete (char*)((ibv_mr*)ptr)->addr;
}
template<typename T>
void General_Destroy(void* ptr){
  delete (T) ptr;
}
static uint64_t  round_to_cacheline(uint64_t size) {
  return ((size + 63) / 64) * 64;
}

/******************************************************************************
* Function: RDMA_Manager

*
* Output
* none
*
*
* Description
* Initialize the resource for RDMA.
******************************************************************************/
    RDMA_Manager::RDMA_Manager(config_t config, size_t remote_block_size)
    : total_registered_size(0),
      cachelin_size(remote_block_size),
      delta_section_size(DELTASECTIONSIZE),
      read_buffer(new ThreadLocalPtr(&Destroy_mr)),
      big_buffer(new ThreadLocalPtr(&Destroy_mr)),
      send_message_buffer(new ThreadLocalPtr(&Destroy_mr)),
      receive_message_buffer(new ThreadLocalPtr(&Destroy_mr)),
      CAS_buffer(new ThreadLocalPtr(&Destroy_mr)),
//      qp_local_write_flush(new ThreadLocalPtr(&UnrefHandle_qp)),
//      cq_local_write_flush(new ThreadLocalPtr(&UnrefHandle_cq)),
//      local_write_flush_qp_info(new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)),
//      qp_local_write_compact(new ThreadLocalPtr(&UnrefHandle_qp)),
//      cq_local_write_compact(new ThreadLocalPtr(&UnrefHandle_cq)),
//      local_write_compact_qp_info(new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)),
//      qp_default(new ThreadLocalPtr(&UnrefHandle_qp)),
//      cq_default(new ThreadLocalPtr(&UnrefHandle_cq)),
//      local_read_qp_info(new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)),
//      node_id(nodeid),
      rdma_config(config)
//      db_name_(db_name),
//      file_to_sst_meta_(file_to_sst_meta),
//      fs_mutex_(fs_mutex)

{
  //  assert(read_block_size <table_size);
  res = new resources();
  node_id = config.node_id;
//  std::string ipString();
//  struct in_addr inaddr{};
//  char buf[INET_ADDRSTRLEN];
//  inet_pton(AF_INET, config.server_name, &inaddr);
//  node_id = static_cast<uint16_t>(inaddr.s_addr);
//  qp_local_write_flush->Reset(new QP_Map());
//  cq_local_write_flush->Reset(new CQ_Map());
//  qp_local_write_compact->Reset(new QP_Map());
//  cq_local_write_compact->Reset(new CQ_Map());
//  qp_default->Reset(new QP_Map());
//  cq_default->Reset(new CQ_Map());
//  local_read_qp_info->Reset(new QP_Info_Map());
//  local_write_flush_qp_info->Reset(new QP_Info_Map());
//  local_write_compact_qp_info->Reset(new QP_Info_Map());
  //Initialize a message memory pool
  uint64_t message_size = round_to_cacheline(std::max(sizeof(RDMA_Request), sizeof(RDMA_Reply)));
  Mempool_initialize(Message,
                     message_size, RECEIVE_OUTSTANDING_SIZE * message_size);
  Mempool_initialize(BigPage, 2*1024ull*1024ull, 16 * 1024 * 1024);
  Mempool_initialize(Regular_Page, remote_block_size, 256ull*1024ull*1024);
    Mempool_initialize(DeltaChunk, delta_section_size, 32*delta_section_size);
    printf("atomic uint8_t, uint16_t, uint32_t and uint64_t are, %lu %lu %lu %lu\n ", sizeof(std::atomic<uint8_t>), sizeof(std::atomic<uint16_t>), sizeof(std::atomic<uint32_t>), sizeof(std::atomic<uint64_t>));
//    if(node_id%2 == 0){
//        Invalidation_bg_threads.SetBackgroundThreads(NUM_QP_ACCROSS_COMPUTE);
//    }
//    page_cache_ = config.cache_prt;
}


/******************************************************************************
* Function: ~RDMA_Manager

*
* Output
* none
*
*
* Description
* Cleanup and deallocate all resources used for RDMA
******************************************************************************/
RDMA_Manager::~RDMA_Manager() {
  if (!res->qp_map.empty())
    for (auto it = res->qp_map.begin(); it != res->qp_map.end(); it++) {
      if (ibv_destroy_qp(it->second)) {
        fprintf(stderr, "failed to destroy QP\n");
      }
    }
  printf("RDMA Manager get destroyed\n");
  if (!local_mem_regions.empty()) {
    for (ibv_mr* p : local_mem_regions) {
        size_t size = p->length;
      ibv_dereg_mr(p);
      //       local buffer is registered on this machine need deregistering.
//      delete (char*)p->addr;
        hugePageDealloc(p,size);
    }
    //    local_mem_regions.clear();
  }

  if (!remote_mem_leaf_pool.empty()) {
    for (auto p : remote_mem_leaf_pool) {
        for(auto iter : *p.second){
            delete iter;
        }
      delete p.second;  // remote buffer is not registered on this machine so just delete the structure
    }
      remote_mem_leaf_pool.clear();
  }
    if (!remote_mem_delta_pool.empty()) {
        for (auto p : remote_mem_delta_pool) {
            for(auto iter : *p.second){
                delete iter;
            }
            delete p.second;  // remote buffer is not registered on this machine so just delete the structure
        }
        remote_mem_delta_pool.clear();
    }
  if (!res->cq_map.empty())
    for (auto it = res->cq_map.begin(); it != res->cq_map.end(); it++) {
      if (ibv_destroy_cq(it->second.first)) {
        fprintf(stderr, "failed to destroy CQ\n");
      }else{
        delete it->second.first;
      }
      if (it->second.second!= nullptr && ibv_destroy_cq(it->second.second)){
        fprintf(stderr, "failed to destroy CQ\n");
      }else{
        delete it->second.second;
      }
    }
  if (!res->qp_map.empty())
    for (auto it = res->qp_map.begin(); it != res->qp_map.end(); it++) {
      if (ibv_destroy_qp(it->second)) {
        fprintf(stderr, "failed to destroy QP\n");
      }else{
        delete it->second;
      }

    }
  if (!res->qp_main_connection_info.empty()){
    for(auto it = res->qp_main_connection_info.begin(); it != res->qp_main_connection_info.end(); it++){
      delete it->second;
    }
  }
  if (res->pd)
    if (ibv_dealloc_pd(res->pd)) {
      fprintf(stderr, "failed to deallocate PD\n");
    }

  if (res->ib_ctx)
    if (ibv_close_device(res->ib_ctx)) {
      fprintf(stderr, "failed to close device context\n");
    }
  if (!res->sock_map.empty())
    for (auto it = res->sock_map.begin(); it != res->sock_map.end(); it++) {
      if (close(it->second)) {
        fprintf(stderr, "failed to close socket\n");
      }
    }
  for (auto pool : name_to_mem_pool) {
    for(auto iter : pool.second){
      delete iter.second;
    }
  }
  for(auto iter : Remote_Leaf_Node_Bitmap){
    for(auto iter1 : *iter.second){
      delete iter1.second;
    }
    delete iter.second;
  }
  delete res;
  for(auto iter :qp_local_write_flush ){
    delete iter.second;
  }
  for(auto iter :local_write_flush_qp_info ){
    delete iter.second;
  }
  for(auto iter :qp_local_write_compact ){
    delete iter.second;
  }
  for(auto iter :cq_local_write_compact ){
    delete iter.second;
  }
  for(auto iter :local_write_compact_qp_info ){
    delete iter.second;
  }
  for(auto iter :qp_data_default ){
    delete iter.second;
  }
  for(auto iter :cq_data_default ){
    delete iter.second;
  }
  for(auto iter :local_read_qp_info ){
    delete iter.second;
  }


}
    RDMA_Manager *RDMA_Manager::Get_Instance(config_t* config) {
        static RDMA_Manager * rdma_mg = nullptr;
        static std::mutex lock;
        if (config == nullptr){
            assert(rdma_mg!= nullptr);
            return rdma_mg;
        }
        lock.lock();
        if (!rdma_mg) {
            rdma_mg = new RDMA_Manager(*config, kLeafPageSize);
            rdma_mg->Client_Set_Up_Resources();
        } else {
            return rdma_mg;
        }
        lock.unlock();
        while(rdma_mg->main_comm_thread_ready_num.load() != rdma_mg->memory_nodes.size());
        return rdma_mg;
}

size_t RDMA_Manager::GetMemoryNodeNum() {
    return memory_nodes.size();
}
size_t RDMA_Manager::GetComputeNodeNum() {
    return compute_nodes.size();
}
    uint64_t RDMA_Manager::FetchAddNextTimestamp() {
        ibv_mr* local_cas_buffer = Get_local_CAS_mr();
        RDMA_FAA(timestamp_oracle,local_cas_buffer,1,1,IBV_SEND_SIGNALED,1);
        assert(*(uint64_t *)local_cas_buffer->addr <0x700d2c00cbe9);
        return *(uint64_t *)local_cas_buffer->addr;
    }
    uint64_t RDMA_Manager::GetTimestamp() {
        ibv_mr* local_cas_buffer = Get_local_CAS_mr();
        //THis RDMA read may have some lag with the RDMA faa, BUT this should be fine.
        RDMA_Read(timestamp_oracle,  local_cas_buffer, 8, IBV_SEND_SIGNALED, 1, 1);
        assert(*(uint64_t *)local_cas_buffer->addr <0x700d2c00cbe9);
        return *(uint64_t *)local_cas_buffer->addr;
    }
bool RDMA_Manager::poll_reply_buffer(RDMA_Reply* rdma_reply) {
  volatile bool* check_byte = &(rdma_reply->received);
//  size_t counter = 0;
  while(!*check_byte){
    _mm_clflush(check_byte);
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
  }
  return true;
}
    bool RDMA_Manager::poll_reply_buffer(RDMA_ReplyXCompute* rdma_reply) {
        volatile Page_Forward_Reply_Type * check_byte = &(rdma_reply->inv_reply_type);
//  size_t counter = 0;
        while(*check_byte == waiting){
            _mm_clflush(check_byte);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
        }
        return *check_byte;
    }
    Page_Forward_Reply_Type RDMA_Manager::poll_reply_buffer(volatile Page_Forward_Reply_Type* reply_buff) {

//  size_t counter = 0;
        while(*reply_buff == waiting){
            _mm_clflush(reply_buff);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
        }
        return *reply_buff;
    }

    void RDMA_Manager::Set_message_handling_func(std::function<void(void* )> &&func, Registered_F_type func_name) {
        std::unique_lock<std::shared_mutex> lck(user_df_map_mutex);
        message_handling_funcs_map.insert({func_name, std::move(func)});
//        message_handling_func = std::move(func);
    }
    void RDMA_Manager::register_message_handling_thread(uint32_t handler_id, Registered_F_type func_name) {
        std::shared_lock<std::shared_mutex> lck(user_df_map_mutex);
        RDMA_Request* request = new RDMA_Request();
        request->command = invalid_command_;
        communication_queues.insert({handler_id, std::queue<RDMA_Request>()});
        communication_mtxs.insert({handler_id, new std::mutex()});
        communication_cvs.insert({handler_id, new std::condition_variable()});
        auto* id_p = new uint32_t(handler_id);
//        std::thread t(message_handling_func, handler_id);
        user_defined_functions_handler.emplace_back(message_handling_funcs_map[func_name], id_p);
        user_defined_functions_handler.back().detach();


    }
    void RDMA_Manager::join_all_handling_thread() {
        //stop all the handling threads.
        handler_is_finish.store(true);
        for (auto iter : communication_cvs) {
            iter.second->notify_all();
        }

//        // join all the handling thread. and deallocate all the communication states.
//        for(auto& iter : user_defined_functions_handler){
//            iter.join();
//        }
//        user_defined_functions_handler.clear();
//        for(auto iter : communication_queues){
//            delete iter.second;
//        }
        communication_queues.clear();
        for(auto iter : communication_mtxs){
            delete iter.second;
        }
        communication_mtxs.clear();
        for(auto iter : communication_cvs){
            delete iter.second;
        }
        communication_cvs.clear();

    }
/******************************************************************************
* Function: sock_connect
*
* Input
* servername URL of server to connect to (NULL for server mode)
* port port of service
*
* Output
* none
*
* Returns
* socket (fd) on success, negative error code on failure
*
* Description
* Connect a socket. If servername is specified a client connection will be
* initiated to the indicated server and port. Otherwise listen on the
* indicated port for an incoming connection.
*
******************************************************************************/
int RDMA_Manager::client_sock_connect(const char* servername, int port) {
  struct addrinfo* resolved_addr = NULL;
  struct addrinfo* iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  int tmp;
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
//  printf("Mark: valgrind socket info1\n");
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
//  printf("Mark: valgrind socket info2\n");
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }
  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    if (sockfd >= 0) {
      if (servername) {
        /* Client mode. Initiate connection to remote */
        if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen))) {
          fprintf(stdout, "failed connect \n");
          close(sockfd);
          sockfd = -1;
        }
        printf("Success to connect to %s\n", servername);
      } else {
        assert(false);

      }
    }

    fprintf(stdout, "TCP connection was established\n");
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


void RDMA_Manager::compute_message_handling_thread(std::string q_id, uint16_t shard_target_node_id) {

  ibv_qp* qp;
  int rc = 0;
  uint64_t miss_poll_counter = 0;
  ibv_mr* recv_mr;
  int buffer_counter;
  //TODO: keep the recv mr in rdma manager so that next time we restart
  // the database we can retrieve from the rdma_mg.
  if (comm_thread_recv_mrs.find(shard_target_node_id) != comm_thread_recv_mrs.end()){
    recv_mr = comm_thread_recv_mrs.at(shard_target_node_id);
    buffer_counter = comm_thread_buffer.at(shard_target_node_id);
  }else{
    // Some where we need to delete the recv_mr in case of memory leak.
    recv_mr = new ibv_mr[RECEIVE_OUTSTANDING_SIZE]();
    for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++){
      Allocate_Local_RDMA_Slot(recv_mr[i], Message);
    }

    for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++) {
      post_receive<RDMA_Request>(&recv_mr[i], shard_target_node_id, q_id);
    }
    buffer_counter = 0;
    comm_thread_recv_mrs.insert({shard_target_node_id, recv_mr});
  }

  ibv_wc wc[3] = {};
  //    RDMA_Request receive_msg_buf;
//  {
//    std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
//    write_stall_cv.notify_one();
//  }
  printf("client handling thread\n");
  std::mutex* mtx_imme = mtx_imme_map.at(shard_target_node_id);
  std::atomic<uint32_t>* imm_gen = imm_gen_map.at(shard_target_node_id);
  uint32_t* imme_data = imme_data_map.at(shard_target_node_id);
  assert(*imme_data == 0);
  uint32_t* byte_len = byte_len_map.at(shard_target_node_id);
  std::condition_variable* cv_imme = cv_imme_map.at(shard_target_node_id);
    main_comm_thread_ready_num.fetch_add(1);

    while (1) {
    // we can only use try_poll... rather than poll_com.. because we need to
    // make sure the shutting down signal can work.
        if (try_poll_completions(wc, 1, q_id, false, shard_target_node_id) == 0){
            // exponetial back off to save cpu cycles.
            if(++miss_poll_counter < 1024){
                continue;
            }
            if(++miss_poll_counter < 2048){
                usleep(10);
                continue ;
            }
            if(++miss_poll_counter < 4096){
                usleep(32);
                continue;
            }else{
                usleep(512);
                continue;
            }
        }
        miss_poll_counter = 0;

      if(wc[0].wc_flags & IBV_WC_WITH_IMM){
        wc[0].imm_data;// use this to find the correct condition variable.
        std::unique_lock<std::mutex> lck(*mtx_imme);
        // why imme_data not zero? some other thread has overwrite this function.
        // buffer overflow?
        assert(*imme_data == 0);
        assert(*byte_len == 0);
        *imme_data = wc[0].imm_data;
        *byte_len = wc[0].byte_len;
        cv_imme->notify_all();
        lck.unlock();
        while (*imme_data != 0 || *byte_len != 0 ){
          cv_imme->notify_one();
        }
        post_receive<RDMA_Request>(&recv_mr[buffer_counter], shard_target_node_id, "main");
        // increase the buffer index
        if (buffer_counter == RECEIVE_OUTSTANDING_SIZE - 1 ){
          buffer_counter = 0;
        } else{
          buffer_counter++;
        }
        continue;
      }
      RDMA_Request* receive_msg_buf = new RDMA_Request();
      memcpy(receive_msg_buf, recv_mr[buffer_counter].addr, sizeof(RDMA_Request));
      //        printf("Buffer counter %d has been used!\n", buffer_counter);

      // copy the pointer of receive buf to a new place because
      // it is the same with send buff pointer.
      if (receive_msg_buf->command == install_version_edit) {
        ((RDMA_Request*) recv_mr[buffer_counter].addr)->command = invalid_command_;
        assert(false);
        post_receive<RDMA_Request>(&recv_mr[buffer_counter],
                                            shard_target_node_id,
                                            "main");
//        install_version_edit_handler(receive_msg_buf, q_id);
#ifdef WITHPERSISTENCE
      } else if(receive_msg_buf->command == persist_unpin_) {
        //TODO: implement the persistent unpin dispatch machenism
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_counter], "main");
        auto start = std::chrono::high_resolution_clock::now();
        Arg_for_handler* argforhandler = new Arg_for_handler{.request=receive_msg_buf,.client_ip = "main"};
        BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforhandler};
        Unpin_bg_pool_.Schedule(&DBImpl::SSTable_Unpin_Dispatch, thread_pool_args);
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        printf("unpin for %lu files time elapse is %ld",
               (receive_msg_buf->content.psu.buffer_size-1)/sizeof(uint64_t),
               duration.count());
#endif
      } else {
        printf("corrupt message from client.");
          assert(false);
        break;
      }
      // increase the buffer index
      if (buffer_counter == RECEIVE_OUTSTANDING_SIZE - 1 ){
        buffer_counter = 0;
      } else{
        buffer_counter++;
      }
    }
  comm_thread_buffer.insert({shard_target_node_id, buffer_counter});
}
void RDMA_Manager::ConnectQPThroughSocket(std::string qp_type, int socket_fd,
                                          uint16_t& target_node_id) {

  struct Registered_qp_config local_con_data;
  struct Registered_qp_config* remote_con_data = new Registered_qp_config();
  struct Registered_qp_config tmp_con_data;
  //  std::string qp_id = "main";


  /* exchange using TCP sockets info required to connect QPs */
  printf("checkpoint1\n");


    bool seperated_cq = true;
    struct ibv_qp_init_attr qp_init_attr;
    /* each side will send only one WR, so Completion Queue with 1 entry is enough
     */
    int cq_size = 1024;
    // cq1 send queue, cq2 receive queue
    ibv_cq* cq1 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    ibv_cq* cq2;
    if (seperated_cq)
      cq2 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);

    if (!cq1) {
      fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
    }



    /* create the Queue Pair */
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq = cq1;
    if (seperated_cq)
      qp_init_attr.recv_cq = cq2;
    else
      qp_init_attr.recv_cq = cq1;
    qp_init_attr.cap.max_send_wr = 2500;
    qp_init_attr.cap.max_recv_wr = 2500;
    qp_init_attr.cap.max_send_sge = 30;
    qp_init_attr.cap.max_recv_sge = 30;
    //  qp_init_attr.cap.max_inline_data = -1;
    ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
    if (!qp) {
      fprintf(stderr, "failed to create QP\n");
    }
//    fprintf(stdout, "QP was created, QP number=0x%x\n", qp->qp_num);
//  Used to be "ibv_qp* qp = create_qp(shard_target_node_id, true, qp_type);", but the
    // shard_target_node_id is not available so we unwrap the function
  local_con_data.qp_num = htonl(qp->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &res->my_gid, 16);
//  printf("checkpoint2");
//
//  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);

  if (sock_sync_data(socket_fd, sizeof(struct Registered_qp_config),
      (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides\n");
      assert(false);
  }
  remote_con_data->qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data->lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data->gid, tmp_con_data.gid, 16);
//  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data->qp_num);
//  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data->lid);
  remote_con_data->node_id = tmp_con_data.node_id;
  target_node_id = tmp_con_data.node_id;
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  res->qp_map[target_node_id] = qp;
  res->cq_map.insert({target_node_id, std::make_pair(cq1, cq2)});
  assert(qp_type != "default");
  assert(qp_type != "write_local_compact");
  assert(qp_type != "write_local_flush");
//  if (qp_type == "read_local" )
//    ((QP_Info_Map*)local_read_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
//  //    local_read_qp_info->Reset(remote_con_data);
//  else if(qp_type == "write_local_compact")
//    ((QP_Info_Map*)local_write_compact_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
////    local_write_compact_qp_info->Reset(remote_con_data);
//  else if(qp_type == "write_local_flush")
//    ((QP_Info_Map*)local_write_flush_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
////    local_write_flush_qp_info->Reset(remote_con_data);
//  else
    res->qp_main_connection_info.insert({target_node_id,remote_con_data});
  l.unlock();
  if (connect_qp(qp, qp_type, target_node_id)) {
    fprintf(stderr, "failed to connect QPs\n");
  }

}
//    Register the memory through ibv_reg_mr on the local side. this function will be called by both of the server side and client side.
bool RDMA_Manager::Local_Memory_Register(char** p2buffpointer,
                                         ibv_mr** p2mrpointer, size_t size,
                                         Chunk_type pool_name) {
    printf("Local memroy register\n");
  int mr_flags = 0;
  if (node_id%2 == 1 && !pre_allocated_pool.empty()  && pool_name != Message ){
      *p2mrpointer = pre_allocated_pool.back();
      pre_allocated_pool.pop_back();
      *p2buffpointer = (char*)(*p2mrpointer)->addr;
      printf("Allcoate from the preallocated pool, total_registered_size is %zu\n", total_registered_size);
      fflush(stdout);
  }else{
      //If this node is a compute node, allocate the memory on demanding.
//      printf("Note: Allocate memory from OS, not allocate from the preallocated pool.\n");
      if (node_id%2 == 1 && pool_name == Regular_Page){
          printf( "Allocate Registered Memory outside the preallocated pool is wrong, the base pointer has been changed\n");
          assert(false);
          exit(0);
      }
      *p2buffpointer = (char*)hugePageAlloc(size);
//      *p2buffpointer = (char*)hugePageAlloc(size);
      if (!*p2buffpointer) {
          fprintf(stderr, "failed to malloc bytes to memory buffer by hugePageAllocation\n");
          return false;
      }
      memset(*p2buffpointer, 0, size);

      /* register the memory buffer */
      mr_flags =
              IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
      //  auto start = std::chrono::high_resolution_clock::now();
      *p2mrpointer = ibv_reg_mr(res->pd, *p2buffpointer, size, mr_flags);
      //  auto stop = std::chrono::high_resolution_clock::now();
      //  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); std::printf("Memory registeration size: %zu time elapse (%ld) us\n", size, duration.count());
      local_mem_regions.push_back(*p2mrpointer);
      fprintf(stdout,
              "New MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x, size=%lu, total registered size is %lu\n",
              (*p2mrpointer)->addr, (*p2mrpointer)->lkey, (*p2mrpointer)->rkey,
              mr_flags, size, total_registered_size);
      fflush(stdout);
  }

  if (!*p2mrpointer) {
    fprintf(
            stderr,
            "ibv_reg_mr failed with mr_flags=0x%x, size = %zu, region num = %zu\n",
            mr_flags, size, local_mem_regions.size());
    return false;
  } else if(node_id %2 == 0 || pool_name == Message) {
      // memory node does not need to create the in_use map except for the message pool.
    int placeholder_num =
        (*p2mrpointer)->length /
        (name_to_chunksize.at(pool_name));  // here we supposing the SSTables are 4 megabytes
    auto* in_use_array = new In_Use_Array(placeholder_num, name_to_chunksize.at(pool_name),
                                          *p2mrpointer);
    // TODO: make the code below protected by mutex in thread local alocator
    name_to_mem_pool.at(pool_name).insert({(*p2mrpointer)->addr, in_use_array});
  }
    else
      printf("Register memory for computing node\n");
  total_registered_size = total_registered_size + (*p2mrpointer)->length;


  return true;
};

ibv_mr * RDMA_Manager::Preregister_Memory(size_t gb_number) {
  int mr_flags = 0;
  uint64_t size = gb_number*define::GB;
//  if (node_id == 2){
//    void* dummy = malloc(size*2);
//  }

    std::fprintf(stderr, "Pre allocate registered memory %zu GB %30s\r", size, "");
    std::fflush(stderr);
    void* buff_pointer = hugePageAlloc(size);
    if (!buff_pointer) {
        fprintf(stderr, "failed to malloc bytes to memory buffer\n");
        return nullptr;
    }
    memset(buff_pointer, 0, size);

    /* register the memory buffer */
    mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    //  auto start = std::chrono::high_resolution_clock::now();
    ibv_mr* mrpointer = ibv_reg_mr(res->pd, buff_pointer, size, mr_flags);
    if (!mrpointer) {
      fprintf(
              stderr,
              "ibv_reg_mr failed with mr_flags=0x%x, size = %zu, region num = %zu\n",
              mr_flags, size, local_mem_regions.size());
      return nullptr;
    }
    local_mem_regions.push_back(mrpointer);
    preregistered_region = mrpointer;
    size_t chunk_number = gb_number*define::GB/define::Alloc_Granu;
    ibv_mr* mrs = new ibv_mr[chunk_number];
    for (int i = 0; i < chunk_number; ++i) {
        mrs[i] = *mrpointer;
        mrs[i].addr = (char*)mrs[i].addr + i*(define::Alloc_Granu);
        mrs[i].length = define::Alloc_Granu;

        pre_allocated_pool.push_back(&mrs[i]);
    }

  return mrpointer;
}

/******************************************************************************
* Function: set_up_RDMA
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* set up the connection to shared memroy.
* memory node ids are even, compute node ids are odd.
******************************************************************************/
void RDMA_Manager::Client_Set_Up_Resources() {
  //  int rc = 1;
  // int trans_times;
  char temp_char;

  std::string connection_conf;
  size_t pos = 0;
  std::ifstream myfile;
  myfile.open (config_file_name, std::ios_base::in);
  std::string space_delimiter = " ";

  std::getline(myfile,connection_conf );
  uint16_t i = 0;
  uint16_t id;
  while ((pos = connection_conf.find(space_delimiter)) != std::string::npos) {
    id = 2*i;
    compute_nodes.insert({id, connection_conf.substr(0, pos)});
    connection_conf.erase(0, pos + space_delimiter.length());
    i++;
  }
  compute_nodes.insert({2*i, connection_conf});
  assert((node_id - 1)/2 <  compute_nodes.size());
  i = 0;
  std::getline(myfile,connection_conf );
  while ((pos = connection_conf.find(space_delimiter)) != std::string::npos) {
    id = 2*i+1;
    memory_nodes.insert({id, connection_conf.substr(0, pos)});
    connection_conf.erase(0, pos + space_delimiter.length());
    i++;
  }
  memory_nodes.insert({2*i + 1, connection_conf});
  i++;
  Initialize_threadlocal_map();
//  std::string ip_add;
//  std::cout << "please insert the ip address for the remote memory" << std::endl;
//  std::cin >> ip_add;
//  rdma_config.server_name = ip_add.c_str();
  /* if client side */
  if (resources_create()) {
    fprintf(stderr, "failed to create resources\n");
    return;
  }
  printf("Compute node size is %lu\n", compute_nodes.size());
  std::vector<std::thread> memory_handler_threads;
        std::vector<std::thread> compute_handler_threads;
  for(int i = 0; i < memory_nodes.size(); i++){
    uint16_t target_node_id =  2*i+1;
    res->sock_map[target_node_id] =
        client_sock_connect(memory_nodes[target_node_id].c_str(), rdma_config.tcp_port);
    printf("connect to node id %d", target_node_id);
    if (res->sock_map[target_node_id] < 0) {
      fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
              memory_nodes[target_node_id].c_str(), rdma_config.tcp_port);
    }
//    assert(memory_nodes.size() == 2);
    //TODO: use mulitple thread to initialize the queue pairs.
    memory_handler_threads.emplace_back(&RDMA_Manager::Get_Remote_qp_Info_Then_Connect, this, target_node_id);
//    Get_Remote_qp_Info_Then_Connect(shard_target_node_id);
    memory_handler_threads.back().detach();
  }
    while (memory_connection_counter.load() != memory_nodes.size())
        ;
#if ACCESS_MODE == 1 || ACCESS_MODE == 2
    for(size_t i = 0; i < compute_nodes.size(); i++){

        uint16_t target_node_id =  2*i;
        if (target_node_id != node_id){
            compute_handler_threads.emplace_back(&RDMA_Manager::Cross_Computes_RPC_Threads_Creator, this, target_node_id);
            compute_handler_threads.back().detach();
        }


    }
    while (compute_connection_counter.load() != compute_nodes.size()-1)
        ;

#endif

  // check whether all the compute nodes are ready.
        sync_with_computes_Cside();
    // connect with the compute nodes below.

//  for (auto & thread : threads) {
//    thread.join();
//  }
}
void RDMA_Manager::Initialize_threadlocal_map(){
  uint16_t target_node_id;
  for (int i = 0; i < memory_nodes.size(); ++i) {
    target_node_id = 2*i+1;
    qp_local_write_flush.insert({target_node_id,new ThreadLocalPtr(&UnrefHandle_qp)});
    cq_local_write_flush.insert({target_node_id, new ThreadLocalPtr(&UnrefHandle_cq)});
    local_write_flush_qp_info.insert({target_node_id, new ThreadLocalPtr(&General_Destroy<Registered_qp_config*>)});
    qp_local_write_compact.insert({target_node_id,new ThreadLocalPtr(&UnrefHandle_qp)});
    cq_local_write_compact.insert({target_node_id, new ThreadLocalPtr(&UnrefHandle_cq)});
    local_write_compact_qp_info.insert({target_node_id, new ThreadLocalPtr(&General_Destroy<Registered_qp_config*>)});
    qp_data_default.insert({target_node_id, new ThreadLocalPtr(&UnrefHandle_qp)});
    cq_data_default.insert({target_node_id, new ThreadLocalPtr(&UnrefHandle_cq)});
    local_read_qp_info.insert({target_node_id, new ThreadLocalPtr(&General_Destroy<Registered_qp_config*>)});
    async_tasks.insert({target_node_id, new ThreadLocalPtr(&General_Destroy <Async_Tasks * >)});
    Remote_Leaf_Node_Bitmap.insert({target_node_id, new std::map<void*, In_Use_Array*>()});
    Remote_Delta_Bitmap.insert({target_node_id, new std::map<void*, In_Use_Array*>()});
    remote_mem_delta_pool.insert({target_node_id, new std::vector<ibv_mr*>()});
    remote_mem_leaf_pool.insert({target_node_id, new std::vector<ibv_mr*>()});
    top.insert({target_node_id,0});
    mtx_imme_map.insert({target_node_id, new std::mutex});
    imm_gen_map.insert({target_node_id, new std::atomic<uint32_t>{0}});
    imme_data_map.insert({target_node_id, new  uint32_t{0}});
    byte_len_map.insert({target_node_id, new  uint32_t{0}});
    cv_imme_map.insert({target_node_id, new std::condition_variable});
  }


}
/******************************************************************************
* Function: resources_create
*
* Input
* res pointer to resources structure to be filled in
*
* Output
* res filled in with resources
*
* Returns
* 0 on success, 1 on failure
*
* Description
*
* This function creates and allocates all necessary system resources. These
* are stored in res.
*****************************************************************************/
int RDMA_Manager::resources_create() {
  struct ibv_device** dev_list = NULL;
  struct ibv_device* ib_dev = NULL;
  //  int iter = 1;
  int i;

  //  int cq_size = 0;
  int num_devices;
  int rc = 0;
  //        ibv_device_attr *device_attr;

  fprintf(stdout, "searching for IB devices in host\n");
  /* get device names in the system */
  dev_list = ibv_get_device_list(&num_devices);
  if (!dev_list) {
    fprintf(stderr, "failed to get IB devices list\n");
    rc = 1;
  }
  /* if there isn't any IB device in host */
  if (!num_devices) {
    fprintf(stderr, "found %d device(s)\n", num_devices);
    rc = 1;
  }
  fprintf(stdout, "found %d device(s)\n", num_devices);
  /* search for the specific device we want to work with */
  for (i = 0; i < num_devices; i++) {
    if (!rdma_config.dev_name) {
      rdma_config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
      fprintf(stdout, "device not specified, using first one found: %s\n",
              rdma_config.dev_name);
    }
    if (!strcmp(ibv_get_device_name(dev_list[i]), rdma_config.dev_name)) {
      ib_dev = dev_list[i];
      break;
    }
  }
  /* if the device wasn't found in host */
  if (!ib_dev) {
    fprintf(stderr, "IB device %s wasn't found\n", rdma_config.dev_name);
    rc = 1;
  }
  /* get device handle */
  res->ib_ctx = ibv_open_device(ib_dev);
  if (!res->ib_ctx) {
    fprintf(stderr, "failed to open device %s\n", rdma_config.dev_name);
    rc = 1;
  }
  /* We are now done with device list, free it */
  ibv_free_device_list(dev_list);
  dev_list = NULL;
  ib_dev = NULL;
  /* query port properties */
  if (ibv_query_port(res->ib_ctx, rdma_config.ib_port, &res->port_attr)) {
    fprintf(stderr, "ibv_query_port on port %u failed\n", rdma_config.ib_port);
    rc = 1;
  }
  /* allocate Protection Domain */
  res->pd = ibv_alloc_pd(res->ib_ctx);
  if (!res->pd) {
    fprintf(stderr, "ibv_alloc_pd failed\n");
    rc = 1;
  }


//  Local_Memory_Register(&(res->send_buf), &(res->mr_send), 2500*4096, Message);
//  Local_Memory_Register(&(res->receive_buf), &(res->mr_receive), 2500*4096,
//                        Message);



  fprintf(stdout, "SST buffer, send&receive buffer were registered with a\n");
  rc = ibv_query_device(res->ib_ctx, &(res->device_attr));
  std::cout << "maximum outstanding wr number is"  << res->device_attr.max_qp_wr <<std::endl;
  std::cout << "maximum query pair number is" << res->device_attr.max_qp
            << std::endl;
    std::cout << "Maximum number of RDMA Read & Atomic operations that can be outstanding per QP " << res->device_attr.max_qp_rd_atom
              << std::endl;
    std::cout << "Maximum number of RDMA Read & Atomic operations that can be outstanding per EEC " << res->device_attr.max_ee_rd_atom
              << std::endl;
    std::cout << "Maximum depth per QP for initiation of RDMA Read & Atomic operations " << res->device_attr.max_qp_init_rd_atom
              << std::endl;
        std::cout << "Maximum number of resources used for RDMA Read & Atomic operations by this HCA as the Target " << res->device_attr.max_res_rd_atom
                  << std::endl;
        std::cout << "Atomic operations support level " << res->device_attr.atomic_cap
                  << std::endl;
  std::cout << "maximum completion queue number is" << res->device_attr.max_cq
            << std::endl;
  std::cout << "maximum memory region number is" << res->device_attr.max_mr
            << std::endl;
  std::cout << "maximum memory region size is" << res->device_attr.max_mr_size
            << std::endl;
//        std::cout << "maximum inline msg size is"  << res->device_attr.max_ <<std::endl;
  return rc;
}

bool RDMA_Manager::Get_Remote_qp_Info_Then_Connect(uint16_t target_node_id) {
  //  Connect Queue Pair through TCPIP
  int rc = 0;
  struct Registered_qp_config local_con_data;
  struct Registered_qp_config* remote_con_data = new Registered_qp_config();
  struct Registered_qp_config tmp_con_data;
  std::string qp_type = "main";
  char temp_receive[4* sizeof(ibv_mr)];
  char temp_send[4* sizeof(ibv_mr)] = "Q";

  union ibv_gid my_gid;
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return rc;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
  /* exchange using TCP sockets info required to connect QPs */
  ibv_qp* qp = create_qp(target_node_id, true, qp_type, ATOMIC_OUTSTANDING_SIZE, RECEIVE_OUTSTANDING_SIZE);
  local_con_data.qp_num = htonl(res->qp_map[target_node_id]->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &my_gid, 16);
  local_con_data.node_id = node_id;
//  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  if (sock_sync_data(res->sock_map[target_node_id], sizeof(struct Registered_qp_config),
                     (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides, node%d and node%d\n", node_id, target_node_id);
    rc = 1;
//    assert(false);
      return rc;
  }

  remote_con_data->qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data->lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data->gid, tmp_con_data.gid, 16);

//  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data->qp_num);
//  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data->lid);
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (qp_type == "default" ){
    assert(local_read_qp_info.at(target_node_id) != nullptr);
    local_read_qp_info.at(target_node_id)->Reset(remote_con_data);
  }
//    ((QP_Info_Map*)local_read_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
  //    local_read_qp_info->Reset(remote_con_data);
  else if(qp_type == "write_local_compact"){
    assert(local_write_compact_qp_info.at(target_node_id) != nullptr);
    local_write_compact_qp_info.at(target_node_id)->Reset(remote_con_data);
  }
//    ((QP_Info_Map*)local_write_compact_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
  //    local_write_compact_qp_info->Reset(remote_con_data);
  else if(qp_type == "write_local_flush"){
    assert(local_write_flush_qp_info.at(target_node_id) != nullptr);
    local_write_flush_qp_info.at(target_node_id)->Reset(remote_con_data);
  }
//    ((QP_Info_Map*)local_write_flush_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
  //    local_write_flush_qp_info->Reset(remote_con_data);

  else
    res->qp_main_connection_info.insert({target_node_id,remote_con_data});
  l.unlock();
  connect_qp(qp, qp_type, target_node_id);
    //Check whether the connection is on through the hearbeat message. Do not do this !!!
//    Send_heart_beat();

    //TODO: it seems sync those memory region through TPC IP is not very stable. better sync through RDMA.
  if (sock_sync_data(res->sock_map[target_node_id], 4 * sizeof(ibv_mr), temp_send,
                     temp_receive)) /* just send a dummy char back and forth */
    {
    fprintf(stderr, "sync error after QPs are were moved to RTS\n");
    rc = 1;
    }
    printf("Finish the connection with node %d\n", target_node_id);
    auto* global_data_mr = new ibv_mr();
    *global_data_mr = ((ibv_mr*) temp_receive)[0];
    mr_map_data.insert({target_node_id, global_data_mr});
    base_addr_map_data.insert({target_node_id, (uint64_t)global_data_mr->addr});
    rkey_map_data.insert({target_node_id, (uint64_t)global_data_mr->rkey});
    assert(global_data_mr->addr != nullptr);
    auto* global_lock_mr = new ibv_mr();
    *global_lock_mr = ((ibv_mr*) temp_receive)[1];
    mr_map_lock.insert({target_node_id, global_lock_mr});
    base_addr_map_lock.insert({target_node_id, (uint64_t)global_lock_mr->addr});
    rkey_map_lock.insert({target_node_id, (uint64_t)global_lock_mr->rkey});
    // Set the remote address for the index table.
    if (target_node_id == 1){
        global_index_table = new ibv_mr();
        *global_index_table= ((ibv_mr*) temp_receive)[2];
        assert(global_index_table->addr != nullptr);
        timestamp_oracle = new ibv_mr();
        *timestamp_oracle = ((ibv_mr*) temp_receive)[3];
        assert(timestamp_oracle->addr != nullptr);
    }


  // sync the communication by rdma.

  //  post_send<int>(res->mr_send, std::string("main"));
  //  ibv_wc wc[2] = {};
  //  if(!poll_completion(wc, 2, std::string("main"))){
  //    return true;
  //  }else{
  //    printf("The main qp not create correctly");
  //    return false;
  //  }


  memory_connection_counter.fetch_add(1);

  compute_message_handling_thread(qp_type, target_node_id);
  return false;
}
void RDMA_Manager::Cross_Computes_RPC_Threads_Creator(uint16_t target_node_id) {
    auto* cq_arr = new  std::array<ibv_cq*, NUM_QP_ACCROSS_COMPUTE*2>();
    auto* qp_arr = new  std::array<ibv_qp*, NUM_QP_ACCROSS_COMPUTE>();
    auto* temp_counter = new std::array<std::atomic<uint16_t>, NUM_QP_ACCROSS_COMPUTE*2>();
    auto* temp_mtx_arr = new std::array<SpinMutex, NUM_QP_ACCROSS_COMPUTE>();
    auto* temp_mtx_async = new std::array<Async_Xcompute_Tasks, NUM_QP_ACCROSS_COMPUTE>();
    assert((*temp_mtx_async)[0].mrs[0]!= nullptr);
    for (int i = 0; i < NUM_QP_ACCROSS_COMPUTE; ++i) {
        (*temp_counter)[i].store(0);
    }
    create_qp_xcompute(target_node_id, cq_arr, qp_arr);
    Put_qp_info_into_RemoteM(target_node_id, cq_arr, qp_arr);

    // Need to sync all threads here because the RPC can get blocked if there is one thread invoke Get_qp_info_from_RemoteM
    sync_invalidation_qp_info_put.fetch_add(1);
    while (sync_invalidation_qp_info_put.load() != compute_nodes.size() -1);
    //    Registered_qp_config_xcompute* qpXcompute = new Registered_qp_config_xcompute();

    Registered_qp_config_xcompute qp_info =  Get_qp_info_from_RemoteM(target_node_id);

    // te,p_buff will have the informatin for the remote query pair,
    // use this information for qp connection.
    connect_qp_xcompute(qp_arr, &qp_info);
    std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
    cq_xcompute.insert({target_node_id, cq_arr});
    qp_xcompute.insert({target_node_id, qp_arr});
    qp_xcompute_os_c.insert({target_node_id, temp_counter});
    qp_xcompute_asyncT.insert({target_node_id, temp_mtx_async});
    qp_xcompute_mtx.insert({target_node_id, temp_mtx_arr});
    // we need lock for post_receive_xcompute because qp_xcompute is not thread safe.
    printf("Prepare receive mr for %hu", target_node_id);
    ibv_mr recv_mr[NUM_QP_ACCROSS_COMPUTE][RECEIVE_OUTSTANDING_SIZE] = {};
    for (int j = 0; j < NUM_QP_ACCROSS_COMPUTE; ++j) {
        for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++){
            Allocate_Local_RDMA_Slot(recv_mr[j][i], Message);
            post_receive_xcompute(&recv_mr[j][i], target_node_id, j);
        }
    }
    l.unlock();
    //TODO: delete the code below.
//    if(node_id == 2 && compute_nodes.size() == 2){
//        Send_heart_beat_xcompute(0);
//
//    }
    compute_connection_counter.fetch_add(1);
    //sync among thread here. If not the since try poll complemtion across compute nodes is not thread safe,
    // the concurrent cq insertion can result in error.
    while (compute_connection_counter.load() != compute_nodes.size() -1);
    //Do we need to sync below?, probably not at below, should be synced outside this function.
    for (int i = 0; i < NUM_QP_ACCROSS_COMPUTE; ++i) {
        std::unique_lock<std::mutex> lck(invalidate_channel_mtx);
//        std::thread p_t(&::DSMEngine::RDMA_Manager::compute_message_handling_thread, this, target_node_id, i, recv_mr[i]);
        Invalidation_bg_threads.emplace_back(&RDMA_Manager::cross_compute_message_handling_worker, this, target_node_id, i, recv_mr[i]);
//        Invalidation_bg_threads.back().detach();
    }
//    std::unique_lock<std::mutex> lck(invalidate_channel_mtx);
//    sleep(2);
    for (auto &iter:Invalidation_bg_threads) {
        // do not detach, because the thread are using the local variable. Or we register the recve buffer inside the thread.
        iter.join();
    }

}

    void RDMA_Manager::cross_compute_message_handling_worker(uint16_t target_node_id, int qp_num, ibv_mr *recv_mr) {
        ibv_wc wc[3] = {};
        int buffer_position= 0;
        int miss_poll_counter= 0;
        while (true) {
            assert(target_node_id != node_id);

//      rdma_mg->poll_completion(wc, 1, client_ip, false, compute_node_id);
            // TODO: try to poll more cycles than now, see what will happen for the performance.
                if (try_poll_completions_xcompute(wc, 1, false, target_node_id, qp_num) == 0){
                    // exponetial back off to save cpu cycles.
                    if(++miss_poll_counter < 20480){
//                        asm("pause");
                        continue;
                    }
                    if(++miss_poll_counter < 40960){
                        pthread_yield();
                        continue ;
                    }
                    if(++miss_poll_counter < 81920){
                        usleep(16);
                        continue;
                    }else{
                        usleep(512);
                        continue;
                    }
                }
                miss_poll_counter = 0;
                int buff_pos = buffer_position;
                // TODO: since we do not copy the received mesage then it is possible that the hnalding time of
                // the function is to long to result in buffer over flow.
                RDMA_Request* receive_msg_buf = new RDMA_Request();
                //TODO change the way we get the recevi buffer, because we may have mulitple channel accross compute nodes.
                *receive_msg_buf = *(RDMA_Request*)recv_mr[buff_pos].addr;
//      memcpy(receive_msg_buf, recv_mr[buffer_position].addr, sizeof(RDMA_Request));

                // copy the pointer of receive buf to a new place because
                // it is the same with send buff pointer.
            switch (receive_msg_buf->command) {
                case writer_invalidate_modified:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Writer_Inv_Modified_handler(receive_msg_buf, target_node_id);
                    break;
                case writer_invalidate_shared:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Writer_Inv_Shared_handler(receive_msg_buf, target_node_id);
                    break;
                case reader_invalidate_modified:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Reader_Inv_Modified_handler(receive_msg_buf, target_node_id);
                    break;
                case broadcast_create_ds:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Create_Delta_Section_handler(receive_msg_buf, target_node_id);
                    break;
                case pull_delta_section:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Pull_Delta_Section_handler(receive_msg_buf, target_node_id);
                    break;
                case push_least_snapshot:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Push_Least_Snapshot_handler(receive_msg_buf, target_node_id);
                    break;
                case heart_beat:
                    printf("heart_beat\n");
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    break;
                case tuple_read_2pc:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Tuple_read_2pc_handler(receive_msg_buf, target_node_id);
                    break;
                case prepare_2pc:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Prepare_2pc_handler(receive_msg_buf, target_node_id);
                    break;
                case commit_2pc:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Commit_2pc_handler(receive_msg_buf, target_node_id);
                    break;
                case abort_2pc:
                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
                    Abort_2pc_handler(receive_msg_buf, target_node_id);
                    break;
                default:
                    printf("corrupt message from client. %d\n", receive_msg_buf->command);
                    assert(false);
                    exit(0);
                    break;
                
            }
//                if (receive_msg_buf->command == writer_invalidate_modified) {
//                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
////                printf("release_write_lock, page_addr is %p\n", receive_msg_buf->content.R_message.page_addr);
//
//
//
//                    //TODO: what shall we do if the read lock is on
//                    //Ans: do nothing
//
////                    BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.rdma_mg = this, .func_args = receive_msg_buf};
////                    Invalidation_bg_threads.Schedule(&RDMA_Manager::Write_Invalidation_Message_Handler, thread_pool_args, i);
//                    Writer_Inv_Modified_handler(receive_msg_buf, target_node_id);
//
//                } else if (receive_msg_buf->command == writer_invalidate_shared) {
//                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
////                printf("release_read_lock, page_addr is %p\n", receive_msg_buf->content.R_message.page_addr);
////                    BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.rdma_mg = this, .func_args = receive_msg_buf};
////                    Invalidation_bg_threads.Schedule(&RDMA_Manager::Read_Invalidation_Message_Handler, thread_pool_args, i);
//                    Writer_Inv_Shared_handler(receive_msg_buf, target_node_id);
//                } else if (receive_msg_buf->command == reader_invalidate_modified) {
//                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
////                printf("release_read_lock, page_addr is %p\n", receive_msg_buf->content.R_message.page_addr);
////                    BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.rdma_mg = this, .func_args = receive_msg_buf};
////                    Invalidation_bg_threads.Schedule(&RDMA_Manager::Read_Invalidation_Message_Handler, thread_pool_args, i);
//                    Reader_Inv_Modified_handler(receive_msg_buf, target_node_id);
//                } else if (receive_msg_buf->command == heart_beat) {
//                    printf("heart_beat\n");
//                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
//                } else if (receive_msg_buf->command == tuple_read_2pc) {
//                    printf("heart_beat\n");
//                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
//                } else if (receive_msg_buf->command == prepare_2pc) {
//                    printf("heart_beat\n");
//                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
//                } else if (receive_msg_buf->command == commit_2pc) {
//                    printf("heart_beat\n");
//                    post_receive_xcompute(&recv_mr[buff_pos],target_node_id,qp_num);
//
//                } else {
//                    printf("corrupt message from client. %d\n", receive_msg_buf->command);
//                    assert(false);
//                    break;
//                }
                // increase the buffer index
                if (buffer_position == RECEIVE_OUTSTANDING_SIZE - 1 ){
                    buffer_position = 0;
                } else{
                    buffer_position++;
                }
        }
        assert(false);
        for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++){
            Deallocate_Local_RDMA_Slot(recv_mr[i].addr, Message);
        }

    }
void RDMA_Manager::Put_qp_info_into_RemoteM(uint16_t target_compute_node_id,
                                            std::array<ibv_cq *, NUM_QP_ACCROSS_COMPUTE * 2> *cq_arr,
                                            std::array<ibv_qp *, NUM_QP_ACCROSS_COMPUTE> *qp_arr) {
    RDMA_Request* send_pointer;
    ibv_mr* send_mr = Get_local_send_message_mr();
    send_pointer = (RDMA_Request*)send_mr->addr;
    send_pointer->command = put_qp_info;
    for (int i = 0; i < NUM_QP_ACCROSS_COMPUTE; ++i) {
        send_pointer->content.qp_config_xcompute.qp_num[i] = (*qp_arr)[i]->qp_num;
//        fprintf(stdout, "\nQP num to be sent = 0x%x\n", (*qp_arr)[i]->qp_num);
    }
    union ibv_gid my_gid;
    int rc;
    if (rdma_config.gid_idx >= 0) {
        rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                           &my_gid);

        if (rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n",
                    rdma_config.ib_port, rdma_config.gid_idx);
            return;
        }
    } else
        memset(&my_gid, 0, sizeof my_gid);
    send_pointer->content.qp_config_xcompute.lid = res->port_attr.lid;
    memcpy(send_pointer->content.qp_config_xcompute.gid, &my_gid, 16);
    send_pointer->content.qp_config_xcompute.node_id_pairs = (uint32_t)target_compute_node_id | ((uint32_t)node_id) << 16;
//    printf("node id pair to be put is %x 1 \n", send_pointer->content.qp_config_xcompute.node_id_pairs);
//    fprintf(stdout, "Local LID = 0x%x\n", res->port_attr.lid);
//    send_pointer->buffer = receive_mr.addr;
//    send_pointer->rkey = receive_mr.rkey;
//    RDMA_Reply* receive_pointer;
    uint16_t target_memory_node_id = 1;
    //Use node 1 memory node as the place to store the temporary QP information
    rc = post_send<RDMA_Request>(send_mr, target_memory_node_id, std::string("main"));
    assert(rc == 0);
    ibv_wc wc[2] = {};
    //  while(wc.opcode != IBV_WC_RECV){
    //    poll_completion(&wc);
    //    if (wc.status != 0){
    //      fprintf(stderr, "Work completion status is %d \n", wc.status);
    //    }
    //
    //  }
    //  assert(wc.opcode == IBV_WC_RECV);
    if (poll_completion(wc, 1, std::string("main"),
                        true, target_memory_node_id)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
        fprintf(stderr, "failed to poll send for qp connection\n");
    }
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );

}

Registered_qp_config_xcompute RDMA_Manager::Get_qp_info_from_RemoteM(uint16_t target_compute_node_id) {
    RDMA_Request* send_pointer;
    ibv_mr* send_mr = Get_local_send_message_mr();
    ibv_mr* receive_mr = Get_local_receive_message_mr();
    send_pointer = (RDMA_Request*)send_mr->addr;
    send_pointer->command = get_qp_info;

    send_pointer->content.target_id_pair = ((uint32_t)node_id) | ((uint32_t) target_compute_node_id) << 16;
    printf("node id pair to be get is %x 2\n", send_pointer->content.target_id_pair );

    send_pointer->buffer = receive_mr->addr;
    send_pointer->rkey = receive_mr->rkey;
    RDMA_Reply* receive_pointer;
    receive_pointer = (RDMA_Reply*)receive_mr->addr;
    //Clear the reply buffer for the polling.
    *receive_pointer = {};
//  post_receive<registered_qp_config>(res->mr_receive, std::string("main"));
    uint16_t target_memory_node_id = 1;
    post_send<RDMA_Request>(send_mr, target_memory_node_id, std::string("main"));
    ibv_wc wc[2] = {};
    //  while(wc.opcode != IBV_WC_RECV){
    //    poll_completion(&wc);
    //    if (wc.status != 0){
    //      fprintf(stderr, "Work completion status is %d \n", wc.status);
    //    }
    //
    //  }
    //  assert(wc.opcode == IBV_WC_RECV);
    if (poll_completion(wc, 1, std::string("main"),
                        true, target_memory_node_id)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
        fprintf(stderr, "failed to poll send for remote memory register\n");
    }
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
    poll_reply_buffer(receive_pointer); // poll the receive for 2 entires
    return receive_pointer->content.qp_config_xcompute;
}


    ibv_mr *RDMA_Manager::create_index_table() {
    std::unique_lock<std::mutex> lck(global_resources_mtx);
    if (global_index_table == nullptr){
        int mr_flags = 0;
        size_t size = 16*1024;
        char* buff = new char[size];
//      *p2buffpointer = (char*)hugePageAlloc(size);
        if (!buff) {
            fprintf(stderr, "failed to malloc bytes to memory buffer create index\n");
            return nullptr;
        }
        memset(buff, 0, size);

        /* register the memory buffer */
        mr_flags =
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
        //  auto start = std::chrono::high_resolution_clock::now();
        global_index_table  = ibv_reg_mr(res->pd, buff, size, mr_flags);
        printf("Global index table address is %p\n", global_index_table->addr);


    }
    return global_index_table;

}
ibv_mr *RDMA_Manager::create_lock_table() {
    std::unique_lock<std::mutex> lck(global_resources_mtx);
    if (global_lock_table == nullptr){
        int mr_flags = 0;
        size_t size = define::kLockChipMemSize;
        char* buff = new char[size];
//      *p2buffpointer = (char*)hugePageAlloc(size);
        if (!buff) {
            fprintf(stderr, "failed to malloc bytes to memory buffer create lock table\n");
            return nullptr;
        }
        memset(buff, 0, size);

        /* register the memory buffer */
        mr_flags =
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
        //  auto start = std::chrono::high_resolution_clock::now();
        global_lock_table  = ibv_reg_mr(res->pd, buff, size, mr_flags);
    }

    return global_lock_table;

}
    ibv_mr *RDMA_Manager::create_timestamp_oracle() {
        if (timestamp_oracle == nullptr){
            int mr_flags = 0;
            size_t size = 8;
            char* buff = (char*)aligned_alloc(8,8);
            if (!buff) {
                fprintf(stderr, "failed to malloc bytes to memory buffer create lock table\n");
                return nullptr;
            }
//            memset(buff, 0, size);
            *(uint64_t* ) buff = 1;// initialize timestamp to 1 for better debugging.
            /* register the memory buffer */
            mr_flags =
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
            //  auto start = std::chrono::high_resolution_clock::now();
            timestamp_oracle  = ibv_reg_mr(res->pd, buff, size, mr_flags);
        }

        return timestamp_oracle;

    }

void RDMA_Manager::sync_with_computes_Cside() {

  char temp_receive[2];
  char temp_send[] = "Q";
  auto start = std::chrono::high_resolution_clock::now();
  //Node 1 is the coordinator server
  sock_sync_data(res->sock_map[1], 1, temp_send,
                 temp_receive);
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  printf("sync wait time is %ld", duration.count());
}
    void RDMA_Manager::sync_with_computes_Mside() {
        char buffer[100];
        int number_of_ready = 0;
        uint64_t rc = 0;
        int round  = 0;
//#ifndef NDEBUG
        std::vector<uint16_t > answered_nodes;
//#endif

        while (1){
            for(auto iter : res->sock_map){
                //Read is a block function
                rc =read(iter.second, buffer, 100);
                if(rc > 0){
                    number_of_ready++;
//#ifndef NDEBUG
                    answered_nodes.push_back(iter.first);
//#endif
                    printf("compute node sync number is %d\n", iter.first );
                    if (number_of_ready == compute_nodes.size()){
                        //TODO: answer back.
                        broadcast_to_computes_through_socket();
                        number_of_ready = 0;
//#ifndef NDEBUG
                        answered_nodes.clear();
                        round++;
//#endif
                    }
                    rc = 0;
                }else if (rc == -1){
                    printf("The socket return is not normal, %lu, target node is \n", rc);
                }
            }

        }


    }
ibv_mr* RDMA_Manager::Get_local_read_mr() {
  ibv_mr* ret;
  ret = (ibv_mr*)read_buffer->Get();
  if (ret == nullptr){
    char* buffer = new char[name_to_chunksize.at(Regular_Page)];
    auto mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    //  auto start = std::chrono::high_resolution_clock::now();
    ret = ibv_reg_mr(res->pd, buffer, name_to_chunksize.at(Regular_Page), mr_flags);
    read_buffer->Reset(ret);
  }
    assert(ret + 0);
  return ret;
}
    ibv_mr* RDMA_Manager::Get_local_big_mr() {
        ibv_mr* ret;
        ret = (ibv_mr*)big_buffer->Get();
        if (ret == nullptr){
            char* buffer = new char[name_to_chunksize.at(DeltaChunk)];
            auto mr_flags =
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
            //  auto start = std::chrono::high_resolution_clock::now();
            ret = ibv_reg_mr(res->pd, buffer, name_to_chunksize.at(DeltaChunk), mr_flags);
            big_buffer->Reset(ret);
        }
        assert(ret + 0);
        return ret;
    }
    ibv_mr* RDMA_Manager::Get_local_send_message_mr() {
        ibv_mr* ret;
        ret = (ibv_mr*)send_message_buffer->Get();
        if (ret == nullptr){
            char* buffer = new char[name_to_chunksize.at(Message)];
            auto mr_flags =
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
            //  auto start = std::chrono::high_resolution_clock::now();
            ret = ibv_reg_mr(res->pd, buffer, name_to_chunksize.at(Message), mr_flags);
            send_message_buffer->Reset(ret);
        }
        assert(ret + 0);
        return ret;
    }
    ibv_mr* RDMA_Manager::Get_local_receive_message_mr() {
        ibv_mr* ret;
        ret = (ibv_mr*)receive_message_buffer->Get();
        if (ret == nullptr){
            char* buffer = new char[name_to_chunksize.at(Message)];
            auto mr_flags =
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
            //  auto start = std::chrono::high_resolution_clock::now();
            ret = ibv_reg_mr(res->pd, buffer, name_to_chunksize.at(Message), mr_flags);
            receive_message_buffer->Reset(ret);
        }
        assert(ret + 0);
        return ret;
    }
    ibv_mr* RDMA_Manager::Get_local_CAS_mr() {
        ibv_mr* ret;
        ret = (ibv_mr*)CAS_buffer->Get();
        if (ret == nullptr){
            // it is 16 bytes aligned so it can be atomic.
            char* buffer = new char[8];
            auto mr_flags =
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
            //  auto start = std::chrono::high_resolution_clock::now();
            ret = ibv_reg_mr(res->pd, buffer, 8, mr_flags);
            CAS_buffer->Reset(ret);
        }
        return ret;
    }

void RDMA_Manager::broadcast_to_computes_through_socket(){
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  char local_data[] = "Q";
  for(auto iter : res->sock_map){
    rc = write(iter.second, local_data, 1);
    assert(rc == 1);
  }

}


ibv_qp* RDMA_Manager::create_qp_Mside(bool seperated_cq,
                                           std::string& qp_id) {
  struct ibv_qp_init_attr qp_init_attr;

  /* each side will send only one WR, so Completion Queue with 1 entry is enough
   */
  int cq_size = 1024;
  // cq1 send queue, cq2 receive queue
  ibv_cq* cq1 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
  ibv_cq* cq2;
  if (seperated_cq)
    cq2 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);

  if (!cq1) {
    fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
  }
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (seperated_cq)
    cq_map_Mside.insert({qp_id, std::make_pair(cq1, cq2)});
  else
    cq_map_Mside.insert({qp_id, std::make_pair(cq1, nullptr)});

  /* create the Queue Pair */
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 0;
  qp_init_attr.send_cq = cq1;
  if (seperated_cq)
    qp_init_attr.recv_cq = cq2;
  else
    qp_init_attr.recv_cq = cq1;
  qp_init_attr.cap.max_send_wr = 8;
  qp_init_attr.cap.max_recv_wr = 8;
  qp_init_attr.cap.max_send_sge = 5;
  qp_init_attr.cap.max_recv_sge = 5;
  //  qp_init_attr.cap.max_inline_data = -1;
  ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
  if (!qp) {
    fprintf(stderr, "failed to create QP\n");
  }

    qp_map_Mside[qp_id] = qp;
//  fprintf(stdout, "QP was created, QP number=0x%x\n", qp->qp_num);
  //  uint16_t* p = qp->gid;
  //  fprintf(stdout,
  //          "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
  //          p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
  //          p[11], p[12], p[13], p[14], p[15]);
  return qp;
}
ibv_qp * RDMA_Manager::create_qp(uint16_t target_node_id, bool seperated_cq, std::string &qp_type,
                                 uint32_t send_outstanding_num,
                                 uint32_t recv_outstanding_num) {
  struct ibv_qp_init_attr qp_init_attr;

  /* each side will send only one WR, so Completion Queue with 1 entry is enough
   */
  int cq_size = 128;
  // cq1 send queue, cq2 receive queue
  ibv_cq* cq1 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
  ibv_cq* cq2;
  if (seperated_cq)
    cq2 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);

  if (!cq1) {
    fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
  }
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (qp_type == "default" ){
    assert(cq_data_default[target_node_id] != nullptr);
    cq_data_default[target_node_id]->Reset(cq1);
  }
//    ((CQ_Map*)cq_data_default->Get())->insert({shard_target_node_id, cq1});
//    cq_data_default->Reset(cq1);
  else if(qp_type == "write_local_compact"){
    assert(cq_local_write_compact[target_node_id]!= nullptr);
    cq_local_write_compact[target_node_id]->Reset(cq1);
  }
//    ((CQ_Map*)cq_local_write_compact->Get())->insert({shard_target_node_id, cq1});
//    cq_local_write_compact->Reset(cq1);
  else if(qp_type == "write_local_flush"){
    assert(cq_local_write_flush[target_node_id]!= nullptr);
    cq_local_write_flush[target_node_id]->Reset(cq1);
    }
//    ((CQ_Map*)cq_local_write_flush->Get())->insert({shard_target_node_id, cq1});
  else if (seperated_cq)
    res->cq_map.insert({target_node_id, std::make_pair(cq1, cq2)});
  else
    res->cq_map.insert({target_node_id, std::make_pair(cq1, nullptr)});

  /* create the Queue Pair */
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 0;
  qp_init_attr.send_cq = cq1;
  if (seperated_cq)
    qp_init_attr.recv_cq = cq2;
  else
    qp_init_attr.recv_cq = cq1;
  qp_init_attr.cap.max_send_wr = send_outstanding_num;
  qp_init_attr.cap.max_recv_wr = recv_outstanding_num;
  qp_init_attr.cap.max_send_sge = 2;
  qp_init_attr.cap.max_recv_sge = 2;
  qp_init_attr.cap.max_inline_data = MAX_INLINE_SIZE;
  //  qp_init_attr.cap.max_inline_data = -1;
  ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
  if (!qp) {
    fprintf(stderr, "failed to create QP\n");
  }
  if (qp_type == "default" ){
    assert(qp_data_default[target_node_id] != nullptr);
    qp_data_default[target_node_id]->Reset(qp);
//    auto counter = new uint32_t(0);
//    async_tasks[target_node_id]->Reset(counter);
  }
//    ((QP_Map*)qp_data_default->Get())->insert({shard_target_node_id, qp});
//    qp_data_default->Reset(qp);
  else if(qp_type == "write_local_flush"){
    assert(qp_local_write_flush[target_node_id]!= nullptr);
    qp_local_write_flush[target_node_id]->Reset(qp);
    }
//    ((QP_Map*)qp_local_write_flush->Get())->insert({shard_target_node_id, qp});
//    qp_local_write_flush->Reset(qp);
  else if(qp_type == "write_local_compact"){
      assert(qp_local_write_compact[target_node_id]!= nullptr);
      qp_local_write_compact[target_node_id]->Reset(qp);
  }
//    ((QP_Map*)qp_local_write_compact->Get())->insert({shard_target_node_id, qp});
//    qp_local_write_compact->Reset(qp);
  else
    res->qp_map[target_node_id] = qp;
//  fprintf(stdout, "QP was created, QP number=0x%x\n", qp->qp_num);
//  uint16_t* p = qp->gid;
//  fprintf(stdout,
//          "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
//          p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
//          p[11], p[12], p[13], p[14], p[15]);
  return qp;
}

    void
    RDMA_Manager::create_qp_xcompute(uint16_t target_node_id, std::array<ibv_cq *, NUM_QP_ACCROSS_COMPUTE * 2> *cq_arr,
                                     std::array<ibv_qp *, NUM_QP_ACCROSS_COMPUTE> *qp_arr) {
        struct ibv_qp_init_attr qp_init_attr;
        assert(target_node_id%2 == 0);
        //TODO: optimize the cq size for xcompute channel.
        int cq_size = 1024;
        // cq1 send queue, cq2 receive queue
        std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);

//        ibv_cq ** cq_arr = new  ibv_cq*[NUM_QP_ACCROSS_COMPUTE*2];
//        ibv_qp ** qp_arr = new  ibv_qp*[NUM_QP_ACCROSS_COMPUTE];
        auto* qp_info = new Registered_qp_config_xcompute();
        for (int i = 0; i < NUM_QP_ACCROSS_COMPUTE; ++i) {
            ibv_cq* cq1 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
            ibv_cq* cq2 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
            if (!cq1 | !cq2) {
                fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
            }
//            res->cq_map.insert({target_node_id, std::make_pair(cq1, cq2)});
            (*cq_arr)[2*i] = cq1;
            (*cq_arr)[2*i+1] = cq2;
            /* create the Queue Pair */
            memset(&qp_init_attr, 0, sizeof(qp_init_attr));
            qp_init_attr.qp_type = IBV_QPT_RC;
            qp_init_attr.sq_sig_all = 0;
            qp_init_attr.send_cq = cq1;
            qp_init_attr.recv_cq = cq2;
            // TODO: we need to maintain a atomic pending request counter to avoid the pend wr exceed max_send_wr.
            qp_init_attr.cap.max_send_wr = SEND_OUTSTANDING_SIZE_XCOMPUTE + 1; // THis should be larger that he maixum core number for the machine.
            qp_init_attr.cap.max_recv_wr = RECEIVE_OUTSTANDING_SIZE; // this can be down graded if we have the invalidation message with reply
            qp_init_attr.cap.max_send_sge = 2;
            qp_init_attr.cap.max_recv_sge = 2;
            qp_init_attr.cap.max_inline_data = MAX_INLINE_SIZE;
            ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
            (*qp_arr)[i] = qp;
            if (!qp) {
                fprintf(stderr, "failed to create QP\n");
            }
//            qp_xcompute_info.insert()

            fprintf(stdout, "Xcompute QPs were created, QP number=0x%x\n", qp->qp_num);
        }
//        cp_xcompute.insert({target_node_id, cq_arr});
//        qp_xcompute.insert({target_node_id, qp_arr});
//  uint16_t* p = qp->gid;
//  fprintf(stdout,
//          "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
//          p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
//          p[11], p[12], p[13], p[14], p[15]);

    }
int RDMA_Manager::connect_qp_Mside(ibv_qp* qp, std::string& q_id) {
  int rc;
  //  ibv_qp* qp;
  //  if (qp_id == "read_local" ){
  //    qp = static_cast<ibv_qp*>(qp_data_default->Get());
  //    assert(qp!= nullptr);
  //  }
  //  else if(qp_id == "write_local"){
  //    qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
  //
  //  }
  //  else{
  //    qp = res->qp_map[qp_id];
  //    assert(qp!= nullptr);
  //  }
  // protect the res->qp_main_connection_info outside this function

  Registered_qp_config* remote_con_data;
  std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);


    remote_con_data = qp_main_connection_info_Mside.at(q_id);
  l.unlock();
  if (rdma_config.gid_idx >= 0) {
    uint8_t* p = remote_con_data->gid;
//    fprintf(stdout,
//            "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
//            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
//            p[11], p[12], p[13], p[14], p[15]);
  }
  /* modify the QP to init */
  rc = modify_qp_to_init(qp);
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }

  /* modify the QP to RTR */
  rc = modify_qp_to_rtr(qp, remote_con_data->qp_num, remote_con_data->lid,
                        remote_con_data->gid);
  if (rc) {
    fprintf(stderr, "Node %u failed to modify QP state to RTR\n", node_id);
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(qp);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTS\n");
    goto connect_qp_exit;
  }
  //  else{
  //    printf("connection built up!\n");
  //  }
  fprintf(stdout, "QP %p state was change to RTS\n", qp);
/* sync to make sure that both sides are in states that they can connect to prevent packet loose */
connect_qp_exit:
  return rc;
}
/******************************************************************************
* Function: connect_qp
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
* Connect the QP. Transition the server side to RTR, sender side to RTS
******************************************************************************/
int RDMA_Manager::connect_qp(ibv_qp* qp, std::string& qp_type,
                             uint16_t target_node_id) {
  int rc;
//  ibv_qp* qp;
//  if (qp_id == "read_local" ){
//    qp = static_cast<ibv_qp*>(qp_data_default->Get());
//    assert(qp!= nullptr);
//  }
//  else if(qp_id == "write_local"){
//    qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
//
//  }
//  else{
//    qp = res->qp_map[qp_id];
//    assert(qp!= nullptr);
//  }
// protect the res->qp_main_connection_info outside this function

  Registered_qp_config* remote_con_data;
  std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);

  if (qp_type == "default" )
    remote_con_data = (Registered_qp_config*)local_read_qp_info[target_node_id]->Get();

//    remote_con_data = ((QP_Info_Map*)local_read_qp_info->Get())->at(shard_target_node_id);
  else if(qp_type == "write_local_compact")
    remote_con_data = (Registered_qp_config*)local_write_compact_qp_info[target_node_id]->Get();
//    remote_con_data = ((QP_Info_Map*)local_write_compact_qp_info->Get())->at(shard_target_node_id);
  else if(qp_type == "write_local_flush")
    remote_con_data = (Registered_qp_config*)local_write_flush_qp_info[target_node_id]->Get();
//    remote_con_data = ((QP_Info_Map*)local_write_flush_qp_info->Get())->at(shard_target_node_id);
  else
    remote_con_data = res->qp_main_connection_info.at(target_node_id);
  l.unlock();
  if (rdma_config.gid_idx >= 0) {
    uint8_t* p = remote_con_data->gid;
//    fprintf(stdout,
//            "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
//            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
//            p[11], p[12], p[13], p[14], p[15]);
  }
  /* modify the QP to init */
  rc = modify_qp_to_init(qp);
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }

  /* modify the QP to RTR */
  rc = modify_qp_to_rtr(qp, remote_con_data->qp_num, remote_con_data->lid,
                        remote_con_data->gid);
  if (rc) {
    fprintf(stderr, "Node %u failed to modify QP state to RTR\n", node_id);
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(qp);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTS\n");
    goto connect_qp_exit;
  }
//  else{
//    printf("connection built up!\n");
//  }
//  fprintf(stdout, "QP %p state was change to RTS\n", qp);
/* sync to make sure that both sides are in states that they can connect to prevent packet loose */
connect_qp_exit:
  return rc;
}
int RDMA_Manager::connect_qp(ibv_qp* qp, Registered_qp_config* remote_con_data) {
  int rc;
  //  ibv_qp* qp;
  //  if (qp_id == "read_local" ){
  //    qp = static_cast<ibv_qp*>(qp_data_default->Get());
  //    assert(qp!= nullptr);
  //  }
  //  else if(qp_id == "write_local"){
  //    qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
  //
  //  }
  //  else{
  //    qp = res->qp_map[qp_id];
  //    assert(qp!= nullptr);
  //  }
  // protect the res->qp_main_connection_info outside this function


  if (rdma_config.gid_idx >= 0) {
    uint8_t* p = remote_con_data->gid;
//    fprintf(stdout,
//            "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
//            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
//            p[11], p[12], p[13], p[14], p[15]);
  }
  /* modify the QP to init */
  rc = modify_qp_to_init(qp);
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }

  /* modify the QP to RTR */
  rc = modify_qp_to_rtr(qp, remote_con_data->qp_num, remote_con_data->lid,
                        remote_con_data->gid);
  if (rc) {
    fprintf(stderr, "Node %u failed to modify QP state to RTR\n", node_id);
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(qp);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTS\n");
    goto connect_qp_exit;
  }
//  fprintf(stdout, "QP %p state was change to RTS\n", qp);
  /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
  connect_qp_exit:
  return rc;
}

int RDMA_Manager::connect_qp_xcompute(std::array<ibv_qp *, NUM_QP_ACCROSS_COMPUTE> *qp_arr,
                                      DSMEngine::Registered_qp_config_xcompute *remote_con_data) {
    int rc = 0;
    if (rdma_config.gid_idx >= 0) {
        uint8_t* p = remote_con_data->gid;
        fprintf(stdout,
                "Remote xcompute GID  =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
                p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
                p[11], p[12], p[13], p[14], p[15]);
    }
    for (int i = 0; i < NUM_QP_ACCROSS_COMPUTE; ++i) {
        /* modify the QP to init */
        rc = modify_qp_to_init((*qp_arr)[i]);
        if (rc) {
            fprintf(stderr, "change QP xcompute state to INIT failed\n");
            goto connect_qp_exit;
        }
        fprintf(stderr, "received QP xcompute number is 0x%x\n", remote_con_data->qp_num[i]);
        /* modify the QP to RTR */
        rc = modify_qp_to_rtr((*qp_arr)[i], remote_con_data->qp_num[i], remote_con_data->lid,
                              remote_con_data->gid);
        if (rc) {
            fprintf(stderr, "failed to modify QP xcompute state to RTR\n");
            goto connect_qp_exit;
        }
        rc = modify_qp_to_rts((*qp_arr)[i]);
        if (rc) {
            fprintf(stderr, "failed to modify QP xcompute state to RTS\n");
            goto connect_qp_exit;
        }
//        fprintf(stdout, "QP xcompute %p state was change to RTS\n", (*qp_arr)[i]);
    }

    /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
    connect_qp_exit:
    return rc;

}


int RDMA_Manager::modify_qp_to_reset(ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RESET;
  flags = IBV_QP_STATE;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RESET\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_init
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RESET to INIT state
******************************************************************************/
int RDMA_Manager::modify_qp_to_init(struct ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = rdma_config.ib_port;
  attr.pkey_index = 0;
  attr.qp_access_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |IBV_ACCESS_REMOTE_ATOMIC;
  flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to INIT\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_rtr
*
* Input
* qp QP to transition
* remote_qpn remote QP number
* dlid destination LID
* dgid destination GID (mandatory for RoCEE)
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the INIT to RTR state, using the specified QP number
******************************************************************************/
int RDMA_Manager::modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn,
                                   uint16_t dlid, uint8_t *dgid) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_4096;
  attr.dest_qp_num = remote_qpn;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = ATOMIC_OUTSTANDING_SIZE; //destination should have a larger pending entries. than the qp send outstanding
  attr.min_rnr_timer = 0xc;
  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = dlid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = rdma_config.ib_port;
  if (rdma_config.gid_idx >= 0) {
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = 1;
    memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 0xFF;
    attr.ah_attr.grh.sgid_index = rdma_config.gid_idx;
    attr.ah_attr.grh.traffic_class = 0;
  }
  flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
          IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "Node %u failed to modify QP state to RTR\n", node_id);
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_rts
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RTR to RTS state
******************************************************************************/
int RDMA_Manager::modify_qp_to_rts(struct ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 0xe;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = 0;
  attr.max_rd_atomic = ATOMIC_OUTSTANDING_SIZE;// allow RDMA atomic andn RDMA read batched.
  flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc)
      fprintf(stderr, "failed to modify QP state to RTS\n");
  return rc;
}
/******************************************************************************
* Function: sock_sync_data
*
* Input
* sock socket to transfer data on
* xfer_size size of data to transfer
* local_data pointer to data to be sent to remote
*
* Output
* remote_data pointer to buffer to receive remote data
*
* Returns
* 0 on success, negative error code on failure
*
* Description
* Sync data across a socket. The indicated local data will be sent to the
* remote. It will then wait for the remote to send its data back. It is
* assumed that the two sides are in sync and call this function in the proper
* order. Chaos will ensue if they are not. :)
*
* Also note this is a blocking function and will wait for the full data to be
* received from the remote.
*
******************************************************************************/
int RDMA_Manager::sock_sync_data(int sock, int xfer_size, char* local_data,
                                 char* remote_data) {
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(sock, local_data, xfer_size);
  if (rc < xfer_size)
    fprintf(stderr,
            "Failed writing data during sock_sync_data, total bytes are %d, erron is %d\n",
            rc, errno);
  else
    rc = 0;
  printf("total bytes: %d", xfer_size);
  while (!rc && total_read_bytes < xfer_size) {
    read_bytes = read(sock, remote_data, xfer_size);
    printf("read byte: %d", read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
//  fprintf(stdout, "The data which has been read through is %s size is %d\n",
//          remote_data, read_bytes);
  return rc;
}
/******************************************************************************
End of socket operations
******************************************************************************/
    int
    RDMA_Manager::RDMA_Read(GlobalAddress remote_ptr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
                            Chunk_type pool_name, std::string qp_type) {

        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr* bad_wr = NULL;
        int rc;
        /* prepare the scatter/gather entry */
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)local_mr->addr;
        sge.length = msg_size;
        sge.lkey = local_mr->lkey;
        /* prepare the send work request */
        memset(&sr, 0, sizeof(sr));
        sr.next = NULL;
        sr.wr_id = 0;
        sr.sg_list = &sge;
        sr.num_sge = 1;
        sr.opcode = IBV_WR_RDMA_READ;
        if (send_flag != 0) sr.send_flags = send_flag;
        switch (pool_name) {
            case Regular_Page:{
                sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_data[remote_ptr.nodeID]);
                sr.wr.rdma.rkey = rkey_map_data[remote_ptr.nodeID];
                break;
            }
            case LockTable:{
                sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_lock[remote_ptr.nodeID]);
                sr.wr.rdma.rkey = rkey_map_lock[remote_ptr.nodeID];
                break;
            }
            default:
                break;
        }
        /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
        //*(start) = std::chrono::steady_clock::now();
        // start = std::chrono::steady_clock::now();
        //  auto stop = std::chrono::high_resolution_clock::now();
        //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); std::printf("rdma read  send prepare for (%zu), time elapse : (%ld)\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
        ibv_qp* qp;
        if (qp_type == "default"){
//    assert(false);// Never comes to here
            // TODO: Need a mutex to protect the map access. (shared exclusive lock)
            qp = static_cast<ibv_qp*>(qp_data_default.at(remote_ptr.nodeID)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
                qp = static_cast<ibv_qp*>(qp_data_default.at(remote_ptr.nodeID)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);
        }else if (qp_type == "write_local_flush"){
//            assert(false);
//    ibv_qp* qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
//    if (qp == NULL) {
//      Remote_Query_Pair_Connection(qp_type);
//      qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
//    }
//    rc = ibv_post_send(qp, &sr, &bad_wr);
        }else if (qp_type == "write_local_compact"){
            assert(false);
//    ibv_qp* qp = static_cast<ibv_qp*>(qp_local_write_compact->Get());
//    if (qp == NULL) {
//      Remote_Query_Pair_Connection(qp_type);
//      qp = static_cast<ibv_qp*>(qp_local_write_compact->Get());
//    }
//    rc = ibv_post_send(qp, &sr, &bad_wr);
        } else {
            assert(false);
//    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
            rc = ibv_post_send(res->qp_map.at(remote_ptr.nodeID), &sr, &bad_wr);
//    l.unlock();
        }
        //    std::cout << " " << msg_size << "time elapse :" <<  << std::endl;
        //  start = std::chrono::high_resolution_clock::now();
        if (rc) {
            fprintf(stderr, "failed to post SR %s \n", qp_type.c_str());
            exit(1);

        } else {
            //      printf("qid: %s", q_id.c_str());
        }
        //  else
        //  {
//      fprintf(stdout, "RDMA Read Request was posted, OPCODE is %d\n", sr.opcode);
        //  }
        ibv_wc* wc;
        if (poll_num != 0) {
            wc = new ibv_wc[poll_num]();
            //  auto start = std::chrono::high_resolution_clock::now();
            //  while(std::chrono::high_resolution_clock::now
            //  ()-start < std::chrono::nanoseconds(msg_size+200000));
            rc = poll_completion(wc, poll_num, qp_type, true, remote_ptr.nodeID);
            if (rc != 0) {
                std::cout << "RDMA Read Failed" << std::endl;
                std::cout << "q id is" << qp_type << std::endl;
                fprintf(stdout, "QP number=0x%x\n", res->qp_map[remote_ptr.nodeID]->qp_num);
            }
            delete[] wc;
        }
//        ibv_wc wc;
//        printf("submit RDMA Read request global ptr is %p, local ptr is %p\n", remote_ptr, local_mr->addr);

        return rc;
}
// return 0 means success
    int RDMA_Manager::RDMA_Read(ibv_mr *remote_mr,  ibv_mr *local_mr, size_t msg_size,
                                size_t send_flag, int poll_num, uint16_t target_node_id,
                                std::string qp_type) {
//#ifdef GETANALYSIS
//  auto start = std::chrono::high_resolution_clock::now();
//#endif
//  assert(poll_num == 1);
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_READ;
  if (send_flag != 0) sr.send_flags = send_flag;
  //  printf("send flag to transform is %u", send_flag);
  //  printf("send flag is %u", sr.send_flags);
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
  //  auto stop = std::chrono::high_resolution_clock::now();
  //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); std::printf("rdma read  send prepare for (%zu), time elapse : (%ld)\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
  ibv_qp* qp;
  if (qp_type == "default"){
//    assert(false);// Never comes to here
    // TODO: Need a mutex to protect the map access. (shared exclusive lock)
    qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    assert(false);
//    ibv_qp* qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
//    if (qp == NULL) {
//      Remote_Query_Pair_Connection(qp_type);
//      qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
//    }
//    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_compact"){
    assert(false);
//    ibv_qp* qp = static_cast<ibv_qp*>(qp_local_write_compact->Get());
//    if (qp == NULL) {
//      Remote_Query_Pair_Connection(qp_type);
//      qp = static_cast<ibv_qp*>(qp_local_write_compact->Get());
//    }
//    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
//    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
      assert(false);
    rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
//    l.unlock();
  }
  //    std::cout << " " << msg_size << "time elapse :" <<  << std::endl;
  //  start = std::chrono::high_resolution_clock::now();

  if (rc) {
    fprintf(stderr, "failed to post SR %s \n", qp_type.c_str());
    exit(1);

  } else {
    //      printf("qid: %s", q_id.c_str());
  }
  //  else
  //  {
//      fprintf(stdout, "RDMA Read Request was posted, OPCODE is %d\n", sr.opcode);
  //  }

//        printf("rdma read for root_ptr\n");
  if (poll_num != 0) {
    ibv_wc* wc = new ibv_wc[poll_num]();
    //  auto start = std::chrono::high_resolution_clock::now();
    //  while(std::chrono::high_resolution_clock::now
    //  ()-start < std::chrono::nanoseconds(msg_size+200000));
    rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
    if (rc != 0) {
      std::cout << "RDMA Read Failed" << std::endl;
      std::cout << "q id is" << qp_type << std::endl;
      fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
    }
    delete[] wc;
  }
  ibv_wc wc;
//#ifdef GETANALYSIS
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
////    std::printf("Get from SSTables (not found) time elapse is %zu\n",  duration.count());
//  if (msg_size <= 8192){
//    RDMA_Manager::RDMAReadTimeElapseSum.fetch_add(duration.count());
//    RDMA_Manager::ReadCount.fetch_add(1);
//  }
//
//#endif
  return rc;
}

    int RDMA_Manager::RDMA_Write(GlobalAddress remote_ptr, ibv_mr *local_mr, size_t msg_size, size_t send_flag,
                                 int poll_num,
                                 Chunk_type pool_name, std::string qp_type) {
        //  auto start = std::chrono::high_resolution_clock::now();
        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr* bad_wr = NULL;
        int rc;
        /* prepare the scatter/gather entry */
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)local_mr->addr;
        sge.length = msg_size;
        sge.lkey = local_mr->lkey;
        /* prepare the send work request */
        memset(&sr, 0, sizeof(sr));
        sr.next = NULL;
        sr.wr_id = 0;
        sr.sg_list = &sge;
        sr.num_sge = 1;
        sr.opcode = IBV_WR_RDMA_WRITE;
        if (send_flag != 0) sr.send_flags = send_flag;
        switch (pool_name) {
            case Regular_Page:{
                sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_data[remote_ptr.nodeID]);
                sr.wr.rdma.rkey = rkey_map_data[remote_ptr.nodeID];
                break;
            }
            case LockTable:{
                sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_lock[remote_ptr.nodeID]);
                sr.wr.rdma.rkey = rkey_map_lock[remote_ptr.nodeID];
                break;
            }
            default:
                break;
        }

        /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
        //*(start) = std::chrono::steady_clock::now();
        // start = std::chrono::steady_clock::now();
        //  auto stop = std::chrono::high_resolution_clock::now();
        //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
        ibv_qp* qp;
        if (qp_type == "default"){
            //    assert(false);// Never comes to here
            qp = static_cast<ibv_qp*>(qp_data_default.at(remote_ptr.nodeID)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
                qp = static_cast<ibv_qp*>(qp_data_default.at(remote_ptr.nodeID)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);
        }else if (qp_type == "write_local_flush"){
            qp = static_cast<ibv_qp*>(qp_local_write_flush.at(remote_ptr.nodeID)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
                qp = static_cast<ibv_qp*>(qp_local_write_flush.at(remote_ptr.nodeID)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);

        }else if (qp_type == "write_local_compact"){
            qp = static_cast<ibv_qp*>(qp_local_write_compact.at(remote_ptr.nodeID)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
                qp = static_cast<ibv_qp*>(qp_local_write_compact.at(remote_ptr.nodeID)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);
        } else {
            assert(false);
            std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
            rc = ibv_post_send(res->qp_map.at(remote_ptr.nodeID), &sr, &bad_wr);
            l.unlock();
        }

        //  start = std::chrono::high_resolution_clock::now();
        if (rc) {
            fprintf(stderr, "failed to post SR, return is %d\n", rc);
            assert(false);
        }
        //  else
        //  {
//      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
        //  }
        if (poll_num != 0) {
            ibv_wc* wc = new ibv_wc[poll_num]();
            //  auto start = std::chrono::high_resolution_clock::now();
            //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
            // wait until the job complete.
            rc = poll_completion(wc, poll_num, qp_type, true, remote_ptr.nodeID);
            if (rc != 0) {
                std::cout << "RDMA Write Failed" << std::endl;
                std::cout << "q id is" << qp_type << std::endl;
                fprintf(stdout, "QP number=0x%x\n", res->qp_map[remote_ptr.nodeID]->qp_num);
            }
            delete[] wc;
        }
//        printf("submit RDMA write request global ptr is %p, local ptr is %p\n", remote_ptr, local_mr->addr);

        //  stop = std::chrono::high_resolution_clock::now();
        //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
        return rc;
    }
int RDMA_Manager::RDMA_Write(ibv_mr *remote_mr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
                             uint16_t target_node_id, std::string qp_type) {
  //  auto start = std::chrono::high_resolution_clock::now();
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_WRITE;
  if (send_flag != 0) sr.send_flags = send_flag;
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;
  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
  //  auto stop = std::chrono::high_resolution_clock::now();
  //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
  ibv_qp* qp;
  if (qp_type == "default"){
    //since we have make qp_data_default filled with empty queue pair during
    qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
      assert(false);
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
    l.unlock();
  }

  //  start = std::chrono::high_resolution_clock::now();
  if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
  //  else
  //  {
//      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0) {
    ibv_wc* wc = new ibv_wc[poll_num]();
    //  auto start = std::chrono::high_resolution_clock::now();
    //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
    // wait until the job complete.
    rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
    if (rc != 0) {
      std::cout << "RDMA Write Failed" << std::endl;
      std::cout << "q id is" << qp_type << std::endl;
      fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
    }
    delete[] wc;
  }
  //  stop = std::chrono::high_resolution_clock::now();
  //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
  return rc;
}
int RDMA_Manager::RDMA_Write(void* addr, uint32_t rkey, ibv_mr* local_mr,
                             size_t msg_size, std::string qp_type,
                             size_t send_flag, int poll_num, uint16_t target_node_id) {
    //  auto start = std::chrono::high_resolution_clock::now();
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr* bad_wr = NULL;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = msg_size;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_WRITE;
    if (send_flag != 0) sr.send_flags = send_flag;
    sr.wr.rdma.remote_addr = (uint64_t)addr;
    sr.wr.rdma.rkey = rkey;
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    //*(start) = std::chrono::steady_clock::now();
    // start = std::chrono::steady_clock::now();
    //  auto stop = std::chrono::high_resolution_clock::now();
    //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
    ibv_qp* qp;
    if (qp_type == "default"){
      //    assert(false);// Never comes to here
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);
    }else if (qp_type == "write_local_flush"){
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);

    }else if (qp_type == "write_local_compact"){
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);
    } else {
      std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
      rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
      l.unlock();
    }

    //  start = std::chrono::high_resolution_clock::now();
    if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
    //  else
    //  {
    //      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
    //  }
    assert(qp_type != std::string("main"));
    if (poll_num != 0) {
      ibv_wc* wc = new ibv_wc[poll_num]();
      //  auto start = std::chrono::high_resolution_clock::now();
      //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
      // wait until the job complete.
      rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
      if (rc != 0) {
        std::cout << "RDMA Write Failed" << std::endl;
        std::cout << "q id is" << qp_type << std::endl;
        fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
      }else{
        DEBUG_PRINT("RDMA write successfully\n");
      }
      delete[] wc;
    }
    //  stop = std::chrono::high_resolution_clock::now();
    //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
    return rc;
}

// the logic below is too complex, we should try to simply use spin mutex whenever submit to the shared queue pair.
//    int RDMA_Manager::RDMA_Write_xcompute(ibv_mr *local_mr, void *addr, uint32_t rkey, size_t msg_size,
//                                          uint16_t target_node_id,
//                                          int num_of_qp, bool is_inline) {
//        struct ibv_send_wr sr;
//        struct ibv_sge sge;
//        struct ibv_send_wr* bad_wr = NULL;
//        int rc = 0;
//        /* prepare the scatter/gather entry */
//        memset(&sge, 0, sizeof(sge));
//        sge.addr = (uintptr_t)local_mr->addr;
//        sge.length = msg_size;
//        sge.lkey = local_mr->lkey;
//        /* prepare the send work request */
//        memset(&sr, 0, sizeof(sr));
//        sr.next = NULL;
//        sr.wr_id = 0;
//        sr.sg_list = &sge;
//        sr.num_sge = 1;
//        sr.opcode = IBV_WR_RDMA_WRITE;
//        sr.wr.rdma.remote_addr = (uint64_t)addr;
//        sr.wr.rdma.rkey = rkey;
//        //TODO: maybe unsingaled wr does not perform well, when there is high concurrrency over the same queue pair, because
//        // we need a lock to protect the outstanding counter. We shall adjust SEND_OUTSTANDING_SIZE_XCOMPUTE to much larger than (2x) the
//        // parallelism of the compute node.
//        std::atomic<uint16_t >* os_start = &(*qp_xcompute_os_c.at(target_node_id))[2*num_of_qp];
//        std::atomic<uint16_t >* os_end = &(*qp_xcompute_os_c.at(target_node_id))[2*num_of_qp+1];
//        auto pending_num = os_start->fetch_add(1);
//        bool need_signal =  pending_num >= SEND_OUTSTANDING_SIZE_XCOMPUTE - 1;
//        if (!need_signal){
//            if (is_inline){
//                sr.send_flags = IBV_SEND_INLINE;
//            }
//            ibv_qp* qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[num_of_qp]);
//            rc = ibv_post_send(qp, &sr, &bad_wr);
//            //os_end is updated after os_start, it is possible to have os_end >= SEND_OUTSTANDING_SIZE_XCOMPUTE - 1
//            // we spin here until the end counter reset.
//            while(os_end->load() >= SEND_OUTSTANDING_SIZE_XCOMPUTE - 1){
//                asm volatile("pause\n": : :"memory");
//            }
//            os_end->fetch_add(1);
//            assert(os_end->load() <= SEND_OUTSTANDING_SIZE_XCOMPUTE - 1);
//        }
//        else{
//            SpinMutex* mtx = &(*qp_xcompute_mtx.at(target_node_id))[num_of_qp];
//            mtx->lock();
//            auto new_pending_num = os_start->fetch_add(1);
//            if (new_pending_num >= SEND_OUTSTANDING_SIZE_XCOMPUTE - 1){
//                if (is_inline){
//                    sr.send_flags = IBV_SEND_SIGNALED|IBV_SEND_INLINE;
//                }else{
//                    sr.send_flags = IBV_SEND_SIGNALED;
//                }
//                ibv_qp* qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[num_of_qp]);
//                //We need to make sure that the wr sequence in the queue is in the ticket order. otherwise, the pending wr
//                // can still exceed the upper bound.
//                while (os_end->load() < SEND_OUTSTANDING_SIZE_XCOMPUTE - 1){
//                    asm volatile("pause\n": : :"memory");
//                }
//                assert(os_end->load() == SEND_OUTSTANDING_SIZE_XCOMPUTE - 1);
//                rc = ibv_post_send(qp, &sr, &bad_wr);
//                ibv_wc wc[2] = {};
//                if (rc) {
//                    assert(false);
//                    fprintf(stderr, "failed to post SR, return is %d\n", rc);
//                }
//                if (poll_completion_xcompute(wc, 1, std::string("main"),
//                                             true, target_node_id, num_of_qp)){
//                    fprintf(stderr, "failed to poll send for remote memory register\n");
//                    assert(false);
//                }
//                os_start->store(0);
//                os_end->store(0);
//                mtx->unlock();
//            }else{
//                if (is_inline){
//                    sr.send_flags = IBV_SEND_INLINE;
//                }
//                ibv_qp* qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[num_of_qp]);
//                rc = ibv_post_send(qp, &sr, &bad_wr);
//                os_end->fetch_add(1);
//                assert(os_end->load() <= SEND_OUTSTANDING_SIZE_XCOMPUTE - 1);
//                mtx->unlock();
//            }
//        }
//        if (rc) {
//            assert(false);
//            fprintf(stderr, "failed to post SR, return is %d， errno is %d\n", rc, errno);
//        }
//        return rc;
//    }


    int RDMA_Manager::RDMA_Write_xcompute(ibv_mr *local_mr, void *addr, uint32_t rkey, size_t msg_size,
                                          uint16_t target_node_id,
                                          int num_of_qp, bool async) {
        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr* bad_wr = NULL;
        int rc = 0;

        /* prepare the send work request */
        memset(&sr, 0, sizeof(sr));
        sr.next = NULL;
        sr.wr_id = 0;
        sr.sg_list = &sge;
        sr.num_sge = 1;
        sr.opcode = IBV_WR_RDMA_WRITE;
        sr.wr.rdma.remote_addr = (uint64_t)addr;
        sr.wr.rdma.rkey = rkey;
        sr.send_flags = msg_size < MAX_INLINE_SIZE ? IBV_SEND_INLINE|IBV_SEND_FENCE : 0|IBV_SEND_FENCE;
        //TODO: maybe unsingaled wr does not perform well, when there is high concurrrency over the same queue pair, because
        // we need a lock to protect the outstanding counter. We shall adjust SEND_OUTSTANDING_SIZE_XCOMPUTE to much larger than (2x) the
        // parallelism of the compute node.
        std::atomic<uint16_t >* os_start = &(*qp_xcompute_os_c.at(target_node_id))[2*num_of_qp];
        SpinMutex* mtx = &(*qp_xcompute_mtx.at(target_node_id))[num_of_qp];
        mtx->lock();
        auto pending_num = os_start->fetch_add(1);
        bool need_signal =  pending_num >= SEND_OUTSTANDING_SIZE_XCOMPUTE - 1;

//        bool need_signal = true; // Let's first test it with all signalled RDMA. Delete it after the debug
        if (!async){
            need_signal = true;
        }
        if (!need_signal){
            ibv_mr* async_buf = (*qp_xcompute_asyncT.at(target_node_id))[num_of_qp].mrs[pending_num];
            assert(local_mr->length >= msg_size);
            assert(async_buf->length >= msg_size);
            memcpy(async_buf->addr, local_mr->addr, msg_size);
            /* prepare the scatter/gather entry */
            memset(&sge, 0, sizeof(sge));
            sge.addr = (uintptr_t)async_buf->addr;
            sge.length = msg_size;
            sge.lkey = async_buf->lkey;
            ibv_qp* qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[num_of_qp]);
            rc = ibv_post_send(qp, &sr, &bad_wr);
        }
        else{
            /* prepare the scatter/gather entry */
            memset(&sge, 0, sizeof(sge));
            sge.addr = (uintptr_t)local_mr->addr;
            sge.length = msg_size;
            sge.lkey = local_mr->lkey;
            sr.send_flags = sr.send_flags|IBV_SEND_SIGNALED;
            ibv_qp* qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[num_of_qp]);
            printf("RDMA write to be posted with signal, message size is %zu, thread id is %d\n", msg_size, thread_id);
            fflush(stdout);
            rc = ibv_post_send(qp, &sr, &bad_wr);
            
            ibv_wc wc[2] = {};
            if (rc) {
                assert(false);
                fprintf(stderr, "failed to post SR, return is %d\n", rc);
            }
            if (poll_completion_xcompute(wc, 1, std::string("main"),
                                         true, target_node_id, num_of_qp)){
                fprintf(stderr, "failed to poll send for remote memory register\n");
                assert(false);
            }
            os_start->store(0);
        }
        mtx->unlock();

        if (rc) {
            assert(false);
            fprintf(stderr, "failed to post SR, return is %d， errno is %d\n", rc, errno);
        }
        return rc;
    }
    int RDMA_Manager::post_send_xcompute(ibv_mr *mr, uint16_t target_node_id, int num_of_qp, size_t msg_size) {
      struct ibv_send_wr sr;
      struct ibv_sge sge;
      struct ibv_send_wr* bad_wr = NULL;
      int rc = 0;

      /* prepare the send work request */
      memset(&sr, 0, sizeof(sr));
      sr.next = NULL;
      sr.wr_id = 0;
      sr.sg_list = &sge;
      sr.num_sge = 1;
      sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
      std::atomic<uint16_t >* os_start = &(*qp_xcompute_os_c.at(target_node_id))[2*num_of_qp];
      SpinMutex* mtx = &(*qp_xcompute_mtx.at(target_node_id))[num_of_qp];
      mtx->lock();
      auto pending_num = os_start->fetch_add(1);
      bool need_signal =  pending_num >= SEND_OUTSTANDING_SIZE_XCOMPUTE - 1;
//        bool need_signal = true; // Let's first test it with all signalled RDMA.
      if (!need_signal){
          ibv_mr* async_buf = (*qp_xcompute_asyncT.at(target_node_id))[num_of_qp].mrs[pending_num];
          assert(mr->length >= msg_size);
          assert(async_buf->length >= msg_size);
          memcpy(async_buf->addr, mr->addr, msg_size);
          memset(&sge, 0, sizeof(sge));
          sge.addr = (uintptr_t)async_buf->addr;
          assert(mr->length != 0);
//    printf("The length of the mr is %lu", mr->length);
          sge.length = msg_size;
          sge.lkey = async_buf->lkey;

          sr.send_flags = IBV_SEND_INLINE;
          ibv_qp* qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[num_of_qp]);
          rc = ibv_post_send(qp, &sr, &bad_wr);
          if (rc) {
            assert(false);
            fprintf(stderr, "failed to post SR, return is %d\n", rc);
          }
      }
      else{
          memset(&sge, 0, sizeof(sge));
          sge.addr = (uintptr_t)mr->addr;
          assert(mr->length != 0);
//    printf("The length of the mr is %lu", mr->length);
          sge.length = msg_size;
          sge.lkey = mr->lkey;

          sr.send_flags = IBV_SEND_SIGNALED|IBV_SEND_INLINE;
          ibv_qp* qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[num_of_qp]);
          printf("RDMA send to be posted with signal, message size is %zu, thread id is %d \n", msg_size, thread_id);
          fflush(stdout);
          rc = ibv_post_send(qp, &sr, &bad_wr);
          
          ibv_wc wc[2] = {};
          if (rc) {
              assert(false);
              fprintf(stderr, "failed to post SR, return is %d\n", rc);
          }
          if (poll_completion_xcompute(wc, 1, std::string("main"),
                                       true, target_node_id, num_of_qp)){
              fprintf(stderr, "failed to poll send for remote memory register\n");
              assert(false);
          }
          os_start->store(0);


      }
      mtx->unlock();

//    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
      /* post the Send Request to the RQ */
//    l.unlock();
      if (rc) {
          assert(false);
          fprintf(stderr, "failed to post SR, return is %d\n", rc);
      }
      return rc;
  }
int RDMA_Manager::RDMA_Write_Imme(void* addr, uint32_t rkey, ibv_mr* local_mr,
                                  size_t msg_size, std::string qp_type,
                                  size_t send_flag, int poll_num,
                                  unsigned int imme, uint16_t target_node_id) {
  //  auto start = std::chrono::high_resolution_clock::now();
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.imm_data = imme;
  sr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  if (send_flag != 0) sr.send_flags = send_flag;
  sr.wr.rdma.remote_addr = (uint64_t)addr;
  sr.wr.rdma.rkey = rkey;
  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
  //  auto stop = std::chrono::high_resolution_clock::now();
  //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
  ibv_qp* qp;
  if (qp_type == "default"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
        assert(false);
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
      assert(false);
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    qp = res->qp_map.at(target_node_id);
    rc = ibv_post_send(qp, &sr, &bad_wr);
    l.unlock();
  }
  assert(rc == 0);
  //  start = std::chrono::high_resolution_clock::now();
  if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
  //  else
  //  {
  //      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0) {
    ibv_wc* wc = new ibv_wc[poll_num]();
    //  auto start = std::chrono::high_resolution_clock::now();
    //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
    // wait until the job complete.
    rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
    if (rc != 0) {
      std::cout << "RDMA Write Failed" << std::endl;
      std::cout << "q id is" << qp_type << std::endl;
      fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
    }else{
      DEBUG_PRINT("RDMA write successfully\n");
    }
    delete[] wc;
  }
  //  stop = std::chrono::high_resolution_clock::now();
  //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
  return rc;
}
int RDMA_Manager::RDMA_CAS(GlobalAddress remote_ptr, ibv_mr *local_mr, uint64_t compare, uint64_t swap, size_t send_flag,
                       int poll_num,
                       Chunk_type pool_name, std::string qp_type) {
    //  auto start = std::chrono::high_resolution_clock::now();
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr* bad_wr = NULL;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = 8;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    if (send_flag != 0) sr.send_flags = send_flag;
    switch (pool_name) {
        case Regular_Page:{
            sr.wr.atomic.rkey = rkey_map_data[remote_ptr.nodeID];
            sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_data[remote_ptr.nodeID]);
            sr.wr.atomic.compare_add = compare; /* expected value in remote address */
            sr.wr.atomic.swap        = swap;
            break;
        }
        case LockTable:{
            sr.wr.atomic.rkey = rkey_map_lock[remote_ptr.nodeID];
            sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_lock[remote_ptr.nodeID]);
            sr.wr.atomic.compare_add = compare; /* expected value in remote address */
            sr.wr.atomic.swap        = swap;
            break;
        }
        default:
            break;
    }
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    //*(start) = std::chrono::steady_clock::now();
    // start = std::chrono::steady_clock::now();
    //  auto stop = std::chrono::high_resolution_clock::now();
    //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();

    ibv_qp* qp;
    if (qp_type == "default"){
        //    assert(false);// Never comes to here
        qp = static_cast<ibv_qp*>(qp_data_default.at(remote_ptr.nodeID)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
            qp = static_cast<ibv_qp*>(qp_data_default.at(remote_ptr.nodeID)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);
    }else if (qp_type == "write_local_flush"){
        assert(false);
        qp = static_cast<ibv_qp*>(qp_local_write_flush.at(remote_ptr.nodeID)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
            qp = static_cast<ibv_qp*>(qp_local_write_flush.at(remote_ptr.nodeID)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);

    }else if (qp_type == "write_local_compact"){
        assert(false);
        qp = static_cast<ibv_qp*>(qp_local_write_compact.at(remote_ptr.nodeID)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
            qp = static_cast<ibv_qp*>(qp_local_write_compact.at(remote_ptr.nodeID)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);
    } else {
        assert(false);
        std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
        qp = res->qp_map.at(remote_ptr.nodeID);
        rc = ibv_post_send(qp, &sr, &bad_wr);
        l.unlock();
    }

    //  start = std::chrono::high_resolution_clock::now();
    if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
    //  else
    //  {
//      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
    //  }
    if (poll_num != 0) {
        ibv_wc* wc = new ibv_wc[poll_num]();
        //  auto start = std::chrono::high_resolution_clock::now();
        //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
        // wait until the job complete.
        rc = poll_completion(wc, poll_num, qp_type, true, remote_ptr.nodeID);
        if (rc != 0) {
            std::cout << "RDMA CAS Failed" << std::endl;
            std::cout << "remote node id is" << remote_ptr.nodeID << std::endl;
            fprintf(stdout, "QP number=0x%x\n", res->qp_map[remote_ptr.nodeID]->qp_num);
            assert(false);
        }
        delete[] wc;
    }
    //  stop = std::chrono::high_resolution_clock::now();
    //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
    return rc;
}
int RDMA_Manager::RDMA_FAA(GlobalAddress remote_ptr, ibv_mr *local_mr, uint64_t add, size_t send_flag, int poll_num,
                           Chunk_type pool_name, std::string qp_type) {
//    printf("RDMA faa, TARGET page is %p, add is %lu\n", remote_ptr, add);
//  auto start = std::chrono::high_resolution_clock::now();
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr* bad_wr = NULL;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = 8;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    if (send_flag != 0) sr.send_flags = send_flag;
    switch (pool_name) {
        case Regular_Page:{
            sr.wr.atomic.rkey = rkey_map_data[remote_ptr.nodeID];
            sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_data[remote_ptr.nodeID]);
            sr.wr.atomic.compare_add = add; /* expected value in remote address */
            break;
        }
        case LockTable:{
            sr.wr.atomic.rkey = rkey_map_lock[remote_ptr.nodeID];
            sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_lock[remote_ptr.nodeID]);
            sr.wr.atomic.compare_add = add; /* expected value in remote address */
            break;
        }
        default:
            break;
    }
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    //*(start) = std::chrono::steady_clock::now();
    // start = std::chrono::steady_clock::now();
    //  auto stop = std::chrono::high_resolution_clock::now();
    //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();

    ibv_qp* qp;
    if (qp_type == "default"){
        //    assert(false);// Never comes to here
        qp = static_cast<ibv_qp*>(qp_data_default.at(remote_ptr.nodeID)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
            qp = static_cast<ibv_qp*>(qp_data_default.at(remote_ptr.nodeID)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);
    }else if (qp_type == "write_local_flush"){
//        assert(false);
        qp = static_cast<ibv_qp*>(qp_local_write_flush.at(remote_ptr.nodeID)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
            qp = static_cast<ibv_qp*>(qp_local_write_flush.at(remote_ptr.nodeID)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);

    }else if (qp_type == "write_local_compact"){
        assert(false);
        qp = static_cast<ibv_qp*>(qp_local_write_compact.at(remote_ptr.nodeID)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,remote_ptr.nodeID);
            qp = static_cast<ibv_qp*>(qp_local_write_compact.at(remote_ptr.nodeID)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);
    } else {
        assert(false);
        std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
        qp = res->qp_map.at(remote_ptr.nodeID);
        rc = ibv_post_send(qp, &sr, &bad_wr);
        l.unlock();
    }

    //  start = std::chrono::high_resolution_clock::now();
    if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
    //  else
    //  {
//      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
    //  }
    if (poll_num != 0) {
        ibv_wc* wc = new ibv_wc[poll_num]();
        //  auto start = std::chrono::high_resolution_clock::now();
        //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
        // wait until the job complete.
        rc = poll_completion(wc, poll_num, qp_type, true, remote_ptr.nodeID);
        if (rc != 0) {
            std::cout << "RDMA CAS Failed" << std::endl;
            std::cout << "remote node id is" << remote_ptr.nodeID << std::endl;
            fprintf(stdout, "QP number=0x%x\n", res->qp_map[remote_ptr.nodeID]->qp_num);
            assert(false);
        }
        delete[] wc;
    }
    //  stop = std::chrono::high_resolution_clock::now();
    //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
    return rc;
}

//No need to add fense for this RDMA wr.
void RDMA_Manager::Prepare_WR_CAS(ibv_send_wr &sr, ibv_sge &sge, GlobalAddress remote_ptr, ibv_mr *local_mr,
                                  uint64_t compare,
                                  uint64_t swap, size_t send_flag, Chunk_type pool_name) {
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = 8;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    if (send_flag != 0) sr.send_flags = send_flag;
    switch (pool_name) {
        case Regular_Page:{
            sr.wr.atomic.rkey = rkey_map_data[remote_ptr.nodeID];
            sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_data[remote_ptr.nodeID]);
            sr.wr.atomic.compare_add = compare; /* expected value in remote address */
            sr.wr.atomic.swap        = swap;
            break;
        }
        case LockTable:{
            sr.wr.atomic.rkey = rkey_map_lock[remote_ptr.nodeID];
            sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_lock[remote_ptr.nodeID]);
            sr.wr.atomic.compare_add = compare; /* expected value in remote address */
            sr.wr.atomic.swap        = swap;
            break;
        }
        default:
            break;
    }
}
void
RDMA_Manager::Prepare_WR_FAA(ibv_send_wr &sr, ibv_sge &sge, GlobalAddress remote_ptr, ibv_mr *local_mr, uint64_t add,
                             size_t send_flag, Chunk_type pool_name) {
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = 8;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    if (send_flag != 0) sr.send_flags = send_flag;
    switch (pool_name) {
        case Regular_Page:{
            sr.wr.atomic.rkey = rkey_map_data[remote_ptr.nodeID];
            sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_data[remote_ptr.nodeID]);
            sr.wr.atomic.compare_add = add; /* expected value in remote address */
//            sr.wr.atomic.swap        = swap;
            break;
        }
        case LockTable:{
            sr.wr.atomic.rkey = rkey_map_lock[remote_ptr.nodeID];
            sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_lock[remote_ptr.nodeID]);
            sr.wr.atomic.compare_add = add; /* expected value in remote address */
//            sr.wr.atomic.swap        = swap;
            break;
        }
        default:
            break;
    }
}
void RDMA_Manager::Prepare_WR_Read(ibv_send_wr &sr, ibv_sge &sge, GlobalAddress remote_ptr, ibv_mr *local_mr,
                                   size_t msg_size,
                                   size_t send_flag, Chunk_type pool_name) {
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = msg_size;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_READ;
    if (send_flag != 0) sr.send_flags = send_flag;
    switch (pool_name) {
        case Regular_Page:{
            sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_data[remote_ptr.nodeID]);
            sr.wr.rdma.rkey = rkey_map_data[remote_ptr.nodeID];
            break;
        }
        case LockTable:{
            sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_lock[remote_ptr.nodeID]);
            sr.wr.rdma.rkey = rkey_map_lock[remote_ptr.nodeID];
            break;
        }
        default:
            break;
    }
}
void RDMA_Manager::Prepare_WR_Write(ibv_send_wr &sr, ibv_sge &sge, GlobalAddress remote_ptr, ibv_mr *local_mr,
                                    size_t msg_size,
                                    size_t send_flag, Chunk_type pool_name) {
    struct ibv_send_wr* bad_wr = NULL;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = msg_size;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_WRITE;
    if (send_flag != 0) sr.send_flags = send_flag;
    switch (pool_name) {
        case Regular_Page:{
            sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_data[remote_ptr.nodeID]);
            sr.wr.rdma.rkey = rkey_map_data[remote_ptr.nodeID];
            break;
        }
        case LockTable:{
            sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_ptr.offset + base_addr_map_lock[remote_ptr.nodeID]);
            sr.wr.rdma.rkey = rkey_map_lock[remote_ptr.nodeID];
            break;
        }
        default:
            break;
    }

}

    int
    RDMA_Manager::Batch_Submit_WRs(ibv_send_wr *sr, int poll_num, uint16_t target_node_id, std::string qp_type) {
        int rc;
        struct ibv_send_wr* bad_wr = NULL;
        ibv_qp* qp;
//#ifdef PROCESSANALYSIS
//        auto start = std::chrono::high_resolution_clock::now();
//#endif
        if (qp_type == "default"){
            //    assert(false);// Never comes to here
            qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, sr, &bad_wr);
        }else if (qp_type == "write_local_flush"){
            qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, sr, &bad_wr);

        }else if (qp_type == "write_local_compact"){
            assert(false);
            qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, sr, &bad_wr);
        } else {
            assert(false);
            std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
            qp = res->qp_map.at(target_node_id);
            rc = ibv_post_send(qp, sr, &bad_wr);
            l.unlock();
        }
//#ifdef PROCESSANALYSIS
//        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
//            auto stop = std::chrono::high_resolution_clock::now();
//            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
////#ifndef NDEBUG
//            printf("find the QP uses (%ld) ns\n", duration.count());
////            TimePrintCounter[RDMA_Manager::thread_id] = 0;
//        }else{
////            TimePrintCounter[RDMA_Manager::thread_id]++;
//        }
////#endif
//#endif
//#ifdef PROCESSANALYSIS
//        start = std::chrono::high_resolution_clock::now();
//#endif
//        DEBUG_PRINT("Batch submit polling\n");
        if (rc) {
            assert(false);
            fprintf(stderr, "failed to post SR, return is %d\n", rc);
            fprintf(stdout, "failed to post SR, return is %d\n", rc);
            fflush(stdout);
            exit(0);
        }

        if (poll_num != 0) {
            ibv_wc* wc = new ibv_wc[poll_num]();
            //  auto start = std::chrono::high_resolution_clock::now();
            //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
            // wait until the job complete.
            rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
            if (rc != 0) {
                std::cout << "RDMA CAS Failed" << std::endl;
                std::cout << "remote node id is" << target_node_id << std::endl;
                fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);

            }
            delete[] wc;
        }
//#ifdef PROCESSANALYSIS
//        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
//            auto stop = std::chrono::high_resolution_clock::now();
//            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//            printf("polling the QP uses (%ld) ns\n", duration.count());
////            TimePrintCounter[RDMA_Manager::thread_id] = 0;
//        }else{
////            TimePrintCounter[RDMA_Manager::thread_id]++;
//        }
//#endif
       return rc;
    }

int RDMA_Manager::RDMA_CAS(ibv_mr *remote_mr, ibv_mr *local_mr, uint64_t compare, uint64_t swap, size_t send_flag,
                           int poll_num,
                           uint16_t target_node_id, std::string qp_type)
{
    //  auto start = std::chrono::high_resolution_clock::now();
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr* bad_wr = NULL;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = 8;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    if (send_flag != 0) sr.send_flags = send_flag;
//    sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
//    sr.wr.rdma.rkey = remote_mr->rkey;
    sr.wr.atomic.rkey = remote_mr->rkey;
    sr.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
    sr.wr.atomic.compare_add = compare; /* expected value in remote address */
    sr.wr.atomic.swap        = swap;
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    //*(start) = std::chrono::steady_clock::now();
    // start = std::chrono::steady_clock::now();
    //  auto stop = std::chrono::high_resolution_clock::now();
    //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();

    ibv_qp* qp;
    if (qp_type == "default"){
        //    assert(false);// Never comes to here
        qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,target_node_id);
            qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);
    }else if (qp_type == "write_local_flush"){
//        assert(false);
        qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,target_node_id);
            qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);

    }else if (qp_type == "write_local_compact"){
        assert(false);
        qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
        if (qp == NULL) {
            Remote_Query_Pair_Connection(qp_type,target_node_id);
            qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
        }
        rc = ibv_post_send(qp, &sr, &bad_wr);
    } else {
        assert(false);
        std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
        qp = res->qp_map.at(target_node_id);
        rc = ibv_post_send(qp, &sr, &bad_wr);
        l.unlock();
    }

    //  start = std::chrono::high_resolution_clock::now();
    if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
    //  else
    //  {
//      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
    //  }
    if (poll_num != 0) {
        ibv_wc* wc = new ibv_wc[poll_num]();
        //  auto start = std::chrono::high_resolution_clock::now();
        //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
        // wait until the job complete.
        rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
        if (rc != 0) {
            std::cout << "RDMA CAS Failed" << std::endl;
            std::cout << "remote node id is" << target_node_id << std::endl;
            fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
            assert(false);
        }
        delete[] wc;
    }
    //  stop = std::chrono::high_resolution_clock::now();
    //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
    return rc;
}
    int
    RDMA_Manager::RDMA_FAA(ibv_mr *remote_mr, ibv_mr *local_mr, uint64_t add, uint16_t target_node_id, size_t send_flag,
                           int poll_num, std::string qp_type) {
//    printf("RDMA faa, TARGET page is %p, add is %lu\n", remote_ptr, add);
//  auto start = std::chrono::high_resolution_clock::now();
        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr* bad_wr = NULL;
        int rc;
        /* prepare the scatter/gather entry */
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)local_mr->addr;
        sge.length = 8;
        sge.lkey = local_mr->lkey;
        /* prepare the send work request */
        memset(&sr, 0, sizeof(sr));
        sr.next = NULL;
        sr.wr_id = 0;
        sr.sg_list = &sge;
        sr.num_sge = 1;
        sr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
        if (send_flag != 0) sr.send_flags = send_flag;

        sr.wr.atomic.rkey = remote_mr->rkey;
        sr.wr.atomic.remote_addr = (uint64_t )remote_mr->addr;
        sr.wr.atomic.compare_add = add; /* expected value in remote address */
//#ifndef NDEBUG
//        ibv_wc wc1[2];
//        if (cq_data_default.at(target_node_id)->Get() != nullptr) {
//            assert(try_poll_completions(wc1, 1, qp_type, true, 1) == 0);
//        }
//#endif
        ibv_qp* qp;
        if (qp_type == "default"){
            //    assert(false);// Never comes to here
            qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);
        }else if (qp_type == "write_local_flush"){
//            assert(false);
            qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);

        }else if (qp_type == "write_local_compact"){
            assert(false);
            qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);
        } else {
            assert(false);
            std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
            qp = res->qp_map.at(target_node_id);
            rc = ibv_post_send(qp, &sr, &bad_wr);
            l.unlock();
        }

        //  start = std::chrono::high_resolution_clock::now();
        if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
        //  else
        //  {
//      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
        //  }
        if (poll_num != 0) {
            ibv_wc* wc = new ibv_wc[poll_num]();
            //  auto start = std::chrono::high_resolution_clock::now();
            //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
            // wait until the job complete.
            rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
            if (rc != 0) {
                std::cout << "RDMA CAS Failed" << std::endl;
                std::cout << "remote node id is" << target_node_id << std::endl;
                fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
                assert(false);
            }
            delete[] wc;
        }
        //  stop = std::chrono::high_resolution_clock::now();
        //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
        return rc;
    }
    uint64_t RDMA_Manager::renew_swap_by_received_state_readlock(uint64_t &received_state) {
        uint64_t returned_state = 0;
        if(received_state == 0){
            // The first time try to lock or last time lock failed because of an unlock.
            // and fill the bitmap for this node id.
//            returned_state = 1ull << 48;
            returned_state = returned_state | (1ull << RDMA_Manager::node_id/2);

        }else if(received_state >= 1ull << 56){
//            assert(false);
            // THere is a write lock on, we can only keep trying to read lock it.
//            returned_state = 1ull << 48;
            returned_state = returned_state | (1ull << RDMA_Manager::node_id/2);
        }else{
//            assert(false);
            // There has already been a read lock holder.
//            uint64_t current_Rlock_holder_num = (received_state >> 48) % 256;
//            assert(current_Rlock_holder_num <= 255);
//            current_Rlock_holder_num++;
            //
//            returned_state = returned_state | (current_Rlock_holder_num << 48);
//            returned_state = returned_state | (received_state << 16 >>16);

//TOTHINK: It is possible that the unlocking for an cache entry being evicted,
// interleave with the an lock acquire for the same page later
            assert(received_state & (1ull << RDMA_Manager::node_id/2 == 0));

            returned_state = returned_state | (1ull << RDMA_Manager::node_id/2);
        }
        assert(returned_state >> 56 ==0);
        return returned_state;
    }
    uint64_t RDMA_Manager::renew_swap_by_received_state_readunlock(uint64_t &received_state) {
        // Note current implementation has not consider the starvation yet.
        uint64_t returned_state = 0;
        if(received_state == ((1ull << RDMA_Manager::node_id/2))){

//            returned_state = 1ull << 48;
            returned_state = received_state & ~(1ull << RDMA_Manager::node_id/2);

        }else if(received_state >= 1ull << 56){
            assert(false);
            printf("trying read unlock during the write lock");
            exit(0);
        }else{
            // There has already been another read lock holder.
//            uint64_t current_Rlock_holder_num = (received_state >> 48) % 256;
//            assert(current_Rlock_holder_num > 0);
//            current_Rlock_holder_num--;
//            //decrease the number of lock holder
//            returned_state = returned_state | (current_Rlock_holder_num << 48);
//            returned_state = returned_state | (received_state << 16 >>16);
            assert(received_state & (1ull << RDMA_Manager::node_id/2 == 1));
            // clear the node id bit.
            returned_state = received_state & ~(1ull << RDMA_Manager::node_id/2);
        }
        assert(returned_state >> 56 ==0);
        return returned_state;
    }
    uint64_t RDMA_Manager::renew_swap_by_received_state_readupgrade(uint64_t &received_state) {
        return 0;
    }
    void RDMA_Manager::global_unlock_addr(GlobalAddress remote_lock_add, CoroContext *cxt, int coro_id, bool async) {
        auto cas_buf = Get_local_CAS_mr();
//    std::cout << "unlock " << lock_addr << std::endl;
        //TODO: Make the unlock based on RDMA CAS so that it is gurantee to be consistent with RDMA FAA,
        // otherwise (RDMA write to do the unlock) the lock word has to be set at the end of the page to guarantee the
        // consistency.
        uint64_t swap = 0;
        uint64_t compare = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
        if (async) {
            *(uint64_t*)cas_buf->addr = 0;

            // important!!! we should never use async if we have both read lock and write lock.
            // send flag 0 means there is no flag
            RDMA_CAS(remote_lock_add, cas_buf, compare, swap, 0, 0, Regular_Page);
            printf("Global page unlock page_addr %p, async %d\n", remote_lock_add, async);
        } else {

        retry:
            *(uint64_t*)cas_buf->addr = 0;

//      std::cout << "Unlock the remote lock" << lock_addr << std::endl;
            RDMA_CAS(remote_lock_add, cas_buf, compare, swap, IBV_SEND_SIGNALED, 1, Regular_Page);
            if(*(uint64_t*)cas_buf->addr != compare){
                // THere is concurrent read lock trying on this lock, but it will released later. If if keep failing
                // then we can add a stavation bit.
                assert((*(uint64_t*)cas_buf->addr)<<56 == compare << 56);
                goto retry;
            }
        }
//        printf("Global page unlock page_addr %p, async %d\n", remote_lock_add, async);

//        releases_local_optimistic_lock(lock_addr);
    }

    bool RDMA_Manager::global_Rlock_and_read_page_with_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
                                                               GlobalAddress lock_addr, ibv_mr* cas_buffer, int r_times, CoroContext *cxt,
                                                               int coro_id) {
        uint64_t add = (1ull << (RDMA_Manager::node_id/2 +1));
        uint64_t substract = (~add) + 1;
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        *(uint64_t *)cas_buffer->addr = 0;
        uint8_t target_compute_node_id = 0;
        uint64_t last_atomic_return = 0;
        uint8_t starvation_level = 0;
        uint64_t page_version = 0;
#ifndef NDEBUG
        auto page  = (InternalPage<uint64_t>*)(page_buffer->addr);
#endif
#ifdef INVALIDATION_STATISTICS
        bool invalidation_counted = false;
#endif
//        printf("READ page %p from remote memory to local mr %p 1, thread_id is %d\n", page_addr, page_buffer->addr, thread_id);

//        printf("global read lock at %p \n", page_addr);
        retry:
        if (r_times >0 && retry_cnt >= r_times){
            return false;
        }
//        retry_cnt++;
        if (retry_cnt++ % INVALIDATION_INTERVAL ==  1) {
//            assert(compare%2 == 0);
            if(retry_cnt < 20){
//                port::AsmVolatilePause();
                //do nothing
            }else if (retry_cnt <40){
                starvation_level = 1;

            }else if (retry_cnt <80){
                starvation_level = 2;
            }
            else if (retry_cnt <160){
                starvation_level = 3;
            }else if (retry_cnt <200){
                starvation_level = 4;
            } else if (retry_cnt <1000){
                starvation_level = 5;
            } else{
                starvation_level = 255 > 5+ retry_cnt/1000? 5+ retry_cnt/1000: 255;
            }
//            assert(target_compute_node_id != (RDMA_Manager::node_id));
            if (target_compute_node_id != (RDMA_Manager::node_id) && target_compute_node_id < compute_nodes.size()*2){
#ifdef INVALIDATION_STATISTICS
                if(!invalidation_counted){
                    invalidation_counted = true;
                    cache_invalidation[RDMA_Manager::thread_id]++;
                }
#endif
//#ifdef RDMAPROCESSANALYSIS
//#endif

                //TODO: change the function below to Reader_Invalidate_Modified_RPC.
                auto ret = Reader_Invalidate_Modified_RPC(page_addr,
                                               page_buffer, target_compute_node_id, starvation_level,
                                               retry_cnt);
                if (ret == processed){
                    return true;
                }
                assert(ret == dropped);

            }else{
                // THis could happen when one compute node (reader) is fowarded and suddenly release its shared latch due to the
                // latch upgrade. The fetch and substract can make the latch word in a faulty intermidiate state. ex.g. (0x2fffffffffffffe).
                // If this node number is happened to be 0x2, then this code path could happen, we can just ignore this case.

                //TODO: what else problem can such an intermidiate state cause?
                printf("Node id %u Write invalidation target compute node is itself1 or is out of range (temporal faulty latch state), page_addr is %p\n", node_id, page_addr);
//                assert(false);
            }

        }
        //TODO: For high starvation level the invalidation interval shall be shorter.
        if (retry_cnt % INVALIDATION_INTERVAL >  4 || retry_cnt % INVALIDATION_INTERVAL < 1){
            if (starvation_level <= 2){
                spin_wait_us(8);
            }else if (starvation_level <= 4){
                //No sleep if the starvation level is high.
                spin_wait_us(4);
            }else if (starvation_level <= 6){
                spin_wait_us(2);
            }else{

            }
        }
//        if (retry_cnt > 210) {
//            std::cout << "Deadlock " << lock_addr << std::endl;
//
//            std::cout << GetMemoryNodeNum() << ", "
//                      << " locked by node  " << (conflict_tag) << std::endl;
//            assert(false);
//            exit(0);
//        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        //Only the second RDMA issue a completion,
        // TODO: We may add a fence for the first request to avoid corruption of the async unlock.
        // The async write back and unlock can result in corrupted data during the buffer recycle.
        Prepare_WR_FAA(sr[0], sge[0], lock_addr, cas_buffer, add, 0, Regular_Page);
        Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Regular_Page);
        sr[0].next = &sr[1];
        *(uint64_t *)cas_buffer->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        Batch_Submit_WRs(sr, 1, page_addr.nodeID);
        uint64_t return_value = *(uint64_t*) cas_buffer->addr;
        //Note that the read latch can not be global hand-overed, because read latch FAA can overflow the read bitmap.
        if ( (return_value >> 56) >= 100  ){

            if (last_atomic_return >> 56 != return_value >> 56){
                // someone else have acquire the latch, immediately issue a invalidation in the next loop.
                //TODO: change the code below.
                retry_cnt = retry_cnt/INVALIDATION_INTERVAL;
            }
            last_atomic_return = return_value;
//            assert(false);
//            assert((return_value & (1ull << (RDMA_Manager::node_id/2 + 1)))== 0);
//            Prepare_WR_FAA(sr[0], sge[0], lock_addr, cas_buffer, -add, 0, Internal_and_Leaf);

            RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 1, Regular_Page);
            //Get the latest page version and make an invalidation message based on current version. (exact once)
//            page_version = ((DataPage*) page_buffer->addr)->hdr.p_version;
            target_compute_node_id = ((return_value >> 56) - 100)*2;
//#ifndef NDEBUG
//            assert((*((uint64_t *)page_buffer->addr+1) << 8) > 0);
//#endif
            goto retry;
        }
        return true;
    }
#if ACCESS_MODE == 0
//TODO: we need to fall back the RDMA SX latch to the one introduced in Tobias paper, because the current implementation
// result in too many FAA on the Read bitmap and could result in bitmap overflow.
    bool RDMA_Manager::global_Rlock_and_read_page_without_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
                                                                  GlobalAddress lock_addr, ibv_mr* cas_buffer, int r_time, CoroContext *cxt,
                                                                  int coro_id) {
        uint64_t add = 1ull;
        uint64_t substract = (~add) + 1;
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        *(uint64_t *)cas_buffer->addr = 0;
        uint8_t target_compute_node_id = 0;
    retry:
        if (r_time >0 && retry_cnt >= r_time){
            return false;
        }
        retry_cnt++;
        if (retry_cnt % INVALIDATION_INTERVAL ==  1 ) {
//            assert(compare%2 == 0);
//            if (retry_cnt < 10) {
////                port::AsmVolatilePause();
//                //do nothing
//            } else if (retry_cnt < 20) {
//                spin_wait_us(2);
//
//            } else if (retry_cnt < 40) {
//                spin_wait_us(8);
//            } else if (retry_cnt < 160) {
//                spin_wait_us(32);
//            } else if (retry_cnt < 200) {
//                usleep(256);
//            } else if (retry_cnt < 1000) {
//                usleep(1024);
//            } else {
//                sleep(1);
//            }
        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        //Only the second RDMA issue a completion,
        // TODO: We may add a fence for the first request to avoid corruption of the async unlock.
        // The async write back and unlock can result in corrupted data during the buffer recycle.
        Prepare_WR_FAA(sr[0], sge[0], lock_addr, cas_buffer, add, 0, Regular_Page);
        Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Regular_Page);
        sr[0].next = &sr[1];
        *(uint64_t *)cas_buffer->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
#ifdef GETANALYSIS
        auto statistic_start = std::chrono::high_resolution_clock::now();
#endif
        Batch_Submit_WRs(sr, 1, page_addr.nodeID);
#ifdef GETANALYSIS
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - statistic_start);
        PrereadTotal.fetch_add(duration.count());
        Prereadcounter.fetch_add(1);
#endif
        uint64_t return_value = *(uint64_t*) cas_buffer->addr;
#ifndef ASYNC_UNLOCK
//        assert((return_value & (1ull << (RDMA_Manager::node_id/2 + 1))) == 0);
#endif
        // TODO: if the starvation bit is on then we release and wait the lock.
        if ((return_value >> 56) >= 100){
//            assert(false);
//            assert((return_value & (1ull << (RDMA_Manager::node_id/2 + 1)))== 0);
//            Prepare_WR_FAA(sr[0], sge[0], lock_addr, cas_buffer, -add, 0, Internal_and_Leaf);
            //TODO: check the starvation bit to decide whether there is an immediate retry. If there is a starvation
            // unlock the lock this time util see a write lock.

            RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 1, Regular_Page);

            target_compute_node_id = ((return_value >> 56) - 100)*2;
            goto retry;
        }
        return true;
    }
    // TODO: current implementation can not guarantee the atomicity of lock upgrade if it return success. If there is another
// node trying to acquire the lock, the lock upgrade can have a deadlock. THerefore, the lock upgarding will only try on time
// of atomically upgrade the lock by CAS. If failed then it will fall back to relase the local one and refetch the exclusive latch.
    bool
    RDMA_Manager::global_Rlock_update(ibv_mr *local_mr, GlobalAddress lock_addr, ibv_mr *cas_buffer) {
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        *(uint64_t *)cas_buffer->addr = 0;
        uint64_t swap = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
        uint64_t compare = (1ull );
        std::vector<uint16_t> read_invalidation_targets;
        uint8_t starvation_level = 0;

//        uint64_t page_version = ((DataPage*) local_mr->addr)->hdr.p_version;

//        int invalidation_RPC_type = 0;
        read_invalidation_targets.clear();
        retry:
        retry_cnt++;
        GlobalAddress page_addr = lock_addr;
        page_addr.offset -= STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        // todo: the read lock release and then lock acquire is not atomic. we need to develop and atomic way
        // for the lock upgrading to gurantee the correctness of 2 phase locking.
        if (retry_cnt > 1){
            global_RUnlock(lock_addr, cas_buffer, false, nullptr);
//            printf("Lock upgrade failed, release the lock, address is %p\n", lock_addr);
            return false;
        }
        if (retry_cnt % 4 ==  2) {
//            assert(compare%2 == 0);
            int i = 0;
            for (auto iter: read_invalidation_targets) {
                //TODO: fill out the stavation level and the page version.
                Writer_Invalidate_Shared_RPC(page_addr, iter, 0, i);
                i++;
            }
#ifdef PARALLEL_INVALIDATION
            Writer_Invalidate_Shared_RPC_Reply(i);
#endif
        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        //Only the second RDMA issue a completion
        Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, compare, swap, IBV_SEND_SIGNALED, Regular_Page);
//        rdma_mg->Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);
//        sr[0].next = &sr[1];
//        *(uint64_t *)cas_buffer->addr = 0;
//        assert(page_addr.nodeID == lock_addr.nodeID);
        Batch_Submit_WRs(sr, 1, lock_addr.nodeID);
        uint64_t cas_value = (*(uint64_t*) cas_buffer->addr);
        if ((cas_value) != compare){
//            page_version = ((DataPage*) page_buffer->addr)->hdr.p_version;

            //TODO: 1)If try one time, issue an RPC if try multiple times try to seperate the
            // upgrade into read release and acquire write lock.
            // 2) what if the other node also what to update the lock and this node's read lock
            // has already be released.
//            conflict_tag = *(uint64_t*)cas_buffer->addr;
//            if (conflict_tag != pre_tag) {
//                retry_cnt = 0;
//                pre_tag = conflict_tag;
//            }
            read_invalidation_targets.clear();

            for (uint32_t i = 1; i < 56; ++i) {
                uint32_t  remain_bit = (cas_value >> i)%2;
                //return false if we find the readlock of this node has already been released.
                if ((i-1)*2 == node_id && remain_bit == 0){
                    //The path is actually impossible, because the lock is hold outside the lock state can not be changed.
                    assert(false);
                    return false;
                }
                if (remain_bit == 1 && (i-1)*2 != node_id){

                    read_invalidation_targets.push_back((i-1)*2);
//                    invalidation_RPC_type = 1;
                }
            }
            if (!read_invalidation_targets.empty()){
                goto retry;
            }
            assert(false);
        }
//        ((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->global_lock = swap;
//        printf("Lock update successful page global addr is %p\n", page_addr);
        return true;
    }
    //TODO: Implement a sync read unlock function.
    bool RDMA_Manager::global_RUnlock(GlobalAddress lock_addr, ibv_mr *cas_buffer, bool async, Cache_Handle* handle) {
//        printf("realse global reader lock on address: %u, %lu, this nodeid: %u\n", lock_addr.nodeID, lock_addr.offset-8, node_id);
        //TODO: Change (RDMA_Manager::node_id/2 +1) to (RDMA_Manager::node_id/2)
        uint64_t add = (1ull);
        uint64_t substract = (~add) + 1;
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
//        *(uint64_t *)cas_buffer->addr = (1ull << RDMA_Manager::node_id/2);

        retry:
//        retry_cnt++;
//
//        if (retry_cnt > 300000) {
//            std::cout << "Deadlock " << lock_addr << std::endl;
//
//            std::cout << GetMemoryNodeNum() << ", "
//                      << " locked by node  " << (conflict_tag) << std::endl;
//            assert(false);
//            exit(0);
//        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
//        GlobalAddress lock_addr =
        //Only the second RDMA issue a completion
//        RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
        // TODO: Read unlock do not need to wait for the completion. However, if we consider partial failure recovery,
        //  we need to consider to implement an synchronized RDMA Read unlocking function.
//        RDMA_FAA(lock_addr, cas_buffer, substract, 0, 0, Internal_and_Leaf);
        if (async){
            uint32_t * counter = (uint32_t *)async_tasks.at(lock_addr.nodeID)->Get();
            if (UNLIKELY(!counter)){
                counter = new uint32_t(0);
                async_tasks[lock_addr.nodeID]->Reset(counter);
            }
            if ( UNLIKELY((*counter % (ATOMIC_OUTSTANDING_SIZE/2 - 1)) == 1)){
                RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 1, Regular_Page);
            }else{
                RDMA_FAA(lock_addr, cas_buffer, substract, 0, 0, Regular_Page);

            }
            *counter = *counter + 1;
        } else{
#ifdef GETANALYSIS

            auto statistic_start = std::chrono::high_resolution_clock::now();
#endif
            RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 1, Regular_Page);
#ifdef GETANALYSIS
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - statistic_start);
            PostreadTotal.fetch_add(duration.count());
            Postreadcounter.fetch_add(1);
#endif
            assert(*(uint64_t*)cas_buffer->addr > 0);
        }



//        uint64_t return_data = (*(uint64_t*) cas_buffer->addr);
//        assert((return_data & (1ull << (RDMA_Manager::node_id/2 + 1))) != 0);



//        printf("Release read lock for %lu\n", lock_addr.offset-8);
        return true;
    }
#else
    // TODO: current implementation can not guarantee the atomicity of lock upgrade if it return success. If there is another
// node trying to acquire the lock, the lock upgrade can have a deadlock. THerefore, the lock upgarding will only try on time
// of atomically upgrade the lock by CAS. If failed then it will fall back to relase the local one and refetch the exclusive latch.
    bool
    RDMA_Manager::global_Rlock_update(ibv_mr *local_mr, GlobalAddress lock_addr, ibv_mr *cas_buffer) {
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        *(uint64_t *)cas_buffer->addr = 0;
        uint64_t swap = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
        uint64_t compare = (1ull << (RDMA_Manager::node_id/2 + 1));
        std::vector<uint16_t> read_invalidation_targets;
        uint8_t starvation_level = 0;

//        uint64_t page_version = ((DataPage*) local_mr->addr)->hdr.p_version;

//        int invalidation_RPC_type = 0;
        read_invalidation_targets.clear();
        retry:
        retry_cnt++;
        GlobalAddress page_addr = lock_addr;
        page_addr.offset -= STRUCT_OFFSET(LeafPage<uint64_t>, global_lock);
        // todo: the read lock release and then lock acquire is not atomic. we need to develop and atomic way
        // for the lock upgrading to gurantee the correctness of 2 phase locking.
        if (retry_cnt > 1){
            // Can not put async unlock here, because the async unlock can result in the assertion fault in the following
            // global exclusive latch acquisition
            // todo: make it async unlock.
            global_RUnlock(lock_addr, cas_buffer, false);
//            printf("Lock upgrade failed, release the lock, address is %p\n", lock_addr);
//            fflush(stdout);
            return false;
        }
        if (retry_cnt % 4 ==  2) {
//            assert(compare%2 == 0);
            int i = 0;
            for (auto iter: read_invalidation_targets) {
                //TODO: fill out the stavation level and the page version.
                Writer_Invalidate_Shared_RPC(page_addr, iter, 0, i);
                i++;
            }
#ifdef PARALLEL_INVALIDATION
            Writer_Invalidate_Shared_RPC_Reply(i);
#endif
        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        //Only the second RDMA issue a completion
        Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, compare, swap, IBV_SEND_SIGNALED, Regular_Page);
//        rdma_mg->Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);
//        sr[0].next = &sr[1];
//        *(uint64_t *)cas_buffer->addr = 0;
//        assert(page_addr.nodeID == lock_addr.nodeID);
        Batch_Submit_WRs(sr, 1, lock_addr.nodeID);
        uint64_t cas_value = (*(uint64_t*) cas_buffer->addr);
        if ((cas_value) != compare){
//            page_version = ((DataPage*) page_buffer->addr)->hdr.p_version;

            //TODO: 1)If try one time, issue an RPC if try multiple times try to seperate the
            // upgrade into read release and acquire write lock.
            // 2) what if the other node also what to update the lock and this node's read lock
            // has already be released.
//            conflict_tag = *(uint64_t*)cas_buffer->addr;
//            if (conflict_tag != pre_tag) {
//                retry_cnt = 0;
//                pre_tag = conflict_tag;
//            }
            read_invalidation_targets.clear();

            for (uint32_t i = 1; i < 56; ++i) {
                uint32_t  remain_bit = (cas_value >> i)%2;
                //return false if we find the readlock of this node has already been released.
//                if ((i-1)*2 == node_id && remain_bit == 0){
//                    //The path is actually impossible, because the lock is hold outside the lock state can not be changed.
//                    assert(false);
//                    return false;
//                }
                if (remain_bit == 1 && (i-1)*2 != node_id){

                    read_invalidation_targets.push_back((i-1)*2);
//                    invalidation_RPC_type = 1;
                }
            }
            goto retry;
//            if (!read_invalidation_targets.empty()){
//                goto retry;
//            }
//            assert(false);
        }
//        ((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->global_lock = swap;
//        printf("Lock update successful page global addr is %p\n", page_addr);

        return true;
    }
    //TODO: Implement a sync read unlock function.
    bool
    RDMA_Manager::global_RUnlock(GlobalAddress lock_addr, ibv_mr *cas_buffer, bool async, Cache_Handle *handle) {

        //TODO: Change (RDMA_Manager::node_id/2 +1) to (RDMA_Manager::node_id/2)
        uint64_t add = (1ull << (RDMA_Manager::node_id/2 +1));
        uint64_t substract = (~add) + 1;
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        bool async_succeed = false;
        if (async){
#if ASYNC_PLAN == 1
            Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(lock_addr.nodeID)->Get();
            if (UNLIKELY(!tasks)){
                tasks = new Async_Tasks();
                async_tasks[lock_addr.nodeID]->Reset(tasks);
            }
            std::string qp_type = "write_local_flush";
//            std::string qp_type = "default";
            uint32_t* counter = &tasks->counter;
            if ( UNLIKELY(*counter >= ATOMIC_OUTSTANDING_SIZE  - 2)){
                RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 1, Regular_Page,qp_type);
                //TODO: clear all the async tasks. for the handle. We need to release the local latch and release the handle.

                *counter = 0;
            }else{
                ibv_mr* async_cas = tasks->mrs[*counter];
                RDMA_FAA(lock_addr, async_cas, substract, 0, 0, Regular_Page, qp_type);
                *counter = *counter + 1;
                tasks->handles[*counter] = handle;
                async_succeed = true;
            }
#else
            Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(lock_addr.nodeID)->Get();
            if (UNLIKELY(!tasks)){
                tasks = new Async_Tasks();
                async_tasks[lock_addr.nodeID]->Reset(tasks);
            }
//            uint32_t* counter = &tasks->counter;
            auto temp_mr = tasks->enqueue(this, lock_addr);
            assert(temp_mr);
            std::string qp_type = "write_local_flush";
            RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 0, Regular_Page, qp_type);
            async_succeed = true;

//            if ( UNLIKELY(*counter >= ATOMIC_OUTSTANDING_SIZE  - 2)){
//                RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 1, Regular_Page);
//                //TODO: clear all the async tasks. for the handle. We need to release the local latch and release the handle.
//
//                *counter = 0;
//            }else{
//                ibv_mr* async_cas = tasks->mrs[*counter];
//                RDMA_FAA(lock_addr, async_cas, substract, 0, 0, Regular_Page);
//                *counter = *counter + 1;
//                async_succeed = true;
//            }
#endif
        } else{
#ifdef GETANALYSIS

            auto statistic_start = std::chrono::high_resolution_clock::now();
#endif
            RDMA_FAA(lock_addr, cas_buffer, substract, IBV_SEND_SIGNALED, 1, Regular_Page);
#ifdef GETANALYSIS
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - statistic_start);
            PostreadTotal.fetch_add(duration.count());
            Postreadcounter.fetch_add(1);
#endif

#ifndef NDEBUG
            if((*(uint64_t*)cas_buffer->addr & (1ull << (RDMA_Manager::node_id/2 + 1))) == 0){
                size_t count = 0;

            retry_check:
                count++;
                uint64_t old_cas = *(uint64_t*)cas_buffer->addr;
                spin_wait_us(100);
                //RDMA read the latch word again and see if it is the same as the compare value.
                RDMA_Read(lock_addr, cas_buffer, 8, IBV_SEND_SIGNALED,1, Regular_Page);
                if((*(uint64_t*)cas_buffer->addr & (1ull << (RDMA_Manager::node_id/2 + 1))) != 0){

                    printf("NodeID %u RDMA write to reader handover over data %p move too fast, resulting in spurious latch word mismatch, latch word is %p\n", node_id, lock_addr, old_cas);
//                    fflush(stdout);
                    if (count >100){
                        assert(false);
                    }
                    goto retry_check;
                }
                printf("Have retry %lu times\n", count);
                //                goto retry;
            }
#endif
//            assert((*(uint64_t*)cas_buffer->addr & (1ull << (RDMA_Manager::node_id/2 + 1))) != 0);
        }
//        uint64_t return_data = (*(uint64_t*) cas_buffer->addr);
//        assert((return_data & (1ull << (RDMA_Manager::node_id/2 + 1))) != 0);
//        printf("Release read lock for %lu\n", lock_addr.offset-8);
        return async_succeed;
    }

#endif

    bool RDMA_Manager::global_Wlock_and_read_page_with_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                                               GlobalAddress lock_addr, ibv_mr *cas_buffer, int r_times, uint8_t *starv_level,
                                                               int coro_id) {
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        *(uint64_t *)cas_buffer->addr = 0;
        std::vector<uint16_t> read_invalidation_targets;
        uint16_t write_invalidation_target = 0-1;
        uint8_t starvation_level = 0;
        uint64_t last_CAS_return = 0;
        uint64_t page_version = 0;
        int invalidation_RPC_type = 0; // 0 no need for invalidaton message, 1 read invalidation message, 2 write invalidation message.
#ifdef INVALIDATION_STATISTICS
        bool invalidation_counted = false;
#endif
    retry:
        if (r_times >0 && retry_cnt >= r_times){
            return false;
        }
        uint64_t compare = 0;
        // We need a + 100 for the id, because id 0 conflict with the unlock bit
        // 100 reserve enough room in case that the write lock handover arrived out of order and the latch word shall
        // never reach 0. we can first handover latch state asynchronously, and then write forward the page.
        uint64_t swap = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
        //TODO: send an RPC to the destination every 4 retries.
        // Check whether the invalidation is write type or read type. If it is a read type
        // we need to broadcast the message to multiple destination.
        if (retry_cnt++ % INVALIDATION_INTERVAL ==  1 ) {
//            assert(compare%2 == 0);
            if(retry_cnt < 20){
//                port::AsmVolatilePause();
                //do nothing
            }else if (retry_cnt <40){
                starvation_level = 1;

            }else if (retry_cnt <80){
                starvation_level = 2;
            }
            else if (retry_cnt <160){
                starvation_level = 3;
            }else if (retry_cnt <200){
                starvation_level = 4;
            } else if (retry_cnt <1000){
                starvation_level = 5;
            } else{
                starvation_level = 255 > 5+ retry_cnt/1000? (5+ retry_cnt/1000): 255;
            }
//            printf("We need invalidation message\n");
            if (invalidation_RPC_type == 1){
                assert(!read_invalidation_targets.empty());
                int i = 0;
                for (auto iter: read_invalidation_targets) {
                    if (iter != (RDMA_Manager::node_id)){
#ifdef INVALIDATION_STATISTICS
                        if(!invalidation_counted){
                            invalidation_counted = true;
                            cache_invalidation[RDMA_Manager::thread_id]++;
                        }
#endif
                        Writer_Invalidate_Shared_RPC(page_addr, iter, starvation_level, i);
                        i++;
                    }else{
                        // This rare case is because under async read release, the cache mutex will be released before the read/write lock release.
                        // If there is another request comes in immediately for the same page before the lock release, this print
                        // below will happen.
                        printf("read invalidation target is itself, this is rare case,, page_addr is %p, retry_cnt is %lu\n", page_addr, retry_cnt);
                        if (retry_cnt > 10000000){
                            assert(false);
                        }
                    }
                }
#ifdef PARALLEL_INVALIDATION
                Writer_Invalidate_Shared_RPC_Reply(i);
#endif
            }else if (invalidation_RPC_type == 2){
                assert(write_invalidation_target != 0-1);
//                assert(write_invalidation_target != node_id);
                if (write_invalidation_target != (RDMA_Manager::node_id)){
#ifdef INVALIDATION_STATISTICS
                    if(!invalidation_counted){
                        invalidation_counted = true;
                        cache_invalidation[RDMA_Manager::thread_id]++;
                    }
#endif
                    auto reply = Writer_Invalidate_Modified_RPC(page_addr,
                                                                page_buffer, write_invalidation_target,
                                                                starvation_level, retry_cnt);
                    if (reply == processed){
//                        printf("Node %u try to acquire exclusive latch from node %u and successfully get forwarded page over data %p\n", RDMA_Manager::node_id, write_invalidation_target, page_addr);
//                        fflush(stdout);
                        //The invlaidation message is processed and page has been forwarded.
                        ((LeafPage<uint64_t>*)(page_buffer->addr))->global_lock = swap;
                        return true;
                    }
                }else{
                    //It is okay to have a write invalidation target is itself, if we enable the async write unlock.
                    printf(" Write invalidation target is itself, this is rare case,, page_addr is %p, retry_cnt is %lu\n", page_addr, retry_cnt);
                }
            }else{
                // if the RDMA return sees a faulty state, it shall ignore that and comes here.
//                assert(false);
            }

            // the compared value is the real id /2 + 1.
        }

        if (retry_cnt % INVALIDATION_INTERVAL >  4 || retry_cnt % INVALIDATION_INTERVAL < 1){
            if (starvation_level <= 2){
                spin_wait_us(8);
            }else if (starvation_level <= 4){
                //No sleep if the starvation level is high.
                spin_wait_us(4);
            }else if (starvation_level <= 6){
                spin_wait_us(2);
            }else{

            }
        }
//        if (retry_cnt > 210) {
//            std::cout << "write lock timeout" << lock_addr << std::endl;
//
//            std::cout << GetMemoryNodeNum() << ", "
//                      << " locked by node  " << (conflict_tag) << std::endl;
//            printf("CAS buffer value is %lu, compare is %lu, swap is %lu\n", (*(uint64_t*) cas_buffer->addr), compare, swap);
//            assert(false);
//            exit(0);
//        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        //Only the second RDMA issue a completion
        Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, compare, swap, 0, Regular_Page);
        Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Regular_Page);
        sr[0].next = &sr[1];
        *(uint64_t *)cas_buffer->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        std::string str("default");
        Batch_Submit_WRs(sr, 1, page_addr.nodeID);
//        printf("READ page %p from remote memory to local mr %p 2 thread_id is %d\n", page_addr, page_buffer->addr, thread_id);

//        invalidation_RPC_type = 0;
        //When the program fail at the code below the remote buffer content (this_page_g_ptr) has already  be incosistent
#ifndef NDEBUG
        auto page = (LeafPage<uint64_t>*)(page_buffer->addr);
//        assert(page_addr == page->hdr.this_page_g_ptr);
#endif
        // Rethink the logic of this part. Can it result in false lock acquire?
        if ((*(uint64_t*) cas_buffer->addr) != compare){
            invalidation_RPC_type = 0;
//            assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));
            if ((*(uint64_t*) cas_buffer->addr) >> 56 == swap >> 56){
                spin_wait_us(100);
                goto retry;
            }
//            if (last_CAS_return != (*(uint64_t*) cas_buffer->addr)){
//                // someone else have acquire the latch, immediately issue an invalidation in the next loop.
//                retry_cnt = retry_cnt/INVALIDATION_INTERVAL;
//            }
            last_CAS_return = (*(uint64_t*) cas_buffer->addr);
            // clear the invalidation targets
            read_invalidation_targets.clear();
            write_invalidation_target = 0-1;
            // use usleep ?
//            conflict_tag = *(uint64_t*)cas_buffer->addr;
//            if (conflict_tag != pre_tag) {
//                retry_cnt = 0;
//                pre_tag = conflict_tag;
//            }
            uint64_t cas_value = (*(uint64_t*) cas_buffer->addr);
            uint64_t write_byte = (cas_value >> 56);
//            page_version = ((DataPage*) page_buffer->addr)->hdr.p_version;
            if (write_byte >= 100){
                if (UNLIKELY(write_byte >= compute_nodes.size() + 100)){
                    // an faulty intermidiate state, wait for state transfer.
                    read_invalidation_targets.clear();
                    write_invalidation_target = 0-1;
                    goto retry;
                }else{
                    invalidation_RPC_type = 2;

                    //The CAS record (ID/2+1), so we need to recover the real ID.
                    write_invalidation_target = (write_byte - 100)*2;
                    goto retry;
                }

            }
//            uint64_t read_bit_pos = 0;





            for (uint32_t i = 1; i < 56; ++i) {
                uint32_t  remain_bit = (cas_value >> i)%2;
                    if (remain_bit == 1){
                        if (UNLIKELY(i > compute_nodes.size())){
                            //an faulty intermidiate state for reader invalidate writer and then lock update (release) is detected.
                            // wait for state transfer.
                            read_invalidation_targets.clear();
                            invalidation_RPC_type = 0;
                            goto retry;
                        }else{
                            read_invalidation_targets.push_back((i-1)*2);
                            invalidation_RPC_type = 1;
                        }

                    }

            }
            if (!read_invalidation_targets.empty()){
//                assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));
                goto retry;
            }
            assert(false);
        }
#ifdef WRITER_STARV_SPIN_BASE
        if (starv_level && starvation_level > 0 && invalidation_RPC_type == 1){
            assert(*starv_level == 0);
            //
            *starv_level = starvation_level;
        }
#endif
        //TODO: remember the starvation level in the cache handle. THis can be used for priority revenge to improve the access fairness.
        ((LeafPage<uint64_t>*)(page_buffer->addr))->global_lock = swap;
        return true;
//        printf("Acquire Write Lock at %lu\n", page_addr);
//        assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));
    }

    void RDMA_Manager::global_Wlock_with_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                                               GlobalAddress lock_addr, ibv_mr *cas_buffer, uint64_t tag, CoroContext *cxt,
                                                               int coro_id) {
//        assert(false);
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        *(uint64_t *)cas_buffer->addr = 0;
        std::vector<uint16_t> read_invalidation_targets;
        uint16_t write_invalidation_target = 0-1;
        uint64_t last_CAS_return = 0;
        uint8_t starvation_level = 0;
        uint64_t page_version = 0;
        int invalidation_RPC_type = 0; // 0 no need for invalidaton message, 1 read invalidation message, 2 write invalidation message.
#ifdef INVALIDATION_STATISTICS
        bool invalidation_counted = false;
#endif

        retry:
        retry_cnt++;
        uint64_t compare = 0;
        // We need a + 1 for the id, because id 0 conflict with the unlock bit
        uint64_t swap = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
        //TODO: send an RPC to the destination every 4 retries.
        // Check whether the invalidation is write type or read type. If it is a read type
        // we need to broadcast the message to multiple destination.
        if (retry_cnt++ % INVALIDATION_INTERVAL ==  1 ) {
//            assert(compare%2 == 0);
            if(retry_cnt < 20){
//                port::AsmVolatilePause();
                //do nothing
            }else if (retry_cnt <40){
                starvation_level = 1;

            }else if (retry_cnt <80){
                starvation_level = 2;
            }
            else if (retry_cnt <160){
                starvation_level = 3;
            }else if (retry_cnt <200){
                starvation_level = 4;
            } else if (retry_cnt <1000){
                starvation_level = 5;
            } else{
                starvation_level = 255 > 5+ retry_cnt/1000? 5+ retry_cnt/1000: 255;
            }
//            printf("We need invalidation message\n");
            if (invalidation_RPC_type == 1){
                assert(false);

                assert(!read_invalidation_targets.empty());
                int i = 0;
                for (auto iter: read_invalidation_targets) {
                    if (iter != (RDMA_Manager::node_id)){
#ifdef INVALIDATION_STATISTICS
                        if(!invalidation_counted){
                            invalidation_counted = true;
                            cache_invalidation[RDMA_Manager::thread_id]++;
                        }
#endif
                        Writer_Invalidate_Shared_RPC(page_addr, iter, starvation_level, i);
                    }else{
                        // This rare case is because the cache mutex will be released before the read/write lock release.
                        // If there is another request comes in immediately for the same page before the lock release, this print
                        // below will happen.
                        printf(" read invalidation target is itself, this is rare case,, page_addr is %p, retry_cnt is %lu\n", page_addr, retry_cnt);
                    }
                    i++;
                }
#ifdef PARALLEL_INVALIDATION
                Writer_Invalidate_Shared_RPC_Reply(i);
#endif
            }else if (invalidation_RPC_type == 2){
                assert(false);

                assert(write_invalidation_target != 0-1);
//                assert(write_invalidation_target != node_id);
                if (write_invalidation_target != (RDMA_Manager::node_id)){
#ifdef INVALIDATION_STATISTICS
                    if(!invalidation_counted){
                        invalidation_counted = true;
                        cache_invalidation[RDMA_Manager::thread_id]++;
                    }
#endif
                    auto reply = Writer_Invalidate_Modified_RPC(page_addr,
                                                                page_buffer, write_invalidation_target,
                                                                starvation_level, retry_cnt);
                    if (reply == processed){
                        //The invlaidation message is processed and page has been forwarded.
                        ((LeafPage<uint64_t>*)(page_buffer->addr))->global_lock = swap;
                        return;
                    }
                }else{
                    //It is okay to have a write invalidation target is itself, if we enable the async write unlock.
                    printf(" Write invalidation target is itself, this is because the previous latch release is async, and has not arrived yet,, page_addr is %p, retry_cnt is %lu\n", page_addr, retry_cnt);
                }
            }

            // the compared value is the real id /2 + 1.
        }
        if (retry_cnt % INVALIDATION_INTERVAL >  4 || retry_cnt % INVALIDATION_INTERVAL < 1){
            if (starvation_level <= 2){
                spin_wait_us(8);
            }else if (starvation_level <= 4){
                //No sleep if the starvation level is high.
                spin_wait_us(4);
            }else if (starvation_level <= 6){
                spin_wait_us(2);
            }else{

            }
        }
//        if (retry_cnt > 210) {
//            std::cout << "write lock timeout" << lock_addr << std::endl;
//
//            std::cout << GetMemoryNodeNum() << ", "
//                      << " locked by node  " << (conflict_tag) << std::endl;
//            printf("CAS buffer value is %lu, compare is %lu, swap is %lu\n", (*(uint64_t*) cas_buffer->addr), compare, swap);
//            assert(false);
//            exit(0);
//        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        //Only the second RDMA issue a completion
        Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, compare, swap, IBV_SEND_SIGNALED, Regular_Page);
//        Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);
        *(uint64_t *)cas_buffer->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        std::string str("default");
        Batch_Submit_WRs(sr, 1, page_addr.nodeID);
        invalidation_RPC_type = 0;
        //When the program fail at the code below the remote buffer content (this_page_g_ptr) has already  be incosistent
#ifndef NDEBUG
        auto page = (LeafPage<uint64_t>*)(page_buffer->addr);
//        assert(page_addr == page->hdr.this_page_g_ptr);
#endif
        if ((*(uint64_t*) cas_buffer->addr) != compare){
//            page_version = ((DataPage*) page_buffer->addr)->hdr.p_version;
//            assert(page_version == 0);
            //TODO: The return may shows that this lock permission has already handover by the previous latch holde,
            // in this case we can jump out of the loop and just use the read buffer.
            if ((*(uint64_t*) cas_buffer->addr) >> 56 == swap >> 56){
                // >> 56 in case there are concurrent reader
                //Other computen node handover for me
                printf("Lock acquirisition find itself already hold global exclusive latch at %p, this node is %u\n", page_addr, RDMA_Manager::node_id);
                fflush(stdout);
//                ((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->global_lock = swap;
//                return;
            }
            if (last_CAS_return != (*(uint64_t*) cas_buffer->addr)){
                // someone else have acquire the latch, immediately issue a invalidation in the next loop.
                retry_cnt = retry_cnt/INVALIDATION_INTERVAL;
            }
            last_CAS_return = (*(uint64_t*) cas_buffer->addr);
//            assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));

            // clear the invalidation targets
            read_invalidation_targets.clear();
            write_invalidation_target = 0-1;
            // use usleep ?
//            conflict_tag = *(uint64_t*)cas_buffer->addr;
//            if (conflict_tag != pre_tag) {
//                retry_cnt = 0;
//                pre_tag = conflict_tag;
//            }
            uint64_t cas_value = (*(uint64_t*) cas_buffer->addr);
            uint64_t write_byte = cas_value >> 56;
            if (write_byte >= 100){
                invalidation_RPC_type = 2;
                //The CAS record (ID/2+1), so we need to recover the real ID.
                write_invalidation_target = (write_byte - 100)*2;
                goto retry;
            }
//            uint64_t read_bit_pos = 0;
            for (uint32_t i = 1; i < 56; ++i) {
                uint32_t  remain_bit = (cas_value >> i)%2;
                if (remain_bit == 1){
                    read_invalidation_targets.push_back((i-1)*2);
                    invalidation_RPC_type = 1;
                }
            }
            if (!read_invalidation_targets.empty()){
//                assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));
                goto retry;
            }
        }
        ((LeafPage<uint64_t>*)(page_buffer->addr))->global_lock = swap;
//        printf("Acquire Write Lock at %lu\n", page_addr);
//        assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));
    }
#if ACCESS_MODE == 0
    bool RDMA_Manager::global_Wlock_without_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                                    GlobalAddress lock_addr, ibv_mr *cas_buffer, int r_time, CoroContext *cxt,
                                                    int coro_id) {

        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        *(uint64_t *)cas_buffer->addr = 0;
        std::vector<uint16_t> read_invalidation_targets;
        uint16_t write_invalidation_target = 0-1;
        uint64_t compare = 0;
        // We need a + 1 for the id, because id 0 conflict with the unlock bit
        uint64_t swap = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
        int invalidation_RPC_type = 0; // 0 no need for invalidaton message, 1 read invalidation message, 2 write invalidation message.
#ifdef INVALIDATION_STATISTICS
        bool invalidation_counted = false;
#endif

    retry:
        if (retry_cnt >0 && retry_cnt > r_time){
            return false;
        }
        retry_cnt++;

        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        //Only the second RDMA issue a completion
        Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, compare, swap, IBV_SEND_SIGNALED, Regular_Page);
//        Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);
        *(uint64_t *)cas_buffer->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        std::string str("default");
        Batch_Submit_WRs(sr, 1, page_addr.nodeID);
        invalidation_RPC_type = 0;
        //When the program fail at the code below the remote buffer content (this_page_g_ptr) has already  be incosistent
#ifndef NDEBUG
        auto page = (LeafPage<uint64_t,uint64_t>*)(page_buffer->addr);
//        assert(page_addr == page->hdr.this_page_g_ptr);
#endif
        if ((*(uint64_t*) cas_buffer->addr) != compare){
            goto retry;
        }
        ((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->global_lock = swap;
//        printf("Acquire Write Lock at %lu\n", page_addr);
//        assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));
    }
    bool RDMA_Manager::global_Wlock_and_read_page_without_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
                                                                  GlobalAddress lock_addr, ibv_mr *cas_buffer, int r_time, CoroContext *cxt,
                                                                  int coro_id) {

        volatile uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
        *(uint64_t *)cas_buffer->addr = 0;
    retry:
        if (retry_cnt >0 && retry_cnt >= r_time){
            return false;
        }
        uint64_t compare = 0;
        // We need a + 1 for the id, because id 0 conflict with the unlock bit
        uint64_t swap = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
        //TODO: send an RPC to the destination every 4 retries.
        // Check whether the invalidation is write type or read type. If it is a read type
        // we need to broadcast the message to multiple destination.
        retry_cnt++;

        if (retry_cnt % INVALIDATION_INTERVAL ==  1 ) {
//            assert(compare%2 == 0);
//            if (retry_cnt < 10) {
////                port::AsmVolatilePause();
//                //do nothing
//            } else if (retry_cnt < 20) {
//                spin_wait_us(2);
//
//            } else if (retry_cnt < 40) {
//                spin_wait_us(8);
//            } else if (retry_cnt < 160) {
//                spin_wait_us(32);
//            } else if (retry_cnt < 200) {
//                usleep(256);
//            } else if (retry_cnt < 1000) {
//                usleep(1024);
//            } else {
//                sleep(1);
//            }
        }
//        if (retry_cnt > 180000) {
//            std::cout << "Deadlock for write lock " << lock_addr << std::endl;
//
//            std::cout << GetMemoryNodeNum() << ", "
//                      << " locked by node  " << (conflict_tag) << std::endl;
//            assert(false);
////            exit(0);
//        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        //Only the second RDMA issue a completion
        Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, compare, swap, 0, Regular_Page);
        Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Regular_Page);
        sr[0].next = &sr[1];
        *(uint64_t *)cas_buffer->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        Batch_Submit_WRs(sr, 1, page_addr.nodeID);
        if ((*(uint64_t*) cas_buffer->addr) != compare){
            // clear the invalidation targets
            goto retry;

        }
        return true;

    }
#endif
    bool RDMA_Manager::global_write_page_and_Wunlock(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                                     GlobalAddress remote_lock_addr, Cache_Handle *handle, bool async) {

        //TODO: If we want to use async unlock, we need to enlarge the max outstand work request that the queue pair support.
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        GlobalAddress tbFlushed_gaddr{};
        ibv_mr tbFlushed_local_mr = *page_buffer;
        auto page = (LeafPage<uint64_t>*)(page_buffer->addr);
        assert(STRUCT_OFFSET(LeafPage<int>, hdr.dirty_upper_bound) == STRUCT_OFFSET(LeafPage<char>, hdr.dirty_upper_bound));
        if (page->hdr.dirty_upper_bound == 0){
            assert(page->hdr.dirty_lower_bound == 0);
            // this means the page does not participate the optimization of dirty-only flush back.
            tbFlushed_gaddr.nodeID = page_addr.nodeID;
            //The header should be the same offset in Leaf or INternal nodes
            assert(STRUCT_OFFSET(LeafPage<int>, hdr) == STRUCT_OFFSET(LeafPage<char>, hdr));
            assert(STRUCT_OFFSET(InternalPage<int>, hdr) == STRUCT_OFFSET(LeafPage<int>, hdr));
            assert(STRUCT_OFFSET(DataPage, hdr) == STRUCT_OFFSET(LeafPage<char>, hdr));
            tbFlushed_gaddr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<int>, hdr);
            //Increase the page version before every page flush back.
            tbFlushed_local_mr.addr = reinterpret_cast<void*>((uint64_t)page_buffer->addr + STRUCT_OFFSET(LeafPage<int>, hdr));
            page_size -=  STRUCT_OFFSET(LeafPage<int>, hdr);
        }else{
            assert(page->hdr.dirty_lower_bound >= sizeof(uint64_t ));
            tbFlushed_gaddr.nodeID = page_addr.nodeID;
            tbFlushed_gaddr.offset = page_addr.offset + page->hdr.dirty_lower_bound;
            tbFlushed_local_mr.addr = reinterpret_cast<void*>((uint64_t)page_buffer->addr + page->hdr.dirty_lower_bound);
            page_size = page->hdr.dirty_upper_bound - page->hdr.dirty_lower_bound;
            page->hdr.dirty_lower_bound = 0;
            page->hdr.dirty_upper_bound = 0;
        }

        assert(remote_lock_addr <= tbFlushed_gaddr - 8);
        bool async_succeed = false;
        size_t send_flags = page_size <= MAX_INLINE_SIZE ? IBV_SEND_INLINE:0;
        // Page write back shall never utilize async unlock. because we can not guarantee, whether the page will be overwritten by
        // another thread before the unlock. It is possible this cache buffer is reused by other cache entry.
        if (async){
            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            *(uint64_t*) local_CAS_mr->addr = 0;
            //TODO: Can we make the RDMA unlock based on RDMA FAA? In this case, we can use async
            // lock releasing to reduce the RDMA ROUND trips in the protocol
            volatile uint64_t add = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            volatile uint64_t substract = (~add) + 1;
#if ASYNC_PLAN == 1
            // The code below is to prevent a work request overflow in the send queue, since we enable
            // async lock releasing.
            Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(page_addr.nodeID)->Get();
            if (UNLIKELY(!tasks)){
                tasks = new Async_Tasks();
                async_tasks[page_addr.nodeID]->Reset(tasks);
            }
            std::string qp_type = "write_local_flush";
//            std::string qp_type = "default";
            uint32_t* counter = &tasks->counter;
            // Every sync unlock submit 2 requests, and we need to reserve another one work request for the RDMA locking which
            // contains one async lock acquiring.
            if (UNLIKELY(*counter >= ATOMIC_OUTSTANDING_SIZE  - 3)){
                Prepare_WR_Write(sr[0], sge[0], tbFlushed_gaddr, &tbFlushed_local_mr, page_size, send_flags, Regular_Page);
                Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, local_CAS_mr, substract, IBV_SEND_SIGNALED, Regular_Page);
                sr[0].next = &sr[1];

                *(uint64_t *)local_CAS_mr->addr = 0;
                assert(page_addr.nodeID == remote_lock_addr.nodeID);
                Batch_Submit_WRs(sr, 1, page_addr.nodeID, qp_type);
                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (add >> 56));
                *counter = 0;
            }else{
                ibv_mr* async_cas = tasks->mrs[*counter];
                ibv_mr* async_buf = tasks->mrs[*counter+1];
                assert(async_buf->length >= page_size);
                memcpy(async_buf->addr, tbFlushed_local_mr.addr, page_size);

                Prepare_WR_Write(sr[0], sge[0], tbFlushed_gaddr, async_buf, page_size, send_flags, Regular_Page);
                Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, async_cas, substract, 0, Regular_Page);
                sr[0].next = &sr[1];

                *(uint64_t *)async_cas->addr = 0;
                assert(page_addr.nodeID == remote_lock_addr.nodeID);
                Batch_Submit_WRs(sr, 0, page_addr.nodeID, qp_type);
                *counter = *counter + 2;
//                tasks->handles[*counter] = handle;
                async_succeed = true;
            }
#else

            Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(page_addr.nodeID)->Get();
            if (UNLIKELY(!tasks)){
                tasks = new Async_Tasks();
                async_tasks[page_addr.nodeID]->Reset(tasks);
            }
//            uint32_t* counter = &tasks->counter;
            auto async_cas = tasks->enqueue(this, tbFlushed_gaddr);
            auto async_buf = tasks->enqueue(this, tbFlushed_gaddr);
            memcpy(async_buf->addr, tbFlushed_local_mr.addr, page_size);
            assert(async_cas);
            assert(async_buf);
            std::string qp_type = "write_local_flush";
//            printf("Enqueue async write unlock for %lu\n",page_addr);
//            fflush(stdout);
            Prepare_WR_Write(sr[0], sge[0], tbFlushed_gaddr, async_buf, page_size, send_flags|IBV_SEND_SIGNALED, Regular_Page);
            Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, async_cas, substract, IBV_SEND_SIGNALED, Regular_Page);
            *(uint64_t *)async_cas->addr = 0;
            Batch_Submit_WRs(sr, 0, page_addr.nodeID, qp_type);
            async_succeed = true;
#endif
//            printf("Release write lock for %lu\n",page_addr);
            //TODO: it could be spuriously failed because of the FAA.so we can not have async
        }else{
//            printf("This code path shall not be entered\n");

//#ifndef NDEBUG
            uint64_t retry_cnt = 0;
//#endif

//        rdma_mg->RDMA_Write(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED ,1, Internal_and_Leaf);
            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            retry:
//#ifndef NDEBUG
            if (retry_cnt++ >5000 && retry_cnt % 1000 == 0){
                printf("RDMA write lock unlock keep spinning but never release, the return value is %lu\n", (*(uint64_t*) local_CAS_mr->addr) );
            }
//#endif

            //TODO: check whether the page's global lock is still write lock
            Prepare_WR_Write(sr[0], sge[0], tbFlushed_gaddr, &tbFlushed_local_mr, page_size, send_flags, Regular_Page);
            *(uint64_t *)local_CAS_mr->addr = 0;
            uint64_t compare = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            volatile uint64_t substract = (~compare) + 1;
            //TODO: USE rdma faa to release the write lock to avoid continuous spurious unlock resulting from the concurrent read lock request.
            Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, local_CAS_mr, substract, IBV_SEND_SIGNALED, Regular_Page);

//            Prepare_WR_CAS(sr[1], sge[1], remote_lock_addr, local_CAS_mr, compare,swap, IBV_SEND_SIGNALED, Internal_and_Leaf);
            sr[0].next = &sr[1];



            assert(page_addr.nodeID == remote_lock_addr.nodeID);
            Batch_Submit_WRs(sr, 1, page_addr.nodeID);
            assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (compare >> 56));
//            if((*(uint64_t*) local_CAS_mr->addr) != compare){
////                printf("RDMA write lock unlock happen with RDMA faa FOR rdma READ LOCK\n");
//                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (compare >> 56));
//
//                goto retry;
//            }

        }
//        printf("Release write lock for %lu\n",page_addr);
//        assert(page_addr == (((LeafPage<int COMMA int>*)(page_buffer->addr))->hdr.this_page_g_ptr));
        return async_succeed;
    }
    bool RDMA_Manager::global_write_page_and_WHandover(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                                       uint8_t next_holder_id, GlobalAddress remote_lock_addr,
                                                       bool async, Cache_Handle* handle) {

        if (next_holder_id >16){
            throw std::invalid_argument( "received wrong handover target node id" );
        }
        //TODO: If we want to use async unlock, we need to enlarge the max outstand work request that the queue pair support.
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        GlobalAddress post_gl_page_addr{};
        post_gl_page_addr.nodeID = page_addr.nodeID;
//        assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));
        //The header should be the same offset in Leaf or INternal nodes
        assert(STRUCT_OFFSET(LeafPage<int>, hdr) == STRUCT_OFFSET(LeafPage<char>, hdr));
        assert(STRUCT_OFFSET(InternalPage<int>, hdr) == STRUCT_OFFSET(LeafPage<int>, hdr));
        post_gl_page_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<int>, hdr);
        ibv_mr post_gl_page_local_mr = *page_buffer;
        //Increase the page version before every page flush back.
//        assert(STRUCT_OFFSET(DataPage, hdr.p_version) == STRUCT_OFFSET(LeafPage<char COMMA char>, hdr.p_version));
//        ((DataPage*)page_buffer->addr)->hdr.p_version++;
        post_gl_page_local_mr.addr = reinterpret_cast<void*>((uint64_t)page_buffer->addr + STRUCT_OFFSET(LeafPage<int>, hdr));
        page_size -=  STRUCT_OFFSET(LeafPage<int>, hdr);
        assert(remote_lock_addr <= post_gl_page_addr - 8);
        bool async_succeed = false;

        // Page write back shall never utilize async unlock. because we can not guarantee, whether the page will be overwritten by
        // another thread before the unlock. It is possible this cache buffer is reused by other cache entry.
        if (async){
#if ASYNC_PLAN == 1
            assert(false);
            Prepare_WR_Write(sr[0], sge[0], post_gl_page_addr, &post_gl_page_local_mr, page_size, 0, Regular_Page);
            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            *(uint64_t*) local_CAS_mr->addr = 0;
            //TODO: Can we make the RDMA unlock based on RDMA FAA? In this case, we can use async
            // lock releasing to reduce the RDMA ROUND trips in the protocol
            volatile uint64_t add = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            volatile uint64_t substract = (~add) + 1;
            add = ((uint64_t)next_holder_id/2 +100) << 56;
            // The code below is to prevent a work request overflow in the send queue, since we enable
            // async lock releasing.
            Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(page_addr.nodeID)->Get();
            if (UNLIKELY(!tasks)){
                tasks = new Async_Tasks();
                async_tasks[page_addr.nodeID]->Reset(tasks);
            }
            uint32_t* counter = &tasks->counter;
            // Every sync unlock submit 2 requests, and we need to reserve another one work request for the RDMA locking which
            // contains one async lock acquiring.
            if ( UNLIKELY(*counter >= (ATOMIC_OUTSTANDING_SIZE / 2 - 1))){
                Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, local_CAS_mr, substract + add, IBV_SEND_SIGNALED, Regular_Page);
                sr[0].next = &sr[1];

                *(uint64_t *)local_CAS_mr->addr = 0;
                assert(page_addr.nodeID == remote_lock_addr.nodeID);
                Batch_Submit_WRs(sr, 1, page_addr.nodeID);
                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (add >> 56));

                for (int i = 0; i < (ATOMIC_OUTSTANDING_SIZE / 2 - 1); ++i) {
                    auto* handle_temp = (Cache::Handle*)tasks->handles[i];
                    assert(handle_temp != nullptr);
                    handle_temp->rw_mtx.unlock();
                    page_cache_->Release(handle_temp);
                    tasks->handles[i] = nullptr;
                }
                *counter = 0;

            }else{
                Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, local_CAS_mr, substract + add, 0, Regular_Page);
                sr[0].next = &sr[1];

                *(uint64_t *)local_CAS_mr->addr = 0;
                assert(page_addr.nodeID == remote_lock_addr.nodeID);
                Batch_Submit_WRs(sr, 0, page_addr.nodeID);
                *counter = *counter + 1;
                tasks->handles[*counter] = handle;
                async_succeed = true;
            }
#endif
            //TODO: it could be spuriously failed because of the FAA.so we can not have async
        }else{
//#ifndef NDEBUG
            uint64_t retry_cnt = 0;
//#endif

            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            retry:
//#ifndef NDEBUG
            if (retry_cnt++ >5000 && retry_cnt % 1000 == 0){
                printf("RDMA write lock unlock keep spinning but never release, the return value is %lu\n", (*(uint64_t*) local_CAS_mr->addr) );
            }
//#endif

            //TODO: check whether the page's global lock is still write lock
            //  0909/2023: the code below seems sometime will get stuck.
            Prepare_WR_Write(sr[0], sge[0], post_gl_page_addr, &post_gl_page_local_mr, page_size, 0, Regular_Page);

            *(uint64_t *)local_CAS_mr->addr = 0;
            //TODO: THe RDMA write unlock can not be guaranteed to be finished after the page write.
            // The RDMA CAS be started strictly after the RDMA write at the remote NIC according to
            // https://docs.nvidia.com/networking/display/MLNXOFEDv451010/Out-of-Order+%28OOO%29+Data+Placement+Experimental+Verbs
            // So it's better to make Unlock another RDMA CAS.
//        rdma_mg->RDMA_CAS( remote_lock_addr, local_CAS_mr, 1,0, IBV_SEND_SIGNALED,1, Internal_and_Leaf);
//        assert(*(uint64_t *)local_CAS_mr->addr == 1);
            //We can apply async unlock here to reduce the latency.
//            uint64_t swap = 0;
            uint64_t compare = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            volatile uint64_t substract = (~compare) + 1;
            volatile uint64_t add = ((uint64_t)next_holder_id/2 +100) << 56;

            //TODO: USE rdma faa to release the write lock to avoid continuous spurious unlock resulting from the concurrent read lock request.
            Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, local_CAS_mr, substract + add, IBV_SEND_SIGNALED, Regular_Page);

//            Prepare_WR_CAS(sr[1], sge[1], remote_lock_addr, local_CAS_mr, compare,swap, IBV_SEND_SIGNALED, Internal_and_Leaf);
            sr[0].next = &sr[1];



            assert(page_addr.nodeID == remote_lock_addr.nodeID);
            Batch_Submit_WRs(sr, 1, page_addr.nodeID);
#ifndef NDEBUG
            if(((*(uint64_t*) local_CAS_mr->addr) >> 56) != (compare >> 56)){
                uint64_t old_cas = (*(uint64_t*) local_CAS_mr->addr);
                usleep(50);
                //RDMA read the latch word again and see if it is the same as the compare value.
                RDMA_Read(remote_lock_addr, local_CAS_mr, 8, IBV_SEND_SIGNALED,1, Regular_Page);
                // todo: may be we need to ignore this assertion?
                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) != (add >> 56));
                printf("Node ID %u RDMA write handover move too fast, resulting in spurious latch word mismatch\n", RDMA_Manager::node_id);
                fflush(stdout);
                //                goto retry;
            }
#endif
//            if((*(uint64_t*) local_CAS_mr->addr) != compare){
////                printf("RDMA write lock unlock happen with RDMA faa FOR rdma READ LOCK\n");
//                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (compare >> 56));
//
//                goto retry;
//            }

        }
//        printf("Release write lock for %lu\n",page_addr);
//        assert(page_addr == (((LeafPage<int COMMA int>*)(page_buffer->addr))->hdr.this_page_g_ptr));
        return async_succeed;
    }

    bool RDMA_Manager::global_WHandover(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                        uint8_t next_holder_id,
                                        GlobalAddress remote_lock_addr, bool async) {

//        if (next_holder_id >16){
//            throw std::invalid_argument( "received wrong handover target node id" );
//        }
        //TODO: If we want to use async unlock, we need to enlarge the max outstand work request that the queue pair support.
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        GlobalAddress post_gl_page_addr{};
        post_gl_page_addr.nodeID = page_addr.nodeID;
//        assert(page_addr == (((LeafPage<uint64_t,uint64_t>*)(page_buffer->addr))->hdr.this_page_g_ptr));
        //The header should be the same offset in Leaf or INternal nodes
        assert(STRUCT_OFFSET(LeafPage<int>, hdr) == STRUCT_OFFSET(LeafPage<char>, hdr));
        assert(STRUCT_OFFSET(InternalPage<int>, hdr) == STRUCT_OFFSET(LeafPage<int>, hdr));
        post_gl_page_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<int>, hdr);
        ibv_mr post_gl_page_local_mr = *page_buffer;
        //Increase the page version before every page flush back.
//        assert(STRUCT_OFFSET(DataPage, hdr.p_version) == STRUCT_OFFSET(LeafPage<char COMMA char>, hdr.p_version));
//        ((DataPage*)page_buffer->addr)->hdr.p_version++;
        post_gl_page_local_mr.addr = reinterpret_cast<void*>((uint64_t)page_buffer->addr + STRUCT_OFFSET(LeafPage<int>, hdr));
        page_size -=  STRUCT_OFFSET(LeafPage<int>, hdr);
        assert(remote_lock_addr <= post_gl_page_addr - 8);
        bool async_succeed = false;

        // Page write back shall never utilize async unlock. because we can not guarantee, whether the page will be overwritten by
        // another thread before the unlock. It is possible this cache buffer is reused by other cache entry.
        if (async){
//            Prepare_WR_Write(sr[0], sge[0], post_gl_page_addr, &post_gl_page_local_mr, page_size, 0, Regular_Page);
            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            *(uint64_t*) local_CAS_mr->addr = 0;
            //TODO: Can we make the RDMA unlock based on RDMA FAA? In this case, we can use async
            // lock releasing to reduce the RDMA ROUND trips in the protocol
            volatile uint64_t compare = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            volatile uint64_t substract = (~compare) + 1;
            volatile uint64_t add = ((uint64_t)next_holder_id/2 +100) << 56;
#if ASYNC_PLAN == 1
            // The code below is to prevent a work request overflow in the send queue, since we enable
            // async lock releasing.
            Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(page_addr.nodeID)->Get();
            if (UNLIKELY(!tasks)){
                tasks = new Async_Tasks();
                async_tasks[page_addr.nodeID]->Reset(tasks);
            }
            uint32_t* counter = &tasks->counter;
            std::string qp_type = "write_local_flush";
//            std::string qp_type = "default";
            // Every sync unlock submit 2 requests, and we need to reserve another one work request for the RDMA locking which
            // contains one async lock acquiring.
            if ( UNLIKELY(*counter >= ATOMIC_OUTSTANDING_SIZE  - 3)){
                Prepare_WR_FAA(sr[0], sge[0], remote_lock_addr, local_CAS_mr, substract + add, IBV_SEND_SIGNALED, Regular_Page);
//                sr[0].next = &sr[1];

                *(uint64_t *)local_CAS_mr->addr = 0;
                assert(page_addr.nodeID == remote_lock_addr.nodeID);
                Batch_Submit_WRs(sr, 1, page_addr.nodeID, qp_type);
//                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (add >> 56));
#ifndef NDEBUG


                uint64_t initial_old_cas = (*(uint64_t*) local_CAS_mr->addr);
                if(((*(uint64_t*) local_CAS_mr->addr) >> 56) != (compare >> 56)){
                    size_t count = 0;

                retry_check:
                    count++;
                    spin_wait_us(100);
                    //RDMA read the latch word again and see if it is the same as the compare value.
                    RDMA_Read(remote_lock_addr, local_CAS_mr, 8, IBV_SEND_SIGNALED,1, Regular_Page);
                    if(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (compare >> 56)){
                        printf("Nodeid %u RDMA write handover move too fast, resulting in spurious latch word mismatch\n", node_id);
                        fflush(stdout);
                        if (count >100){
                            assert(false);
                        }
                        goto retry_check;
                    }
//                    assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (add >> 56));

                    //                goto retry;
                }
#endif
                *counter = 0;
            }else{
                ibv_mr* async_cas = tasks->mrs[*counter];
                Prepare_WR_FAA(sr[0], sge[0], remote_lock_addr, async_cas, substract + add, 0, Regular_Page);
//                sr[0].next = &sr[1];
                *(uint64_t *)async_cas->addr = 0;
                assert(page_addr.nodeID == remote_lock_addr.nodeID);
                Batch_Submit_WRs(sr, 0, page_addr.nodeID, qp_type);
                *counter = *counter + 1;
                async_succeed = true;
            }
            //TODO: it could be spuriously failed because of the FAA.so we can not have async
#else
                Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(page_addr.nodeID)->Get();
                if (UNLIKELY(!tasks)){
                    tasks = new Async_Tasks();
                    async_tasks[page_addr.nodeID]->Reset(tasks);
                }
                auto async_cas = tasks->enqueue(this, page_addr);
                assert(async_cas);
                std::string qp_type = "write_local_flush";
                *(uint64_t *)async_cas->addr = 0;
                RDMA_FAA(remote_lock_addr, async_cas, substract + add, IBV_SEND_SIGNALED, 0, Regular_Page, qp_type);
                async_succeed = true;
#endif

        }else{
//#ifndef NDEBUG
            uint64_t retry_cnt = 0;
//#endif

            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            retry:
//#ifndef NDEBUG
            if (retry_cnt++ >5000 && retry_cnt % 1000 == 0){
                printf("RDMA write lock unlock keep spinning but never release, the return value is %lu\n", (*(uint64_t*) local_CAS_mr->addr) );
            }
//#endif

            //TODO: check whether the page's global lock is still write lock
            //  0909/2023: the code below seems sometime will get stuck.
//            Prepare_WR_Write(sr[0], sge[0], post_gl_page_addr, &post_gl_page_local_mr, page_size, 0, Regular_Page);

            *(uint64_t *)local_CAS_mr->addr = 0;
            //TODO: THe RDMA write unlock can not be guaranteed to be finished after the page write.
            // The RDMA CAS be started strictly after the RDMA write at the remote NIC according to
            // https://docs.nvidia.com/networking/display/MLNXOFEDv451010/Out-of-Order+%28OOO%29+Data+Placement+Experimental+Verbs
            // So it's better to make Unlock another RDMA CAS.
//        rdma_mg->RDMA_CAS( remote_lock_addr, local_CAS_mr, 1,0, IBV_SEND_SIGNALED,1, Internal_and_Leaf);
//        assert(*(uint64_t *)local_CAS_mr->addr == 1);
            //We can apply async unlock here to reduce the latency.
//            uint64_t swap = 0;
            uint64_t compare = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            volatile uint64_t substract = (~compare) + 1;
            volatile uint64_t add = ((uint64_t)next_holder_id/2 +100) << 56;

            //TODO: USE rdma faa to release the write lock to avoid continuous spurious unlock resulting from the concurrent read lock request.
            Prepare_WR_FAA(sr[0], sge[0], remote_lock_addr, local_CAS_mr, substract + add, IBV_SEND_SIGNALED, Regular_Page);

//            Prepare_WR_CAS(sr[1], sge[1], remote_lock_addr, local_CAS_mr, compare,swap, IBV_SEND_SIGNALED, Internal_and_Leaf);
//            sr[0].next = &sr[1];



            assert(page_addr.nodeID == remote_lock_addr.nodeID);
            Batch_Submit_WRs(sr, 1, page_addr.nodeID);
#ifndef NDEBUG
            uint64_t initial_old_cas = (*(uint64_t*) local_CAS_mr->addr);
            if(((*(uint64_t*) local_CAS_mr->addr) >> 56) != (compare >> 56)){
                assert(false);
                size_t count = 0;

            retry_check2:
                count++;
                spin_wait_us(100);
                //RDMA read the latch word again and see if it is the same as the compare value.
                RDMA_Read(remote_lock_addr, local_CAS_mr, 8, IBV_SEND_SIGNALED,1, Regular_Page);
                if(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (compare >> 56)){
                    printf("Nodeid %u RDMA write handover move too fast, resulting in spurious latch word mismatch\n", node_id);
                    fflush(stdout);
                    if (count >100){
                        assert(false);
                    }
                    goto retry_check2;
                }
//                    assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (add >> 56));

                //                goto retry;
            }
#endif

//            if((*(uint64_t*) local_CAS_mr->addr) != compare){
////                printf("RDMA write lock unlock happen with RDMA faa FOR rdma READ LOCK\n");
//                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (compare >> 56));
//
//                goto retry;
//            }

        }
//        printf("Release write lock for %lu\n",page_addr);
//        assert(page_addr == (((LeafPage<int COMMA int>*)(page_buffer->addr))->hdr.this_page_g_ptr));
        return async_succeed;
    }


    bool RDMA_Manager::global_write_page_and_WdowntoR(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                                      GlobalAddress remote_lock_addr, uint8_t next_holder_id,
                                                      bool async,
                                                      Cache_Handle *handle) {

        //TODO: If we want to use async unlock, we need to enlarge the max outstand work request that the queue pair support.
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];
        GlobalAddress tbFlushed_gaddr{};
        ibv_mr tbFlushed_local_mr = *page_buffer;
        auto page = (LeafPage<uint64_t>*)(page_buffer->addr);
        assert(STRUCT_OFFSET(LeafPage<int>, hdr.dirty_upper_bound) == STRUCT_OFFSET(LeafPage<char>, hdr.dirty_upper_bound));
        if (page->hdr.dirty_upper_bound == 0){
            assert(page->hdr.dirty_lower_bound == 0);
            tbFlushed_gaddr.nodeID = page_addr.nodeID;
            //The header should be the same offset in Leaf or INternal nodes
            assert(STRUCT_OFFSET(LeafPage<int>, hdr) == STRUCT_OFFSET(LeafPage<char>, hdr));
            assert(STRUCT_OFFSET(InternalPage<int>, hdr) == STRUCT_OFFSET(LeafPage<int>, hdr));
            assert(STRUCT_OFFSET(DataPage, hdr) == STRUCT_OFFSET(LeafPage<char>, hdr));
            tbFlushed_gaddr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<int>, hdr);
            //Increase the page version before every page flush back.
            tbFlushed_local_mr.addr = reinterpret_cast<void*>((uint64_t)page_buffer->addr + STRUCT_OFFSET(LeafPage<int>, hdr));
            page_size -=  STRUCT_OFFSET(LeafPage<int>, hdr);
        }else{
            assert(page->hdr.dirty_lower_bound >= sizeof(uint64_t ));
            tbFlushed_gaddr.nodeID = page_addr.nodeID;
            tbFlushed_gaddr.offset = page_addr.offset + page->hdr.dirty_lower_bound;
            tbFlushed_local_mr.addr = reinterpret_cast<void*>((uint64_t)page_buffer->addr + page->hdr.dirty_lower_bound);
            page_size = page->hdr.dirty_upper_bound - page->hdr.dirty_lower_bound;
            page->hdr.dirty_lower_bound = 0;
            page->hdr.dirty_upper_bound = 0;
        }

        size_t send_flags = (page_size <= MAX_INLINE_SIZE) ? IBV_SEND_INLINE:0;
//        assert(send_flags == 0);
        bool async_succeed = false;
        // Page write back shall never utilize async unlock. because we can not guarantee, whether the page will be overwritten by
        // another thread before the unlock. It is possible this cache buffer is reused by other cache entry.
        if (async){
//            assert(false);
            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            *(uint64_t*) local_CAS_mr->addr = 0;
            //TODO: Can we make the RDMA unlock based on RDMA FAA? In this case, we can use async
            // lock releasing to reduce the RDMA ROUND trips in the protocol
            volatile uint64_t this_node_exclusive = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            volatile uint64_t this_node_shared = (1ull << (RDMA_Manager::node_id/2 +1));
            volatile uint64_t inv_sender_shared = (1ull << (next_holder_id/2 +1));
            assert(this_node_shared != inv_sender_shared);

            volatile uint64_t substract = (~(this_node_exclusive)) + 1;
            volatile uint64_t add = this_node_shared + inv_sender_shared;
#if ASYNC_PLAN == 1
            // The code below is to prevent a work request overflow in the send queue, since we enable
            // async lock releasing.
            Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(page_addr.nodeID)->Get();
            if (UNLIKELY(!tasks)){
                tasks = new Async_Tasks();
                async_tasks[page_addr.nodeID]->Reset(tasks);
            }
            std::string qp_type = "write_local_flush";
//            std::string qp_type = "default";

            uint32_t* counter = &tasks->counter;
            // Every sync unlock submit 2 requests, and we need to reserve another one work request for the RDMA locking which
            // contains one async lock acquiring.
            if ( UNLIKELY(*counter >= ATOMIC_OUTSTANDING_SIZE  - 3)){
                Prepare_WR_Write(sr[0], sge[0], tbFlushed_gaddr, &tbFlushed_local_mr, page_size, send_flags, Regular_Page);
                Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, local_CAS_mr, substract +add, IBV_SEND_SIGNALED, Regular_Page);
                sr[0].next = &sr[1];

                *(uint64_t *)local_CAS_mr->addr = 0;
                assert(page_addr.nodeID == remote_lock_addr.nodeID);
                Batch_Submit_WRs(sr, 1, page_addr.nodeID, qp_type);

                *counter = 0;
            }else{

                ibv_mr* async_cas = tasks->mrs[*counter];
                ibv_mr* async_buf = tasks->mrs[*counter+1];
                memcpy(async_buf->addr, tbFlushed_local_mr.addr, page_size);
                Prepare_WR_Write(sr[0], sge[0], tbFlushed_gaddr, async_buf, page_size, send_flags, Regular_Page);
                Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, async_cas, substract +add, 0, Regular_Page);
                sr[0].next = &sr[1];
                *(uint64_t *)async_cas->addr = 0;
                assert(page_addr.nodeID == remote_lock_addr.nodeID);
                Batch_Submit_WRs(sr, 0, page_addr.nodeID, qp_type);
                *counter = *counter + 2;
                async_succeed = true;
            }
#else
            Async_Tasks * tasks = (Async_Tasks *)async_tasks.at(page_addr.nodeID)->Get();
            if (UNLIKELY(!tasks)){
                tasks = new Async_Tasks();
                async_tasks[page_addr.nodeID]->Reset(tasks);
            }
//            uint32_t* counter = &tasks->counter;
            auto async_cas = tasks->enqueue(this, tbFlushed_gaddr);
            auto async_buf = tasks->enqueue(this, tbFlushed_gaddr);
            memcpy(async_buf->addr, tbFlushed_local_mr.addr, page_size);
            assert(async_cas);
            assert(async_buf);
            std::string qp_type = "write_local_flush";
            Prepare_WR_Write(sr[0], sge[0], tbFlushed_gaddr, async_buf, page_size, send_flags|IBV_SEND_SIGNALED, Regular_Page);
            Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, async_cas, substract + add, IBV_SEND_SIGNALED, Regular_Page);
            *(uint64_t *)async_cas->addr = 0;
            Batch_Submit_WRs(sr, 0, page_addr.nodeID, qp_type);
            async_succeed = true;
#endif
//            printf("Release write lock for %lu\n",page_addr);
            //TODO: it could be spuriously failed because of the FAA.so we can not have async
        }else{
//            printf("This code path shall not be entered\n");

//#ifndef NDEBUG
            uint64_t retry_cnt = 0;
//#endif

//        rdma_mg->RDMA_Write(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED ,1, Internal_and_Leaf);
            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            retry:
//#ifndef NDEBUG
//            if (retry_cnt++ >5000 && retry_cnt % 1000 == 0){
//                printf("RDMA write lock unlock keep spinning but never release, the return value is %lu\n", (*(uint64_t*) local_CAS_mr->addr) );
//            }
//#endif

            //TODO: check whether the page's global lock is still write lock
            //  0909/2023: the code below seems sometime will get stuck.
            Prepare_WR_Write(sr[0], sge[0], tbFlushed_gaddr, &tbFlushed_local_mr, page_size, send_flags, Regular_Page);

            *(uint64_t *)local_CAS_mr->addr = 0;
            volatile uint64_t this_node_exclusive = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            volatile uint64_t this_node_shared = (1ull << (RDMA_Manager::node_id/2 +1));
            volatile uint64_t inv_sender_shared = (1ull << (next_holder_id/2 +1));
            assert(this_node_shared != inv_sender_shared);

            volatile uint64_t substract = (~(this_node_exclusive)) + 1;
            volatile uint64_t add = this_node_shared + inv_sender_shared;
            //TODO: USE rdma faa to release the write lock to avoid continuous spurious unlock resulting from the concurrent read lock request.
            Prepare_WR_FAA(sr[1], sge[1], remote_lock_addr, local_CAS_mr, substract + add, IBV_SEND_SIGNALED, Regular_Page);
            sr[0].next = &sr[1];
            assert(page_addr.nodeID == remote_lock_addr.nodeID);
            Batch_Submit_WRs(sr, 1, page_addr.nodeID);
#ifndef NDEBUG
            if(((*(uint64_t*) local_CAS_mr->addr) >> 56) != (this_node_exclusive >> 56)){
                auto old_cas = (*(uint64_t*) local_CAS_mr->addr);
                usleep(40);
                //RDMA read the latch word again and see if it is the same as the compare value.
                RDMA_Read(remote_lock_addr, local_CAS_mr, 8, IBV_SEND_SIGNALED,1, Regular_Page);
//                assert((*(uint64_t*)local_CAS_mr->addr & inv_sender_shared) != 0);
                assert((*(uint64_t*)local_CAS_mr->addr & this_node_shared) != 0);
                assert(((*(uint64_t*)local_CAS_mr->addr) >> 56) == 0);
//                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == add >> 56);
                printf("Nodeid %u RDMA write to write handover move too fast, resulting in spurious latch word mismatch\n", RDMA_Manager::node_id);
                fflush(stdout);
                //                goto retry;
            }
#endif


        }
//        printf("Atomic lock downgrade %lu\n",page_addr);
//        assert(page_addr == (((LeafPage<int COMMA int>*)(page_buffer->addr))->hdr.this_page_g_ptr));
        return async_succeed;
    }
    void RDMA_Manager::global_write_tuple_and_Wunlock(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
                                                     GlobalAddress remote_lock_addr, CoroContext *cxt, int coro_id, bool async) {

        //TODO: If we want to use async unlock, we need to enlarge the max outstand work request that the queue pair support.
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];

        if (async){
            assert(false);
            Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 0, Regular_Page);
            ibv_mr* local_CAS_mr = Get_local_CAS_mr();
            *(uint64_t*) local_CAS_mr->addr = 0;
            //TODO 1: Make the unlocking based on RDMA CAS.
            //TODO 2: implement a retry mechanism based on RDMA CAS. THe write unlock can be failed because the RDMA FAA test and reset the lock words.
            uint64_t swap = 0;
            uint64_t compare = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            Prepare_WR_CAS(sr[1], sge[1], remote_lock_addr, local_CAS_mr, compare, swap, 0, Regular_Page);
            sr[0].next = &sr[1];


            *(uint64_t *)local_CAS_mr->addr = 0;
            assert(page_addr.nodeID == remote_lock_addr.nodeID);
            Batch_Submit_WRs(sr, 0, page_addr.nodeID);
            //TODO: it could be spuriously failed because of the FAA.so we can not have async
        }else{



//        rdma_mg->RDMA_Write(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED ,1, Internal_and_Leaf);
            ibv_mr* local_CAS_mr = Get_local_CAS_mr();

retry:

            //TODO: check whether the page's global lock is still write lock
            Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 0, Regular_Page);
            *(uint64_t *)local_CAS_mr->addr = 0;
            //TODO: THe RDMA write unlock can not be guaranteed to be finished after the page write.
            // The RDMA CAS be started strictly after the RDMA write at the remote NIC according to
            // https://docs.nvidia.com/networking/display/MLNXOFEDv451010/Out-of-Order+%28OOO%29+Data+Placement+Experimental+Verbs
            // So it's better to make Unlock another RDMA CAS.
//        rdma_mg->RDMA_CAS( remote_lock_addr, local_CAS_mr, 1,0, IBV_SEND_SIGNALED,1, Internal_and_Leaf);
//        assert(*(uint64_t *)local_CAS_mr->addr == 1);
            //We can apply async unlock here to reduce the latency.
            uint64_t swap = 0;
            uint64_t compare = ((uint64_t)RDMA_Manager::node_id/2 + 100) << 56;
            Prepare_WR_CAS(sr[1], sge[1], remote_lock_addr, local_CAS_mr, compare, swap, IBV_SEND_SIGNALED, Regular_Page);
            sr[0].next = &sr[1];



            assert(page_addr.nodeID == remote_lock_addr.nodeID);
            Batch_Submit_WRs(sr, 1, page_addr.nodeID);
            if((*(uint64_t*) local_CAS_mr->addr) != compare){
                assert(((*(uint64_t*) local_CAS_mr->addr) >> 56) == (compare >> 56));
                goto retry;
            }

        }
//        printf("Global write tuple page_addr %p, async %d\n", page_addr, async);

    }

// int RDMA_Manager::post_atomic(int opcode)
//{
//  struct ibv_send_wr sr;
//  struct ibv_sge sge;
//  struct ibv_send_wr* bad_wr = NULL;
//  int rc;
//  extern int msg_size;
//  /* prepare the scatter/gather entry */
//  memset(&sge, 0, sizeof(sge));
//  sge.addr = (uintptr_t)res->send_buf;
//  sge.length = msg_size;
//  sge.lkey = res->mr_receive->lkey;
//  /* prepare the send work request */
//  memset(&sr, 0, sizeof(sr));
//  sr.next = NULL;
//  sr.wr_id = 0;
//  sr.sg_list = &sge;
//  sr.num_sge = 1;
//  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
//  sr.send_flags = IBV_SEND_SIGNALED;
//  if (opcode != IBV_WR_SEND)
//  {
//    sr.wr.rdma.remote_addr = res->mem_regions.addr;
//    sr.wr.rdma.rkey = res->mem_regions.rkey;
//  }
//  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
//  //*(start) = std::chrono::steady_clock::now();
//  //start = std::chrono::steady_clock::now();
//  rc = ibv_post_send(res->qp, &sr, &bad_wr);
//  if (rc)
//    fprintf(stderr, "failed to post SR\n");
//  else
//  {
//    /*switch (opcode)
//    {
//    case IBV_WR_SEND:
//            fprintf(stdout, "Send Request was posted\n");
//            break;
//    case IBV_WR_RDMA_READ:
//            fprintf(stdout, "RDMA Read Request was posted\n");
//            break;
//    case IBV_WR_RDMA_WRITE:
//            fprintf(stdout, "RDMA Write Request was posted\n");
//            break;
//    default:
//            fprintf(stdout, "Unknown Request was posted\n");
//            break;
//    }*/
//  }
//  return rc;
//}

int RDMA_Manager::post_send(ibv_mr* mr, std::string qp_type, size_t size,
                            uint16_t target_node_id) {
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  //  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)mr->addr;
  sge.length = size;
  sge.lkey = mr->lkey;
  //  }
  //  else {
  //    /* prepare the scatter/gather entry */
  //    memset(&sge, 0, sizeof(sge));
  //    sge.addr = (uintptr_t)res->send_buf;
  //    sge.length = size;
  //    sge.lkey = res->mr_send->lkey;
  //  }

  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
  sr.send_flags = IBV_SEND_SIGNALED;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();

//  if (rdma_config.server_name)
//    rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
//  else
//    rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
  ibv_qp* qp;
  if (qp_type == "default"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
    l.unlock();
  }
#ifndef NDEBUG
  if (rc)
    fprintf(stderr, "failed to post SR\n");
  else {
    fprintf(stdout, "Send Request was posted\n");
  }
#endif
  return rc;
}
int RDMA_Manager::post_send(ibv_mr** mr_list, size_t sge_size,
                            std::string qp_type, uint16_t target_node_id) {
  struct ibv_send_wr sr;
  struct ibv_sge sge[sge_size];
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  //  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */
  for (size_t i = 0; i < sge_size; i++) {
    memset(&sge[i], 0, sizeof(sge));
    sge[i].addr = (uintptr_t)mr_list[i]->addr;
    sge[i].length = mr_list[i]->length;
    sge[i].lkey = mr_list[i]->lkey;
  }

  //  }
  //  else {
  //    /* prepare the scatter/gather entry */
  //    memset(&sge, 0, sizeof(sge));
  //    sge.addr = (uintptr_t)res->send_buf;
  //    sge.length = size;
  //    sge.lkey = res->mr_send->lkey;
  //  }

  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = sge;
  sr.num_sge = sge_size;
  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
  sr.send_flags = IBV_SEND_SIGNALED;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();

//  if (rdma_config.server_name)
//    rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
//  else
//    rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
  ibv_qp* qp;
  if (qp_type == "default"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
    l.unlock();
  }
#ifndef NDEBUG
  if (rc)
    fprintf(stderr, "failed to post SR\n");
  else {
    fprintf(stdout, "Send Request was posted\n");
  }
#endif
  return rc;
}
int RDMA_Manager::post_receive(ibv_mr** mr_list, size_t sge_size,
                               std::string qp_type, uint16_t target_node_id) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge[sge_size];
  struct ibv_recv_wr* bad_wr;
  int rc;
  //  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */

  for (size_t i = 0; i < sge_size; i++) {
    memset(&sge[i], 0, sizeof(sge));
    sge[i].addr = (uintptr_t)mr_list[i]->addr;
    sge[i].length = mr_list[i]->length;
    sge[i].lkey = mr_list[i]->lkey;
  }

  //  }
  //  else {
  //    /* prepare the scatter/gather entry */
  //    memset(&sge, 0, sizeof(sge));
  //    sge.addr = (uintptr_t)res->receive_buf;
  //    sge.length = size;
  //    sge.lkey = res->mr_receive->lkey;
  //  }

  /* prepare the receive work request */
  memset(&rr, 0, sizeof(rr));
  rr.next = NULL;
  rr.wr_id = 0;
  rr.sg_list = sge;
  rr.num_sge = sge_size;
  /* post the Receive Request to the RQ */
//  if (rdma_config.server_name)
//    rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
//  else
//    rc = ibv_post_recv(res->qp_map[qp_id], &rr, &bad_wr);
  ibv_qp* qp;
  if (qp_type == "default"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_recv(res->qp_map.at(target_node_id), &rr, &bad_wr);
    l.unlock();
  }
  if (rc)
    fprintf(stderr, "failed to post RR\n");
  else
    fprintf(stdout, "Receive Request was posted\n");
  return rc;
}
int RDMA_Manager::post_receive_xcompute(ibv_mr *mr, uint16_t target_node_id, int num_of_qp) {
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr* bad_wr;
    int rc;
    //    /* prepare the scatter/gather entry */

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)mr->addr;
    assert(mr->length != 0);
//    printf("The length of the mr is %lu", mr->length);
    sge.length = mr->length;
    sge.lkey = mr->lkey;

    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
    /* post the Receive Request to the RQ */
    ibv_qp* qp;
    {
        qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[num_of_qp]);
    }

    rc = ibv_post_recv(qp, &rr, &bad_wr);

    return rc;
}
    
int RDMA_Manager::post_receive(ibv_mr* mr, std::string qp_type, size_t size,
                               uint16_t target_node_id) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge;
  struct ibv_recv_wr* bad_wr;
  int rc;
  //  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */

  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)mr->addr;
  sge.length = size;
  sge.lkey = mr->lkey;

  //  }
  //  else {
  //    /* prepare the scatter/gather entry */
  //    memset(&sge, 0, sizeof(sge));
  //    sge.addr = (uintptr_t)res->receive_buf;
  //    sge.length = size;
  //    sge.lkey = res->mr_receive->lkey;
  //  }

  /* prepare the receive work request */
  memset(&rr, 0, sizeof(rr));
  rr.next = NULL;
  rr.wr_id = 0;
  rr.sg_list = &sge;
  rr.num_sge = 1;
  /* post the Receive Request to the RQ */
//  if (rdma_config.server_name)
//    rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
//  else
//    rc = ibv_post_recv(res->qp_map[q_id], &rr, &bad_wr);
  ibv_qp* qp;
  if (qp_type == "default"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_recv(res->qp_map.at(target_node_id), &rr, &bad_wr);
    l.unlock();
  }
  if (rc)
    fprintf(stderr, "failed to post RR\n");
  else
    fprintf(stdout, "Receive Request was posted\n");
  return rc;
}
/* poll_completion */
/******************************************************************************
* Function: poll_completion
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, 1 on failure
*
* Description
* Poll the completion queue for a single event. This function will continue to
* poll the queue until MAX_POLL_CQ_TIMEOUT milliseconds have passed.
*
******************************************************************************/
int RDMA_Manager::poll_completion(ibv_wc* wc_p, int num_entries,
                                  std::string qp_type, bool send_cq,
                                  uint16_t target_node_id) {
  // unsigned long start_time_msec;
  // unsigned long cur_time_msec;
  // struct timeval cur_time;
  int poll_result;
  int poll_num = 0;
  int rc = 0;
  ibv_cq* cq;
  ibv_qp* qp;
  /* poll the completion for a while before giving up of doing it .. */
  // gettimeofday(&cur_time, NULL);
  // start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
  std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (qp_type == "write_local_flush"){
    cq = (ibv_cq*)cq_local_write_flush.at(target_node_id)->Get();
    qp = (ibv_qp*)qp_local_write_flush.at(target_node_id)->Get();
    assert(cq != nullptr);
  }else if (qp_type == "write_local_compact"){
    cq = (ibv_cq*)cq_local_write_compact.at(target_node_id)->Get();
    qp = (ibv_qp*)qp_local_write_compact.at(target_node_id)->Get();
//    cq = ((CQ_Map*)cq_local_write_compact->Get())->at(shard_target_node_id);
//    cq = static_cast<ibv_cq*>(cq_local_write_compact->Get());
    assert(cq != nullptr);

  }else if (qp_type == "default"){
    cq = (ibv_cq*)cq_data_default.at(target_node_id)->Get();
    qp = (ibv_qp*)qp_data_default.at(target_node_id)->Get();
//    cq = ((CQ_Map*)cq_data_default->Get())->at(shard_target_node_id);
//    cq = static_cast<ibv_cq*>(cq_data_default->Get());
    assert(cq != nullptr);
  }
  else{
//    assert(res->cq_map.contains());
    if (send_cq)
      cq = res->cq_map.at(target_node_id).first;
      
    else
      cq = res->cq_map.at(target_node_id).second;
    assert(cq != nullptr);
    qp = res->qp_map.at(target_node_id);
  }
  l.unlock();
  size_t count = 0;
  do {
    poll_result = ibv_poll_cq(cq, num_entries, &wc_p[poll_num]);
    if (poll_result < 0)
      break;
    else
      poll_num = poll_num + poll_result;
      
      ibv_qp_attr qp_init_attr;
      ibv_qp_init_attr qp_state;
      if (count++ > 1000000){
          ibv_query_qp(qp, &qp_init_attr, IBV_QP_STATE, &qp_state);
          assert(qp_init_attr.qp_state == IBV_QPS_RTS);
      }
    
    /*gettimeofday(&cur_time, NULL);
    cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);*/
  } while (poll_num < num_entries);  // && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
  //*(end) = std::chrono::steady_clock::now();
  // end = std::chrono::steady_clock::now();
  assert(poll_num == num_entries);
  if (poll_result < 0) {
    /* poll CQ failed */
    fprintf(stderr, "poll CQ failed\n");
      assert(false);
    rc = 1;
  } else if (poll_result == 0) { /* the CQ is empty */
      assert(false);

      fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
    rc = 1;
  } else {
    /* CQE found */
    // fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
    /* check the completion status (here we don't care about the completion opcode */
    for (auto i = 0; i < num_entries; i++) {
      if (wc_p[i].status !=
          IBV_WC_SUCCESS)  // TODO:: could be modified into check all the entries in the array
      {
        fprintf(stderr,
                "Node %d number %d got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
                node_id, i, wc_p[i].status, wc_p[i].vendor_err);
        assert(false);
        rc = 1;
      }
    }
  }
//        if (target_node_id == 1 && qp_type == "default"){
//            printf("poll a completion through default queuepair\n");
//        }

//        printf("Get a completion from queue\n");
  return rc;
}

int RDMA_Manager::try_poll_completions(ibv_wc* wc_p,
                                                   int num_entries,
                                                   std::string& qp_type,
                                                   bool send_cq,
                                                   uint16_t target_node_id) {
  int poll_result = 0;
  int poll_num = 0;
  ibv_cq* cq;
  /* poll the completion for a while before giving up of doing it .. */
  // gettimeofday(&cur_time, NULL);
  // start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (qp_type == "write_local_flush"){
    cq = (ibv_cq*)cq_local_write_flush.at(target_node_id)->Get();
    assert(cq != nullptr);
  }else if (qp_type == "write_local_compact"){
    cq = (ibv_cq*)cq_local_write_compact.at(target_node_id)->Get();
    //    cq = ((CQ_Map*)cq_local_write_compact->Get())->at(shard_target_node_id);
    //    cq = static_cast<ibv_cq*>(cq_local_write_compact->Get());
    assert(cq != nullptr);

  }else if (qp_type == "default"){
    cq = (ibv_cq*)cq_data_default.at(target_node_id)->Get();
    //    cq = ((CQ_Map*)cq_data_default->Get())->at(shard_target_node_id);
    //    cq = static_cast<ibv_cq*>(cq_data_default->Get());
    assert(cq != nullptr);
  }
  else{
    if (send_cq)
      cq = res->cq_map.at(target_node_id).first;
    else
      cq = res->cq_map.at(target_node_id).second;
    assert(cq != nullptr);
  }

  poll_result = ibv_poll_cq(cq, num_entries, &wc_p[poll_num]);
#ifndef NDEBUG
  if (poll_result > 0){
      for (int i = 0; i < poll_result; ++i) {
          if (wc_p[i].status !=IBV_WC_SUCCESS)
          {
              fprintf(stderr,
                      "Node %d number %d got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
                      node_id, poll_result-1, wc_p[poll_result-1].status, wc_p[poll_result-1].vendor_err);
              assert(false);
          }
      }

//      printf("Get a completion from try queue\n");

  }
#endif
  return poll_result;
}

int RDMA_Manager::try_poll_completions_xcompute(ibv_wc *wc_p, int num_entries, bool send_cq, uint16_t target_node_id,
                                                int num_of_cp) {
    assert(target_node_id%2 == 0);
    int poll_result = 0;
    int poll_num = 0;
    /* poll the completion for a while before giving up of doing it .. */
    // gettimeofday(&cur_time, NULL);
    // start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    ibv_cq* cq;
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    if (send_cq)
        cq = (*cq_xcompute.at(target_node_id))[num_of_cp*2];
    else
        cq = (*cq_xcompute.at(target_node_id))[num_of_cp*2+1];
    l.unlock();
    poll_result = ibv_poll_cq(cq, num_entries, &wc_p[poll_num]);
#ifndef NDEBUG
    if (poll_result > 0){
        if (wc_p[poll_result-1].status !=
            IBV_WC_SUCCESS)  // TODO:: could be modified into check all the entries in the array
        {
            fprintf(stderr,
                    "Node %d number %d got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
                    node_id, poll_result-1, wc_p[poll_result-1].status, wc_p[poll_result-1].vendor_err);
            assert(false);
        }
    }
#endif
    return poll_result;
}
/******************************************************************************
* Function: print_config
*
* Input
* none
*
* Output
* none
*
* Returns
* none
*
* Description
* Print out config information
******************************************************************************/
void RDMA_Manager::print_config() {
  fprintf(stdout, " ------------------------------------------------\n");
  fprintf(stdout, " Device name : \"%s\"\n", rdma_config.dev_name);
  fprintf(stdout, " IB port : %u\n", rdma_config.ib_port);
  if (rdma_config.server_name)
    fprintf(stdout, " IP : %s\n", rdma_config.server_name);
  fprintf(stdout, " TCP port : %u\n", rdma_config.tcp_port);
  if (rdma_config.gid_idx >= 0)
    fprintf(stdout, " GID index : %u\n", rdma_config.gid_idx);
  fprintf(stdout, " ------------------------------------------------\n\n");
}

/******************************************************************************
* Function: usage
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* print a description of command line syntax
******************************************************************************/
void RDMA_Manager::usage(const char* argv0) {
  fprintf(stdout, "Usage:\n");
  fprintf(stdout, " %s start a server and wait for connection\n", argv0);
  fprintf(stdout, " %s <host> connect to server at <host>\n", argv0);
  fprintf(stdout, "\n");
  fprintf(stdout, "Options:\n");
  fprintf(
      stdout,
      " -p, --port <port> listen on/connect to port <port> (default 18515)\n");
  fprintf(
      stdout,
      " -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
  fprintf(stdout,
          " -i, --ib-port <port> use port <port> of IB device (default 1)\n");
  fprintf(stdout,
          " -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
}

bool RDMA_Manager::Remote_Memory_Register(size_t size, uint16_t target_node_id, Chunk_type pool_name) {
//  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  // register the memory block from the remote memory

  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
  ibv_mr receive_mr = {};
  Allocate_Local_RDMA_Slot(send_mr, Message);
  Allocate_Local_RDMA_Slot(receive_mr, Message);
  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = create_mr_128MB_;
  send_pointer->content.mem_size = size;
  send_pointer->buffer = receive_mr.addr;
  send_pointer->rkey = receive_mr.rkey;

  RDMA_Reply* receive_pointer;
  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the polling.
  *receive_pointer = {};
  post_send<RDMA_Request>(&send_mr, target_node_id, std::string("main"));
  ibv_wc wc[2] = {};


  if (poll_completion(wc, 1, std::string("main"), true, target_node_id)){
    fprintf(stderr, "failed to poll send for remote memory register\n");
    return false;
  }
//  asm volatile ("sfence\n" : : );
//  asm volatile ("lfence\n" : : );
//  asm volatile ("mfence\n" : : );
  printf("Remote memory registeration at node %u, size: %zu\n", target_node_id, size);
  poll_reply_buffer(receive_pointer); // poll the receive for 2 entires
  printf("polled reply buffer\n");
  auto* temp_pointer = new ibv_mr();
  // Memory leak?, No, the ibv_mr pointer will be push to the remote mem pool,
  // Please remember to delete it when diregistering mem region from the remote memory
  *temp_pointer = receive_pointer->content.mr;  // create a new ibv_mr for storing the new remote memory region handler
    std::map<uint16_t, std::map<void *, In_Use_Array *> *>* Bitmap_map;
    std::map<uint16_t, std::vector<ibv_mr *> *>* remote_mem_pool;
    uint64_t chunk_size = 0;
  switch (pool_name) {
        case Chunk_type::Regular_Page:
            Bitmap_map = &Remote_Leaf_Node_Bitmap;
            remote_mem_pool = &remote_mem_leaf_pool;
            chunk_size = name_to_chunksize.at(pool_name);
            break;
        case Chunk_type::DeltaChunk:
            Bitmap_map = &Remote_Delta_Bitmap;
            remote_mem_pool = &remote_mem_delta_pool;
            chunk_size = name_to_chunksize.at(pool_name);

            break;
        default:
            assert(false);
    }
  remote_mem_pool->at(target_node_id)->push_back(
      temp_pointer);  // push the new pointer for the new ibv_mr (different from the receive buffer) to remote_mem_pool
    assert(temp_pointer->length == size);

    //put the rkey in the rkey map
//    rkey_map_data[pool_name] = temp_pointer->rkey;
  // push the bitmap of the new registed buffer to the bitmap vector in resource.
  int placeholder_num =
      static_cast<int>(temp_pointer->length) /
      (chunk_size);  // here we supposing the SSTables are 4 megabytes
  In_Use_Array* in_use_array = new In_Use_Array(placeholder_num, chunk_size, temp_pointer);
  //    std::unique_lock l(remote_pool_mutex);
    Bitmap_map->at(target_node_id)->insert({temp_pointer->addr, in_use_array});
  //    l.unlock();
  //  l.unlock();
  Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
  Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
  return true;
}

Page_Forward_Reply_Type
RDMA_Manager::Writer_Invalidate_Modified_RPC(GlobalAddress global_ptr, ibv_mr *page_buffer, uint16_t target_node_id,
                                             uint8_t &starv_level, uint64_t &retry_cnt) {
//    printf(" send write invalidation message to other nodes %p\n", global_ptr);
    RDMA_Request* send_pointer;
    ibv_mr* send_mr = Get_local_send_message_mr();
    ibv_mr* recv_mr = page_buffer;
//    clear_page_forward_flag(static_cast<char *>(page_buffer->addr));

//    ibv_mr* receive_mr = {};


//    send_pointer->buffer = receive_mr.addr;
//    send_pointer->rkey = receive_mr.rkey;

    Page_Forward_Reply_Type* receive_pointer;
    receive_pointer = (Page_Forward_Reply_Type*)((char*)page_buffer->addr + kLeafPageSize - sizeof(Page_Forward_Reply_Type));
    //Clear the reply buffer for the polling.
    *receive_pointer = waiting;
#ifndef NDEBUG
    memset(page_buffer->addr, 0, page_buffer->length);
#endif
    //USE static ticket to minuimize the conflict.
    int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
    send_pointer = (RDMA_Request*)send_mr->addr;
    send_pointer->command = writer_invalidate_modified;
    send_pointer->content.inv_message.pending_reminder = false;
    bool was_pending = false;
inv_resend:
    if(retry_cnt < 20){
//                port::AsmVolatilePause();
        //do nothing
    }else if (retry_cnt <40){
        starv_level = 1;

    }else if (retry_cnt <80){
        starv_level = 2;
    }
    else if (retry_cnt <160){
        starv_level = 3;
    }else if (retry_cnt <200){
        starv_level = 4;
    } else if (retry_cnt <1000){
        starv_level = 5;
    } else{
        starv_level = 255 > 5+ retry_cnt/1000? (5+ retry_cnt/1000): 255;
    }

    send_pointer->content.inv_message.page_addr = global_ptr;
    send_pointer->content.inv_message.starvation_level = starv_level;
    send_pointer->buffer = recv_mr->addr;
    send_pointer->rkey = recv_mr->rkey;
    //TODO: no need to be signaled, can make it without completion.
    post_send_xcompute(send_mr, target_node_id, qp_id, sizeof(RDMA_Request));
    ibv_wc wc[2] = {};
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );

    while(*receive_pointer == waiting){
        _mm_clflush(receive_pointer);
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
    }
    if (*receive_pointer == pending){
        was_pending = true;
        TimeMeasurer timer;
        timer.StartTimer();
        timer.EndTimer();
        uint64_t  count = 0;
        while(*receive_pointer == pending){
            _mm_clflush(receive_pointer);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
            if(count++ > 10){
                timer.EndTimer();
                count = 0;
            }
            if (starv_level <= 2 ){
                if (timer.GetElapsedMicroSeconds() > 13){
                    send_pointer->content.inv_message.pending_reminder = true;
                    goto inv_resend;
                }

            }else if (starv_level <= 4){
                if (timer.GetElapsedMicroSeconds() > 9){
                    send_pointer->content.inv_message.pending_reminder = true;
                    goto inv_resend;
                }
            }else if (starv_level <= 6){
                if (timer.GetElapsedMicroSeconds() > 7){
                    send_pointer->content.inv_message.pending_reminder = true;
                    goto inv_resend;
                }
            }else if (starv_level <= 12){
                if (timer.GetElapsedMicroSeconds() > 4){
                    send_pointer->content.inv_message.pending_reminder = true;
                    goto inv_resend;
                }
            }else{
                if (timer.GetElapsedMicroSeconds() > 2){
                    send_pointer->content.inv_message.pending_reminder = true;
                    goto inv_resend;
                }
            }
        }
    }
    if (was_pending && *receive_pointer == dropped){
        auto page = (LeafPage<uint64_t>*)(page_buffer->addr);
//        printf("Inv message send from NOde %u to Node %u over data %p was pending and then get dropped\n", node_id, target_node_id, global_ptr);
//        fflush(stdout);
        assert(page->hdr.this_page_g_ptr == GlobalAddress::Null());
        assert(page->hdr.p_type == P_Plain);
    }
    assert(*receive_pointer == processed || *receive_pointer == dropped);
    return *receive_pointer;

}

    Page_Forward_Reply_Type
    RDMA_Manager::Reader_Invalidate_Modified_RPC(GlobalAddress global_ptr, ibv_mr *page_mr, uint16_t target_node_id,
                                                 uint8_t &starv_level, uint64_t &retry_cnt) {
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = Get_local_send_message_mr();
        ibv_mr* recv_mr = page_mr;
//    clear_page_forward_flag(static_cast<char *>(page_buffer->addr));

//    ibv_mr* receive_mr = {};


//    send_pointer->buffer = receive_mr.addr;
//    send_pointer->rkey = receive_mr.rkey;

        Page_Forward_Reply_Type* receive_pointer;
        receive_pointer = (Page_Forward_Reply_Type*)((char*)page_mr->addr + kLeafPageSize - sizeof(Page_Forward_Reply_Type));
        //Clear the reply buffer for the polling.
        *receive_pointer = waiting;
//#ifndef NDEBUG
//        memset(page_mr->addr, 0, page_mr->length);
//#endif
        //USE static ticket to minuimize the conflict.
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = reader_invalidate_modified;
        send_pointer->content.inv_message.pending_reminder = false;
        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
        inv_resend:
        if(retry_cnt < 20){
//                port::AsmVolatilePause();
            //do nothing
        }else if (retry_cnt <40){
            starv_level = 1;

        }else if (retry_cnt <80){
            starv_level = 2;
        }
        else if (retry_cnt <160){
            starv_level = 3;
        }else if (retry_cnt <200){
            starv_level = 4;
        } else if (retry_cnt <1000){
            starv_level = 5;
        } else{
            starv_level = 255 > 5+ retry_cnt/1000? (5+ retry_cnt/1000): 255;
        }

        send_pointer->content.inv_message.page_addr = global_ptr;
        send_pointer->content.inv_message.starvation_level = starv_level;
        send_pointer->buffer = recv_mr->addr;
        send_pointer->rkey = recv_mr->rkey;
        //TODO: no need to be signaled, can make it without completion.
        post_send_xcompute(send_mr, target_node_id, qp_id, sizeof(RDMA_Request));
        ibv_wc wc[2] = {};
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );

        while(*receive_pointer == waiting){
            _mm_clflush(receive_pointer);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );

        }
        if (*receive_pointer == pending){
            TimeMeasurer timer;
            timer.StartTimer();
            timer.EndTimer();
            uint64_t  count = 0;
            while(*receive_pointer == pending){
                _mm_clflush(receive_pointer);
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
                if(count++ > 10){
                    timer.EndTimer();
                    count = 0;
                }
                if (starv_level <= 2 ){
                    if (timer.GetElapsedMicroSeconds() > 13){
                        send_pointer->content.inv_message.pending_reminder = true;
                        goto inv_resend;
                    }

                }else if (starv_level <= 4){
                    if (timer.GetElapsedMicroSeconds() > 9){
                        send_pointer->content.inv_message.pending_reminder = true;
                        goto inv_resend;
                    }
                }else if (starv_level <= 6){
                    if (timer.GetElapsedMicroSeconds() > 7){
                        send_pointer->content.inv_message.pending_reminder = true;
                        goto inv_resend;
                    }
                }else if (starv_level <= 12){
                    if (timer.GetElapsedMicroSeconds() > 4){
                        send_pointer->content.inv_message.pending_reminder = true;
                        goto inv_resend;
                    }
                }else{
                    if (timer.GetElapsedMicroSeconds() > 2){
                        send_pointer->content.inv_message.pending_reminder = true;
                        goto inv_resend;
                    }
                }
            }
        }

        assert(*receive_pointer == processed || *receive_pointer == dropped);
        return *receive_pointer;
    }

    bool RDMA_Manager::Writer_Invalidate_Shared_RPC(GlobalAddress g_ptr, uint16_t target_node_id, uint8_t starv_level,
                                                    uint8_t pos) {
        assert(target_node_id!= node_id);
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = Get_local_send_message_mr();
        ibv_mr* recv_mr = Get_local_read_mr();
        assert(recv_mr->length >= sizeof(Page_Forward_Reply_Type)*56);
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = writer_invalidate_shared;
        send_pointer->content.inv_message.page_addr = g_ptr;
        send_pointer->content.inv_message.starvation_level = starv_level;
//        send_pointer->content.inv_message.p_version = page_version;
        send_pointer->buffer = (char*)recv_mr->addr + pos*sizeof(Page_Forward_Reply_Type);
        send_pointer->rkey = recv_mr->rkey;


        Page_Forward_Reply_Type* receive_pointer;
    receive_pointer = (Page_Forward_Reply_Type*)((char*)recv_mr->addr + pos*sizeof(Page_Forward_Reply_Type));
        //Clear the reply buffer for the polling.
    *receive_pointer = waiting;

        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;

        post_send_xcompute(send_mr, target_node_id, qp_id, sizeof(RDMA_Request));
        ibv_wc wc[2] = {};
        assert(send_pointer->command!= create_qp_);
        // Check the completion outside this function
#ifndef PARALLEL_INVALIDATION
//        if (poll_completion_xcompute(wc, 1, std::string("main"), true, target_node_id, qp_id)){
//            fprintf(stderr, "failed to poll send for remote memory register\n");
//            return false;
//        }
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
        // TODO: this read invalidaiton messages reply shall be polled together
        poll_reply_buffer(receive_pointer); // poll the receive for 2 entires
#endif
        return true;
    }

    void RDMA_Manager::Sync_Create_Delta_Section_RPC(GlobalAddress ds_ptr, uint8_t compute_node_id) {
        RDMA_Request* send_pointer;
//        ibv_mr send_mr = {};
//    ibv_mr receive_mr = {};

        ibv_mr* send_mr = Get_local_send_message_mr();
//        Allocate_Local_RDMA_Slot(send_mr, Message);
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = broadcast_create_ds;
        send_pointer->content.create_ds.ds_gaddr = ds_ptr;
        send_pointer->content.create_ds.compute_node_id = compute_node_id;
        for(auto iter: compute_nodes){
            if (iter.first == node_id){
                continue;
            }
            int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
            post_send_xcompute(send_mr, iter.first, qp_id, sizeof(RDMA_Request));
        }
    }
    void RDMA_Manager::Writer_Invalidate_Shared_RPC_Reply(int num_of_poll){
        ibv_mr* recv_mr = Get_local_read_mr();
        Page_Forward_Reply_Type* receive_pointer;
        std::vector<int> history;
        for (int i = 0; i < num_of_poll; ++i) {
            history.push_back(i);
            receive_pointer = (Page_Forward_Reply_Type*)((char*)recv_mr->addr + i*sizeof(Page_Forward_Reply_Type));
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
            poll_reply_buffer(receive_pointer); // poll the receive for 2 entires
        }
    }
    bool
    RDMA_Manager::Tuple_Read_2PC_RPC(uint16_t target_node_id, uint64_t primary_key, size_t table_id, size_t tuple_size,
                                     char *&tuple_buffer, size_t access_type, bool log_enabled) {
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = Get_local_send_message_mr();
        ibv_mr* recv_mr = Get_local_read_mr();
        tuple_buffer = (char*)recv_mr->addr;
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = tuple_read_2pc;
        send_pointer->content.tuple_info.primary_key = primary_key;
        send_pointer->content.tuple_info.table_id = table_id;
        send_pointer->content.tuple_info.thread_id = thread_id;
        send_pointer->content.tuple_info.tuple_size = tuple_size;
        send_pointer->content.tuple_info.log_enabled = log_enabled;
        send_pointer->content.tuple_info.access_type = access_type;
        send_pointer->buffer = recv_mr->addr;
        send_pointer->rkey = recv_mr->rkey;


        RDMA_ReplyXCompute* receive_pointer = (RDMA_ReplyXCompute*)((char*)recv_mr->addr + tuple_size);
        //Clear the reply buffer for the polling.
        memset(recv_mr->addr, 0, recv_mr->length);
//        *receive_pointer = {};

        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;

        post_send_xcompute(send_mr, target_node_id, qp_id, sizeof(RDMA_Request));
        ibv_wc wc[2] = {};
        assert(send_pointer->command!= create_qp_);
//        printf("Tuple read request sent from %u to %u\n", node_id, target_node_id);
//        fflush(stdout);
////         Check the completion outside this function
//        if (poll_completion_xcompute(wc, 1, std::string("main"), true, target_node_id, qp_id)){
//            fprintf(stderr, "failed to poll send for remote memory register\n");
//            return false;
//        }
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
        volatile uint8_t * check_byte = (uint8_t*)&receive_pointer->toPC_reply_type;
        while(!*check_byte){
            _mm_clflush(check_byte);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
        }
        if (receive_pointer->toPC_reply_type == 1){
            return true;
        }else{
            return false;
        }

    }
    bool RDMA_Manager::Prepare_2PC_RPC(uint16_t target_node_id, bool log_enabled) {
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = Get_local_send_message_mr();
        ibv_mr* recv_mr = Get_local_read_mr();
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = prepare_2pc;
        send_pointer->content.prepare.thread_id = thread_id;
        send_pointer->content.prepare.log_enabled = log_enabled;
        send_pointer->buffer = recv_mr->addr;
        send_pointer->rkey = recv_mr->rkey;


        RDMA_ReplyXCompute* receive_pointer = (RDMA_ReplyXCompute*)recv_mr->addr;
        //Clear the reply buffer for the polling.
        *receive_pointer = {};

        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;

        post_send_xcompute(send_mr, target_node_id, qp_id, sizeof(RDMA_Request));
        ibv_wc wc[2] = {};
        assert(send_pointer->command!= create_qp_);
//        printf("Prepare request sent from %u to %u\n", node_id, target_node_id);
//        fflush(stdout);
////         Check the completion outside this function
//        if (poll_completion_xcompute(wc, 1, std::string("main"), true, target_node_id, qp_id)){
//            fprintf(stderr, "failed to poll send for remote memory register\n");
//            return false;
//        }
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
        volatile uint8_t * check_byte = (uint8_t*)&receive_pointer->toPC_reply_type;
        while(!*check_byte){
            _mm_clflush(check_byte);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
        }
        if (receive_pointer->toPC_reply_type == 1){
            return true;
        }else{
            return false;
        }

    }
    bool RDMA_Manager::Commit_2PC_RPC(uint16_t target_node_id, bool log_enabled) {
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = Get_local_send_message_mr();
        ibv_mr* recv_mr = Get_local_read_mr();
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = commit_2pc;
        send_pointer->content.commit.thread_id = thread_id;
        send_pointer->content.commit.log_enabled = log_enabled;

        send_pointer->buffer = recv_mr->addr;
        send_pointer->rkey = recv_mr->rkey;


        RDMA_ReplyXCompute* receive_pointer = (RDMA_ReplyXCompute*)recv_mr->addr;
        //Clear the reply buffer for the polling.
        *receive_pointer = {};

        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;

        post_send_xcompute(send_mr, target_node_id, qp_id, sizeof(RDMA_Request));
        ibv_wc wc[2] = {};
        assert(send_pointer->command!= create_qp_);
//        printf("Commit request sent from %u to %u\n", node_id, target_node_id);
//        fflush(stdout);
////         Check the completion outside this function
//        if (poll_completion_xcompute(wc, 1, std::string("main"), true, target_node_id, qp_id)){
//            fprintf(stderr, "failed to poll send for remote memory register\n");
//            return false;
//        }
        return true;
    }
    bool RDMA_Manager::Abort_2PC_RPC(uint16_t target_node_id, bool log_enabled) {
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = Get_local_send_message_mr();
        ibv_mr* recv_mr = Get_local_read_mr();
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = abort_2pc;
        send_pointer->content.abort.thread_id = thread_id;
        send_pointer->content.abort.log_enabled = log_enabled;


        send_pointer->buffer = recv_mr->addr;
        send_pointer->rkey = recv_mr->rkey;


        RDMA_ReplyXCompute* receive_pointer = (RDMA_ReplyXCompute*)recv_mr->addr;
        //Clear the reply buffer for the polling.
        *receive_pointer = {};

        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;

        post_send_xcompute(send_mr, target_node_id, qp_id, sizeof(RDMA_Request));
        ibv_wc wc[2] = {};
        assert(send_pointer->command!= create_qp_);
//        printf("Abort request sent from %u to %u\n", node_id, target_node_id);
//        fflush(stdout);
////         Check the completion outside this function
//        if (poll_completion_xcompute(wc, 1, std::string("main"), true, target_node_id, qp_id)){
//            fprintf(stderr, "failed to poll send for remote memory register\n");
//            return false;
//        }
        return true;

    }

    bool RDMA_Manager::Send_heart_beat() {
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = Get_local_send_message_mr();
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = heart_beat;


//    send_pointer->buffer = receive_mr.addr;
//    send_pointer->rkey = receive_mr.rkey;
//    RDMA_Reply* receive_pointer;
        uint16_t target_memory_node_id = 1;
        //Use node 1 memory node as the place to store the temporary QP information
        post_send<RDMA_Request>(send_mr, target_memory_node_id, std::string("main"));
        ibv_wc wc[2] = {};
        if (poll_completion(wc, 1, std::string("main"),
                            true, target_memory_node_id)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
            fprintf(stderr, "failed to poll send for remote memory register\n");
            assert(false);
        }
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
        return true;
    }
    bool RDMA_Manager::Send_heart_beat_xcompute(uint16_t target_memory_node_id) {
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = Get_local_send_message_mr();
        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = heart_beat;


//    send_pointer->buffer = receive_mr.addr;
//    send_pointer->rkey = receive_mr.rkey;
//    RDMA_Reply* receive_pointer;

        //Use node 1 memory node as the place to store the temporary QP information
        post_send_xcompute(send_mr, target_memory_node_id, 0, sizeof(RDMA_Request));

        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
        return true;
    }


    bool RDMA_Manager::Remote_Query_Pair_Connection(std::string& qp_type,
                                                uint16_t target_node_id) {
  ibv_qp* qp = create_qp(target_node_id, false, qp_type, ATOMIC_OUTSTANDING_SIZE, 1);// No need for receive queue.

  union ibv_gid my_gid;
  int rc;
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);

    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return false;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
//  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  // lock should be here because from here on we will modify the send buffer.
  // TODO: Try to understand whether this kind of memcopy without serialization is correct.
  // Could be wrong on different machine, because of the alignment
  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
  ibv_mr receive_mr = {};
  Allocate_Local_RDMA_Slot(send_mr, Message);
  Allocate_Local_RDMA_Slot(receive_mr, Message);
  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = create_qp_;
  send_pointer->content.qp_config.qp_num = qp->qp_num;
//  fprintf(stdout, "\nQP num to be sent = 0x%x\n", qp->qp_num);
  send_pointer->content.qp_config.lid = res->port_attr.lid;
  memcpy(send_pointer->content.qp_config.gid, &my_gid, 16);
//  fprintf(stdout, "Local LID = 0x%x\n", res->port_attr.lid);
  send_pointer->buffer = receive_mr.addr;
  send_pointer->rkey = receive_mr.rkey;
  RDMA_Reply* receive_pointer;
  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the polling.
  *receive_pointer = {};
//  post_receive<registered_qp_config>(res->mr_receive, std::string("main"));
  post_send<RDMA_Request>(&send_mr, target_node_id, std::string("main"));
  ibv_wc wc[2] = {};
  //  while(wc.opcode != IBV_WC_RECV){
  //    poll_completion(&wc);
  //    if (wc.status != 0){
  //      fprintf(stderr, "Work completion status is %d \n", wc.status);
  //    }
  //
  //  }
  //  assert(wc.opcode == IBV_WC_RECV);
  if (poll_completion(wc, 1, std::string("main"),
                      true, target_node_id)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
    fprintf(stderr, "failed to poll send for remote memory register\n");
    return false;
  }
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  poll_reply_buffer(receive_pointer); // poll the receive for 2 entires
  Registered_qp_config* temp_buff = new Registered_qp_config(receive_pointer->content.qp_config);
  std::shared_lock<std::shared_mutex> l1(qp_cq_map_mutex);
  if (qp_type == "default" )
    local_read_qp_info.at(target_node_id)->Reset(temp_buff);
//    ((QP_Info_Map*)local_read_qp_info->Get())->insert({shard_target_node_id, temp_buff});
//    local_read_qp_info->Reset(temp_buff);
  else if(qp_type == "write_local_compact")
    local_write_compact_qp_info.at(target_node_id)->Reset(temp_buff);
//    ((QP_Info_Map*)local_write_compact_qp_info->Get())->insert({shard_target_node_id, temp_buff});
//    local_write_compact_qp_info->Reset(temp_buff);
  else if(qp_type == "write_local_flush")
    local_write_flush_qp_info.at(target_node_id)->Reset(temp_buff);
//    ((QP_Info_Map*)local_write_flush_qp_info->Get())->insert({shard_target_node_id, temp_buff});
//    local_write_flush_qp_info->Reset(temp_buff);
  else
    res->qp_main_connection_info.insert({target_node_id,temp_buff});
  l1.unlock();
//  fprintf(stdout, "Remote QP number=0x%x\n", temp_buff->qp_num);
//  fprintf(stdout, "Remote LID = 0x%x\n", temp_buff->lid);
  // te,p_buff will have the informatin for the remote query pair,
  // use this information for qp connection.
  connect_qp(qp, qp_type, target_node_id);
  Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
  Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
  return true;
  //  // sync the communication by rdma.
  //  post_receive<registered_qp_config>(receive_pointer, std::string("main"));
  //  post_send<computing_to_memory_msg>(send_pointer, std::string("main"));
  //  if(!poll_completion(wc, 2, std::string("main"))){
  //    return true;
  //  }else
  //    return false;
}

void RDMA_Manager::Allocate_Remote_RDMA_Slot(ibv_mr &remote_mr, Chunk_type pool_name, uint16_t target_node_id) {
    std::map<uint16_t, std::map<void *, In_Use_Array *> *>* Bitmap_map;
    std::map<uint16_t, std::vector<ibv_mr *> *>* remote_mem_pool;
    uint64_t chunk_size = 0;
    switch (pool_name) {
        case Chunk_type::Regular_Page:
            Bitmap_map = &Remote_Leaf_Node_Bitmap;
            remote_mem_pool = &remote_mem_leaf_pool;
            chunk_size = name_to_chunksize.at(pool_name);;
            break;
        case Chunk_type::DeltaChunk:
            Bitmap_map = &Remote_Delta_Bitmap;
            remote_mem_pool = &remote_mem_delta_pool;
            chunk_size = name_to_chunksize.at(pool_name);
            break;
        default:
            assert(false);
    }
        // If the Remote buffer is empty, register one from the remote memory.
        //  remote_mr = new ibv_mr;
        if (Bitmap_map->at(target_node_id)->empty()) {
            // this lock is to prevent the system register too much remote memory at the
            // begginning.
            std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_mutex);
            if (Bitmap_map->at(target_node_id)->empty()) {
                Remote_Memory_Register(define::Alloc_Granu, target_node_id, pool_name);
            }
            mem_write_lock.unlock();
        }
        std::shared_lock<std::shared_mutex> mem_read_lock(remote_mem_mutex);
        auto ptr = Bitmap_map->at(target_node_id)->begin();

        while (ptr != Bitmap_map->at(target_node_id)->end()) {
            // iterate among all the remote memory region
            // find the first empty SSTable Placeholder's iterator, iterator->first is ibv_mr* second is the bool vector for this ibv_mr*. Each ibv_mr is the origin block get from the remote memory. The memory was divided into chunks with size == SSTable size.
            int sst_index = ptr->second->allocate_memory_slot();
            if (sst_index >= 0) {
                remote_mr = *((ptr->second)->get_mr_ori());
                remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) +
                                                    sst_index * chunk_size);
                remote_mr.length = chunk_size;

//        remote_data_mrs->fname = file_name;
//        remote_data_mrs->map_pointer =
//          (ptr->second).get_mr_ori();  // it could be confused that the map_pointer is for the memtadata deletion
// so that we can easily find where to deallocate our RDMA buffer. The key is a pointer to ibv_mr.
//      remote_data_mrs->file_size = 0;
//      DEBUG_arg("Allocate Remote pointer %p",  remote_mr.addr);
                return;
            } else
                ptr++;
        }
        mem_read_lock.unlock();
        // If not find remote buffers are all used, allocate another remote memory region.
        std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_mutex);
        Remote_Memory_Register(define::Alloc_Granu, target_node_id, pool_name);
        //  fs_meta_save();
        ibv_mr* mr_last;
        mr_last = remote_mem_pool->at(target_node_id)->back();
        int sst_index = Bitmap_map->at(target_node_id)->at(mr_last->addr)->allocate_memory_slot();
        assert(sst_index >= 0);
        mem_write_lock.unlock();

        //  sst_meta->mr = new ibv_mr();
        remote_mr = *(mr_last);
        remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) +
                                            sst_index * chunk_size);
        remote_mr.length = chunk_size;
   }

GlobalAddress RDMA_Manager::Allocate_Remote_RDMA_Slot(Chunk_type pool_name, uint16_t target_node_id) {
    std::map<uint16_t, std::map<void *, In_Use_Array *> *>* Bitmap_map;
    std::map<uint16_t, std::vector<ibv_mr *> *>* remote_mem_pool;
    uint64_t chunk_size = 0;
    switch (pool_name) {
        case Chunk_type::Regular_Page:
            Bitmap_map = &Remote_Leaf_Node_Bitmap;
            remote_mem_pool = &remote_mem_leaf_pool;
            chunk_size = name_to_chunksize.at(pool_name);;
            break;
        case Chunk_type::DeltaChunk:
            Bitmap_map = &Remote_Delta_Bitmap;
            remote_mem_pool = &remote_mem_delta_pool;
            chunk_size = name_to_chunksize.at(pool_name);
            break;
        default:
            assert(false);
    }
    // If the Remote buffer is empty, register one from the remote memory.
    //  remote_mr = new ibv_mr;
    if (Bitmap_map->at(target_node_id)->empty()) {
        // this lock is to prevent the system register too much remote memory at the
        // begginning.
        std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_mutex);
        if (Bitmap_map->at(target_node_id)->empty()) {
            Remote_Memory_Register(define::Alloc_Granu, target_node_id, pool_name);
          //      fs_meta_save();
        }
        mem_write_lock.unlock();
    }
    std::shared_lock<std::shared_mutex> mem_read_lock(remote_mem_mutex);
    auto ptr = Bitmap_map->at(target_node_id)->begin();
    GlobalAddress ret;
    ibv_mr remote_mr;
    while (ptr != Bitmap_map->at(target_node_id)->end()) {
        // iterate among all the remote memory region
        // find the first empty SSTable Placeholder's iterator, iterator->first is ibv_mr* second is the bool vector for this ibv_mr*. Each ibv_mr is the origin block get from the remote memory. The memory was divided into chunks with size == SSTable size.
        int sst_index = ptr->second->allocate_memory_slot();
        assert(ptr->second->get_chunk_size() == chunk_size);
        if(pool_name == Chunk_type::DeltaChunk){
            assert(chunk_size == 10485760);
        }
        if (sst_index >= 0) {

          remote_mr = *((ptr->second)->get_mr_ori());
          remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) +
                                              sst_index * chunk_size);
          remote_mr.length = chunk_size;
          ret.nodeID = target_node_id;
          ret.offset = static_cast<char*>(remote_mr.addr) - (char*)base_addr_map_data[target_node_id];
            assert(ret.offset<69055800320ull);
          return ret;
        } else
          ptr++;
    }
    mem_read_lock.unlock();
    // If not find remote buffers are all used, allocate another remote memory region.
    std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_mutex);
    //Not necessaryly be the last one
    // the pulled mr may not belong to this bitmap, need to fix it.
    ibv_mr* mr_last = remote_mem_pool->at(target_node_id)->back();
    int sst_index = -1;
    In_Use_Array* last_element = Bitmap_map->at(target_node_id)->at(mr_last->addr);
    assert(last_element->get_chunk_size() == chunk_size);
    if (last_element->get_chunk_size() == chunk_size){
        sst_index = last_element->allocate_memory_slot();
    }else{
        assert(false);
    }
    if (sst_index>=0){
        remote_mr = *(last_element->get_mr_ori());
        remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) +
                                            sst_index * chunk_size);
        remote_mr.length = chunk_size;
        ret.nodeID = target_node_id;
        ret.offset = static_cast<char*>(remote_mr.addr) - (char*)base_addr_map_data[target_node_id];
        assert(ret.offset<69055800320ull);
        return ret;
    }else{
        Remote_Memory_Register(define::Alloc_Granu, target_node_id, pool_name);
        //  fs_meta_save();
        //  ibv_mr* mr_last;
        mr_last = remote_mem_pool->at(target_node_id)->back();
        sst_index = Bitmap_map->at(target_node_id)->at(mr_last->addr)->allocate_memory_slot();
        assert(sst_index >= 0);
        mem_write_lock.unlock();

        //  sst_meta->mr = new ibv_mr();
        remote_mr = *(mr_last);
        remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) +
                                            sst_index * chunk_size);
        remote_mr.length = chunk_size;
        ret.nodeID = target_node_id;
        ret.offset = static_cast<char*>(remote_mr.addr) - (char*)base_addr_map_data[target_node_id];
        //    remote_data_mrs->fname = file_name;
        //    remote_data_mrs->map_pointer = mr_last;
        //  DEBUG_arg("Allocate Remote pointer %p",  remote_mr.addr);
        assert(ret.offset<69055800320ull);
        return ret;
    }
}
// A function try to allocate RDMA registered local memory
// TODO: implement sharded allocators by cpu_core_id, when allocate a memory use the core
// id to reduce the contention, when deallocate a memory search the allocator to deallocate.
void RDMA_Manager::Allocate_Local_RDMA_Slot(ibv_mr& mr_input,
                                            Chunk_type pool_name) {

        // allocate the RDMA slot is seperate into two situation, read and write.
        size_t chunk_size;
        retry:
        std::shared_lock<std::shared_mutex> mem_read_lock(local_mem_mutex);
        chunk_size = name_to_chunksize.at(pool_name);
        if (name_to_mem_pool.at(pool_name).empty()) {
            mem_read_lock.unlock();
            std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_mutex);
            if (name_to_mem_pool.at(pool_name).empty()) {
                ibv_mr* mr;
                char* buff;
                // the developer can define how much memory cna one time RDMA allocation get.
                Local_Memory_Register(&buff, &mr,
                                      name_to_allocated_size.at(pool_name) == 0 ?
                                      1024*1024*1024:name_to_allocated_size.at(pool_name), pool_name);
                if (node_id%2 == 0)
                    printf("Memory used up, Initially, allocate new one, memory pool is %s, total memory this pool is %lu\n",
                           EnumStrings[pool_name], name_to_mem_pool.at(pool_name).size());
            }
            mem_write_lock.unlock();
            mem_read_lock.lock();
        }
//  std::shared_lock<std::shared_mutex> mem_read_lock(local_mem_mutex);
        auto ptr = name_to_mem_pool.at(pool_name).begin();

        while (ptr != name_to_mem_pool.at(pool_name).end()) {
            size_t region_chunk_size = ptr->second->get_chunk_size();
            if (region_chunk_size != chunk_size) {
                ptr++;
                continue;
            }
            int block_index = ptr->second->allocate_memory_slot();
            if (block_index >= 0) {
//      mr_input = new ibv_mr();
                //      map_pointer = (ptr->second).get_mr_ori();
                mr_input = *((ptr->second)->get_mr_ori());
                mr_input.addr = static_cast<void*>(static_cast<char*>(mr_input.addr) +
                                                   block_index * chunk_size);
                mr_input.length = chunk_size;
//      DEBUG_arg("Allocate pointer %p", mr_input.addr);
                return;
            } else
                ptr++;
        }
        mem_read_lock.unlock();
        // if not find available Local block buffer then allocate a new buffer. then
        // pick up one buffer from the new Local memory region.
        // TODO:: It could happen that the local buffer size is not enough, need to reallocate a new buff again,
        // TODO:: Because there are two many thread going on at the same time.


        std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_mutex);
        // The other threads may have already allocate a large chunk of memory. first check
        // the last chunk bit mapm and if it is full then allocate new big chunk of memory.
        ibv_mr* mr_last = local_mem_regions.back();
        int block_index = -1;
        In_Use_Array* last_element;
        // If other thread pool is allocated during the lock waiting, then the last element chunck size is not the target chunck size
        // in this thread. Optimisticall, we need to search the map again, but we directly allocate a new one for simplicity.
        if (name_to_mem_pool.at(pool_name).find(mr_last->addr) != name_to_mem_pool.at(pool_name).end()){
            last_element = name_to_mem_pool.at(pool_name).at(mr_last->addr);
            block_index = last_element->allocate_memory_slot();
        }
        if( block_index>=0){

            mr_input = *(last_element->get_mr_ori());
            mr_input.addr = static_cast<void*>(static_cast<char*>(mr_input.addr) +
                                               block_index * chunk_size);
            mr_input.length = chunk_size;

            return;
        }else{
            ibv_mr* mr_to_allocate = new ibv_mr();
            char* buff = new char[chunk_size];
            Local_Memory_Register(&buff, &mr_to_allocate,name_to_allocated_size.at(pool_name) == 0 ?
                                                         1024*1024*1024:name_to_allocated_size.at(pool_name), pool_name);
            if (node_id%2 == 0)
                printf("Memory used up, allocate new one, memory pool is %s, total memory is %lu\n",
                       EnumStrings[pool_name], Calculate_size_of_pool(Regular_Page) +
                                               Calculate_size_of_pool(Message));
            block_index = name_to_mem_pool.at(pool_name)
                    .at(mr_to_allocate->addr)
                    ->allocate_memory_slot();
            mem_write_lock.unlock();
            assert(block_index >= 0);
            //    mr_input = new ibv_mr();
            //    map_pointer = mr_to_allocate;
            mr_input = *(mr_to_allocate);
            mr_input.addr = static_cast<void*>(static_cast<char*>(mr_input.addr) +
                                               block_index * chunk_size);
            mr_input.length = chunk_size;
            //    DEBUG_arg("Allocate pointer %p", mr_input.addr);
            //  mr_input.fname = file_name;
            return;

        }
}


size_t RDMA_Manager::Calculate_size_of_pool(Chunk_type pool_name) {
  size_t Sum = 0;
  Sum = name_to_mem_pool.at(pool_name).size()
        *name_to_allocated_size.at(pool_name);
  return Sum;
}
//void RDMA_Manager::BatchGarbageCollection(uint64_t* ptr, size_t size) {
//  for (int i = 0; i < size/ sizeof(uint64_t); ++i) {
////    assert()
//    bool result = Deallocate_Local_RDMA_Slot((void*)ptr[i], FlushBuffer);
//    assert(result);
////#ifndef NDEBUG
////    printf("Sucessfully delete a SSTable %p", (void*)ptr[i]);
////    assert(result);
////#endif
//  }
//}

// Remeber to delete the mr because it was created be new, otherwise memory leak.
bool RDMA_Manager::Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                              Chunk_type buffer_type) {
  size_t buff_offset =
      static_cast<char*>(mr->addr) - static_cast<char*>(map_pointer->addr);
  size_t chunksize = name_to_chunksize.at(buffer_type);
  assert(buff_offset % chunksize == 0);
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
  return name_to_mem_pool.at(buffer_type)
      .at(map_pointer->addr)
      ->deallocate_memory_slot(buff_offset / chunksize);
}
bool RDMA_Manager::Deallocate_Local_RDMA_Slot(void* p, Chunk_type buff_type) {
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
#ifndef NDEBUG
//    assert(*(uint64_t*)p == 1);
    *(uint64_t*)p = 0;
#endif
//  DEBUG_arg("Deallocate pointer %p\n", p);
  std::map<void*, In_Use_Array*>* Bitmap;
  Bitmap = &name_to_mem_pool.at(buff_type);
  auto mr_iter = Bitmap->upper_bound(p);
  if (mr_iter == Bitmap->begin()) {
    return false;
  } else if (mr_iter == Bitmap->end()) {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      assert(buff_offset / mr_iter->second->get_chunk_size() <= std::numeric_limits<int>::max());
      bool status = mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
      assert(status);
      return status;
    }
    else
      return false;
  } else {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);

      bool status = mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
      assert(status);
      return status;
    }
    else
      return false;

  }
  return false;
}
bool RDMA_Manager::Deallocate_Remote_RDMA_Slot(void* p,
                                               uint16_t target_node_id) {
//  DEBUG_arg("Delete Remote pointer %p", p);
  std::shared_lock<std::shared_mutex> read_lock(remote_mem_mutex);
  std::map<void*, In_Use_Array*>* Bitmap;
  Bitmap = Remote_Leaf_Node_Bitmap.at(target_node_id);
  auto mr_iter = Bitmap->upper_bound(p);
  if (mr_iter == Bitmap->begin()) {
    return false;
  } else if (mr_iter == Bitmap->end()) {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      bool status = mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
      assert(status);
      return status;
    }
    else
      return false;
  } else {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      bool status = mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
      assert(status);
      return status;
    }else{
      return false;
    }

  }
  return false;
}
// bool RDMA_Manager::Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta)  {
//
//  int buff_offset = static_cast<char*>(sst_meta->mr->addr) -
//                    static_cast<char*>(sst_meta->map_pointer->addr);
//  assert(buff_offset % Table_Size == 0);
//#ifndef NDEBUG
////  std::cout <<"Chunk deallocate at" << sst_meta->mr->addr << "index: " << buff_offset/Table_Size << std::endl;
//#endif
//  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
//  return Remote_Leaf_Node_Bitmap->at(sst_meta->map_pointer->addr)
//      .deallocate_memory_slot(buff_offset / Table_Size);
//}

bool RDMA_Manager::CheckInsideLocalBuff(
    void* p,
    std::_Rb_tree_iterator<std::pair<void* const, In_Use_Array>>& mr_iter,
    std::map<void*, In_Use_Array>* Bitmap) {
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
  if (Bitmap != nullptr) {
    mr_iter = Bitmap->upper_bound(p);
    if (mr_iter == Bitmap->begin()) {
      return false;
    } else if (mr_iter == Bitmap->end()) {
      mr_iter--;
      size_t buff_offset =
          static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
      //      assert(buff_offset>=0);
      if (buff_offset < mr_iter->second.get_mr_ori()->length)
        return true;
      else
        return false;
    } else {
      size_t buff_offset =
          static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
      //      assert(buff_offset>=0);
      if (buff_offset < mr_iter->second.get_mr_ori()->length) return true;
    }
  } else {
    // TODO: Implement a iteration to check that address in all the mempool, in case that the block size has been changed.
    return false;
  }
  return false;
}
bool RDMA_Manager::CheckInsideRemoteBuff(void* p, uint16_t target_node_id) {
  std::shared_lock<std::shared_mutex> read_lock(remote_mem_mutex);
  std::map<void*, In_Use_Array*>* Bitmap;
  Bitmap = Remote_Leaf_Node_Bitmap.at(target_node_id);
  auto mr_iter = Bitmap->upper_bound(p);
  if (mr_iter == Bitmap->begin()) {
    return false;
  } else if (mr_iter == Bitmap->end()) {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      return true;
    }
    else
      return false;
  } else {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      return true;
    }else{
      return false;
    }

  }
  return false;
}
bool RDMA_Manager::Mempool_initialize(Chunk_type pool_name, size_t chunk_size,
                                      size_t allocat_num_per_time) {

  if (name_to_mem_pool.find(pool_name) != name_to_mem_pool.end()) return false;

  std::map<void*, In_Use_Array*> mem_sub_pool;
  // check whether pool name has already exist.
  name_to_mem_pool.insert(std::pair<Chunk_type, std::map<void*, In_Use_Array*>>(
      {pool_name, mem_sub_pool}));
  name_to_chunksize.insert({pool_name, chunk_size});
  name_to_allocated_size.insert({pool_name, allocat_num_per_time});
  return true;
}
// serialization for Memory regions
void RDMA_Manager::mr_serialization(char*& temp, size_t& size, ibv_mr* mr) {
  void* p = mr->addr;
  memcpy(temp, &p, sizeof(void*));
  temp = temp + sizeof(void*);
  uint32_t rkey = mr->rkey;
  uint32_t rkey_net = htonl(rkey);
  memcpy(temp, &rkey_net, sizeof(uint32_t));
  temp = temp + sizeof(uint32_t);
  uint32_t lkey = mr->lkey;
  uint32_t lkey_net = htonl(lkey);
  memcpy(temp, &lkey_net, sizeof(uint32_t));
  temp = temp + sizeof(uint32_t);
}

void RDMA_Manager::mr_deserialization(char*& temp, size_t& size, ibv_mr*& mr) {
  void* addr_p = nullptr;
  memcpy(&addr_p, temp, sizeof(void*));
  temp = temp + sizeof(void*);

  uint32_t rkey_net;
  memcpy(&rkey_net, temp, sizeof(uint32_t));
  uint32_t rkey = htonl(rkey_net);
  temp = temp + sizeof(uint32_t);

  uint32_t lkey_net;
  memcpy(&lkey_net, temp, sizeof(uint32_t));
  uint32_t lkey = htonl(lkey_net);
  temp = temp + sizeof(uint32_t);

  mr->addr = addr_p;
  mr->rkey = rkey;
  mr->lkey = lkey;
}
void RDMA_Manager::fs_deserilization(
    char*& buff, size_t& size, std::string& db_name,
    std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
    std::map<void*, In_Use_Array*>& remote_mem_bitmap, ibv_mr* local_mr) {
  auto start = std::chrono::high_resolution_clock::now();
  char* temp = buff;
  size_t namenumber_net;
  memcpy(&namenumber_net, temp, sizeof(size_t));
  size_t namenumber = htonl(namenumber_net);
  temp = temp + sizeof(size_t);

  char dbname_[namenumber + 1];
  memcpy(dbname_, temp, namenumber);
  dbname_[namenumber] = '\0';
  temp = temp + namenumber;

  assert(db_name == std::string(dbname_));
  size_t filenumber_net;
  memcpy(&filenumber_net, temp, sizeof(size_t));
  size_t filenumber = htonl(filenumber_net);
  temp = temp + sizeof(size_t);

  for (size_t i = 0; i < filenumber; i++) {
    size_t filename_length_net;
    memcpy(&filename_length_net, temp, sizeof(size_t));
    size_t filename_length = ntohl(filename_length_net);
    temp = temp + sizeof(size_t);

    char filename[filename_length + 1];
    memcpy(filename, temp, filename_length);
    filename[filename_length] = '\0';
    temp = temp + filename_length;

    unsigned int file_size_net = 0;
    memcpy(&file_size_net, temp, sizeof(unsigned int));
    unsigned int file_size = ntohl(file_size_net);
    temp = temp + sizeof(unsigned int);

    size_t list_len_net = 0;
    memcpy(&list_len_net, temp, sizeof(size_t));
    size_t list_len = htonl(list_len_net);
    temp = temp + sizeof(size_t);

    SST_Metadata* meta_head;
    SST_Metadata* meta = new SST_Metadata();

    meta->file_size = file_size;

    meta_head = meta;
    size_t length_map_net = 0;
    memcpy(&length_map_net, temp, sizeof(size_t));
    size_t length_map = htonl(length_map_net);
    temp = temp + sizeof(size_t);

    void* context_p = nullptr;
    // TODO: It can not be changed into net stream.
    memcpy(&context_p, temp, sizeof(void*));
    //    void* p_net = htonll(context_p);
    temp = temp + sizeof(void*);

    void* pd_p = nullptr;
    memcpy(&pd_p, temp, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t handle_net;
    memcpy(&handle_net, temp, sizeof(uint32_t));
    uint32_t handle = htonl(handle_net);
    temp = temp + sizeof(uint32_t);

    size_t length_mr_net = 0;
    memcpy(&length_mr_net, temp, sizeof(size_t));
    size_t length_mr = htonl(length_mr_net);
    temp = temp + sizeof(size_t);

    for (size_t j = 0; j < list_len; j++) {
      meta->mr = new ibv_mr;
      meta->mr->context = static_cast<ibv_context*>(context_p);
      meta->mr->pd = static_cast<ibv_pd*>(pd_p);
      meta->mr->handle = handle;
      meta->mr->length = length_mr;
      // below could be problematic.
      meta->fname = std::string(filename);
      mr_deserialization(temp, size, meta->mr);
      meta->map_pointer = new ibv_mr;
      *(meta->map_pointer) = *(meta->mr);

      void* start_key;
      memcpy(&start_key, temp, sizeof(void*));
      temp = temp + sizeof(void*);

      meta->map_pointer->length = length_map;
      meta->map_pointer->addr = start_key;
      if (j != list_len - 1) {
        meta->next_ptr = new SST_Metadata();
        meta = meta->next_ptr;
      }
    }
    file_to_sst_meta.insert({std::string(filename), meta_head});
  }
  // desirialize the Bit map
  size_t bitmap_number_net = 0;
  memcpy(&bitmap_number_net, temp, sizeof(size_t));
  size_t bitmap_number = htonl(bitmap_number_net);
  temp = temp + sizeof(size_t);
  for (size_t i = 0; i < bitmap_number; i++) {
    void* p_key;
    memcpy(&p_key, temp, sizeof(void*));
    temp = temp + sizeof(void*);
    size_t element_size_net = 0;
    memcpy(&element_size_net, temp, sizeof(size_t));
    size_t element_size = htonl(element_size_net);
    temp = temp + sizeof(size_t);
    size_t chunk_size_net = 0;
    memcpy(&chunk_size_net, temp, sizeof(size_t));
    size_t chunk_size = htonl(chunk_size_net);
    temp = temp + sizeof(size_t);
    auto* in_use = new std::atomic<bool>[element_size];

    void* context_p = nullptr;
    // TODO: It can not be changed into net stream.
    memcpy(&context_p, temp, sizeof(void*));
    //    void* p_net = htonll(context_p);
    temp = temp + sizeof(void*);

    void* pd_p = nullptr;
    memcpy(&pd_p, temp, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t handle_net;
    memcpy(&handle_net, temp, sizeof(uint32_t));
    uint32_t handle = htonl(handle_net);
    temp = temp + sizeof(uint32_t);

    size_t length_mr_net = 0;
    memcpy(&length_mr_net, temp, sizeof(size_t));
    size_t length_mr = htonl(length_mr_net);
    temp = temp + sizeof(size_t);
    auto* mr_inuse = new ibv_mr();
    mr_inuse->context = static_cast<ibv_context*>(context_p);
    mr_inuse->pd = static_cast<ibv_pd*>(pd_p);
    mr_inuse->handle = handle;
    mr_inuse->length = length_mr;
    bool bit_temp;
    for (size_t j = 0; j < element_size; j++) {
      memcpy(&bit_temp, temp, sizeof(bool));
      in_use[j] = bit_temp;
      temp = temp + sizeof(bool);
    }

    mr_deserialization(temp, size, mr_inuse);
    In_Use_Array* in_use_array = new In_Use_Array(element_size, chunk_size, mr_inuse, in_use);
    remote_mem_bitmap.insert({p_key, in_use_array});
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  printf("fs pure deserialization time elapse: %ld\n", duration.count());
  ibv_dereg_mr(local_mr);
  free(buff);
}

    int RDMA_Manager::poll_completion_xcompute(ibv_wc *wc_p, int num_entries, std::string qp_type, bool send_cq,
                                               uint16_t target_node_id,
                                               int num_of_cp) {
// unsigned long start_time_msec;
        // unsigned long cur_time_msec;
        // struct timeval cur_time;
        int poll_result;
        int poll_num = 0;
        int rc = 0;
        ibv_cq* cq;
        /* poll the completion for a while before giving up of doing it .. */
        // gettimeofday(&cur_time, NULL);
        // start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
        if (send_cq)
            cq = (*cq_xcompute.at(target_node_id))[num_of_cp*2];
        else
            cq = (*cq_xcompute.at(target_node_id))[num_of_cp*2+1];
        size_t count = 0;
        do {
            poll_result = ibv_poll_cq(cq, num_entries, &wc_p[poll_num]);
            if (poll_result < 0)
                break;
            else
                poll_num = poll_num + poll_result;
            ibv_qp_attr qp_init_attr;
            ibv_qp_init_attr qp_state;
            if (count++ > 1000000){
                ibv_query_qp((*qp_xcompute.at(target_node_id))[num_of_cp], &qp_init_attr, IBV_QP_STATE, &qp_state);
                assert(qp_init_attr.qp_state == IBV_QPS_RTS);
            }
            /*gettimeofday(&cur_time, NULL);
            cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);*/
        } while (poll_num < num_entries);  // && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
        //*(end) = std::chrono::steady_clock::now();
        // end = std::chrono::steady_clock::now();
        assert(poll_num == num_entries);
        if (poll_result < 0) {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
            rc = 1;
        } else if (poll_result == 0) { /* the CQ is empty */
            fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
            rc = 1;
        } else {
            /* CQE found */
            // fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
            /* check the completion status (here we don't care about the completion opcode */
            for (auto i = 0; i < num_entries; i++) {
                if (wc_p[i].status !=
                    IBV_WC_SUCCESS)  // TODO:: could be modified into check all the entries in the array
                {
                    fprintf(stderr,
                            "Node %d number %d got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
                            node_id, i, wc_p[i].status, wc_p[i].vendor_err);
                    assert(false);
                    rc = 1;
                }
            }
        }
        return rc;
    }


//    void RDMA_Manager::Writer_Inv_Shared_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
//        ibv_mr* cas_mr =  Get_local_CAS_mr();
//        GlobalAddress g_ptr = receive_msg_buf->content.inv_message.page_addr;
//        uint8_t starv_level = receive_msg_buf->content.inv_message.starvation_level;
//        Slice upper_node_page_id((char*)&g_ptr, sizeof(GlobalAddress));
//        printf("Node %u receive writer invalidate shared invalidation message from node %u over data %p\n", node_id, target_node_id, g_ptr);
//        fflush(stdout);
//        Cache::Handle* handle = page_cache_->Lookup(upper_node_page_id);
//        //The template will not impact the offset of level in the header so we can random give the tempalate a Type to access the leve in ther header.
//        assert(STRUCT_OFFSET(Header_Index<uint64_t>, level) == STRUCT_OFFSET(Header_Index<char>, level));
//        Page_Forward_Reply_Type reply_type = waiting;
//        if (handle) {
////            printf("writer invalid Shared lock Handle found %u, %lu\n", handle->gptr.nodeID, handle->gptr.offset);
//            ibv_mr *page_mr = (ibv_mr *) handle->value;
//            GlobalAddress lock_gptr = g_ptr;
//            Header_Index<uint64_t> *header = (Header_Index<uint64_t> *) ((char *) ((ibv_mr *) handle->value)->addr +
//                                                                         (STRUCT_OFFSET(InternalPage<uint64_t>, hdr)));
//            if (handle->remote_lock_status.load() != 1 ) {
//                //TODO: Use try lock instead of lock.
////                std::unique_lock<std::shared_mutex> lck(handle->rw_mtx);
//                if(handle->rw_mtx.try_lock(64)){
//                    if (starv_level >= handle->buffer_inv_message.starvation_priority){
//                        if ( handle->remote_lock_status.load() == 1){
//                            // Asyc lock releasing, with the local latch of handle on,
//                            // THe latch will be released by a callback when the aysnc functin is finished.
//                            //TODO: what if a callback is never triggered again, and the local lathc will never be released.
//                            // make a macro for the async
//#ifdef ASYNC_UNLOCK
//                            if (global_RUnlock(lock_gptr, cas_mr, true, handle)){
//                                handle->remote_lock_status.store(0);
//                                inv_reply_type = 1;
//                                handle->clear_states();
//                                goto message_reply;
//                            }
//#else
//                            global_RUnlock(lock_gptr, cas_mr);
////                            handle->last_modifier_thread_id = thread_id;
//#endif
//                            handle->remote_lock_status.store(0);
//                            reply_type = processed;
//                            handle->clear_pending_inv_states();
//
//                        }
//                    }else{
//                        handle->buffered_inv_mtx.lock();
//                        handle->process_buffered_inv_message(g_ptr, page_mr->length, lock_gptr, page_mr, false);
//                        handle->buffered_inv_mtx.unlock();
//                    }
//                    handle->rw_mtx.unlock();
//
//                }else{
//                    handle->buffered_inv_mtx.lock();
//                    if (handle->remote_lock_status.load() == 1){
//                        if (handle->buffer_inv_message.starvation_priority < starv_level ){
//                            handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer, receive_msg_buf->rkey, starv_level, receive_msg_buf->command);
//                            handle->remote_urging_type.store(2);
//                        }
//                        reply_type = pending;
//                    }
//                    handle->buffered_inv_mtx.unlock();
//
//                    // In case that this node upgrade the lock to a Exclusive lock.
//                    //TODO: do I need specify what is the state in orginal invlaidaotn message sender, other wise, this node may not know,
//                    // whether it should handover to the next.
//
//                }
//            }
////            else{
////                printf("Writer_Inv_Shared_handler Handle already invlaidated\n");
////            }
//            page_cache_->Release(handle);
//        }
//message_reply:
//        if (reply_type == waiting){
//            reply_type = dropped;
//        }
//        switch (reply_type) {
//            case processed:
//                printf("Node %u processed the writer invalidate shared message from node %u over data %p, message processed, starv level is %u\n", node_id, target_node_id, g_ptr, starv_level);
//
//                break;
//            case pending:
//                printf("Node %u pending the writer invalidate shared message from node %u over data %p, message pending, starv level is %u\n", node_id, target_node_id, g_ptr, starv_level);
//                break;
//            case dropped:
//                printf("Node %u dropped the writer invalidate shared message from node %u over data %p, message dropped, starv level is %u\n", node_id, target_node_id, g_ptr, starv_level);
//                break;
//            default:
//                assert(false);
//        }
//        fflush(stdout);
//        ibv_mr* local_mr = Get_local_send_message_mr();
//        *((Page_Forward_Reply_Type* )local_mr->addr) = reply_type;
//        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
//        RDMA_Write_xcompute(local_mr, receive_msg_buf->buffer, receive_msg_buf->rkey, sizeof(Page_Forward_Reply_Type),
//                            target_node_id, qp_id, true);
//        delete receive_msg_buf;
//
//    }
    void RDMA_Manager::Writer_Inv_Shared_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
        GlobalAddress g_ptr = receive_msg_buf->content.inv_message.page_addr;
        uint8_t starv_level = receive_msg_buf->content.inv_message.starvation_level;
//        bool pending_reminder = receive_msg_buf->content.inv_message.pending_reminder;
        Slice upper_node_page_id((char*)&g_ptr, sizeof(GlobalAddress));
        assert(page_cache_ != nullptr);
//        printf("Node %u receive writer invalidate shared invalidation message from node %u over data %p\n", node_id, target_node_id, g_ptr);
//        fflush(stdout);
        Cache::Handle* handle = page_cache_->Lookup(upper_node_page_id);
        Page_Forward_Reply_Type reply_type = waiting;
        ibv_mr* page_mr = nullptr;
        GlobalAddress lock_gptr = g_ptr;
        Header_Index<uint64_t>* header = nullptr;
        if (!handle) {
            reply_type = dropped;  // Handle not found
            goto message_reply;
        }

        page_mr = (ibv_mr*)handle->value;
        header = (Header_Index<uint64_t>*) ((char *) ((ibv_mr*)handle->value)->addr + (STRUCT_OFFSET(InternalPage<uint64_t>, hdr)));
        assert(STRUCT_OFFSET(LeafPage<uint64_t>, global_lock) == STRUCT_OFFSET(InternalPage<uint64_t>, global_lock));
        assert(STRUCT_OFFSET(DataPage, global_lock) == STRUCT_OFFSET(InternalPage<uint64_t>, global_lock));
        //TODO: we can first check whether the remote lock status is shared, if not drop the message directly. THis can
        // simplify the code logic and make it more readable.
        if (!handle->rw_mtx.try_lock(48)){
            //double check locking to reduce the lock conflict on buffered_inv_mtx
            if(handle->remote_lock_status.load() == 1) {
                handle->buffered_inv_mtx.lock();
                if (handle->remote_lock_status.load() == 1) {
                    //  handle->lock_pending_num >0 is to avoid the case that the message is buffered but there is no local thread process it
                    // in the future.
//                if (handle->buffer_inv_message.starvation_priority < starv_level ){
                    if (handle->buffer_inv_message.next_holder_id == Invalid_Node_ID) {
                        handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer,
                                                             receive_msg_buf->rkey, starv_level,
                                                             receive_msg_buf->command);
                        handle->remote_urging_type.store(2);
                    } else {
                        assert(handle->remote_urging_type == 2);
                        assert(handle->buffer_inv_message.next_inv_message_type == writer_invalidate_shared);
                    }
//                }
                }
                handle->buffered_inv_mtx.unlock();
            }
            reply_type = dropped;
            page_cache_->Release(handle);
            goto message_reply;
        }else{
            handle->buffered_inv_mtx.lock();

            if (handle->remote_lock_status.load() == 1){

                if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID ){
                    assert(handle->buffer_inv_message.next_inv_message_type == writer_invalidate_shared);
                }
                // push current invalidation message into the handle buffer.
                handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer, receive_msg_buf->rkey, starv_level, receive_msg_buf->command);
                handle->remote_urging_type.store(2);
//                assert(handle->read_lock_counter == 0 && handle->write_lock_counter == 0);
                handle->buffered_inv_mtx.unlock();
                reply_type = processed;
                //do not release the lock here!!!!!!
                page_cache_->Release(handle);
                goto message_reply;

            }

            handle->buffered_inv_mtx.unlock();
            handle->rw_mtx.unlock();
            reply_type = dropped;
            page_cache_->Release(handle);
            goto message_reply;

        }


        message_reply:
        ibv_mr* local_mr = nullptr;
        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
        //TODO: the same global cache line should better be transferred by the same qp.
        // int qp_id = g_ptr % NUM_QP_ACCROSS_COMPUTE;
        switch (reply_type) {
            case processed:
                handle->buffered_inv_mtx.lock();
                //the writer invalidate shared will not be replied inside the funciton below. Why?
                handle->process_buffered_inv_message(g_ptr, page_mr->length, lock_gptr, page_mr, false);
                local_mr = Get_local_send_message_mr();
                *((Page_Forward_Reply_Type* )local_mr->addr) = reply_type;
                RDMA_Write_xcompute(local_mr, receive_msg_buf->buffer, receive_msg_buf->rkey,
                                    sizeof(Page_Forward_Reply_Type),
                                    target_node_id, qp_id, true);
                handle->buffered_inv_mtx.unlock();
                handle->rw_mtx.unlock();
//                printf("Node %u receive writer invalidate shared invalidation message from node %u over data %p get processed, priority is %u\n", node_id, target_node_id, g_ptr, starv_level);
//                fflush(stdout);
                break;
            case pending:
                assert(false);
                break;
            case waiting:
                assert(false);
                break;
            case dropped:
                local_mr = Get_local_send_message_mr();
                *((Page_Forward_Reply_Type* )local_mr->addr) = reply_type;
                RDMA_Write_xcompute(local_mr, receive_msg_buf->buffer, receive_msg_buf->rkey,
                                    sizeof(Page_Forward_Reply_Type),
                                    target_node_id, qp_id, true);
//                printf("Node %u receive writer invalidate shared invalidation message from node %u over data %p get dropped, starv level is %u\n", node_id, target_node_id, g_ptr, starv_level);
//                fflush(stdout);
                break;
            default:
                assert(false);
                break;

        }

        delete receive_msg_buf;

    }
    void RDMA_Manager::Reader_Inv_Modified_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
        GlobalAddress g_ptr = receive_msg_buf->content.inv_message.page_addr;
        uint8_t starv_level = receive_msg_buf->content.inv_message.starvation_level;
        bool pending_reminder = receive_msg_buf->content.inv_message.pending_reminder;
        Slice upper_node_page_id((char*)&g_ptr, sizeof(GlobalAddress));
        assert(page_cache_ != nullptr);
//        printf("Node %u receive reader invalidate modified invalidation message from node %u over data %p\n", node_id, target_node_id, g_ptr);
//        fflush(stdout);
        Cache::Handle* handle = page_cache_->Lookup(upper_node_page_id);
        Page_Forward_Reply_Type reply_type = waiting;
        ibv_mr* page_mr = nullptr;
        GlobalAddress lock_gptr = g_ptr;
        Header_Index<uint64_t>* header = nullptr;
#ifdef STARV_REVENGE
        uint8_t priority_to_meet = 0;
#endif
        if (!handle) {
            reply_type = dropped;  // Handle not found
            goto message_reply;
        }
#ifdef STARV_REVENGE
        priority_to_meet = ((uint64_t)handle->last_writer_starvation_priority+0) <= 255 ? handle->last_writer_starvation_priority + 0 : 255;
        if (handle->last_writer_starvation_priority && priority_to_meet > starv_level){
//            printf("STARV_REVENGE triggered\n");
            reply_type = dropped;

            page_cache_->Release(handle);
            goto message_reply;
        }
#endif
        page_mr = (ibv_mr*)handle->value;
        header = (Header_Index<uint64_t>*) ((char *) ((ibv_mr*)handle->value)->addr + (STRUCT_OFFSET(InternalPage<uint64_t>, hdr)));
        assert(STRUCT_OFFSET(LeafPage<uint64_t>, global_lock) == STRUCT_OFFSET(InternalPage<uint64_t>, global_lock));
        assert(STRUCT_OFFSET(DataPage, global_lock) == STRUCT_OFFSET(InternalPage<uint64_t>, global_lock));
        if ( !handle->rw_mtx.try_lock(48)){
            // (Solved) problem 1. There is a potential bug that the message is cached locally, but never get processed. If one front-end thread just
            // finished the code from cache.cc:1063-1071. Then the message is pushed and will never get processed.

            //TODO: It seems that pending reminder have a lot of bugs.
            if(handle->remote_lock_status.load() == 2) {
                handle->buffered_inv_mtx.lock();
                if (handle->remote_lock_status.load() == 2) {
                    if (pending_reminder){
                        if (handle->buffer_inv_message.next_holder_id == target_node_id) {
                            assert(receive_msg_buf->buffer == handle->buffer_inv_message.next_receive_page_buf);
                            assert(receive_msg_buf->rkey == handle->buffer_inv_message.next_receive_rkey);
                            // update the priority
                            handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer,
                                                                 receive_msg_buf->rkey, starv_level,
                                                                 receive_msg_buf->command);
                            handle->remote_urging_type.store(1);
//                            handle->buffered_inv_mtx.unlock();
//                            page_cache_->Release(handle);

                        }
                        handle->buffered_inv_mtx.unlock();
                        reply_type = dropped;
                        page_cache_->Release(handle);
                        goto message_reply;
                    }

                    //  handle->lock_pending_num >0 is to avoid the case that the message is buffered but there is no local thread process it
                    // in the future.
                    if (handle->buffer_inv_message.starvation_priority < starv_level) {
                        if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID) {
                            ibv_mr *local_mr = Get_local_send_message_mr();
                            // drop the old invalidation message.
                            handle->drop_buffered_inv_message(local_mr, this);
                        }
                        handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer,
                                                             receive_msg_buf->rkey, starv_level,
                                                             receive_msg_buf->command);
                        handle->remote_urging_type.store(1);
                        assert(!pending_reminder);
                        reply_type = pending;
                        // We do not release the buffered_inv_mtx to guaratee that pending flag will arrived before the processed flag
//                        handle->buffered_inv_mtx.unlock();
                        page_cache_->Release(handle);
                        goto message_reply;
                    }
                }
                handle->buffered_inv_mtx.unlock();
            }
            reply_type = dropped;
            page_cache_->Release(handle);
            goto message_reply;
        }else{
            if (pending_reminder){
                if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID){
                    reply_type = processed;
                    page_cache_->Release(handle);
                    goto message_reply;
                }else{
                    reply_type = dropped;
                    handle->rw_mtx.unlock();
                    page_cache_->Release(handle);
                    goto message_reply;
                }
            }


            // THe lock has been acquired.

            // there should be no buffered invalidation request
            // (Solved) problem 2: there could be unsolved invalidaiton message existed in the buffer.
            // E.g., a thread just finished the code from cache.cc:1062. shared lock release and the invalidaton message
            // comes in, before it update the lock to exclusive one.

            //Possible solution, if detect there is a buffered invalidaton message check the priority and decide weather drop the old and process
            // the starved one.


            handle->buffered_inv_mtx.lock();
            if (starv_level >= handle->buffer_inv_message.starvation_priority){
                if (handle->remote_lock_status.load() == 2){

                    if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID ){
                        ibv_mr* local_mr = Get_local_send_message_mr();
                        // drop the old invalidation message.
                        handle->drop_buffered_inv_message(local_mr, this);
                    }
                    // push current invalidation message into the handle buffer.
                    handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer, receive_msg_buf->rkey, starv_level, receive_msg_buf->command);
                    handle->remote_urging_type.store(1);
//                assert(handle->read_lock_counter == 0 && handle->write_lock_counter == 0);
                    handle->buffered_inv_mtx.unlock();
                    reply_type = processed;
                    //do not release the lock here!!!!!!
                    page_cache_->Release(handle);
                    goto message_reply;

                }
            }else if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID){
                handle->process_buffered_inv_message(g_ptr, page_mr->length, lock_gptr, page_mr, false);
            }
            handle->buffered_inv_mtx.unlock();
            handle->rw_mtx.unlock();
            reply_type = dropped;
            page_cache_->Release(handle);
            goto message_reply;

        }


        message_reply:
        ibv_mr* local_mr = nullptr;
        //TODO: the same global cache line should better be transferred by the same qp.
        // int qp_id = g_ptr % NUM_QP_ACCROSS_COMPUTE;
        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;

        switch (reply_type) {
            case processed:
                handle->buffered_inv_mtx.lock();
                //forward the page to concurrent writer.
                handle->process_buffered_inv_message(g_ptr, page_mr->length, lock_gptr, page_mr, false);
                handle->buffered_inv_mtx.unlock();
                handle->rw_mtx.unlock();
//                printf("Node %u receive reader invalidate modified invalidation message from node %u over data %p get processed\n", node_id, target_node_id, g_ptr);
//                fflush(stdout);
                break;
            case pending:
                // TODO: what if the pending message is processed before we send the reply message back, this can result
                // in the processed flag overwritten by the pending flag.
                assert(!pending_reminder);
                // After install the buffered invalidation message, we can try the lock again incase that there is no pending
                // reader/writer waiting for the local latch and the cached inv message never get processed
//                if (handle->rw_mtx.try_lock()){
//                    handle->buffered_inv_mtx.lock();
//                    if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID){
//                        handle->process_buffered_inv_message(g_ptr, page_mr->length, lock_gptr, page_mr, false);
//                    }
//                    handle->buffered_inv_mtx.unlock();
//                    handle->rw_mtx.unlock();
//
//                }else{
                local_mr = Get_local_send_message_mr();
                *((Page_Forward_Reply_Type* )local_mr->addr) = reply_type;
                RDMA_Write_xcompute(local_mr, (char *) receive_msg_buf->buffer + kLeafPageSize -
                                              sizeof(Page_Forward_Reply_Type), receive_msg_buf->rkey,
                                    sizeof(Page_Forward_Reply_Type),
                                    target_node_id, qp_id, false);
                assert(!pending_reminder);
                handle->buffered_inv_mtx.unlock();

//                printf("Node %u receive reader invalidate modified invalidation message from node %u over data %p get pending, starv level is %u\n", node_id, target_node_id, g_ptr, starv_level);
//                fflush(stdout);
//                }

                break;
            case waiting:
                assert(false);
                break;
            case dropped:
                if(!pending_reminder){
                    local_mr = Get_local_send_message_mr();
                    *((Page_Forward_Reply_Type* )local_mr->addr) = reply_type;
                    RDMA_Write_xcompute(local_mr,
                                        (char *) receive_msg_buf->buffer + kLeafPageSize -
                                        sizeof(Page_Forward_Reply_Type),
                                        receive_msg_buf->rkey, sizeof(Page_Forward_Reply_Type),
                                        target_node_id, qp_id, false);
                }
//                printf("Node %u receive reader invalidate modified invalidation message from node %u over data %p get dropped, starv level is %u\n", node_id, target_node_id, g_ptr, starv_level);
//                fflush(stdout);
                break;
            default:
                assert(false);
                break;

        }

        delete receive_msg_buf;
    }
    void RDMA_Manager::Writer_Inv_Modified_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
//        printf("Writer_Inv_Modified_handler\n");
        GlobalAddress g_ptr = receive_msg_buf->content.inv_message.page_addr;
        uint8_t starv_level = receive_msg_buf->content.inv_message.starvation_level;
        bool pending_reminder = receive_msg_buf->content.inv_message.pending_reminder;
        Slice upper_node_page_id((char*)&g_ptr, sizeof(GlobalAddress));
        assert(page_cache_ != nullptr);
//        printf("Node %u receive writer invalidate modified invalidation message from node %u over data %p\n", node_id, target_node_id, g_ptr);
//        fflush(stdout);
        Cache::Handle* handle = page_cache_->Lookup(upper_node_page_id);
        Page_Forward_Reply_Type reply_type = waiting;
        ibv_mr* page_mr = nullptr;
        GlobalAddress lock_gptr = g_ptr;
        Header_Index<uint64_t>* header = nullptr;
        if (!handle) {
            reply_type = dropped;  // Handle not found
            goto message_reply;
        }

        page_mr = (ibv_mr*)handle->value;
        header = (Header_Index<uint64_t>*) ((char *) ((ibv_mr*)handle->value)->addr + (STRUCT_OFFSET(InternalPage<uint64_t>, hdr)));
        assert(STRUCT_OFFSET(LeafPage<uint64_t>, global_lock) == STRUCT_OFFSET(InternalPage<uint64_t>, global_lock));
        assert(STRUCT_OFFSET(DataPage, global_lock) == STRUCT_OFFSET(InternalPage<uint64_t>, global_lock));
        if ( !handle->rw_mtx.try_lock(32)){
            // (Solved) problem 1. There is a potential bug that the message is cached locally, but never get processed. If one front-end thread just
            // finished the code from cache.cc:1063-1071. Then the message is pushed and will never get processed.
            if(handle->remote_lock_status.load() == 2) {
                handle->buffered_inv_mtx.lock();
                if (handle->remote_lock_status.load() == 2) {
                    if (pending_reminder){
                        if (handle->buffer_inv_message.next_holder_id == target_node_id) {
                            assert(receive_msg_buf->buffer == handle->buffer_inv_message.next_receive_page_buf);
                            assert(receive_msg_buf->rkey == handle->buffer_inv_message.next_receive_rkey);
                            // update the priority
                            handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer,
                                                                 receive_msg_buf->rkey, starv_level,
                                                                 receive_msg_buf->command);
                            handle->remote_urging_type.store(2);
//                            page_cache_->Release(handle);

                        }
                        handle->buffered_inv_mtx.unlock();
                        reply_type = ignored;
//                        page_cache_->Release(handle);
                        goto message_reply;
                    }
                    //  handle->lock_pending_num >0 is to avoid the case that the message is buffered but there is no local thread process it
                    // in the future.
                    if (handle->buffer_inv_message.starvation_priority < starv_level) {
                        assert(handle->buffer_inv_message.next_holder_id != target_node_id);
                        if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID) {
                            ibv_mr *local_mr = Get_local_send_message_mr();
                            // drop the old invalidation message.
                            handle->drop_buffered_inv_message(local_mr, this);
                        }
                        handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer,
                                                             receive_msg_buf->rkey, starv_level,
                                                             receive_msg_buf->command);
                        handle->remote_urging_type.store(2);
                        reply_type = pending;
                        // We do not release the buffered_inv_mtx to guaratee that pending flag will arrived before the processed flag
                        assert(!pending_reminder);
//                        handle->buffered_inv_mtx.unlock();
//                        page_cache_->Release(handle);
                        goto message_reply;
                    }
                }
                handle->buffered_inv_mtx.unlock();
            }
            reply_type = dropped;
//            page_cache_->Release(handle);
            goto message_reply;
        }else{
            // the && handle->buffer_inv_message.next_holder_id == target_node_id can actually be commented out.
            if (pending_reminder){
                //todo: check the remote lock status here other wise
                if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID){
                    reply_type = processed;
//                    page_cache_->Release(handle);
                    goto message_reply;
                }else{
                    reply_type = ignored;
                    handle->rw_mtx.unlock();
//                    page_cache_->Release(handle);
                    goto message_reply;
                }
            }

            // THe rw lock has been acquired.

            // there should be no buffered invalidation request
            // (Solved) problem 2: there could be unsolved invalidaiton message existed in the buffer.
            // E.g., a thread just finished the code from cache.cc:1062. shared lock release and the invalidaton message
            // comes in, before it update the lock to exclusive one.

            //Possible solution, if detect there is a buffered invalidaton message check the priority and decide weather drop the old and process
            // the starved one.
            handle->buffered_inv_mtx.lock();
            if (starv_level >= handle->buffer_inv_message.starvation_priority){
                if (handle->remote_lock_status.load() == 2){

                    if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID ){
                        ibv_mr* local_mr = Get_local_send_message_mr();
                        // drop the old invalidation message.
                        handle->drop_buffered_inv_message(local_mr, this);
                    }
                    // push current invalidation message into the handle buffer.
                    handle->buffer_inv_message.SetStates(target_node_id, receive_msg_buf->buffer, receive_msg_buf->rkey, starv_level, receive_msg_buf->command);
                    handle->remote_urging_type.store(2);
//                assert(handle->read_lock_counter == 0 && handle->write_lock_counter == 0);
                    handle->buffered_inv_mtx.unlock();
                    reply_type = processed;
                    //do not release the lock here!!!!!!
//                    page_cache_->Release(handle);
                    goto message_reply;

                }
            }else if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID){
                handle->process_buffered_inv_message(g_ptr, page_mr->length, lock_gptr, page_mr, false);
            }
            handle->buffered_inv_mtx.unlock();
            handle->rw_mtx.unlock();
            reply_type = dropped;
//            page_cache_->Release(handle);
            goto message_reply;

        }


    message_reply:
        ibv_mr* local_mr = nullptr;
        int qp_id = qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
        //TODO: the same global cache line should better be transferred by the same qp.
        // int qp_id = g_ptr % NUM_QP_ACCROSS_COMPUTE;
        switch (reply_type) {
            case processed:
                handle->buffered_inv_mtx.lock();
                //forward the page to concurrent writer.
                handle->process_buffered_inv_message(g_ptr, page_mr->length, lock_gptr, page_mr, false);
                handle->buffered_inv_mtx.unlock();
                handle->rw_mtx.unlock();
//                printf("Node %u receive writer invalidate modified invalidation message from node %u over data %p get processed\n", node_id, target_node_id, g_ptr);
//                fflush(stdout);
                break;
            case pending:
                assert(!pending_reminder);
                // After install the buffered invalidation message, we can try the lock again incase that there is no pending
                // reader/writer waiting for the local latch and the cached inv message never get processed
//                if (handle->rw_mtx.try_lock()){
//                    handle->buffered_inv_mtx.lock();
//                    if (handle->buffer_inv_message.next_holder_id != Invalid_Node_ID){
//                        handle->process_buffered_inv_message(g_ptr, page_mr->length, lock_gptr, page_mr, false);
//                    }
//                    handle->buffered_inv_mtx.unlock();
//                    handle->rw_mtx.unlock();
//
//                }else{

                local_mr = Get_local_send_message_mr();
                *((Page_Forward_Reply_Type* )local_mr->addr) = reply_type;
                // The pending message has to be synchronous to avoid it overwrite the processed flag.
                RDMA_Write_xcompute(local_mr, (char *) receive_msg_buf->buffer + kLeafPageSize -
                                              sizeof(Page_Forward_Reply_Type), receive_msg_buf->rkey,
                                    sizeof(Page_Forward_Reply_Type),
                                    target_node_id, qp_id, false);
                handle->buffered_inv_mtx.unlock();
                assert(!pending_reminder);
//                printf("Node %u receive writer invalidate modified invalidation message from node %u over data %p get pending, starv level is %u\n", node_id, target_node_id, g_ptr, starv_level);
//                fflush(stdout);
//                }

                break;
            case waiting:
                assert(false);
                break;
            case ignored:
                break;
            case dropped:
//                assert(!pending_reminder);
                if (!pending_reminder){
                    local_mr = Get_local_send_message_mr();
                    *((Page_Forward_Reply_Type* )local_mr->addr) = reply_type;
                    RDMA_Write_xcompute(local_mr,
                                        (char *) receive_msg_buf->buffer + kLeafPageSize -
                                        sizeof(Page_Forward_Reply_Type),
                                        receive_msg_buf->rkey, sizeof(Page_Forward_Reply_Type),
                                        target_node_id, qp_id, false);
                }

//                printf("Node %u receive writer invalidate modified invalidation message from node %u over data %p get dropped, starv level is %u\n", node_id, target_node_id, g_ptr, starv_level);
//                fflush(stdout);
                break;
            default:
                assert(false);
                break;

        }
        if (handle){
            page_cache_->Release(handle);
        }

        delete receive_msg_buf;
        }

    void RDMA_Manager::Create_Delta_Section_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
//        GlobalAddress ds_gaddr = receive_msg_buf->content.create_ds.ds_gaddr;
//        uint8_t compute_node_id = receive_msg_buf->content.create_ds.compute_node_id;
//        ibv_mr* local_mr = new ibv_mr{};
//        Allocate_Local_RDMA_Slot(*local_mr, DeltaChunk);
//        // TODO: there is compilation error becuae DSMengine does not contain defination for transaction.
//        // we need to wrap the funciton to a function pointer or funciton object.
//        auto* ds_for_write= new DeltaSection(0, 0, compute_node_id, ds_gaddr, delta_section_size, local_mr);
//        {
//            std::unique_lock<std::shared_mutex> lck(TransactionManager::delta_map_mtx);
//            TransactionManager::delta_sections.insert(std::make_pair(ds_gaddr, ds_for_write));
//        }
//        delete receive_msg_buf;
    std::shared_lock<std::shared_mutex> read_lock(user_df_map_mutex);
    while(message_handling_funcs_map.find(DeltaCreate) == message_handling_funcs_map.end()){
        // wait for the front end thread register the message handling function.
        read_lock.unlock();
        usleep(10);
        read_lock.lock();
//        ddd
    }
        message_handling_funcs_map.at(DeltaCreate)(receive_msg_buf);
    }
    void RDMA_Manager::Pull_Delta_Section_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
//        GlobalAddress ds_gaddr = receive_msg_buf->content.create_ds.ds_gaddr;
//        uint8_t compute_node_id = receive_msg_buf->content.create_ds.compute_node_id;
//        ibv_mr* local_mr = new ibv_mr{};
//        Allocate_Local_RDMA_Slot(*local_mr, DeltaChunk);
//        // TODO: there is compilation error becuae DSMengine does not contain defination for transaction.
//        // we need to wrap the funciton to a function pointer or funciton object.
//        auto* ds_for_write= new DeltaSection(0, 0, compute_node_id, ds_gaddr, delta_section_size, local_mr);
//        {
//            std::unique_lock<std::shared_mutex> lck(TransactionManager::delta_map_mtx);
//            TransactionManager::delta_sections.insert(std::make_pair(ds_gaddr, ds_for_write));
//        }
//        delete receive_msg_buf;
        std::shared_lock<std::shared_mutex> read_lock(user_df_map_mutex);
        while(message_handling_funcs_map.find(DeltaPull) == message_handling_funcs_map.end()){
            // wait for the front end thread register the message handling function.
            read_lock.unlock();
            usleep(10);
            read_lock.lock();
//        ddd
        }
        message_handling_funcs_map.at(DeltaPull)(receive_msg_buf);
    }
    void RDMA_Manager::Push_Least_Snapshot_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
        assert(receive_msg_buf->command == push_least_snapshot);
        std::shared_lock<std::shared_mutex> read_lock(user_df_map_mutex);
        while(message_handling_funcs_map.find(SnapshotPush) == message_handling_funcs_map.end()){
            // wait for the front end thread register the message handling function.
            read_lock.unlock();
            usleep(10);
            read_lock.lock();
        }
        message_handling_funcs_map.at(SnapshotPush)(receive_msg_buf);
    }
    


    void RDMA_Manager::Write_Invalidation_Message_Handler(void* thread_args) {
        BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
        ((RDMA_Manager *) p->rdma_mg)->Writer_Inv_Modified_handler((RDMA_Request *) p->func_args, 0);//be carefull.
        delete static_cast<BGThreadMetadata*>(thread_args);
    }
    void RDMA_Manager::Read_Invalidation_Message_Handler(void* thread_args) {
        BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
        ((RDMA_Manager *) p->rdma_mg)->Writer_Inv_Shared_handler((RDMA_Request *) p->func_args, 0);
        delete static_cast<BGThreadMetadata*>(thread_args);
    }

    void RDMA_Manager::Tuple_read_2pc_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
    uint16_t thread_id_remote = receive_msg_buf->content.tuple_info.thread_id;
    uint32_t handling_id = ((uint32_t)target_node_id << 16) | thread_id_remote;

    std::shared_lock<std::shared_mutex> read_lock(user_df_map_mutex);
    if (communication_queues.find(handling_id) == communication_queues.end()){
        read_lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(user_df_map_mutex);
        register_message_handling_thread(handling_id, TwoPC);
        //wait for the handling thread ready to receive the message.
        usleep(100);
        write_lock.unlock();
        read_lock.lock();
    }
    auto& communication_queue = communication_queues.find(handling_id)->second;
    auto communication_mtx = communication_mtxs.find(handling_id)->second;
    auto communication_cv = communication_cvs.find(handling_id)->second;
    read_lock.unlock();
    {
        std::unique_lock<std::mutex> lck_comm(*communication_mtx);
        communication_queue.push(*receive_msg_buf);
//        printf("Request is pushed into the queue on node %u\n", node_id);
//        fflush(stdout);
        communication_cv->notify_one();
    }
    delete receive_msg_buf;
//    auto communication_ready = communication_ready_flags.find(handling_id)->second;


    // first check whetehr the
}
    void RDMA_Manager::Prepare_2pc_handler(DSMEngine::RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
        uint16_t thread_id_remote = receive_msg_buf->content.prepare.thread_id;
        uint32_t handling_id = ((uint32_t)target_node_id << 16) | thread_id_remote;
        std::shared_lock<std::shared_mutex> read_lock(user_df_map_mutex);
        if (communication_queues.find(handling_id) == communication_queues.end()){
            assert(false);
        }
        auto& communication_queue = communication_queues.find(handling_id)->second;
        auto communication_mtx = communication_mtxs.find(handling_id)->second;
        auto communication_cv = communication_cvs.find(handling_id)->second;
        read_lock.unlock();
        {
            std::unique_lock<std::mutex> lck_comm(*communication_mtx);
            communication_queue.push(*receive_msg_buf);
            communication_cv->notify_one();
        }
        delete receive_msg_buf;

    }
    void RDMA_Manager::Commit_2pc_handler(DSMEngine::RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
        uint16_t thread_id = receive_msg_buf->content.commit.thread_id;
        uint32_t handling_id = ((uint32_t)target_node_id << 16) | thread_id;
        std::shared_lock<std::shared_mutex> read_lock(user_df_map_mutex);
        if (communication_queues.find(handling_id) == communication_queues.end()){
            assert(false);
        }
        auto& communication_queue = communication_queues.find(handling_id)->second;
        auto communication_mtx = communication_mtxs.find(handling_id)->second;
        auto communication_cv = communication_cvs.find(handling_id)->second;
        read_lock.unlock();
        {
            std::unique_lock<std::mutex> lck_comm(*communication_mtx);
            communication_queue.push(*receive_msg_buf);
            communication_cv->notify_one();
        }
        delete receive_msg_buf;

    }
    void RDMA_Manager::Abort_2pc_handler(DSMEngine::RDMA_Request *receive_msg_buf, uint8_t target_node_id) {
        uint16_t thread_id = receive_msg_buf->content.abort.thread_id;
        uint32_t handling_id = ((uint32_t)target_node_id << 16) | thread_id;
        std::shared_lock<std::shared_mutex> read_lock(user_df_map_mutex);
        if (communication_queues.find(handling_id) == communication_queues.end()){
            assert(false);
        }
        auto& communication_queue = communication_queues.find(handling_id)->second;
        auto communication_mtx = communication_mtxs.find(handling_id)->second;
        auto communication_cv = communication_cvs.find(handling_id)->second;
        read_lock.unlock();
        {
            std::unique_lock<std::mutex> lck_comm(*communication_mtx);
            communication_queue.push(*receive_msg_buf);
            communication_cv->notify_one();
        }
        delete receive_msg_buf;

    }


}