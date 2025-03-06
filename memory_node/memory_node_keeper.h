//
// Created by ruihong on 7/29/21.
//

#ifndef DSMEngine_HOME_NODE_KEEPER_H
#define DSMEngine_HOME_NODE_KEEPER_H


#include <queue>
//#include <fcntl.h>
#include "storage/rdma.h"
//#include "utils/env_posix.h"
#include "utils/ThreadPool.h"
//#include "db/log_writer.h"
//#include "db/version_set.h"
#include "Tools/options.h"

namespace DSMEngine {

//struct Arg_for_persistent{
//  VersionEdit_Merger* edit_merger;
//  std::string client_ip;
//};
class Memory_Node_Keeper {
 public:
//  friend class RDMA_Manager;
    Memory_Node_Keeper(bool use_sub_compaction, uint32_t tcp_port, int pr_s, DSMEngine::config_t &config);
  ~Memory_Node_Keeper();
//  void Schedule(
//      void (*background_work_function)(void* background_work_arg),
//      void* background_work_arg, ThreadPoolType type);
  void JoinAllThreads(bool wait_for_jobs_to_complete);

  // this function is for the server.
  void Server_to_Client_Communication();
  void SetBackgroundThreads(int num,  ThreadPoolType type);
  void ExitAllThreads(){
      exit_all_threads_ = true;
      JoinAllThreads(false);
  }
//  void MaybeScheduleCompaction(std::string& client_ip);
//  static void BGWork_Compaction(void* thread_args);
//  static void RPC_Compaction_Dispatch(void* thread_args);
//  static void RPC_Garbage_Collection_Dispatch(void* thread_args);
//  static void Persistence_Dispatch(void* thread_args);
//  void BackgroundCompaction(void* p);

  static std::shared_ptr<RDMA_Manager> rdma_mg;
//  RDMA_Manager* rdma_mg;
 private:
  int pr_size;
  bool exit_all_threads_ = false;
  std::unordered_map<unsigned int, std::pair<std::mutex, std::condition_variable>> imm_notifier_pool;
  unsigned int imm_temp = 1;
  std::mutex mtx_temp;
  std::condition_variable cv_temp;
  std::shared_ptr<Options> opts;

  uint64_t manifest_file_number_ = 1;
  bool usesubcompaction;
  std::vector<std::thread> main_comm_threads;
  ThreadPool Compactor_pool_;
  ThreadPool Message_handler_pool_;
  ThreadPool Persistency_bg_pool_;
  std::shared_mutex qp_info_mtx;
    std::map<uint32_t, Registered_qp_config_xcompute> qp_info_map_xcompute;
    std::atomic<bool> ready_for_get = false;
  std::atomic<bool> check_point_t_ready = true;
  std::mutex merger_mtx;
//  std::mutex test_compaction_mutex;
#ifndef NDEBUG
  std::atomic<size_t> debug_counter = 0;


#endif

  int server_sock_connect(const char* servername, int port);
  /***
   * RPC handling threads, every connection to the compute node should have at least one RPC handling threads.
   * @param client_ip
   * @param socket_fd
   */
  void server_communication_thread(std::string client_ip, int socket_fd);
  void create_mr_1GB_handler(RDMA_Request* request, std::string& client_ip,
                             uint8_t target_node_id);
  void create_qp_handler(RDMA_Request* request, std::string& client_ip,
                         uint8_t target_node_id);

//  void install_version_edit_handler(RDMA_Request* request,
//                                    std::string& client_ip,
//                                    uint8_t target_node_id);
//  void sst_garbage_collection(void* arg);
//
//  void sst_compaction_handler(void* arg);

  void qp_reset_handler(RDMA_Request* request, std::string& client_ip,
                        int socket_fd, uint8_t target_node_id);
  void sync_option_handler(RDMA_Request* request, std::string& client_ip,
                           uint8_t target_node_id);
    void Get_qp_info_handler(RDMA_Request* request, std::string& client_ip,
                             uint8_t target_node_id);
//  void version_unpin_handler(RDMA_Request* request, std::string& client_ip);

};
}
#endif  // DSMEngine_HOME_NODE_KEEPER_H