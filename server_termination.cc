#include <iostream>
#include <memory_node/memory_node_keeper.h>
#include <unistd.h>

#include "storage/rdma.h"
#include "DDSM.h"

int main(int argc,char* argv[])
{
  DSMEngine::Memory_Node_Keeper* mn_keeper;
  if (argc == 5){
    uint32_t tcp_port;
    int pr_size;
    uint16_t Memory_server_id;
    char* value = argv[1];
    std::stringstream strValue1;
    strValue1 << value;
    strValue1 >> tcp_port;
    value = argv[2];
    std::stringstream strValue2;
    //  strValue.str("");
    strValue2 << value;
    strValue2 >> pr_size;
    value = argv[3];
    std::stringstream strValue3;
    //  strValue.str("");
    strValue3 << value;
    strValue3 >> Memory_server_id;
    value = argv[4];
    std::stringstream strValue4;
    //  strValue.str("");
    strValue4 << value;
    uint64_t allocated_mem_size;
    strValue4 >> allocated_mem_size;
    allocated_mem_size = allocated_mem_size*1024ull*1024*1024;
      struct DSMEngine::config_t config = {
              NULL,  /* dev_name */
              NULL,  /* server_name */
              tcp_port, /* tcp_port */
              1,	 /* ib_port */
              1, /* gid_idx */
              0,
              Memory_server_id};
     mn_keeper = new DSMEngine::Memory_Node_Keeper(true, tcp_port, pr_size, config);
//     DSMEngine::RDMA_Manager::node_id = 2* Memory_server_id + 1;
  }else{
    struct DSMEngine::config_t config = {
            NULL,  /* dev_name */
            NULL,  /* server_name */
            19843, /* tcp_port */
            1,	 /* ib_port */
            1, /* gid_idx */
            0,
            1};
    mn_keeper = new DSMEngine::Memory_Node_Keeper(true, 19843, 88, config);
//    DSMEngine::RDMA_Manager::node_id = 1;
  }
  
  // Start server communication thread
  std::thread* TPC_connection_handler = new std::thread(&DSMEngine::Memory_Node_Keeper::Server_to_Client_Communication, mn_keeper);
  TPC_connection_handler->detach();
  DSMEngine::DDSM ddsm(nullptr, mn_keeper->rdma_mg.get());
  
  // Wait for "benchmark_end_node_X" signals from all compute nodes
  // memGet is blocking, so it will wait until the key is available
  int compute_num = mn_keeper->rdma_mg->GetComputeNodeNum();
  printf("Memory Server %d: Waiting for 'benchmark_end' signals from %d compute nodes...\n", 
         mn_keeper->rdma_mg->node_id, compute_num);
  
  char benchmark_end_key[64];
  
  // Get signals from all compute nodes (memGet blocks until available)
  for (int i = 0; i < compute_num; i++) {
    snprintf(benchmark_end_key, sizeof(benchmark_end_key), "benchmark_end_node_%d", i * 2);
    
    size_t len = 0;
    char* ret = ddsm.memGet(benchmark_end_key, strlen(benchmark_end_key), &len);    
    if (ret != nullptr) {
      free(ret);
    }
  }
  
  printf("Memory Server %d: All compute nodes completed. Shutting down...\n", 
         mn_keeper->rdma_mg->node_id);
  mn_keeper->ExitAllThreads();
  delete mn_keeper;
  delete TPC_connection_handler;

  return 0;
}