#include <iostream>
#include <memory_node/memory_node_keeper.h>

#include "storage/rdma.h"
#include "DDSM.h"
//namespace DSMEngine{
uint64_t allocated_mem_size = 0;
uint64_t SYNC_KEY = SYNC_XALL_OFFSET;
int Memcache_offset = 1024;

int main(int argc,char* argv[])
{
  DSMEngine::Memory_Node_Keeper* mn_keeper;
  if (argc == 4){
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
//    value = argv[4];
//    std::stringstream strValue4;
//    //  strValue.str("");
//    strValue4 << value;
//    strValue4 >> allocated_mem_size;
//    allocated_mem_size = allocated_mem_size*1024ull*1024*1024;
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
//    SYNC_KEY = allocated_mem_size/(kLeafPageSize);
//  mn_keeper->SetBackgroundThreads(0, DSMEngine::ThreadPoolType::CompactionThreadPool);
  std::thread* TPC_connection_handler = new std::thread(&DSMEngine::Memory_Node_Keeper::Server_to_Client_Communication, mn_keeper);
  TPC_connection_handler->detach();
  DSMEngine::DDSM ddsm(nullptr, mn_keeper->rdma_mg.get());
    uint64_t temp = SYNC_KEY +  mn_keeper->rdma_mg->node_id;
    ddsm.memSet((char*)&temp, sizeof(temp), (char*)&mn_keeper->rdma_mg->node_id, sizeof(mn_keeper->rdma_mg->node_id));
    char* ret;
    for (int i = 0; i < mn_keeper->rdma_mg->GetComputeNodeNum() + mn_keeper->rdma_mg->GetPhysicalMemNodeNum(); i++) {
        temp = SYNC_KEY + i;
        size_t len;
        ret = ddsm.memGet((char*)&temp, sizeof(temp), &len);
    }
    SYNC_KEY += mn_keeper->rdma_mg->GetComputeNodeNum() + mn_keeper->rdma_mg->GetPhysicalMemNodeNum();
    mn_keeper->ExitAllThreads();
  delete mn_keeper;
  delete TPC_connection_handler;

  return 0;
}