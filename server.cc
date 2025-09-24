#include <iostream>
#include <memory_node/memory_node_keeper.h>

#include "storage/rdma.h"

//namespace DSMEngine{
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
      struct DSMEngine::config_t config = {
              NULL,  /* dev_name */
              NULL,  /* server_name */
              tcp_port, /* tcp_port */
              1,	 /* ib_port */
              1, /* gid_idx */
              0,
              Memory_server_id
      };
     mn_keeper = new DSMEngine::Memory_Node_Keeper(true, tcp_port, pr_size, config);
//     DSMEngine::RDMA_Manager::node_id = 2* Memory_server_id + 1;
  }else{
      struct DSMEngine::config_t config = {
              .dev_name = NULL,  /* dev_name */
              .server_name = NULL,  /* server_name */
              .tcp_port = 19843, /* tcp_port */
              .ib_port = 1,	 /* ib_port */
              .gid_idx = 1, /* gid_idx */
              .init_local_buffer_size = 0,
              .node_id = 1,
              .replication_num = 3
      };
    mn_keeper = new DSMEngine::Memory_Node_Keeper(true, 19843, 88, config);
//    DSMEngine::RDMA_Manager::node_id = 1;
  }

//  mn_keeper->SetBackgroundThreads(12, DSMEngine::ThreadPoolType::CompactionThreadPool);
  mn_keeper->Server_to_Client_Communication();
  delete mn_keeper;

  return 0;
}