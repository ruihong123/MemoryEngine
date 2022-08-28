#include <iostream>
#include "Btr.h"
using namespace DSMEngine;
int main() {


    uint32_t tcp_port = 19843;
    uint32_t size = 8*1024;
    struct DSMEngine::config_t config = {
            NULL,  /* dev_name */
            NULL,  /* server_name */
            tcp_port, /* tcp_port */
            1,	 /* ib_port */
            1, /* gid_idx */
            0};
    RDMA_Manager* rdma_mg;
    rdma_mg = RDMA_Manager::Get_Instance(config);
    rdma_mg->Mempool_initialize(DataChunk, INDEX_BLOCK, 0);
    rdma_mg->node_id = 0;
    auto tree = new Btr(rdma_mg);
    std::map<Key, Value> in_memory_records;

    for (int i = 0; i < 1000000; ++i) {
        Key k = i;
        Value v = i;
        tree->insert(k,v);
        in_memory_records.insert({k,v});

    }
    for (int i = 0; i < 1000000; ++i) {
        Key k = i;
        Value v;
        tree->search(i, v);
        assert(in_memory_records.at(k) = v);
    }

    return 0;
}
