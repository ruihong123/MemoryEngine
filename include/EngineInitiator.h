// Copyright (c) 2025, SELCC authors All rights reserved.
//  Use of this source code is governed by a GPL-2.0 license that can be found in the LICENSE file. 

#ifndef __DATABASE_BENCHMARK_INITIATOR_H__
#define __DATABASE_BENCHMARK_INITIATOR_H__

#include "DDSM.h"
#include "ClusterHelper.h"
#include "ClusterConfig.h"
#include "StorageManager.h"
#include "Profiler.h"
#include "PerfStatistics.h"

namespace DSMEngine {
class EngineInitiator {
 public:
  EngineInitiator(const size_t& thread_count,
                  ClusterConfig* config)
      : thread_count_(thread_count),
        config_(config) {
  }

  void InitGAllocator() {
    ServerInfo master = config_->GetMasterHostInfo();
    ServerInfo myhost = config_->GetMyHostInfo();

      struct DSMEngine::config_t config = {
              NULL,  /* dev_name */
              NULL,  /* server_name */
              conn_port, /* tcp_port */
              1,	 /* ib_port */ //physical
              1, /* gid_idx */
              4*10*1024*1024, /*initial local buffer size*/ // depracated.
              RDMA_Manager::node_id
      };
//    DSMEngine::RDMA_Manager::node_id = ThisNodeID;

      auto rdma_mg = DSMEngine::RDMA_Manager::Get_Instance(&config);
      DSMEngine::Cache* cache_ptr = DSMEngine::NewLRUCache(cache_size);
      rdma_mg->set_page_cache(cache_ptr);
      assert(cache_ptr->GetCapacity()> 10000);
      default_gallocator = new DDSM(cache_ptr, rdma_mg);
      std::cout << "create default gallocator" << std::endl;
    gallocators = new DDSM*[thread_count_];
    for (size_t i = 0; i < thread_count_; ++i) {
      gallocators[i] = default_gallocator;
    }
  }

  char* InitStorage() {
      char* storage_addr = static_cast<char *>(malloc(StorageManager::GetSerializeSize()));
      int my_partition_id = config_->GetMyPartitionId();
    int partition_num = config_->GetPartitionNum();
    if (config_->IsMaster()) {
      // RecordSchema
      std::vector<RecordSchema*> schemas;
      this->RegisterSchemas(schemas);
      // TODO: utilize memcached to sync the storage metadata
      this->RegisterTables(storage_addr, schemas);
    } 
    return storage_addr;
  }
  
 protected:
  virtual void RegisterTables(char* const storage_addr,
      const std::vector<RecordSchema*>& schemas) {}

  virtual void RegisterSchemas(std::vector<RecordSchema*>& schemas) {}

  const size_t thread_count_;
  ClusterConfig* config_;
};
}

#endif
