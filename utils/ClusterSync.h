#ifndef __DATABASE_UTILS_CLUSTER_SYNCHRONIZER_H__
#define __DATABASE_UTILS_CLUSTER_SYNCHRONIZER_H__

#include "DDSM.h"
#include "ClusterConfig.h"
#include "storage/rdma.h"

namespace DSMEngine {
class ClusterSync{
public:
  ClusterSync(ClusterConfig *config) : config_(config) {
      sync_key_xcompute_ = 0;
      sync_key_xall_ = SYNC_XALL_OFFSET;
  }

    // sync use RDMA node_id as the key
    void Fence_XALLNodes() {
        uint16_t node_id = default_gallocator->GetID();
        uint16_t* id;
        uint64_t temp_sync_key = sync_key_xall_ + node_id;
        default_gallocator->memSet((char*)&temp_sync_key, sizeof(uint64_t), (char*)&node_id, sizeof(node_id));
        
        // Get all actual compute and memory node IDs from RDMA_Manager
        RDMA_Manager* rdma_mg = RDMA_Manager::Get_Instance();
        std::vector<uint16_t> compute_node_ids = rdma_mg->GetAllComputeNodeIds();
        std::vector<uint16_t> memory_node_ids = rdma_mg->GetAllMemoryNodeIds();
        
        // Collect all node IDs
        std::vector<uint16_t> all_node_ids;
        all_node_ids.reserve(compute_node_ids.size() + memory_node_ids.size());
        all_node_ids.insert(all_node_ids.end(), compute_node_ids.begin(), compute_node_ids.end());
        all_node_ids.insert(all_node_ids.end(), memory_node_ids.begin(), memory_node_ids.end());
        
        // Wait for all actual nodes (not assuming equal counts or contiguous IDs)
        for (uint16_t target_node_id : all_node_ids) {
            temp_sync_key = sync_key_xall_ + target_node_id;
            size_t get_size = 0;
            id = (uint16_t*)default_gallocator->memGet((char*)&temp_sync_key, sizeof(uint64_t), &get_size);
            assert(get_size == sizeof(uint16_t));
            assert(*id == target_node_id);
        }
        sync_key_xall_ += all_node_ids.size();
    }
    //Sync use partition_id as the key
  void FenceXComputes() {
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    bool *flags = new bool[partition_num];
    memset(flags, 0, sizeof(bool)*partition_num);
    this->MasterCollect<bool>(flags + partition_id, flags);
    this->MasterBroadcast<bool>(flags + partition_id);
    delete[] flags;
    flags = nullptr;
  }

  template<class T>
  void MasterCollect(T *send, T *receive) {
    char* data;
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    uint64_t temp_sync_key = sync_key_xcompute_;
    if (config_->IsMaster()) {
      for (size_t i = 0; i < partition_num; ++i) {
        if (i != partition_id) {
            temp_sync_key = sync_key_xcompute_ + i;
            size_t get_size = 0;
            data = default_gallocator->memGet(
                  (char*)&temp_sync_key, sizeof(uint64_t), &get_size);
            assert(get_size == sizeof(T));
            memcpy(receive + i, data, sizeof(T));
        }
        else {
          memcpy(receive + i, send, sizeof(T));
        }
      }
    }
    else {
        temp_sync_key = sync_key_xcompute_ + partition_id;
      default_gallocator->memSet((char*)&temp_sync_key, sizeof(uint64_t), (char*)send, sizeof(T));
    }
      sync_key_xcompute_ += partition_num;
  }

  template<class T>
  void MasterBroadcast(T *send) {
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    uint64_t temp_sync_key = sync_key_xcompute_;
    if (config_->IsMaster()) {
      assert(partition_id == 0);
      temp_sync_key = sync_key_xcompute_ + partition_id;
      default_gallocator->memSet((char*)&temp_sync_key, sizeof(uint64_t), (char*)send, sizeof(T));
    }
    else {
      const size_t master_partition_id = 0;
        temp_sync_key = sync_key_xcompute_ + master_partition_id;
        size_t get_size = 0;
        void* ret = default_gallocator->memGet((char*)&temp_sync_key, sizeof(uint64_t), &get_size);
        assert(get_size == sizeof(T));
        memcpy(send, ret, sizeof(T));
    }
      sync_key_xcompute_ += partition_num;
  }

    void MasterBroadcast(char* key, size_t key_size, char* buff, size_t size) {
        size_t partition_id = config_->GetMyPartitionId();
        size_t partition_num = config_->GetPartitionNum();

        if (config_->IsMaster()) {
            assert(partition_id == 0);
            default_gallocator->memSet(key, key_size, (char*)buff, size);
        }
        else {
            size_t get_size = 0;
            void* ret = default_gallocator->memGet(key, key_size, &get_size);
            assert(get_size == size);
            memcpy(buff, ret, size);
        }
        sync_key_xcompute_ += partition_num;
    }

private:
  ClusterConfig *config_;
  uint64_t sync_key_xcompute_;
  uint64_t sync_key_xall_;
};
}

#endif
