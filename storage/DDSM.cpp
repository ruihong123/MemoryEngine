//
// Created by ruihong on 10/1/23.
//

#include <fstream>
#include "DDSM.h"
extern uint64_t cache_lookup_total[MAX_APP_THREAD];
extern uint64_t cache_lookup_times[MAX_APP_THREAD];

uint16_t implicit_ThisNodeID = 0;
uint16_t implicit_tcp_port=19843;
namespace DSMEngine {
    std::string trim(const std::string &s) {
        std::string res = s;
        if (!res.empty()) {
            res.erase(0, res.find_first_not_of(" "));
            res.erase(res.find_last_not_of(" ") + 1);
        }
        return res;
    }
#if ACCESS_MODE == 1 || ACCESS_MODE == 2
    void DDSM::SELCC_Shared_Lock(void *& page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
        assert((page_addr.offset % 1ULL*1024ULL*1024ULL*1024ULL)% kLeafPageSize == 0);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));
#ifdef TIMEPRINT
        auto start = std::chrono::high_resolution_clock::now();
#endif
        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
#ifdef TIMEPRINT
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            cache_lookup_total[RDMA_Manager::thread_id] += duration.count();
            cache_lookup_times[RDMA_Manager::thread_id]++;
#endif
        assert(handle != nullptr);
        handle->reader_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
        page_buffer = mr->addr;
        assert(STRUCT_OFFSET(LeafPage, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
        assert(((LeafPage*)page_buffer)->global_lock);
        assert(handle->gptr == page_addr);
        assert(((LeafPage*)page_buffer)->hdr.this_page_g_ptr == GlobalAddress::Null()||((LeafPage*)page_buffer)->hdr.this_page_g_ptr == page_addr);

    }
    //TODO: local TRY multiple times for local latch
    bool DDSM::TrySELCC_Shared_Lock(void *& page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
        assert((page_addr.offset % 1ULL*1024ULL*1024ULL*1024ULL)% kLeafPageSize == 0);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));
        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        if(!handle->try_reader_pre_access(page_addr, kLeafPageSize, lock_addr, mr)){
            page_cache->Release(handle);
            return false;
        }
        page_buffer = mr->addr;
        assert(STRUCT_OFFSET(LeafPage, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
        assert(((LeafPage*)page_buffer)->global_lock);
        assert(handle->gptr == page_addr);
        assert(((LeafPage*)page_buffer)->hdr.this_page_g_ptr == GlobalAddress::Null()||((LeafPage*)page_buffer)->hdr.this_page_g_ptr == page_addr);
        return true;
    }

    void DDSM::SELCC_Shared_UnLock(GlobalAddress page_addr, Cache::Handle *&handle) {
        assert((page_addr.offset % 1ULL*1024ULL*1024ULL*1024ULL)% kLeafPageSize == 0);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        ibv_mr *local_mr = (ibv_mr *) handle->value;
        handle->reader_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
        page_cache->Release(handle);
        handle = nullptr;

    }

    void DDSM::SELCC_Exclusive_Lock_noread(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
//        printf("PRE Write page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
        assert(TOPAGE(page_addr) == page_addr);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        handle->writer_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
        page_buffer = mr->addr;
        // reset the local buffer beyond the lock word.
        memset((char*)page_buffer + 16,0, kLeafPageSize - 16);
//        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
//        assert(handle->gptr == page_addr);
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);
    }
//    bool DDSM::TrySELCC_Exclusive_Lock_noread(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
////        printf("PRE Write page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
//        assert(TOPAGE(page_addr) == page_addr);
//        GlobalAddress lock_addr;
//        lock_addr.nodeID = page_addr.nodeID;
//
//        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
//        ibv_mr *mr = nullptr;
//        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));
//
//        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
//        assert(handle != nullptr);
//        bool success = handle->try_writer_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
//        if (success){
//            page_buffer = mr->addr;
//            // reset the local buffer beyond the lock word.
//            memset((char*)page_buffer + 16,0, kLeafPageSize - 16);
//        }
//        return success;
////        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
////        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
////        assert(handle->gptr == page_addr);
////        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);
//    }

    void DDSM::SELCC_Exclusive_UnLock_noread(GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        ibv_mr *local_mr = (ibv_mr *) handle->value;
        handle->writer_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
        page_cache->Release(handle);
        handle = nullptr;
    }

    void DDSM::SELCC_Exclusive_Lock(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
//        printf("PRE Update page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
        assert(TOPAGE(page_addr) == page_addr);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));
#ifdef TIMEPRINT
        auto start = std::chrono::high_resolution_clock::now();
#endif
        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
#ifdef TIMEPRINT
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        cache_lookup_total[RDMA_Manager::thread_id] += duration.count();
        cache_lookup_times[RDMA_Manager::thread_id]++;
#endif
        assert(handle != nullptr);
        //TODO: unwarp the updater_pre_access.
        handle->updater_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
        page_buffer = mr->addr;
//        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
//        assert(handle->gptr == page_addr);
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);

    }
    bool DDSM::TrySELCC_Exclusive_Lock(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
//        printf("PRE Update page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
        assert(TOPAGE(page_addr) == page_addr);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        //TODO: unwarp the updater_pre_access.
        if(!handle->try_updater_pre_access(page_addr, kLeafPageSize, lock_addr, mr)){
            page_cache->Release(handle);
            return false;
        }
        page_buffer = mr->addr;
        return true;
        //        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
//        assert(handle->gptr == page_addr);
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);

    }

    bool DDSM::SELCC_Lock_Upgrade(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *handle) {
        assert(handle != nullptr);
        assert(handle->remote_lock_status == 1);
        assert(handle->rw_mtx.issharelocked());
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

//        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle->value != nullptr);
        //TODO: unwarp the updater_pre_access.
//        if (!handle->rw_mtx.try_upgrade()){
//            return false;
//        }
        if (!handle->try_upgrade_pre_access(page_addr, kLeafPageSize, lock_addr, mr)){
            return false;
        }
        page_buffer = mr->addr;
        assert(STRUCT_OFFSET(LeafPage, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
        assert(((LeafPage*)page_buffer)->global_lock);
        assert(handle->gptr == page_addr);
        assert(((LeafPage*)page_buffer)->hdr.this_page_g_ptr == page_addr);
        return true;
    }

    void DDSM::SELCC_Exclusive_UnLock(GlobalAddress page_addr, Cache::Handle *handle) {
//        printf("POST Update or Write page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        //The assetion below is not always true.
        assert(handle->refs >1);
        ibv_mr *local_mr = (ibv_mr *) handle->value;
        assert(STRUCT_OFFSET(LeafPage, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
        auto page_buffer = local_mr->addr;
        assert(((DataPage*)page_buffer)->global_lock);
        handle->updater_writer_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
        page_cache->Release(handle);
        //TODO: delete the assert.
//        assert(!handle->rw_mtx.islocked());
//        handle = nullptr;

//        assert(((DataPage*)page_buffer)->hdr.this_page_g_ptr == page_addr);
    }
#elif ACCESS_MODE == 0

    bool DDSM::TrySELCC_Shared_Lock(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();

        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();

        if(!rdma_mg->global_Rlock_and_read_page_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr, 5)){
            page_buffer = nullptr;
            return false;
        }

        // maybe we need to copy it to a new buffer to avoid overwrite.
        page_buffer = new char[kLeafPageSize];
        memcpy(page_buffer, mr->addr, kLeafPageSize);
//        page_buffer = mr->addr;
        handle = new Cache::Handle();
        handle->value = page_buffer;
        handle->gptr = page_addr;
        return true;
    }
    bool DDSM::TrySELCC_Exclusive_Lock(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if(!rdma_mg->global_Wlock_and_read_page_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr, 5)){
            return false;
        }
        page_buffer = new char[kLeafPageSize];
        memcpy(page_buffer, mr->addr, kLeafPageSize);
//        page_buffer = mr->addr;
        handle = new Cache::Handle();
        handle->value = page_buffer;
        handle->gptr = page_addr;
        return true;
    }
    bool DDSM::SELCC_Lock_Upgrade(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *handle) {
        assert(handle);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (!rdma_mg->global_Rlock_update(mr,  lock_addr, cas_mr)){
            return false;
        }
        return true;
    }
    void DDSM::SELCC_Shared_Lock(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        rdma_mg->global_Rlock_and_read_page_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr);
        page_buffer = new char[kLeafPageSize];
        memcpy(page_buffer, mr->addr, kLeafPageSize);
//        page_buffer = mr->addr;
        handle = new Cache::Handle();
        handle->value = page_buffer;
        handle->gptr = page_addr;
    }

    void DDSM::SELCC_Shared_UnLock(GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        rdma_mg->global_RUnlock(lock_addr, cas_mr, false);
        delete[] (char*)handle->value;
        delete handle;
    }

    void DDSM::SELCC_Exclusive_Lock_noread(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        rdma_mg->global_Wlock_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr);
        page_buffer = new char[kLeafPageSize];
        memcpy(page_buffer, mr->addr, kLeafPageSize);
//        page_buffer = mr->addr;
        handle = new Cache::Handle();
        handle->value = page_buffer;
        handle->gptr = page_addr;

    }

    void DDSM::SELCC_Exclusive_UnLock_noread(GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *local_mr = rdma_mg->Get_local_read_mr();
        memcpy(local_mr->addr, handle->value, kLeafPageSize);
        rdma_mg->global_write_page_and_Wunlock(local_mr, page_addr, kLeafPageSize, lock_addr, nullptr, false);
        delete[] (char*)handle->value;
        delete handle;

    }

    void DDSM::SELCC_Exclusive_Lock(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        rdma_mg->global_Wlock_and_read_page_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr);
        page_buffer = new char[kLeafPageSize];
        memcpy(page_buffer, mr->addr, kLeafPageSize);
//        page_buffer = mr->addr;
        handle = new Cache::Handle();
        handle->value = page_buffer;
        handle->gptr = page_addr;

    }

    void DDSM::SELCC_Exclusive_UnLock(GlobalAddress page_addr, Cache::Handle * handle){
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *local_mr = rdma_mg->Get_local_read_mr();
        memcpy(local_mr->addr, handle->value, kLeafPageSize);
        rdma_mg->global_write_page_and_Wunlock(local_mr, page_addr, kLeafPageSize, lock_addr, nullptr, false);
        delete[] (char*)handle->value;
        delete handle;

    }
#endif

    // connectMemcached and disconnectMemcached are now handled by RDMA_Manager
    void DDSM::memSet(const char *key, uint32_t klen, const char *val,
                        uint32_t vlen) {
        // Use RDMA_Manager's memcached interface
        if (rdma_mg) {
            rdma_mg->memcachedSet(key, klen, val, vlen);
        } else {
            fprintf(stderr, "DDSM: RDMA_Manager not available for memcached operation\n");
        }
    }

    char *DDSM::memGet(const char *key, uint32_t klen, size_t *v_size) {
        // Use RDMA_Manager's memcached interface
        if (rdma_mg) {
            return rdma_mg->memcachedGet(key, klen, v_size);
        } else {
            fprintf(stderr, "DDSM: RDMA_Manager not available for memcached operation\n");
            return nullptr;
        }
    }

    uint64_t DDSM::memFetchAndAdd(const char *key, uint32_t klen) {
        // Use RDMA_Manager's memcached interface
        if (rdma_mg) {
            return rdma_mg->memcachedIncrement(key, klen, 1);
        } else {
            fprintf(stderr, "DDSM: RDMA_Manager not available for memcached operation\n");
            return 0;
        }
    }

    GlobalAddress DDSM::Allocate_Remote(Chunk_type pool_name) {
        if (rdma_mg) {
            uint64_t& target_node_counter = rdma_mg->round_robin_cur;
            uint8_t target_node = 2*(target_node_counter++ % rdma_mg->memory_nodes.size()) +1;
            auto ret = rdma_mg->Allocate_Remote_RDMA_Slot(pool_name, target_node);
            return ret;
        } else {
            assert(false);
            DEBUG_PRINT("RDMA manager is not initialized\n");
            return GlobalAddress::Null();
        }
    }

    uint64_t DDSM::ClusterSum(const std::string &sum_key, uint64_t value) {
        std::string key_prefix = std::string("sum-") + sum_key;

        std::string key = key_prefix + std::to_string(this->GetID());
        memSet(key.c_str(), key.size(), (char *)&value, sizeof(value));

        uint64_t ret = 0;
        for (size_t i = 0; i < this->rdma_mg->GetComputeNodeNum(); ++i) {
            key = key_prefix + std::to_string(2*i);
            ret += *(uint64_t *)memGet(key.c_str(), key.size());
        }

        return ret;
    }

    DDSM* DDSM::Get_Instance() {
        struct DSMEngine::config_t config = {
                NULL,  /* dev_name */
                NULL,  /* server_name */
                implicit_tcp_port, /* tcp_port */
                1,	 /* ib_port */ //physical
                1, /* gid_idx */
                4*10*1024*1024, /*initial local buffer size*/
                implicit_ThisNodeID
        };
//    DSMEngine::RDMA_Manager::node_id = ThisNodeID;
        static DDSM * ddsm = nullptr;
        static std::mutex lock;
        if (ddsm) {
            return ddsm;
        }else{
            lock.lock();
            if (!ddsm) {
                auto rdma_mg = RDMA_Manager::Get_Instance(&config);
                DSMEngine::Cache* cache_ptr = NewLRUCache(define::kIndexCacheSize*define::MB);
                rdma_mg->set_page_cache(cache_ptr);
                ddsm = new DDSM(cache_ptr, rdma_mg);
            }

            lock.unlock();

            return ddsm;
        }

    }


}
