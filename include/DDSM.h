//
// Created by ruihong on 10/1/23.
//

#ifndef SELCC_DDSM_H
#define SELCC_DDSM_H
#include "cache.h"
#include "storage/page.h"
#include <libmemcached/memcached.h>
namespace DSMEngine {
    static void Deallocate_MR_WITH_CCP(Cache::Handle *handle) {
        // TOFIX: The code below is not protected by the lock shared mutex. It is Okay because,
        // there is definitely no other thread accessing it if a page is destroyed (refs == 0)
        assert(handle->refs.load() == 0);
        auto rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        //TODO: we need to check whether the cache handle have buffered inv message, if so we need to process it.
        // However, this shall rarely happen.
        auto mr = (ibv_mr*) handle->value;
        if (handle->buffer_inv_message.next_holder_id!= Invalid_Node_ID){
            handle->process_buffered_inv_message(handle->gptr, kLeafPageSize, handle->gptr, (ibv_mr*)handle->value, false);
        }
//        if (handle->strategy == 1){
            GlobalAddress lock_gptr = handle->gptr;
            //TODO: Figure out Leafpage or internal page?
            lock_gptr.offset = lock_gptr.offset + STRUCT_OFFSET(LeafPage<uint64_t>, global_lock);
            assert(STRUCT_OFFSET(LeafPage<uint64_t>, global_lock) == STRUCT_OFFSET(InternalPage<uint64_t>, global_lock));
            if (handle->remote_lock_status == 1){

                // RDMA read unlock
//            printf("release the read lock during the handle destroy\n ");
                rdma_mg->global_RUnlock(lock_gptr, rdma_mg->Get_local_CAS_mr());
//                handle->last_modifier_thread_id = 256;
                handle->remote_lock_status.store(0);

            }else if(handle->remote_lock_status == 2){

                // TODO: shall we not consider the global lock word when flushing back the page?
                assert(mr->addr!= nullptr );

//                TODO: recover the assert below if we are testing the blind write operation.
                LeafPage<uint64_t>* page = ((LeafPage<uint64_t>*)mr->addr);
#ifndef NDEBUG
                assert(STRUCT_OFFSET(InternalPage<uint64_t >, global_lock) == STRUCT_OFFSET(LeafPage<uint64_t>, global_lock));
                if (page->hdr.p_type == P_Internal_P){
//                    printf("Internal page is being destroyed %p\n", handle->gptr);
                }
#endif
                assert(page->hdr.this_page_g_ptr == GlobalAddress::Null() || page->hdr.this_page_g_ptr == handle->gptr);
                assert(page->global_lock);
//                assert(handle->gptr == ((LeafPage<uint64_t,uint64_t>*)mr->addr)->hdr.this_page_g_ptr);

                //TODO: make handle state change and page dirty content flush back a atomic function in Cache_handle class.

                // RDMA write unlock and write back the data. THis shall be a sync write back, because the buffer will
                // be handover to other cache entry after this function. It is possible that the page content is changed when the
                // RDMA write back has not been finished. The write unlock for page invalidation can be a sync write back.
                rdma_mg->global_write_page_and_Wunlock(mr, handle->gptr, kLeafPageSize, lock_gptr, handle);
                handle->remote_lock_status.store(0);
            }else{
                //An invalidated page, do nothing
            }

        handle->clear_pending_inv_states();
#ifdef WRITER_STARV_SPIN_BASE
        handle->reader_spin_time.store(0);
#endif
#ifndef PAGE_FREE_LIST
        if (!handle->keep_the_mr){
            rdma_mg->Deallocate_Local_RDMA_Slot(mr->addr, Regular_Page);
            delete mr;
        }
#endif
        assert(handle->refs.load() == 0);
    }


    class DDSM {
    public:
        Cache *page_cache;
        RDMA_Manager *rdma_mg = nullptr;
        //TODO: implement a thread local cache line hold memo.
        memcached_st *memc;
        std::mutex memc_mutex;
        GlobalAddress catalog_ptr = GlobalAddress::Null();
        DDSM(Cache *page_cache, RDMA_Manager *rdma_mg = nullptr) : page_cache(page_cache), rdma_mg(rdma_mg) {
            if (!connectMemcached()) {
                printf("Failed to connect to memcached\n");
                return;
            }
            char temp[100] = "Try me ahahahahaha! kkk";
            memSet(reinterpret_cast<const char *>(&temp), 100, reinterpret_cast<const char *>(&temp), 100);
            if (rdma_mg->node_id == 0){
                catalog_ptr = Allocate_Remote(Regular_Page);
                //we get the GCL addr of catalog from the global index table.
                ibv_mr remote_mr = *rdma_mg->global_index_table;
                remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*0);
                auto cas_buffer = rdma_mg->Get_local_CAS_mr();
                // not necessary to use RDMA cas, use RDMA write instead.
                rdma_mg->RDMA_CAS(&remote_mr, cas_buffer, 0, catalog_ptr, IBV_SEND_SIGNALED, 1, 1);
            }
        };
        ~DDSM(){
            disconnectMemcached();
        }
        void Update_Root_GCL(uint16_t tree_id, GlobalAddress new_root_gptr){
            if (catalog_ptr == GlobalAddress::Null()){
                // Allocate one remote GCL.
                auto read_mr = rdma_mg->Get_local_read_mr();
                ibv_mr remote_mr = *rdma_mg->global_index_table;
                remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*0);
                rdma_mg->RDMA_Read(&remote_mr, read_mr, sizeof(GlobalAddress), IBV_SEND_SIGNALED, 1, 1);
                assert(*((GlobalAddress*)read_mr->addr) != GlobalAddress::Null());
                catalog_ptr = *((GlobalAddress*)read_mr->addr);
            }
            void* catlog_buf = nullptr;
            Cache_Handle* catlog_h;
            SELCC_Exclusive_Lock(catlog_buf, catalog_ptr, catlog_h );
            auto catlog = (CatalogPage*) catlog_buf;
            catlog->root_gptrs[tree_id] = new_root_gptr;
            SELCC_Exclusive_UnLock(catalog_ptr, catlog_h);
        }
        GlobalAddress Get_Root_GCL(uint16_t tree_id){
            if (catalog_ptr == GlobalAddress::Null()){
                // Allocate one remote GCL.
                auto read_mr = rdma_mg->Get_local_read_mr();
                ibv_mr remote_mr = *rdma_mg->global_index_table;
                remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*0);
                rdma_mg->RDMA_Read(&remote_mr, read_mr, sizeof(GlobalAddress), IBV_SEND_SIGNALED, 1, 1);
                assert(*((GlobalAddress*)read_mr->addr) != GlobalAddress::Null());
                catalog_ptr = *((GlobalAddress*)read_mr->addr);
            }
            GlobalAddress ret ;
            void* catlog_buf = nullptr;
            Cache_Handle* catlog_h;
            SELCC_Shared_Lock(catlog_buf, catalog_ptr, catlog_h );
            auto catlog = (CatalogPage*) catlog_buf;
            ret = catlog->root_gptrs[tree_id];
            SELCC_Shared_UnLock(catalog_ptr, catlog_h);
            return ret;
        }
        static DDSM *Get_Instance();
        //todo: do we need destroy instance?
        void SELCC_Shared_Lock(void*& page_buffer, GlobalAddress page_addr, Cache::Handle*& handle);
        bool TrySELCC_Shared_Lock(void*& page_buffer, GlobalAddress page_addr, Cache::Handle*& handle);
        void SELCC_Shared_UnLock(GlobalAddress page_addr, Cache::Handle *&handle);
        void SELCC_Exclusive_Lock_noread(void*& page_buffer, GlobalAddress page_addr, Cache::Handle *&handle);
//        bool TrySELCC_Exclusive_Lock_noread(void*& page_buffer, GlobalAddress page_addr, Cache::Handle *&handle);

        void SELCC_Exclusive_UnLock_noread(GlobalAddress page_addr, Cache::Handle *&handle);
        void SELCC_Exclusive_Lock(void*& page_buffer, GlobalAddress page_addr, Cache::Handle *&handle);
        bool TrySELCC_Exclusive_Lock(void*& page_buffer, GlobalAddress page_addr, Cache::Handle *&handle);

        // Handle should be not nullptr
        //TODO: make the hierachical lock upgrade atomaticlly, currently we release and then acquire the lock.
        bool SELCC_Lock_Upgrade(void*& page_buffer, GlobalAddress page_addr, Cache::Handle* handle);

        void SELCC_Exclusive_UnLock(GlobalAddress page_addr, Cache::Handle *handle);
        bool connectMemcached();
        bool disconnectMemcached();
        void memSet(const char *key, uint32_t klen, const char *val, uint32_t vlen);
        //blocking function.
        char *memGet(const char *key, uint32_t klen, size_t *v_size = nullptr);
        uint64_t ClusterSum(const std::string &sum_key, uint64_t value);
        uint64_t memFetchAndAdd(const char *key, uint32_t klen);
        GlobalAddress Allocate_Remote(Chunk_type pool_name); // allocate
        void Deallocate_Remote(Chunk_type pool_name, GlobalAddress gaddr){}; // free
        uint16_t GetID(){
            return rdma_mg->node_id;
        }
        static uint64_t GetNextIndexID(){
            static std::atomic<uint64_t> index_id = {0};
            return index_id.fetch_add(1);
        }
    private:
//        std::atomic<uint64_t > target_node_counter = {0};
    };
    class SELCC_Guard{
    public:
        enum LockType{
            Shared,
            Exclusive
        };
        GlobalAddress page_addr_ = GlobalAddress::Null();
        Cache::Handle* handle_ = nullptr;
        LockType lock_type_;
        SELCC_Guard(){};
        SELCC_Guard(const SELCC_Guard&) = delete;
        SELCC_Guard(SELCC_Guard&) = delete;
        SELCC_Guard(SELCC_Guard&& other) noexcept {
            other.page_addr_ = GlobalAddress::Null();
            other.handle_ = nullptr;
            page_addr_ = other.page_addr_;
            handle_ = other.handle_;
            lock_type_ = other.lock_type_;

        }
        //        SELCC_Guard(void* &page_buffer, GlobalAddress page_addr, Cache::Handle* & handle){} = default;
        // TODO: make the destructor a virtual function
        virtual ~SELCC_Guard(){
            if (handle_){
                if(lock_type_ == Shared){
                    DDSM::Get_Instance()->SELCC_Shared_UnLock(page_addr_, handle_);
                }else {
                    DDSM::Get_Instance()->SELCC_Exclusive_UnLock(page_addr_, handle_);
                }
            }

        }
    };
    class Exclusive_Guard : public SELCC_Guard{
    public:
//        GlobalAddress page_addr_;
//        Cache::Handle* handle_;
        Exclusive_Guard(void* &page_buffer, GlobalAddress page_addr, Cache::Handle* & handle){
            if (page_addr == GlobalAddress::Null()){
                handle = nullptr;
                return;
            }
            DDSM::Get_Instance()->SELCC_Exclusive_Lock(page_buffer, page_addr, handle);
            handle_ = handle;
            page_addr_ = page_addr;

        }
        void Reset(void* &page_buffer, GlobalAddress page_addr, Cache::Handle* & handle){
            destroy();
            if (page_addr == GlobalAddress::Null()){
                handle = nullptr;
                return;
            }
            DDSM::Get_Instance()->SELCC_Exclusive_Lock(page_buffer, page_addr, handle);
            handle_ = handle;
            page_addr_ = page_addr;
        }
        inline void destroy(){
            if (page_addr_ == GlobalAddress::Null()){
                return;
            }
            DDSM::Get_Instance()->SELCC_Exclusive_UnLock(page_addr_, handle_);

        }
        Exclusive_Guard(const Exclusive_Guard&) = delete;
        ~Exclusive_Guard() override {
            destroy();
        }
    };
    class Shared_Guard: public SELCC_Guard{
    public:
//        GlobalAddress page_addr_;
//        Cache::Handle* handle_;
        Shared_Guard(void* &page_buffer, GlobalAddress page_addr, Cache::Handle* & handle){
            DDSM::Get_Instance()->SELCC_Shared_Lock(page_buffer, page_addr, handle);
            handle_ = handle;
            page_addr_ = page_addr;

        }
        ~Shared_Guard() override{
            DDSM::Get_Instance()->SELCC_Shared_UnLock(page_addr_, handle_);

        }
    };

}


#endif //SELCC_DDSM_H
