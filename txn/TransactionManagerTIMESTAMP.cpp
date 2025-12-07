#if defined(TIMESTAMP)
#include "GlobalTimestamp.h"
#include "TransactionManager.h"

namespace DSMEngine {
    WritableFile* TransactionManager::log_file = nullptr;
    bool TransactionManager::AcquireLatchForTuple(
        char*& tuple_buffer, GlobalAddress tuple_gaddr, AccessType access_type) {
        GlobalAddress page_gaddr = TOPAGE(tuple_gaddr);
        assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
        void* page_buff;
        Cache::Handle* handle;
        if (locked_handles_.find(page_gaddr) == locked_handles_.end()) {
            if (access_type == READ_ONLY) {
                PROFILE_TIME_START(thread_id_, LOCK_READ);
                default_gallocator->SELCC_Shared_Lock(page_buff, page_gaddr, handle);
                assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
                tuple_buffer = (char*) page_buff + (tuple_gaddr.offset - handle->gptr.offset);
                locked_handles_.insert({page_gaddr, {handle, access_type}});
                assert(page_gaddr != GlobalAddress::Null());
                assert(access_type < READ_WRITE);
                PROFILE_TIME_END(thread_id_, LOCK_READ);
            } else {
                // DELETE_ONLY, READ_WRITE
                PROFILE_TIME_START(thread_id_, LOCK_WRITE);
                default_gallocator->SELCC_Exclusive_Lock(page_buff, page_gaddr, handle);
                assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
                tuple_buffer = (char*) page_buff + (tuple_gaddr.offset - handle->gptr.offset);
                locked_handles_.insert({page_gaddr, {handle, access_type}});
                assert(page_gaddr != GlobalAddress::Null());
                assert(access_type <= READ_WRITE);
                PROFILE_TIME_END(thread_id_, LOCK_WRITE);
            }

        } else {
            handle = locked_handles_.at(page_gaddr).first;
            // TODO: update the hierachical lock atomically, if the lock is shared lock
            if (access_type > READ_ONLY && locked_handles_[page_gaddr].second == READ_ONLY) {
                assert(false);
                default_gallocator->SELCC_Lock_Upgrade(page_buff, page_gaddr, handle);
                locked_handles_[page_gaddr].second = access_type;
            }
#if ACCESS_MODE == 1
            page_buff = ((ibv_mr*) handle->value)->addr;
#elif ACCESS_MODE == 0
            page_buff        = handle->value;
#endif
            tuple_buffer = (char*) page_buff + (tuple_gaddr.offset - handle->gptr.offset);
            assert(page_gaddr != GlobalAddress::Null());
            assert(access_type <= READ_WRITE);
        }
    }

    bool TransactionManager::AcquireXLatchForTuple(
        char*& tuple_buffer, GlobalAddress tuple_gaddr, Cache::Handle*& handle) {
        GlobalAddress page_gaddr = TOPAGE(tuple_gaddr);
        assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
        void* page_buff;
        PROFILE_TIME_START(thread_id_, LOCK_WRITE);

        if (locked_handles_.find(page_gaddr) == locked_handles_.end()) {
            // Hot scanner thread (thread_id_ == thread_count_) uses blocking locks for long-running transactions
            // All other threads use try locks
            bool is_hot_scanner = (thread_id_ == thread_count_);
            if (is_hot_scanner) {
                // Use blocking lock for hot scanner thread - will retry until success
                default_gallocator->SELCC_Exclusive_Lock(page_buff, page_gaddr, handle);
            } else {
                // Use try lock for normal transactions
                if (!default_gallocator->TrySELCC_Exclusive_Lock(page_buff, page_gaddr, handle)) {
                    PROFILE_TIME_END(thread_id_, LOCK_WRITE);
                    return false;
                }
            }
            assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
            tuple_buffer = (char*) page_buff + (tuple_gaddr.offset - handle->gptr.offset);
            // Store READ_WRITE as default since all locks are exclusive in timestamp CC
            // The actual access_type will be updated in SelectRecordCC after calling this function
            locked_handles_.insert({page_gaddr, {handle, READ_WRITE}});
            assert(page_gaddr != GlobalAddress::Null());
        } else {
            handle = locked_handles_.at(page_gaddr).first;
#if ACCESS_MODE == 1
            page_buff = ((ibv_mr*) handle->value)->addr;
#elif ACCESS_MODE == 0
            page_buff        = handle->value;
#endif
            tuple_buffer = (char*) page_buff + (tuple_gaddr.offset - handle->gptr.offset);
            assert(page_gaddr != GlobalAddress::Null());
        }
        PROFILE_TIME_END(thread_id_, LOCK_WRITE);
        return true;
    }

    void TransactionManager::ReleaseLatchForTuple(GlobalAddress tuple_addr, Cache::Handle* handle) {
        return;
    }

    void TransactionManager::ReleaseLatchForGCL(GlobalAddress page_gaddr, Cache::Handle* handle) {
        return;
    }
    inline bool TransactionManager::ClearAllLatches() {
        // todo: why the locked handles are not empty? It seems when I check with gdb it is empty but the program still
        // enter the loop and the if condition below.
        //        assert(!locked_handles_.empty());
        for (auto iter : locked_handles_) {
            default_gallocator->SELCC_Exclusive_UnLock(iter.second.first->gptr, iter.second.first);
        }
        if (!locked_handles_.empty()) {
            locked_handles_.clear();
        }
    }
    bool TransactionManager::AllocateNewRecord(
        size_t table_id, Cache::Handle*& handle, GlobalAddress& tuple_gaddr, Record*& tuple) {
        if (is_first_access_ == true) {
#if defined(BATCH_TIMESTAMP)
            if (!batch_ts_.IsAvailable()) {
                batch_ts_.InitTimestamp(GlobalTimestamp::GetBatchMonotoneTimestamp());
            }
            start_timestamp_ = batch_ts_.GetTimestamp();
#else
            start_timestamp_ = GlobalTimestamp::FetchAddMonotoneTimestamp();
#endif
            is_first_access_ = false;
        }

        char* tuple_buffer;
        Table* table = storage_manager_->tables_[table_id];
        // TODO: need to remember the latch, so that the latch can be released when the transaction abort.
        // besides, the allocatenew tuple function shall be implemented seperated to the one in the table.
        // The table one is for loading wiithout concurrency control considering.
        void* page_buffer;
        GlobalAddress* gcl_addr = table->GetOpenedBlock();
        DataPage* page          = nullptr;
        DDSM* gallocator        = gallocators[thread_id_];
        if (gcl_addr) {
            GlobalAddress cacheline_g_addr = *gcl_addr;
            if (locked_handles_.find(cacheline_g_addr) == locked_handles_.end()) {
                // Hot scanner thread uses blocking locks, others use try locks
                bool is_hot_scanner = (thread_id_ == thread_count_);
                if (is_hot_scanner) {
                    gallocator->SELCC_Exclusive_Lock(page_buffer, *gcl_addr, handle);
                } else {
                    if (!gallocator->TrySELCC_Exclusive_Lock(page_buffer, *gcl_addr, handle)) {
                        this->AbortTransaction();
                        return false;
                    }
                }
                assert(((DataPage*) page_buffer)->hdr.table_id == table_id);
                (locked_handles_)[cacheline_g_addr] = std::pair(handle, INSERT_ONLY);
                page                                = reinterpret_cast<DataPage*>(page_buffer);
            } else {
                handle = locked_handles_.at(cacheline_g_addr).first;
#if ACCESS_MODE == 1
                page_buffer = ((ibv_mr*) handle->value)->addr;
#elif ACCESS_MODE == 0
                page_buffer        = handle->value;
#endif
                page        = reinterpret_cast<DataPage*>(page_buffer);
            }
        } else {
            gcl_addr  = new GlobalAddress();
            *gcl_addr = gallocator->Allocate_Remote(Regular_Page);
            table->SetOpenedBlock(gcl_addr);
            // Hot scanner thread uses blocking locks, others use try locks
            bool is_hot_scanner = (thread_id_ == thread_count_);
            if (is_hot_scanner) {
                gallocator->SELCC_Exclusive_Lock(page_buffer, *gcl_addr, handle);
            } else {
                if (!gallocator->TrySELCC_Exclusive_Lock(page_buffer, *gcl_addr, handle)) {
                    delete gcl_addr;
                    this->AbortTransaction();
                    return false;
                }
            }

            uint64_t cardinality =
                8ull * (kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) / (8ull * table->GetSchemaSize() + 1);
            page                         = new (page_buffer) DataPage(*gcl_addr, cardinality, table_id);
            (locked_handles_)[*gcl_addr] = std::pair(handle, INSERT_ONLY);
        }
        //           table->Allo/cateNewTuple(tuple_buffer, tuple_gaddr, handle, default_gallocator, nullptr);
        RecordSchema* schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
        int cnt                  = 0;
        volatile bool ret        = page->AllocateRecord(cnt, schema_ptr, tuple_gaddr, tuple_buffer);
        assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
        assert((char*) tuple_buffer - (char*) page_buffer > STRUCT_OFFSET(DataPage, data_));
        assert(((DataPage*) page_buffer)->hdr.this_page_g_ptr != GlobalAddress::Null());
        assert(ret);
        if (cnt == page->hdr.kDataCardinality) {
            delete gcl_addr;
            table->SetOpenedBlock(nullptr);
        }
        // todo: update the write time stamp here, get the txn timestamp in this function as well.
        tuple = new Record(schema_ptr, tuple_buffer);
        tuple->PutWTS(start_timestamp_);
        Access* access                = access_list_.NewAccess();
        access->access_type_          = INSERT_ONLY;
        access->access_global_record_ = tuple;
        access->access_addr_          = tuple_gaddr;
        //        printf("AllocateNewRecord: thread_id=%zu,table_id=%zu,access_type=%u,data_addr=%lx, start
        //        SelectRecordCC\n",
        //               thread_id_, table_id, INSERT_ONLY, tuple_gaddr.val);
        //            fflush(stdout);
        return true;

        //        GlobalAddress* g_addr = table->GetOpenedBlock();
        //        if ( g_addr == nullptr){
        //            g_addr = new GlobalAddress();
        //            *g_addr = default_gallocator->Allocate_Remote(Regular_Page);
        //            table->SetOpenedBlock(g_addr);
        //        }
        //        assert(handle != nullptr);
        //        assert(page_buffer != nullptr);
        //        uint64_t cardinality = 8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) /
        //        (8ull*table->GetSchema()->GetRecordTotalSize() +1); auto* page = new(page_buffer) DataPage(*g_addr,
        //        cardinality, table_id); int cnt = 0; bool ret = page->AllocateRecord(cnt, table->GetSchema() ,
        //        tuple_gaddr, tuple_buffer); assert(ret);
        //        // if the cache line is full, set the thread local ptr as null, and allocate a new page next time.
        //        if(cnt == page->hdr.kDataCardinality){
        //            table->SetOpenedBlock(nullptr);
        //        }


        //        default_gallocator->SELCC_Exclusive_Lock_noread(page_buffer, g_addr, handle);
    }
    bool TransactionManager::InsertRecord(size_t table_id, const DynamicCompoundKey keys, size_t key_num,
        Record* record, Cache::Handle* handle, const GlobalAddress tuple_gaddr) {
        if (is_first_access_ == true) {
#if defined(BATCH_TIMESTAMP)
            if (!batch_ts_.IsAvailable()) {
                batch_ts_.InitTimestamp(GlobalTimestamp::GetBatchMonotoneTimestamp());
            }
            start_timestamp_ = batch_ts_.GetTimestamp();
#else
            start_timestamp_ = GlobalTimestamp::FetchAddMonotoneTimestamp();
#endif
            is_first_access_ = false;
        }
        record->is_visible_ = false;
        PROFILE_TIME_START(thread_id_, INDEX_INSERT);
        // todo: move the index insertion to the commit phase.
        bool ret = storage_manager_->tables_[table_id]->InsertPriIndex(keys, key_num, tuple_gaddr);
        PROFILE_TIME_END(thread_id_, INDEX_INSERT);
        PROFILE_TIME_END(thread_id_, CC_INSERT);
        // Locks are held until commit/abort - do not release here
        return true;
    }

    // Assert that there is no latch still hold in the before the transaction abort. makesure that txn release the last
    // tuple's, latch access the next one. Never let a transaction holding two latch at the same time!!!!
    bool TransactionManager::SelectRecordCC(
        size_t table_id, Record*& record, const GlobalAddress& tuple_gaddr, AccessType access_type) {
        assert(start_timestamp_ < 0x700066737575);

        if (is_first_access_ == true) {

            start_timestamp_ = GlobalTimestamp::FetchAddMonotoneTimestamp();
            assert(start_timestamp_ < 0x700066737575);
            is_first_access_ = false;
        }
        assert(start_timestamp_ < 0x700066737575);

        PROFILE_TIME_START(thread_id_, CC_SELECT);
        GlobalAddress page_gaddr = TOPAGE(tuple_gaddr);
        assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
        RecordSchema* schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
        void* page_buff;
        Cache::Handle* handle;
        char* tuple_buffer;
        assert(start_timestamp_ < 0x700066737575);

        // In timestamp CC, all locks are exclusive - use AcquireXLatchForTuple for all access types
        if (!AcquireXLatchForTuple(tuple_buffer, tuple_gaddr, handle)) {
            PROFILE_TIME_END(thread_id_, CC_SELECT);
            this->AbortTransaction();
            return false;
        }
        assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
        assert(start_timestamp_ < 0x700066737575);

        record = new Record(schema_ptr, tuple_buffer);
        record->Set_Handle(handle);
        assert(start_timestamp_ < 0x700066737575);

        Access* access                = access_list_.NewAccess();
        access->access_type_          = access_type;
        access->access_global_record_ = record;
        access->access_addr_          = tuple_gaddr;
        if (access_type == READ_WRITE) {
            // local tuple servers as the roll back tuple, in TO transaction concurrency control.
            // This has to be done before update the write time stamp.
            Record* local_tuple = new Record(schema_ptr);
            local_tuple->CopyFrom(record);
            access->txn_local_tuple_ = local_tuple;
        }
        assert(start_timestamp_ < 0x700066737575);

        // TODO: need to remember the latch, so that the latch can be released when the transaction abort.
        if (access_type == READ_ONLY) {


            // Check timestamp for read-only access
            uint64_t wts = record->GetWTS();
            if (wts > start_timestamp_) {
                PROFILE_TIME_END(thread_id_, CC_SELECT);
                this->AbortTransaction();
                return false;
            } else {
                record->PutRTS(start_timestamp_);
            }
        } else {
            // Read_Write, Delete_Only, Insert_Only
            uint64_t rts = record->GetRTS();
            uint64_t wts = record->GetWTS();
            if (rts > start_timestamp_ || wts > start_timestamp_) {
                PROFILE_TIME_END(thread_id_, CC_SELECT);
                this->AbortTransaction();
                return false;
            } else {
                assert(start_timestamp_ < 0x700066737575);
                record->PutWTS(start_timestamp_);
            }
        }

        if (access_type == DELETE_ONLY) {
            record->SetVisible(false);
        }

        PROFILE_TIME_END(thread_id_, CC_SELECT);
        assert(record->schema_ptr_ == storage_manager_->tables_[table_id]->GetSchema());
        return true;
    }

    bool TransactionManager::CommitTransaction(CharArray& ret_str) {
        PROFILE_TIME_START(thread_id_, CC_COMMIT);
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);

            if (access->access_type_ == DELETE_ONLY) {
                // TODO: implement the delete function.
                //        gallocators[thread_id_]->Free(access->access_addr_);
                //        access->access_addr_ = Gnullptr;
            }
            delete access->access_global_record_;
            access->access_global_record_ = nullptr;
            access->access_addr_          = GlobalAddress::Null();
            if (access->txn_local_tuple_ != nullptr) {
                assert(access->access_type_ == READ_WRITE);
                delete access->txn_local_tuple_;
                access->txn_local_tuple_ = nullptr;
            }
        }
        access_list_.Clear();
        
        // Release all locks collectively (all locks are exclusive in timestamp CC)
        for (auto iter : locked_handles_) {
            assert(iter.second.second == READ_ONLY || iter.second.second == DELETE_ONLY
                   || iter.second.second == INSERT_ONLY || iter.second.second == READ_WRITE);
            assert(iter.second.first->remote_lock_status == 2);
            default_gallocator->SELCC_Exclusive_UnLock(iter.second.first->gptr, iter.second.first);
        }
        locked_handles_.clear();
        is_first_access_ = true;
        PROFILE_TIME_END(thread_id_, CC_COMMIT);
        return true;
    }

    void TransactionManager::AbortTransaction() {
        PROFILE_TIME_START(thread_id_, CC_ABORT);
        std::map<uint64_t, Access*> sorted_access;
        // lock the access list in order to avoid deadlock.
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access             = access_list_.GetAccess(i);
            GlobalAddress tuple_g_addr = access->access_addr_;
            sorted_access.insert({tuple_g_addr, access});
        }
        for (auto iter : sorted_access) {
            Access* access = iter.second;
            GlobalAddress page_gaddr;
            Cache::Handle* handle;
            char* tuple_buffer;
            if (access->access_type_ != READ_ONLY) {
                // Try to rollback changes if locks are still held
                GlobalAddress& tuple_gaddr = access->access_addr_;
                page_gaddr                 = TOPAGE(tuple_gaddr);
                assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
                
                // Only rollback if we still hold the lock
                if (locked_handles_.find(page_gaddr) != locked_handles_.end()) {
                    handle = locked_handles_.at(page_gaddr).first;
#if ACCESS_MODE == 1
                    tuple_buffer = (char*)((ibv_mr*) handle->value)->addr + (tuple_gaddr.offset - handle->gptr.offset);
#elif ACCESS_MODE == 0
                    tuple_buffer = (char*)handle->value + (tuple_gaddr.offset - handle->gptr.offset);
#endif
                    access->access_global_record_->ReSetRecordBuff(
                        tuple_buffer, access->access_global_record_->GetRecordSize(), false);
                }
            }
            if (access->access_type_ == INSERT_ONLY) {
                // Mark as not visible - locks may have been released already
                if (access->access_global_record_ != nullptr) {
                    access->access_global_record_->SetVisible(false);
                }
                delete access->access_global_record_;
                access->access_global_record_ = nullptr;
                // todo: Deallocate the space of inserted tuples.
            } else if (access->access_type_ == READ_WRITE) {
                // Rollback changes if we have the local tuple copy
                if (access->txn_local_tuple_ != nullptr && access->access_global_record_ != nullptr) {
                    // Only rollback if we still hold the lock
                    if (locked_handles_.find(TOPAGE(access->access_addr_)) != locked_handles_.end()) {
                        access->access_global_record_->CopyFrom(access->txn_local_tuple_);
                    }
                    delete access->txn_local_tuple_;
                    access->txn_local_tuple_ = nullptr;
                }
            } else if (access->access_type_ == DELETE_ONLY) {
                // Restore visibility if we still hold the lock
                if (access->access_global_record_ != nullptr && 
                    locked_handles_.find(TOPAGE(access->access_addr_)) != locked_handles_.end()) {
                    access->access_global_record_->SetVisible(true);
                }
            }

            delete access->access_global_record_;
            access->access_global_record_ = nullptr;
            access->access_addr_          = GlobalAddress::Null();
        }
        access_list_.Clear();
        
        // Release all remaining locks collectively (all locks are exclusive in timestamp CC)
        for (auto iter : locked_handles_) {
            assert(iter.second.second == READ_ONLY || iter.second.second == DELETE_ONLY
                   || iter.second.second == INSERT_ONLY || iter.second.second == READ_WRITE);
            assert(iter.second.first->remote_lock_status == 2);
            default_gallocator->SELCC_Exclusive_UnLock(iter.second.first->gptr, iter.second.first);
        }
        locked_handles_.clear();
        is_first_access_ = true;
        PROFILE_TIME_END(thread_id_, CC_ABORT);
    }


} // namespace DSMEngine

#endif
