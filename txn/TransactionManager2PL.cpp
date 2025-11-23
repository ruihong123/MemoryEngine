// NOTICE: this file is adapted from Cavalia
#if defined(LOCK)
#include "TransactionManager.h"

namespace DSMEngine {
    WritableFile* TransactionManager::log_file = nullptr;
    bool TransactionManager::AllocateNewRecord(
        size_t table_id, Cache::Handle*& handle, GlobalAddress& tuple_gaddr, Record*& tuple) {
        char* tuple_buffer;
        Table* table = storage_manager_->tables_[table_id];
        if (!table->AllocateNewTuple(tuple_buffer, tuple_gaddr, handle, default_gallocator, &locked_handles_)) {
            this->AbortTransaction();
            return false;
        }
        RecordSchema* schema_ptr      = storage_manager_->tables_[table_id]->GetSchema();
        tuple                         = new Record(schema_ptr, tuple_buffer);
        Access* access                = access_list_.NewAccess();
        access->access_type_          = INSERT_ONLY;
        access->access_global_record_ = tuple;
        access->access_addr_          = tuple_gaddr;
        assert(locked_handles_.find(TOPAGE(tuple_gaddr)) != locked_handles_.end());
        //        printf("AllocateNewRecord: thread_id=%zu,table_id=%zu,access_type=%u,data_addr=%lx, start
        //        SelectRecordCC\n",
        //               thread_id_, table_id, INSERT_ONLY, tuple_gaddr.val);
        //        fflush(stdout);
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
    // The hierachy lock shall be acquired outside of this function.
    bool TransactionManager::InsertRecord(size_t table_id, const DynamicCompoundKey keys, size_t key_num,
        Record* record, Cache::Handle* handle, const GlobalAddress tuple_gaddr) {
        PROFILE_TIME_START(thread_id_, CC_INSERT);
        //    RecordSchema *schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
        record->SetVisible(true);
        //    Access* access = access_list_.NewAccess();
        //    access->access_type_ = INSERT_ONLY;
        //    access->access_global_record_ = record;
        ////    access->access_handle_ = hadndle;
        //    access->access_addr_ = tuple_gaddr;
        PROFILE_TIME_START(thread_id_, INDEX_INSERT);
        // TODO: comment the index insertion for a fair comparision
        bool ret = storage_manager_->tables_[table_id]->InsertPriIndex(keys, key_num, tuple_gaddr);

        PROFILE_TIME_END(thread_id_, INDEX_INSERT);
        PROFILE_TIME_END(thread_id_, CC_INSERT);
        return true;
    }
    // TODO: Implement the delete record function.

    bool TransactionManager::SelectRecordCC(
        size_t table_id, Record*& record, const GlobalAddress& tuple_gaddr, AccessType access_type) {
        //    printf(LOG_DEBUG, "thread_id=%u,table_id=%u,access_type=%u,data_addr=%lx, start SelectRecordCC",
        //        thread_id_, table_id, access_type, data_addr);
        PROFILE_TIME_START(thread_id_, CC_SELECT);
        GlobalAddress page_gaddr = TOPAGE(tuple_gaddr);
        assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
        RecordSchema* schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
        void* page_buff;
        Cache::Handle* handle;
        //    GlobalAddress page_addr = TOPAGE(tuple_gaddr);
        char* tuple_buffer;
        if (locked_handles_.find(page_gaddr) == locked_handles_.end()) {
            if (access_type == READ_ONLY) {
                PROFILE_TIME_START(thread_id_, LOCK_READ);
                // Hot scanner thread (thread_id_ == thread_count_) uses blocking locks for long-running reads
                // All other threads use try locks
                bool is_hot_scanner = (thread_id_ == thread_count_);
                if (is_hot_scanner) {
                    // Use blocking lock for hot scanner thread - will retry until success
                    default_gallocator->SELCC_Shared_Lock(page_buff, page_gaddr, handle);
                } else {
                    // Use try lock for normal transactions
                    if (!default_gallocator->TrySELCC_Shared_Lock(page_buff, page_gaddr, handle)) {
                        this->AbortTransaction();
                        return false;
                    }
                }
                assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
                tuple_buffer = (char*) page_buff + (tuple_gaddr.offset - handle->gptr.offset);
                //              locked_handles_[page_gaddr] = std::pair(handle, access_type);
                locked_handles_.insert({page_gaddr, {handle, access_type}});
                assert(page_gaddr != GlobalAddress::Null());
                assert(access_type < READ_WRITE);
                //              printf("Threadid %zu Acquire read lock for nodeid %d, offset %lu --- %p, lock handle
                //              number is %zu\n", thread_id_, page_gaddr.nodeID, page_gaddr.offset, page_gaddr,
                //              locked_handles_.size());
                PROFILE_TIME_END(thread_id_, LOCK_READ);
            } else {
                // DELETE_ONLY, READ_WRITE
                PROFILE_TIME_START(thread_id_, LOCK_WRITE);
                //              default_gallocator->SELCC_Exclusive_Lock(page_buff, page_gaddr, handle);
                if (!default_gallocator->TrySELCC_Exclusive_Lock(page_buff, page_gaddr, handle)) {
                    //                  printf("Abort at local tuple read\n");
                    //                  fflush(stdout);
                    this->AbortTransaction();
                    return false;
                }
                assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
                tuple_buffer = (char*) page_buff + (tuple_gaddr.offset - handle->gptr.offset);
                //              locked_handles_[page_gaddr] = std::pair(handle, access_type);
                locked_handles_.insert({page_gaddr, {handle, access_type}});
                assert(page_gaddr != GlobalAddress::Null());
                assert(access_type <= READ_WRITE);
                //              printf("Threadid %zu Acquire write lock for nodeid %d, offset %lu --- %p, lock handle
                //              number is %zu\n", thread_id_, page_gaddr.nodeID, page_gaddr.offset, page_gaddr,
                //              locked_handles_.size());
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
            // TODO: change the code below for SEL-DM.
#if ACCESS_MODE == 1
            page_buff = ((ibv_mr*) handle->value)->addr;
#elif ACCESS_MODE == 0
            page_buff = handle->value;
#endif
            tuple_buffer = (char*) page_buff + (tuple_gaddr.offset - handle->gptr.offset);
            assert(page_gaddr != GlobalAddress::Null());
            assert(access_type <= READ_WRITE);
        }

        record                        = new Record(schema_ptr, tuple_buffer);
        Access* access                = access_list_.NewAccess();
        access->access_type_          = access_type;
        access->access_global_record_ = record;
        access->access_addr_          = tuple_gaddr;
        if (access_type == DELETE_ONLY) {
            record->SetVisible(false);
        }
        if (access_type == READ_WRITE) {
            // TODO: roll back according to via the undo log segment
            Record* local_tuple = new Record(schema_ptr);
            local_tuple->CopyFrom(record);
            access->txn_local_tuple_ = local_tuple;
        }
        PROFILE_TIME_END(thread_id_, CC_SELECT);
        return true;
        //    }
        //    else { // fail to acquire lock
        //      PROFILE_TIME_END(thread_id_, CC_SELECT);
        //      epicLog(LOG_DEBUG, "thread_id=%u,table_id=%u,access_type=%u,data_addr=%lx,lock fail, abort",
        //          thread_id_, table_id, access_type,data_addr);
        //      this->AbortTransaction();
        //      return false;
        //    }
    }
    bool TransactionManager::CoordinatorPrepare() {
        PROFILE_TIME_START(thread_id_, CC_COMMIT);
        if (log_enabled_) {
            for (size_t i = 0; i < access_list_.access_count_; ++i) {
                Access* access = access_list_.GetAccess(i);
                Slice log_record =
                    Slice(access->access_global_record_->data_ptr_, access->access_global_record_->data_size_);
                log_file->Append(log_record);
            }
        }

        if (log_enabled_) {
            WritePrepareLog();
        }
        //      assert(locked_handles_.size() == access_list_.access_count_);
        assert(locked_handles_.size() > 0);
        for (auto iter : locked_handles_) {
            assert(iter.second.second == READ_ONLY || iter.second.second == DELETE_ONLY
                   || iter.second.second == INSERT_ONLY || iter.second.second == READ_WRITE);
            GlobalAddress page_addr = iter.second.first->gptr;
            if (iter.second.second == READ_ONLY) {
                assert(iter.second.first->remote_lock_status >= 1);

                default_gallocator->SELCC_Shared_UnLock(iter.second.first->gptr, iter.second.first);
                assert(iter.first == page_addr.val);
                //            printf("Threadid %zu Release read lock for nodeid %d, offset %lu lock handle number is
                //            %zu\n", thread_id_, page_addr.nodeID, page_addr.offset, locked_handles_.size());
            } else {
                assert(iter.second.first->remote_lock_status == 2);
                default_gallocator->SELCC_Exclusive_UnLock(iter.second.first->gptr, iter.second.first);
                //            printf("Threadid %zu Release write lock for nodeid %d, offset %lu lock handle number is
                //            %zu\n", thread_id_, page_addr.nodeID, page_addr.offset, locked_handles_.size());
            }
        }
        // GC
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            //        if (log_enabled_){
            //            Slice log_record = Slice(access->access_global_record_->data_ptr_,
            //            access->access_global_record_->data_size_); log_file->Append(log_record);
            //        }
            if (access->access_type_ == DELETE_ONLY) {
                // TODO: implement the delete function.
                //        gallocators[thread_id_]->Free(access->access_addr_);
                //        access->access_addr_ = Gnullptr;
            }
            //        printf("this access index is %zu\n",i);
            //        fflush(stdout);
            delete access->access_global_record_;
            access->access_global_record_ = nullptr;
            access->access_addr_          = GlobalAddress::Null();
            if (access->txn_local_tuple_ != nullptr) {
                assert(access->access_type_ == READ_WRITE);
                delete access->txn_local_tuple_;
                access->txn_local_tuple_ = nullptr;
            }
        }


        participants.clear();

        access_list_.Clear();
        locked_handles_.clear();
        PROFILE_TIME_END(thread_id_, CC_COMMIT);
        return true;
    }

    bool TransactionManager::CommitTransaction(CharArray& ret_str) {
        PROFILE_TIME_START(thread_id_, CC_COMMIT);
        if (log_enabled_) {
            for (size_t i = 0; i < access_list_.access_count_; ++i) {
                Access* access = access_list_.GetAccess(i);
                Slice log_record =
                    Slice(access->access_global_record_->data_ptr_, access->access_global_record_->data_size_);
                log_file->Append(log_record);
            }
        }
        //      if (log_enabled_){
        //          WritePrepareLog();
        //      }
        if (sharding_) {
            //          assert(log_enabled_);
            if (log_enabled_ && !participants.empty()) {
                WritePrepareLog();
                //              std::string ret_str_temp("Prepare\n");
                //              Slice log_record = Slice(ret_str_temp.c_str(), ret_str_temp.size());
                //              log_file->Append(log_record);
                //              log_file->Flush();
                //              log_file->Sync();
            }
            bool success = true;
            for (auto iter : participants) {
                success = success && default_gallocator->rdma_mg->Prepare_2PC_RPC(iter, log_enabled_);
            }
            if (success) {
                for (auto iter : participants) {
                    default_gallocator->rdma_mg->Commit_2PC_RPC(iter, log_enabled_);
                }
            } else {
                //              printf("Abort at commit\n");
                //              fflush(stdout);
                AbortTransaction();
                return false;
            }
            participants.clear();
        }
        if (log_enabled_) {
            WriteCommitLog();
        }
        //      assert(locked_handles_.size() == access_list_.access_count_);
        assert(locked_handles_.size() > 0);
        for (auto iter : locked_handles_) {
            assert(iter.second.second == READ_ONLY || iter.second.second == DELETE_ONLY
                   || iter.second.second == INSERT_ONLY || iter.second.second == READ_WRITE);
            GlobalAddress page_addr = iter.second.first->gptr;
            if (iter.second.second == READ_ONLY) {
                assert(iter.second.first->remote_lock_status >= 1);

                default_gallocator->SELCC_Shared_UnLock(iter.second.first->gptr, iter.second.first);
                assert(iter.first == page_addr.val);
                //            printf("Threadid %zu Release read lock for nodeid %d, offset %lu lock handle number is
                //            %zu\n", thread_id_, page_addr.nodeID, page_addr.offset, locked_handles_.size());
            } else {
                assert(iter.second.first->remote_lock_status == 2);
                default_gallocator->SELCC_Exclusive_UnLock(iter.second.first->gptr, iter.second.first);
                //            printf("Threadid %zu Release write lock for nodeid %d, offset %lu lock handle number is
                //            %zu\n", thread_id_, page_addr.nodeID, page_addr.offset, locked_handles_.size());
            }
        }
        // GC
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            //        if (log_enabled_){
            //            Slice log_record = Slice(access->access_global_record_->data_ptr_,
            //            access->access_global_record_->data_size_); log_file->Append(log_record);
            //        }
            if (access->access_type_ == DELETE_ONLY) {
                // TODO: implement the delete function.
                //        gallocators[thread_id_]->Free(access->access_addr_);
                //        access->access_addr_ = Gnullptr;
            }
            //        printf("this access index is %zu\n",i);
            //        fflush(stdout);
            delete access->access_global_record_;
            access->access_global_record_ = nullptr;
            access->access_addr_          = GlobalAddress::Null();
            if (access->txn_local_tuple_ != nullptr) {
                assert(access->access_type_ == READ_WRITE);
                delete access->txn_local_tuple_;
                access->txn_local_tuple_ = nullptr;
            }
        }


        participants.clear();

        access_list_.Clear();
        locked_handles_.clear();
        PROFILE_TIME_END(thread_id_, CC_COMMIT);
        return true;
    }

    void TransactionManager::AbortTransaction() {
        //    printf( "thread_id=%zu,abort\n", thread_id_);
        PROFILE_TIME_START(thread_id_, CC_ABORT);
        if (sharding_) {
            for (auto iter : participants) {
                default_gallocator->rdma_mg->Abort_2PC_RPC(iter, log_enabled_);
            }
            participants.clear();
        }

        if (log_enabled_ && access_list_.access_count_ > 0) {
            WriteAbortLog();
        }
        // TODO: roll back the data changes.
        // GC
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            if (access->access_type_ == INSERT_ONLY) {
                access->access_global_record_->SetVisible(false);
                // todo: Deallcoate the space of inserted tuples.
            } else if (access->access_type_ == READ_WRITE) {
                if (!sharding_) {
                    assert(access->txn_local_tuple_ != nullptr);
                }
                if (access->txn_local_tuple_ != nullptr) {
                    access->access_global_record_->CopyFrom(access->txn_local_tuple_);
                    delete access->txn_local_tuple_;
                }

            } else if (access->access_type_ == DELETE_ONLY) {
                access->access_global_record_->SetVisible(true);
            }
            delete access->access_global_record_;
            access->access_global_record_ = nullptr;
            access->access_addr_          = GlobalAddress::Null();
        }

        for (auto iter : locked_handles_) {
            assert(iter.second.second == READ_ONLY || iter.second.second == DELETE_ONLY
                   || iter.second.second == INSERT_ONLY || iter.second.second == READ_WRITE);
            if (iter.second.second == READ_ONLY) {
                assert(iter.second.first->remote_lock_status >= 1);
                default_gallocator->SELCC_Shared_UnLock(iter.second.first->gptr, iter.second.first);
            } else {
                assert(iter.second.first->remote_lock_status == 2);
                default_gallocator->SELCC_Exclusive_UnLock(iter.second.first->gptr, iter.second.first);
            }
            // unlock
        }

        //      require_2pc = false;

        access_list_.Clear();
        locked_handles_.clear();

        PROFILE_TIME_END(thread_id_, CC_ABORT);
    }


} // namespace DSMEngine
#endif
