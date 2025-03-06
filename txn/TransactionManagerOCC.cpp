#if defined(OCC)
#include "TransactionManager.h"
#include "GlobalTimestamp.h"
namespace DSMEngine{
        WritableFile* TransactionManager::log_file = nullptr;

    //TODO: this CC algirhtm will be blocked at delivery query when there are 64 warehouse, need to understand why.
    // and this problem seems only happen in the release mode.
        bool TransactionManager::AllocateNewRecord(TxnContext *context, size_t table_id, Cache::Handle *&handle,
                                                   GlobalAddress &tuple_gaddr, Record*& tuple) {
            char* tuple_buffer;
            Table* table = storage_manager_->tables_[table_id];
            //TODO: need to remember the latch, so that the latch can be released when the transaction abort.
            // besides, the allocatenew tuple function shall be implemented seperated to the one in the table.
            // The table one is for loading wiithout concurrency control considering.
            // here we shall
            void* page_buffer;
            GlobalAddress* gcl_addr = table->GetOpenedBlock();
            DataPage *page = nullptr;
            DDSM* gallocator = gallocators[thread_id_];
            bool new_created = false;
            if (gcl_addr){
                GlobalAddress cacheline_g_addr = *gcl_addr;
                gallocator->SELCC_Exclusive_Lock(page_buffer, *gcl_addr, handle);
//                    if (!gallocator->TrySELCC_Exclusive_Lock(page_buffer, *gcl_addr, handle)){
//                        return false;
//                    }
                assert(((DataPage*)page_buffer)->hdr.table_id == table_id);
                page = reinterpret_cast<DataPage*>(page_buffer);

            }else{
                gcl_addr = new GlobalAddress();
                *gcl_addr = gallocator->Allocate_Remote(Regular_Page);
                table->SetOpenedBlock(gcl_addr);
                gallocator->SELCC_Exclusive_Lock(page_buffer, *gcl_addr, handle);

                uint64_t cardinality = 8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) / (8ull*table->GetSchemaSize() +1);
                page = new(page_buffer) DataPage(*gcl_addr, cardinality, table_id);
                new_created = true;
            }
            RecordSchema *schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
            int cnt = 0;
            bool ret = page->AllocateRecord(cnt, schema_ptr , tuple_gaddr, tuple_buffer);
            assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
            assert((char*)tuple_buffer - (char*)page_buffer > STRUCT_OFFSET(DataPage, data_));
            assert(((DataPage*)page_buffer)->hdr.this_page_g_ptr != GlobalAddress::Null());
            assert(ret);

//           table->Allo/cateNewTuple(tuple_buffer, tuple_gaddr, handle, default_gallocator, nullptr);
            Record* global_record = new Record(schema_ptr, tuple_buffer);
            //Reset the tuple timestamp.
            global_record->PutWTS(0);
            Access* access = access_list_.NewAccess();
            access->access_type_ = INSERT_ONLY;
            access->access_global_record_ = global_record;
            Record* local_tuple = new Record(schema_ptr);
            local_tuple->CopyFrom(global_record);
            access->txn_local_tuple_ = local_tuple;
            tuple = local_tuple;
            access->access_addr_ = tuple_gaddr;
            assert(cnt == page->hdr.number_of_records);
            auto cardinality = page->hdr.kDataCardinality;
            gallocator->SELCC_Exclusive_UnLock(*gcl_addr, handle);

            if(cnt == cardinality){
                delete gcl_addr;
                table->SetOpenedBlock(nullptr);
            }

//        printf("AllocateNewRecord: thread_id=%zu,table_id=%zu,access_type=%u,data_addr=%lx, start SelectRecordCC\n",
//               thread_id_, table_id, INSERT_ONLY, tuple_gaddr.val);
//            fflush(stdout);
            return true;

//        GlobalAddress* gcl_addr = table->GetOpenedBlock();
//        if ( gcl_addr == nullptr){
//            gcl_addr = new GlobalAddress();
//            *gcl_addr = default_gallocator->Allocate_Remote(Regular_Page);
//            table->SetOpenedBlock(gcl_addr);
//        }
//        assert(handle != nullptr);
//        assert(page_buffer != nullptr);
//        uint64_t cardinality = 8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) / (8ull*table->GetSchema()->GetSchemaSize() +1);
//        auto* page = new(page_buffer) DataPage(*gcl_addr, cardinality, table_id);
//        int cnt = 0;
//        bool ret = page->AllocateRecord(cnt, table->GetSchema() , tuple_gaddr, tuple_buffer);
//        assert(ret);
//        // if the cache line is full, set the thread local ptr as null, and allocate a new page next time.
//        if(cnt == page->hdr.kDataCardinality){
//            table->SetOpenedBlock(nullptr);
//        }


//        default_gallocator->SELCC_Exclusive_Lock_noread(page_buffer, gcl_addr, handle);
        }
		bool TransactionManager::InsertRecord(TxnContext* context,
                                              size_t table_id, const IndexKey* keys,
                                              size_t key_num, Record *record, Cache::Handle* handle, const GlobalAddress tuple_gaddr){

			record->is_visible_ = false;
            PROFILE_TIME_START(thread_id_, INDEX_INSERT);
            bool ret = storage_manager_->tables_[table_id]->InsertPriIndex(keys, key_num, tuple_gaddr);
            PROFILE_TIME_END(thread_id_, INDEX_INSERT);
            PROFILE_TIME_END(thread_id_, CC_INSERT);
//            gallocators[thread_id_]->SELCC_Exclusive_UnLock(TOPAGE(handle->gptr), handle);
            return true;
			//}
			//else{
			//	// if the record has already existed, then we need to lock the original record.
			//	END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			//	return true;
			//}
		}

    // Assert that there is no latch still hold in the before the transaction abort. makesure that txn release the last tuple's,
    // latch access the next one. Never let a transaction holding two latch at the same time!!!!
    bool TransactionManager::SelectRecordCC(TxnContext* context, size_t table_id,
            Record *&record, const GlobalAddress &tuple_gaddr, AccessType access_type) {

            PROFILE_TIME_START(thread_id_, CC_SELECT);
            GlobalAddress page_gaddr = TOPAGE(tuple_gaddr);
            assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
            RecordSchema *schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
            void*  page_buff;
            Cache::Handle* handle;
            char* tuple_buffer;
            //TODO: need to remember the latch, so that the latch can be released when the transaction abort.
            if (access_type == READ_ONLY) {
//                uint64_t wts = record->GetWTS();
                default_gallocator->SELCC_Shared_Lock(page_buff, page_gaddr, handle);

            } else  {
                //Read_Write, Delete_Only, Insert_Only
                default_gallocator->SELCC_Exclusive_Lock(page_buff, page_gaddr, handle);

            }
        assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
        tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);

//        record->Set_Handle(handle);

        Access* access = access_list_.NewAccess();
        access->access_type_ = access_type;
        access->access_global_record_ = new Record(schema_ptr, tuple_buffer);
        record = new Record(schema_ptr);
        record->CopyFrom(access->access_global_record_);
        access->txn_local_tuple_ = record;
        access->access_addr_ = tuple_gaddr;
        if (access_type == DELETE_ONLY) {
            record->SetVisible(false);
        }
        if (access_type == READ_ONLY) {
//                uint64_t wts = record->GetWTS();
            default_gallocator->SELCC_Shared_UnLock(page_gaddr, handle);

        } else  {
            //Read_Write, Delete_Only, Insert_Only
            default_gallocator->SELCC_Exclusive_UnLock(page_gaddr, handle);

        }
        PROFILE_TIME_END(thread_id_, CC_SELECT);
        return true;
    }
    // Contain validation and commit stages.
    bool TransactionManager::CommitTransaction(TxnContext* context,
                                               TxnParam* param, CharArray& ret_str) {
        PROFILE_TIME_START(thread_id_, CC_COMMIT);
        uint64_t commit_ts = GlobalTimestamp::GetMonotoneTimestamp();
        assert(locked_handles_.empty());
        std::map<uint64_t, Access*> sorted_access;
        // lock the access list in order to avoid deadlock.
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            GlobalAddress g_addr = access->access_addr_;
            sorted_access.insert({g_addr, access});
        }
        // First let us check whether the transaciton need to abort. (validate stage)
        for (auto iter : sorted_access){
            Access* access = iter.second;
            void*  page_buff;
            Cache::Handle* handle;
            char* tuple_buffer;
            GlobalAddress page_gaddr;
            GlobalAddress &tuple_gaddr = access->access_addr_;
            page_gaddr = TOPAGE(tuple_gaddr);
            AccessType access_type = access->access_type_;
            RecordSchema *schema_ptr = storage_manager_->tables_[access->access_global_record_->GetTableId()]->GetSchema();
            //TODO: check the l
            if (access_type == DELETE_ONLY) {
            //todo: check whether the record version now is larger than the local record, if so,
            // abort the transaction.
                assert(false);

            }else if (access_type == READ_WRITE || access_type == INSERT_ONLY){

                if (locked_handles_.find(page_gaddr) == locked_handles_.end()){
                    //No matter write or read we need acquire exclusive latch.
                    assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
                    default_gallocator->SELCC_Exclusive_Lock(page_buff, page_gaddr, handle);
                    assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
                    tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
                    access->access_global_record_->ReSetRecordBuff(tuple_buffer, access->access_global_record_->GetRecordSize(), false);
                    locked_handles_.insert({page_gaddr, {handle, access_type}});

                }else{
                    handle = locked_handles_.at(page_gaddr).first;
                    //TODO: update the hierachical lock atomically, if the lock is shared lock
                    if (locked_handles_[page_gaddr].second == READ_ONLY){
                        assert(false);
                        default_gallocator->SELCC_Lock_Upgrade(page_buff, page_gaddr, handle);
                        locked_handles_[page_gaddr].second = access_type;
                    }
#if ACCESS_MODE == 1
          page_buff = ((ibv_mr*)handle->value)->addr;
#elif ACCESS_MODE == 0
            page_buff = handle->value;
#endif
                    tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
                    assert(page_gaddr!=GlobalAddress::Null());
                    assert(access_type <= READ_WRITE);
                    access->access_global_record_->ReSetRecordBuff(tuple_buffer, access->access_global_record_->GetRecordSize(), false);

                }
                if (access->access_global_record_->GetWTS() > access->txn_local_tuple_->GetWTS()){
                    AbortTransaction();
                    return false;
                }
                // THE updates are conducted concentrately in the commit stage. no need to update the record here.
//                access->access_global_record_->CopyFrom(access->txn_local_tuple_);
//                access->access_global_record_->PutWTS(commit_ts);

            }else {
                if (locked_handles_.find(page_gaddr) == locked_handles_.end()){
                    //No matter write or read we need acquire exclusive latch.
                    assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
                    default_gallocator->SELCC_Shared_Lock(page_buff, page_gaddr, handle);
                    assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
                    tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
                    access->access_global_record_->ReSetRecordBuff(tuple_buffer, access->access_global_record_->GetRecordSize(), false);
                    locked_handles_.insert({page_gaddr, {handle, access_type}});

                }else{
                    handle = locked_handles_.at(page_gaddr).first;
                    //TODO: update the hierachical lock atomically, if the lock is shared lock

#if ACCESS_MODE == 1
          page_buff = ((ibv_mr*)handle->value)->addr;
#elif ACCESS_MODE == 0
            page_buff = handle->value;
#endif
                    tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
                    assert(page_gaddr!=GlobalAddress::Null());
                    assert(access_type <= READ_WRITE);
                    access->access_global_record_->ReSetRecordBuff(tuple_buffer, access->access_global_record_->GetRecordSize(), false);
                }
                if (access->access_global_record_->GetWTS() > access->txn_local_tuple_->GetWTS()){
                    AbortTransaction();
                    return false;
                }
            }

        }
        // Then let us write the data and commit. (commit stage)
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            AccessType access_type = access->access_type_;
            if (access_type == READ_WRITE || access_type == INSERT_ONLY) {
                access->access_global_record_->CopyFrom(access->txn_local_tuple_);
                access->access_global_record_->PutWTS(commit_ts);
            }
            delete access->access_global_record_;
            access->access_global_record_ = nullptr;
            access->access_addr_ = GlobalAddress::Null();
            if (access->txn_local_tuple_!= nullptr){
                delete access->txn_local_tuple_;
                access->txn_local_tuple_ = nullptr;
            }
        }
        access_list_.Clear();
        for (auto iter : locked_handles_){
            assert(iter.second.second == READ_ONLY ||
                   iter.second.second == DELETE_ONLY ||
                   iter.second.second == INSERT_ONLY ||
                   iter.second.second == READ_WRITE);
            if (iter.second.second == READ_ONLY){
                default_gallocator->SELCC_Shared_UnLock(iter.second.first->gptr, iter.second.first);
            }
            else {
                default_gallocator->SELCC_Exclusive_UnLock(iter.second.first->gptr, iter.second.first);
            }
            // unlock
        }
        locked_handles_.clear();
//        is_first_access_ = true;
        PROFILE_TIME_END(thread_id_, CC_COMMIT);
        return true;
		}

		void TransactionManager::AbortTransaction() {
            PROFILE_TIME_START(thread_id_, CC_ABORT);
            for (size_t i = 0; i < access_list_.access_count_; ++i) {
                Access* access = access_list_.GetAccess(i);
                delete access->txn_local_tuple_;
                access->txn_local_tuple_ = nullptr;
                delete access->access_global_record_;
                access->access_global_record_ = nullptr;
                access->access_addr_ = GlobalAddress::Null();
            }
			access_list_.Clear();
            // Clear the grabbed SELCC latch.
            for (auto iter : locked_handles_){
                assert(iter.second.second == READ_ONLY ||
                       iter.second.second == DELETE_ONLY ||
                       iter.second.second == INSERT_ONLY ||
                       iter.second.second == READ_WRITE);
                if (iter.second.second == READ_ONLY){
                    default_gallocator->SELCC_Shared_UnLock(iter.second.first->gptr, iter.second.first);
                }
                else {
                    default_gallocator->SELCC_Exclusive_UnLock(iter.second.first->gptr, iter.second.first);
                }
                // unlock
            }
            locked_handles_.clear();
            PROFILE_TIME_END(thread_id_, CC_ABORT);

        }
    bool TransactionManager::CoordinatorPrepare() {
        return false;
    }
    bool TransactionManager::AcquireLatchForTuple(char*& tuple_buffer,GlobalAddress tuple_gaddr, AccessType access_type){
            GlobalAddress page_gaddr = TOPAGE(tuple_gaddr);
            assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
            void*  page_buff;
            Cache::Handle* handle;
            assert(false);
//            if (locked_handles_.find(page_gaddr) == locked_handles_.end()){
//                if (access_type == READ_ONLY) {
//                    PROFILE_TIME_START(thread_id_, LOCK_READ);
//                    default_gallocator->SELCC_Shared_Lock(page_buff, page_gaddr, handle);
//                    assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
//                    tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
//                    locked_handles_.insert({page_gaddr, {handle, access_type}});
//                    assert(page_gaddr!=GlobalAddress::Null());
//                    assert(access_type < READ_WRITE);
//                    PROFILE_TIME_END(thread_id_, LOCK_READ);
//                }
//                else {
//                    // DELETE_ONLY, READ_WRITE
//                    PROFILE_TIME_START(thread_id_, LOCK_WRITE);
//                    default_gallocator->SELCC_Exclusive_Lock(page_buff, page_gaddr, handle);
//                    assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
//                    tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
//                    locked_handles_.insert({page_gaddr, {handle, access_type}});
//                    assert(page_gaddr!=GlobalAddress::Null());
//                    assert(access_type <= READ_WRITE);
//                    PROFILE_TIME_END(thread_id_, LOCK_WRITE);
//                }
//
//            }else{
//                handle = locked_handles_.at(page_gaddr).first;
//                //TODO: update the hierachical lock atomically, if the lock is shared lock
//                if (access_type > READ_ONLY && locked_handles_[page_gaddr].second == READ_ONLY){
//                    assert(false);
//                    default_gallocator->SELCC_Lock_Upgrade(page_buff, page_gaddr, handle);
//                    locked_handles_[page_gaddr].second = access_type;
//                }
//#if ACCESS_MODE == 1
//          page_buff = ((ibv_mr*)handle->value)->addr;
//#elif ACCESS_MODE == 0
//            page_buff = handle->value;
//#endif
//                tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
//                assert(page_gaddr!=GlobalAddress::Null());
//                assert(access_type <= READ_WRITE);
//            }
    }


}

#endif
