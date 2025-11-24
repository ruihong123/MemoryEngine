#if defined(MVOCC)
#include "TransactionManager.h"
#include "GlobalTimestamp.h"

namespace DSMEngine{

        WritableFile* TransactionManager::log_file = nullptr;
        std::atomic<uint64_t > TransactionManager::largest_sp_acquired = 0;
        RWSpinMutex TransactionManager::delta_map_mtx;
        std::map<GlobalAddress, DeltaSectionWrap*, std::greater<GlobalAddress>> TransactionManager::delta_sections;
        RWSpinMutex TransactionManager::garb_mtx;
        SpinMutex TransactionManager::pin_sp_mtx;
        std::map<uint64_t, uint16_t> TransactionManager::pined_snapshot_this_node;
//        uint64_t TransactionManager::last_broadcasted_sp = 0;
        SpinMutex TransactionManager::c_l_mtx;
        std::map<uint16_t, uint64_t > TransactionManager::cluster_least_sp_;
        SpinMutex TransactionManager::gc_thread_control_mtx;
        std::atomic<bool> TransactionManager::gc_should_run{false};
        std::atomic<uint32_t> TransactionManager::active_manager_count{0};
        std::thread* TransactionManager::gc_thread = nullptr;
        uint64_t delta_pull_num[MAX_APP_THREAD];
        uint64_t roll_back_num[MAX_APP_THREAD];
#ifdef SINGLE_DELTA_PER_NODE
        // todo: we need to make the mulitple writable delta sections per compute node, for our log-as-replica design.
        DeltaSectionWrap* TransactionManager::ds_for_write = nullptr;
#endif

        bool TransactionManager::AllocateNewRecord(size_t table_id,
                                                   Cache::Handle *&handle,
                                                   GlobalAddress &tuple_gaddr,
                                                   Record *&tuple) {
            char* tuple_buffer;
            Table* table = storage_manager_->tables_[table_id];
            void* page_buffer;
            //TODO: if there are only insert during the transcation, it can still be counted as read only transaction,
            // because it is not possible to have an update operation.
            if (pure_read_txn){
                pure_read_txn = false;
            }
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
            global_record->PutWTS(UINT64_MAX);
            global_record->SetVisible(false);
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

            return true;


//        default_gallocator->SELCC_Exclusive_Lock_noread(page_buffer, gcl_addr, handle);
        }
        // we can not insert the record and make it visible here, because we need to revert it when the transaction aborts.
        // Instead, we shall insert the index during the commit so that we don't need to revert the index insertion when the transaction aborts.
        bool TransactionManager::InsertRecord(size_t table_id,
                                              const DynamicCompoundKey keys,
                                              size_t key_num, Record *record,
                                              Cache::Handle *handle,
                                              const GlobalAddress tuple_gaddr) {

//			record->is_visible_ = false;
            PROFILE_TIME_START(thread_id_, INDEX_INSERT);
//            bool ret = storage_manager_->tables_[table_id]->InsertPriIndex(keys, key_num, tuple_gaddr);
//            record->primary_key = keys[0];
            
            // Copy primary key directly from the key parameter to the record buffer
            record->primary_key_length_ = keys.schema_ptr->GetPrimaryKeyLength();
            assert(record->primary_key_length_ <= 64); // Ensure key fits in fixed buffer
            memcpy(record->primary_key_buffer_, keys.start, record->primary_key_length_);
            
            PROFILE_TIME_END(thread_id_, INDEX_INSERT);
            PROFILE_TIME_END(thread_id_, CC_INSERT);
//            gallocators[thread_id_]->SELCC_Exclusive_UnLock(TOPAGE(handle->gptr), handle);
            return true;
		}

    // Assert that there is no latch still hold in the before the transaction abort. makesure that txn release the last tuple's,
    // latch access the next one. Never let a transaction holding two latch at the same time!!!!
        bool TransactionManager::SelectRecordCC(size_t table_id, Record *&record,
                    const GlobalAddress &tuple_gaddr, AccessType access_type) {
        if (is_first_access_){
            GetSnapshot();

            is_first_access_ = false;
        }

        PROFILE_TIME_START(thread_id_, CC_SELECT);
        GlobalAddress page_gaddr = TOPAGE(tuple_gaddr);
        assert(page_gaddr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
        RecordSchema *schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
        void*  page_buff;
        Cache::Handle* handle;
        char* tuple_buffer;
        //TODO: need to remember the latch, so that the latch can be released when the transaction abort.
        
#ifdef ENABLE_MVOCC_RETRY_OPTIMIZATION
        // For retried transactions, treat READ_ONLY as READ_WRITE
        AccessType effective_access_type = access_type;
        if (is_retry_ && access_type == READ_ONLY) {
            effective_access_type = READ_WRITE;
            if (pure_read_txn){
                pure_read_txn = false;
            }
        }
#else
        AccessType effective_access_type = access_type;
#endif
        
        if (effective_access_type == READ_ONLY) {
//                uint64_t wts = record->GetWTS();
            default_gallocator->SELCC_Shared_Lock(page_buff, page_gaddr, handle);

        } else  {
            if (pure_read_txn){
                pure_read_txn = false;
            }
            //Read_Write, Delete_Only, Insert_Only
            // There is no actual update on the record temporarily, so we can use shared lock.
            default_gallocator->SELCC_Exclusive_Lock(page_buff, page_gaddr, handle);

        }
        assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
        tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);

//        record->Set_Handle(handle);

        Access* access = access_list_.NewAccess();
        access->access_type_ = effective_access_type;
        access->access_global_record_ = new Record(schema_ptr, tuple_buffer);
        record = new Record(schema_ptr);
        record->CopyFrom(access->access_global_record_);
        access->txn_local_tuple_ = record;
        access->access_addr_ = tuple_gaddr;
        volatile uint64_t ts = record->GetWTS();
        RegisterSeenTs(ts);
        assert(buffer_is_not_all_zero(record->data_ptr_, schema_ptr->GetRecordTotalSize()));
        // todo: for serializable isolation level, a larger tuple timestamps means that we need to abort this txn.
#ifdef EARLYABORT
        if (isolation_level ==SERIALIZABLE){
            if (!pure_read_txn && (have_rolled_back) && (ts > snapshot_ts)){
                if (effective_access_type == READ_ONLY) {
//                uint64_t wts = record->GetWTS();
                    default_gallocator->SELCC_Shared_UnLock(page_gaddr, handle);

                } else {
                    //Read_Write, Delete_Only, Insert_Only
                    default_gallocator->SELCC_Exclusive_UnLock(page_gaddr, handle);

                }
                AbortTransaction();
                return false;
            }
            if (!pure_read_txn && ((ts > snapshot_ts) && !have_rolled_back) ){
                // IF we have not roll back and we find the snapshot is too small for current operation, we can simply fall back to the traditional OCC algorithm.
                plain_occ = true;
                //todo: we can release the snapshot early here
            }

        }
        if (isolation_level ==SNAPSHOT_ISOLATION){
            if ( ts > snapshot_ts && effective_access_type == READ_WRITE){
                // if (have_rolled_back) {
                    //Read_Write, Delete_Only, Insert_Only
                    default_gallocator->SELCC_Exclusive_UnLock(page_gaddr, handle);
                    AbortTransaction();
                    return false;
                // }else {
                //     // you can not update the snapshot number because you need to make sure the previous
                //     plain_occ = true;
                // }
            }
        }


#endif

            volatile size_t lc = 0;
            while (!plain_occ && ts > snapshot_ts) {
                if (!have_rolled_back) {
                    have_rolled_back = true;
                }
                roll_back_num[thread_id_]++;
                lc++;
                // TODO: ROll back old version of the data.
                MetaColumn meta = record->GetMeta();
                GlobalAddress prev_delta = meta.prev_version_;
                // todo: if this record is new inserted by an ongoing tranaction, the prev_delta is null, we can simply abort this transaction.
                // actually, this should never happen in TPC-C benchmark.
                if (prev_delta == GlobalAddress::Null()) {
                    assert(false);
                    if (effective_access_type == READ_ONLY) {
                        default_gallocator->SELCC_Shared_UnLock(page_gaddr, handle);
                    } else  {
                        //Read_Write, Delete_Only, Insert_Only
                        default_gallocator->SELCC_Exclusive_UnLock(page_gaddr, handle);
                    }
                    AbortTransaction();
                    return false;
                }
                assert(prev_delta != GlobalAddress::Null());

                // implement a mechanism to detect whether the local copy of delta section is up to date.
                // if not, we need to fetch the latest version of the delta section.

                DeltaSectionWrap *delta_section = nullptr;
                uint64_t ds_head = 0;
                uint64_t ds_tail = 0;
#ifdef SINGLE_DELTA_PER_NODE
                uint64_t ds_tail_allocate = 0;
#endif
                uint64_t ds_epoch = 0;
                {
                    std::shared_lock<RWSpinMutex> l(delta_map_mtx);
                    auto iter = delta_sections.lower_bound(prev_delta);
                    assert(iter != delta_sections.end());
                    delta_section = iter->second;
//                largest_ds_timestamp = delta_section->GetMaxTimestamp();
                    ds_head = delta_section->GetHead();
                    ds_tail = delta_section->GetTail();
#ifdef SINGLE_DELTA_PER_NODE
                    ds_tail_allocate = delta_section->GetTailAllocate();
#endif
                    ds_epoch = delta_section->GetEpoch();
                    assert(iter != delta_sections.end());
                    assert(iter->first.nodeID == prev_delta.nodeID);
                    assert(prev_delta.offset - iter->first.offset <= ds_for_write->seg_real_size_);
                }
//            uint64_t delta_offset = prev_delta.offset - delta_section->seg_addr_.offset;
                // check whether the prev delta is the latest version. check whether the prev_delta is within the head and tail plus checking
                // whether the epoch is the same.


                //check whether the delta_offset is within the ring buffer by tail and head.
#ifndef NDEBUG
                bool need_pull_update = false;
#endif
                long offset = prev_delta.offset - delta_section->seg_addr_.offset - STRUCT_OFFSET(DeltaSection, local_addr_);

                {
                    // Calculate offset from global address
                    
                    // we use the latch for shadow copy because the garbage collection or delta appending shall not fail
                    // the delta section validation in any way. In other word,  the new tail or epoch will not make the prev delta invalid.
                    // and the garbage collection shall never collect the delta record that this transaciton snapshot can still see.
                    // std::shared_lock<RWSpinMutex> slck(delta_section->shadow_mtx_);
                    // std::shared_lock<RWSpinMutex> slck(delta_section->shadow_mtx_);
                    if (delta_section->inner_section->is_empty_ || 
                        !delta_section->isOffsetValid(offset, meta.prev_delta_epoch_)) {
                        // slck.unlock();
                        std::unique_lock<RWSpinMutex> lck(delta_section->shadow_mtx_);
                        if (delta_section->inner_section->is_empty_ ||
                            !delta_section->isOffsetValid(offset, meta.prev_delta_epoch_)) {
                            // Pull updates from remote node
                            if (delta_section->owner_compute_node_id_ != RDMA_Manager::Get_Instance()->node_id) {
                                delta_section->PullUpdates();
                            }
                        }
                    }else{
                        while (delta_section->isOffsetDangerous(offset, meta.prev_delta_epoch_)) {
                            _mm_pause();
                        }  
                    }
                }


//                std::shared_lock<std::shared_mutex> lck(delta_section->shadow_mtx_);

                assert(meta.prev_delta_epoch_ <= delta_section->GetEpoch());

#ifndef NDEBUG
//                ds_tail = delta_section->GetTail();
//                ds_head = delta_section->GetHead();
                assert(!delta_section->inner_section->is_empty_ &&
                       delta_section->isOffsetValid(offset, meta.prev_delta_epoch_));
                
                if (ds_tail >= ds_head) {
                    assert(delta_section->inner_section->tail_ - offset > STRUCT_OFFSET(DeltaRecord, data_));
                }
#endif
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
                DeltaRecord *delta_record = (DeltaRecord *) ((char *) delta_section->seg_local_mr_->addr +
                                                             (prev_delta.offset - delta_section->seg_addr_.offset));
#ifndef NDEBUG
                size_t record_size = delta_record->current_record_data_size_;
                char mark = delta_record->marker_;
                void* p = malloc(record_size+200);
                DeltaRecord* check_record = (DeltaRecord*)((char*)p+200);
                memcpy(p, (char*)delta_record -200, record_size+200);
                assert(mark == '&');
                free(p);
#endif
                record->roll_back(delta_record);
                ts = record->GetWTS();
            }
            assert(buffer_is_not_all_zero(record->data_ptr_, schema_ptr->GetRecordTotalSize()));

        if (effective_access_type == DELETE_ONLY) {
            record->PutWTS(UINT64_MAX);
            record->SetVisible(false);
        }
        if (effective_access_type == READ_ONLY) {
//                uint64_t wts = record->GetWTS();
            default_gallocator->SELCC_Shared_UnLock(page_gaddr, handle);

        } else  {
            //Read_Write, Delete_Only, Insert_Only
            default_gallocator->SELCC_Exclusive_UnLock(page_gaddr, handle);

        }
        PROFILE_TIME_END(thread_id_, CC_SELECT);
        return true;
    }


    void TransactionManager::LogDataUpdateOperation(Access* access, uint64_t commit_ts) {
        // Use LogCodec to encode only modified columns (similar to serialize_to_delta)
        LogCodec::Encoder encoder;
        
        uint16_t logical_region_id = access->access_addr_.nodeID;
        
        // Only log modified columns (using dirty_col_ids like delta records)
        if (!access->txn_local_tuple_->dirty_col_ids.empty()) {
            for (auto col_id : access->txn_local_tuple_->dirty_col_ids) {
                size_t column_size = access->txn_local_tuple_->schema_ptr_->GetColumnSize(col_id);
                size_t column_offset = access->txn_local_tuple_->schema_ptr_->GetColumnOffset(col_id);
                
                // Log UPDATE_BYTES for this specific column
                encoder.AddUpdateBytes(column_offset, 
                                     access->txn_local_tuple_->data_ptr_ + column_offset, 
                                     column_size);
            }
            
            // For dirty column updates, also log timestamp update separately
            size_t meta_col_id = access->txn_local_tuple_->schema_ptr_->GetMetaColumnId();
            size_t meta_offset = access->txn_local_tuple_->schema_ptr_->GetColumnOffset(meta_col_id);
            size_t wts_offset = meta_offset + offsetof(MetaColumn, Wts_);
            encoder.AddSetU64LE(wts_offset, commit_ts);
        } else {
            // Fallback: log entire record if no dirty tracking (includes MetaColumn with timestamp)
            size_t record_size = access->txn_local_tuple_->GetRecordSize();
            encoder.AddUpdateBytes(0, access->txn_local_tuple_->data_ptr_, record_size);
            // No separate timestamp log needed - it's already included in the full record
        }
        
        // Get current page version and increment it for this log record
        GlobalAddress target_page = TOPAGE(access->access_addr_);
        
        // Get the page buffer from the already acquired lock
        GlobalAddress page_gaddr = TOPAGE(access->access_addr_);
        assert(locked_handles_.find(page_gaddr) != locked_handles_.end());
        Cache::Handle* handle = locked_handles_.at(page_gaddr).first;
        // NOTE: handle->value is a pointer to ibv_mr in ACCESS_MODE==1, or the buffer directly in ACCESS_MODE==0
#if ACCESS_MODE == 1
        void* page_buffer = ((ibv_mr*)handle->value)->addr;
#elif ACCESS_MODE == 0
        void* page_buffer = handle->value;
#endif
        
        uint64_t current_page_version = GetCurrentPageVersion(page_buffer);
        uint64_t new_page_version = current_page_version + 1;
        
        // Update the page version on the compute node (primary copy)
        SetCurrentPageVersion(page_buffer, new_page_version);
        
        // Append to redo log with the new page version
        // Get shared RedoLogger from DDSM (singleton shared across all threads)
        RedoLogger* redo_logger = default_gallocator->GetRedoLogger(log_enabled_);
        if (redo_logger) {
            redo_logger->Append(logical_region_id, target_page, new_page_version, encoder.Buffer().data(), encoder.Buffer().size());
            
            printf("RedoLogger: Logged data update (%zu dirty cols, %zu bytes) for record at page=0x%lx, logical_region=%u\n",
                   access->txn_local_tuple_->dirty_col_ids.size(), encoder.Buffer().size(),
                   target_page.val, logical_region_id);
        }
    }

    void TransactionManager::LogIndexInsertOperation(Access* access, const DynamicCompoundKey& primary_key, uint64_t commit_ts) {
        // No-op: Index redo logging disabled for now
        // TODO: Implement proper index redo logging later
    }

    uint64_t TransactionManager::GetCurrentPageVersion(void* page_buffer) {
        // Get the current page version from the already locked page buffer
        if (page_buffer == nullptr) {
            printf("TransactionManager: Warning - Page buffer is null\n");
            assert(false);
            return 0;
        }
        
        // Get the page header and read version
        DSMEngine::DataPage* data_page = reinterpret_cast<DSMEngine::DataPage*>(page_buffer);
        return data_page->hdr.p_version;
    }

    void TransactionManager::SetCurrentPageVersion(void* page_buffer, uint64_t version) {
        // Update the page version in the already locked page buffer
        if (page_buffer == nullptr) {
            printf("TransactionManager: Warning - Page buffer is null\n");
            return;
        }
        
        // Update the page header with new version
        DSMEngine::DataPage* data_page = reinterpret_cast<DSMEngine::DataPage*>(page_buffer);
        data_page->hdr.p_version = version;
        
        printf("TransactionManager: Updated page version to %lu for page buffer %p\n", version, page_buffer);
    }
    // Contain validation and commit stages.
    bool TransactionManager::CommitTransaction(CharArray &ret_str) {
        PROFILE_TIME_START(thread_id_, CC_COMMIT);

        assert(locked_handles_.empty());
        std::map<uint64_t, Access*> sorted_access;
        // lock the access list in order to avoid deadlock.
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            GlobalAddress g_addr = access->access_addr_;
            sorted_access.insert({g_addr, access});
        }
//        uint64_t commit_ts = GlobalTimestamp::FetchAddMonotoneTimestamp();
        // -----------------(validate stage)----------------------------------------------
        // First let us check whether the transaciton need to abort.
        if (!pure_read_txn){
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
                    assert(!pure_read_txn);
                    if (locked_handles_.find(page_gaddr) == locked_handles_.end()){
                        //Acquire the exclusive latch and put the latch in to the lock handles table.
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
                        RegisterSeenTs(access->access_global_record_->GetWTS());
                        AbortTransaction();
                        return false;
                    }
                    // THE updates are conducted concentrately in the commit stage. no need to update the record here.
//                access->access_global_record_->CopyFrom(access->txn_local_tuple_);
//                access->access_global_record_->PutWTS(commit_ts);

                }else if (isolation_level == SERIALIZABLE){
                    // only check the read set for serializable isolation level.
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
                        RegisterSeenTs(access->access_global_record_->GetWTS());
                        AbortTransaction();
                        return false;
                    }
                }
            }
        }
        // -----------------(commit stage)----------------------------------------------
#ifdef USE_SNAPSHOT_MANAGER
        uint64_t required_floor = max_seen_ts_;
        if (required_floor != 0) {
            GlobalTimestamp::EnsureCommitFloor(required_floor + 1);
        }
#endif
        // get the commit timestamp right before the commit phase.
        uint64_t commit_ts = GlobalTimestamp::FetchAddMonotoneTimestamp();

        // Write the data, commit, and generate redo logs
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            AccessType access_type = access->access_type_;
            if (access_type == READ_WRITE) {
                // TODO: Create a new delta_record in the delta section and update the prev_delta in the record's metadata.
                //  Will this help in reduce the overhead of locking? probably not. Need experiment.
                GlobalAddress delta_gadd = GlobalAddress::Null();
                size_t delta_size = 0;
#ifdef SINGLE_DELTA_PER_NODE
                ds_for_write->fill_in_delta_record_single(access->txn_local_tuple_, access->access_global_record_,
                                                                delta_gadd,
                                                                delta_size, commit_ts);
#else
                ds_for_write->fill_in_delta_record_thread_local(access->txn_local_tuple_, access->access_global_record_,
                                                                delta_gadd,
                                                                delta_size, commit_ts);
#endif
                MetaColumn meta = access->txn_local_tuple_->GetMeta();
                assert(delta_gadd.offset - ds_for_write->seg_addr_.offset < ds_for_write->seg_real_size_ + STRUCT_OFFSET(DeltaSection, local_addr_));
                meta.prev_version_ = delta_gadd;
                meta.prev_delta_epoch_ = ds_for_write->GetEpoch();
                meta.prev_delta_data_size_ = delta_size;
                meta.Wts_ = commit_ts;

                access->txn_local_tuple_->PutMeta(meta);
                
                // Log data update operation before performing it
                if (log_enabled_) {
                    LogDataUpdateOperation(access, commit_ts);
                }
                
                // todo: delete the asertion below.
                access->access_global_record_->CopyFrom(access->txn_local_tuple_);
//                access->access_global_record_->PutWTS(commit_ts);

            }else if(access_type == INSERT_ONLY){
                assert(locked_handles_.find(TOPAGE(access->access_addr_)) != locked_handles_.end());
                access->txn_local_tuple_->PutWTS(commit_ts);
                assert(commit_ts < 0x100d2c00cbe9);
                
                // Log data update operation before performing it
                if (log_enabled_) {
                    LogDataUpdateOperation(access, commit_ts);
                }
                
                access->access_global_record_->CopyFrom(access->txn_local_tuple_);
                
                // Primary key should have been extracted in InsertRecord and stored in local tuple
                assert(access->txn_local_tuple_->primary_key_length_ > 0); // Ensure primary key was stored
                RecordSchema *index_schema_ptr = storage_manager_->tables_[access->access_global_record_->GetTableId()]->GetPrimaryIndexSchema();
                DynamicCompoundKey primary_key(access->txn_local_tuple_->primary_key_buffer_, index_schema_ptr);
                
                // Log index insertion operation before performing it
                if (log_enabled_) {
                    LogIndexInsertOperation(access, primary_key, commit_ts);
                }
                
                storage_manager_->tables_[access->txn_local_tuple_->schema_ptr_->GetTableId()]->InsertPriIndex(primary_key, 1, access->access_addr_);
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
        ClearStates();
#ifdef USE_SNAPSHOT_MANAGER
        max_seen_ts_ = commit_ts;
#endif
        PROFILE_TIME_END(thread_id_, CC_COMMIT);
        return true;
		}

#ifdef USE_SNAPSHOT_MANAGER
        void TransactionManager::RegisterSeenTs(uint64_t ts) {
            if (ts > max_seen_ts_) {
                max_seen_ts_ = ts;
            }
        }
#endif
        void TransactionManager::AbortTransaction() {
            PROFILE_TIME_START(thread_id_, CC_ABORT);
            for (size_t i = 0; i < access_list_.access_count_; ++i) {
                Access* access = access_list_.GetAccess(i);
                delete access->txn_local_tuple_;
                access->txn_local_tuple_ = nullptr;
                delete access->access_global_record_;
                access->access_global_record_ = nullptr;
                access->access_addr_ = GlobalAddress::Null();
//                if (access->access_type_ == INSERT_ONLY){
//                    GlobalAddress tuple_addr = access->access_addr_;
//                    GlobalAddress page_gaddr = TOPAGE(tuple_addr);
//                    DataPage* page = (DataPage*)access->access_global_record_->data_ptr_ - (tuple_addr.offset - page_gaddr.offset);
//                    page->DeleteRecord(tuple_addr, access->access_global_record_->schema_ptr_);
//                }
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
#ifdef USE_SNAPSHOT_MANAGER
            if (max_seen_ts_ != 0) {
                GlobalTimestamp::EnsureCommitFloor(max_seen_ts_ + 1);
            }
#endif
            ClearStates();
#ifdef ENABLE_MVOCC_RETRY_OPTIMIZATION
            // Mark this transaction as a retry for the next execution
            // Set after ClearStates() so it persists for the retried transaction
            is_retry_ = true;
#endif
            PROFILE_TIME_END(thread_id_, CC_ABORT);

        }
        void TransactionManager::GetSnapshot() {
            // need to fix.
            // todo: there is a potential bug. If the snapshot is acquire but this thread is yield, then the global cluster may not detect that this snapshot number is pinned and the background thread may clean up
            // the old version for this snapshot number. We can make the timestamp acquire inside the spin lock, but it may cause the performance issue.
            // Another solution could be using another spin mutex to use a shared lock to block the garbage collector when we are calling Get snapshot function
            garb_mtx.lock_shared(); // this garb_mtx is necessary to guarantee the correctness of garbage collection.
            
#ifdef ENABLE_MVOCC_RETRY_OPTIMIZATION
            // For retried transactions, use local_ts_next without incrementing it
            if (is_retry_) {
#ifdef USE_SNAPSHOT_MANAGER
                GlobalTimestamp::EnsureSnapshotThreadStarted();
                snapshot_ts = GlobalTimestamp::local_ts_next.load(std::memory_order_acquire);
#else
                snapshot_ts = GlobalTimestamp::GetMonotoneTimestamp();
#endif
            } else {
                snapshot_ts = GlobalTimestamp::GetMonotoneTimestamp();
            }
#else
            snapshot_ts = GlobalTimestamp::GetMonotoneTimestamp();
#endif

            std::unique_lock<SpinMutex> psp_lck(pin_sp_mtx);

            largest_sp_acquired.store(largest_sp_acquired.load() < snapshot_ts ? snapshot_ts : largest_sp_acquired.load()); // atomic is actually not necessary here.
            if(pined_snapshot_this_node.count(snapshot_ts) == 0){
                pined_snapshot_this_node[snapshot_ts] = 1;
            }else{
                pined_snapshot_this_node[snapshot_ts]++;
            }
            psp_lck.unlock();
            garb_mtx.unlock_shared();
            // todo: may I insert this snapshot to the cluster_least_sp_?
        }
        void TransactionManager::ReleaseSnapshot() {
            std::unique_lock<SpinMutex> psp_lck(pin_sp_mtx);
            assert(pined_snapshot_this_node.count(snapshot_ts) != 0);
            bool least_sp_change = false;
            uint64_t old_least_sp = 0;
            if (pined_snapshot_this_node.begin()->first == snapshot_ts && pined_snapshot_this_node.begin()->second == 1){
                least_sp_change = true;
                old_least_sp = (pined_snapshot_this_node.begin()++)->first;
            }

            if(pined_snapshot_this_node[snapshot_ts] == 1){
                pined_snapshot_this_node.erase(snapshot_ts);
            }else{
                pined_snapshot_this_node[snapshot_ts]--;
                assert(pined_snapshot_this_node[snapshot_ts] > 0);
            };
            snapshot_ts = 0;
            psp_lck.unlock();

            // if (least_sp_change){
            //     // I think current strategy to sync the global least sp for this node is too eager, we can do it lazily
            //     auto rdma_mg = RDMA_Manager::Get_Instance();
            //     std::unique_lock<SpinMutex> cl_lck(c_l_mtx);
            //     cluster_least_sp_[rdma_mg->node_id] = old_least_sp;
            // }
        }

    void TransactionManager::ProcessDeltaCreate(void* args){

        auto* rdma_mg = RDMA_Manager::Get_Instance();
        auto *receive_msg_buf = (RDMA_Request*)args;
        GlobalAddress ds_gaddr = receive_msg_buf->content.create_ds.ds_gaddr;
        uint8_t compute_node_id = receive_msg_buf->content.create_ds.compute_node_id;
        ibv_mr* local_mr = new ibv_mr{};
        rdma_mg->Allocate_Local_RDMA_Slot(*local_mr, DeltaChunk);
        // TODO: there is compilation error becuae DSMengine does not contain defination for transaction.
        // we need to wrap the funciton to a function pointer or funciton object.
        auto* ds_= new DeltaSectionWrap(compute_node_id, ds_gaddr, rdma_mg->delta_section_size, local_mr);
        {
            std::unique_lock<RWSpinMutex> lck(TransactionManager::delta_map_mtx);
            TransactionManager::delta_sections.insert(std::make_pair(ds_gaddr, ds_));
        }
        delete receive_msg_buf;
    }

     void TransactionManager::ProcessDeltaPull(void* args){

        auto* rdma_mg = RDMA_Manager::Get_Instance();
        auto *receive_msg_buf = (RDMA_Request*)args;
         assert(receive_msg_buf->command == pull_delta_section);
        GlobalAddress ds_gaddr = receive_msg_buf->content.pull_ds.ds_gaddr;
        uint64_t old_head_ = receive_msg_buf->content.pull_ds.old_head;
        uint64_t old_tail_ = receive_msg_buf->content.pull_ds.old_tail;
        uint64_t old_max_ts = receive_msg_buf->content.pull_ds.old_max_ts;
        uint64_t old_epoch = receive_msg_buf->content.pull_ds.old_epoch;
        uint8_t requester_node_id = receive_msg_buf->content.pull_ds.requester_node_id;

        DeltaSectionWrap* ds_w = nullptr;
        uint64_t calculated_danger_size = 0;
        
        {
            std::shared_lock<RWSpinMutex> map_lck(TransactionManager::delta_map_mtx);
            auto it = TransactionManager::delta_sections.find(ds_gaddr);
            map_lck.unlock();

            if (it == TransactionManager::delta_sections.end()) {
                delete receive_msg_buf;
                assert(false);
                return;
            }

            ds_w = it->second;
            std::shared_lock<RWSpinMutex> delta_lck(ds_w->main_mtx_);
            while (ds_w->inner_section->tail_ != ds_w->inner_section->tail_allocated) {
                _mm_pause();
            }
            assert(!ds_w->inner_section->is_empty_);
            
            // Capture head and tail values at function start to ensure consistency
            uint64_t captured_head = ds_w->GetHead();
            uint64_t captured_tail = ds_w->GetTail();
            
            int qp_id = rdma_mg->GetQPForDeltaPull();
            ibv_mr local_mr = *ds_w->seg_local_mr_;
            char* remote_addr = (char*)receive_msg_buf->buffer;
            uint8_t* polling_byte = (uint8_t*)((uint8_t*)local_mr.addr + rdma_mg->delta_section_size - 1);
            assert(ds_w->inner_section->tail_ != ds_w->inner_section->head_ || ds_w->inner_section->is_empty_);
            *polling_byte = 5;
            
            std::vector<std::pair<uint64_t, uint64_t>> boundaries;
            // Calculate boundaries and danger_size without modifying delta section (holding shared lock)
            calculated_danger_size = ds_w->CalculateWriteBoundaries(boundaries, old_head_, old_tail_, old_epoch);
            
            // Print boundaries information
            //  printf("[Delta Pull Handler] Node %d -> Node %d: old_head=%lu, old_tail=%lu, old_epoch=%lu, "
            //         "current_head=%lu, current_tail=%lu, current_epoch=%lu, danger_size=%lu, num_boundaries=%lu\n",
            //         rdma_mg->node_id, requester_node_id, old_head_, old_tail_, old_epoch,
            //         ds_w->GetHead(), ds_w->GetTail(), ds_w->GetEpoch(),
            //         calculated_danger_size, boundaries.size());
            //  for (size_t i = 0; i < boundaries.size(); i++) {
            //      printf("  Boundary %lu: start=%lu, end=%lu, size=%lu bytes\n",
            //             i, boundaries[i].first, boundaries[i].second,
            //             boundaries[i].second - boundaries[i].first);
            //  }
            //  fflush(stdout);
            
            // danger_size is now calculated automatically in CalculateWriteBoundaries
            
            int count = 0;
            for (auto pair: boundaries) {
                qp_id = rdma_mg->GetQPForDeltaPull();
                
                if (count == 0) {
                  local_mr = *rdma_mg->Get_local_big_mr();
                  remote_addr = (char*)receive_msg_buf->buffer;
                  uint64_t start = pair.first;
                  uint64_t end = pair.second;
                  size_t write_size = end - start;
                  // copy the header to a local buffer
                  memcpy(local_mr.addr, ds_w->seg_local_mr_->addr, write_size);
                  // set the danger size in the local_mr.
                  ((DeltaSection*)local_mr.addr)->danger_size.store(calculated_danger_size, std::memory_order_release);
                  
                  assert(write_size >= STRUCT_OFFSET(DeltaSection, local_addr_));
                  bool async = true;
                    // Verify that head and tail have not changed during function execution
                    assert(captured_head == ds_w->GetHead() && "Head changed during delta pull!");
                    assert(captured_tail == ds_w->GetTail() && "Tail changed during delta pull!");
                  rdma_mg->RDMA_Write_xcompute_localcopy(&local_mr, remote_addr, receive_msg_buf->rkey,
                    write_size, requester_node_id, qp_id, true, &delta_lck);
                } else {
                  local_mr = *ds_w->seg_local_mr_;
                  remote_addr = (char*)receive_msg_buf->buffer;
                  uint64_t start = pair.first;
                  uint64_t end = pair.second;
                  size_t write_size = end - start;
                  bool async = true;

                  local_mr.addr = (void*)((char*)local_mr.addr + start);
                  remote_addr += start;
                  std::atomic_thread_fence(std::memory_order_release);
                  //todo: may be use RDMA atomic operation can help?
                  rdma_mg->RDMA_Write_xcompute_localcopy(&local_mr, remote_addr, receive_msg_buf->rkey,
                    write_size, requester_node_id, qp_id, true, nullptr);
                }
                count++;
             }
             

         }
         delete receive_msg_buf;
             // todo: implement the async write according to the returned boundaries.
    }

    void TransactionManager::ProcessSnapshotPush(void* args){

        auto* rdma_mg = RDMA_Manager::Get_Instance();
        auto *receive_msg_buf = (RDMA_Request*)args;
        uint8_t node_id = receive_msg_buf->content.snapshot_push.node_id;
        uint64_t least_spn = receive_msg_buf->content.snapshot_push.least_snapshot;
        {
            std::unique_lock<SpinMutex> psp_lck(c_l_mtx);
            if(cluster_least_sp_.count(node_id) == 0){
                cluster_least_sp_[node_id] = least_spn;
            }else{
                assert(cluster_least_sp_[node_id] <= least_spn);
                cluster_least_sp_[node_id] = least_spn;
            }
        }


        delete receive_msg_buf;
    }
    void TransactionManager::BroadCastLeastSP(uint64_t least_sp){
        auto* rdma_mg = RDMA_Manager::Get_Instance();
        RDMA_Request* send_pointer;
        ibv_mr* send_mr = rdma_mg->Get_local_send_message_mr();

        send_pointer = (RDMA_Request*)send_mr->addr;
        send_pointer->command = push_least_snapshot;
        send_pointer->content.snapshot_push.least_snapshot = least_sp;
        send_pointer->content.snapshot_push.node_id = rdma_mg->node_id;
        for (uint8_t i = 0; i < rdma_mg->GetComputeNodeNum(); i++){
            uint8_t target_node_id = 2*i;
            if (target_node_id != rdma_mg->node_id){
                int qp_id = rdma_mg->GetQPForDeltaPull();
                rdma_mg->post_send_xcompute(send_mr, target_node_id, qp_id, sizeof(RDMA_Request));
            }
        }
    }
    void TransactionManager::GarbageCollection(){
        auto rdma_mg = RDMA_Manager::Get_Instance();
        uint64_t last_broadcasted_sp = 0;
        uint64_t last_gc_ts = 0;
        uint64_t largest_snapshot = largest_sp_acquired.load();
        while (gc_should_run.load(std::memory_order_acquire)){
            if (largest_snapshot < largest_sp_acquired.load()){
                // Get the largest snapshot till now in this compute node.
                largest_snapshot = largest_sp_acquired.load();
            }else{
                std::unique_lock<SpinMutex> psp_lck(pin_sp_mtx);
                largest_snapshot = GlobalTimestamp::GetMonotoneTimestamp();
                largest_sp_acquired.store(largest_snapshot);
            }


            //step 1: update the least sp of this node and broadcast.
            garb_mtx.lock();
            std::unique_lock<SpinMutex> psp_lck(pin_sp_mtx);
            uint64_t least_sp_this_node;
            if(pined_snapshot_this_node.empty()){
                least_sp_this_node = largest_snapshot;
            }else{
                least_sp_this_node = pined_snapshot_this_node.begin()->first;
            }
            psp_lck.unlock();
            garb_mtx.unlock();

            if (least_sp_this_node != last_broadcasted_sp){
               BroadCastLeastSP(least_sp_this_node);
                last_broadcasted_sp = least_sp_this_node;

            }
            {
                std::unique_lock<SpinMutex> lck2(c_l_mtx);
                // need to initialize the cluster_least_sp_.
                if (cluster_least_sp_.empty()) {
                    for (uint16_t i = 0; i < rdma_mg->GetComputeNodeNum(); i++) {
                        cluster_least_sp_[2 * i] = 0;
                    }
                }
            }


            //step 2: do garbage collection according to the least sp across the cluster.
            std::unique_lock<SpinMutex> cl_lck(c_l_mtx);
            cluster_least_sp_[rdma_mg->node_id] = least_sp_this_node;
            uint64_t least_sp_across_cluster = UINT64_MAX;
            for(auto iter: cluster_least_sp_){
                if (iter.second < least_sp_across_cluster){
                    least_sp_across_cluster = iter.second;
                }
            }
            cl_lck.unlock();
            if (least_sp_across_cluster > last_gc_ts){
                // do garbage collection.
                last_gc_ts = least_sp_across_cluster;
#ifdef SINGLE_DELTA_PER_NODE
                ds_for_write->GarbageCollectionBySnapshot(least_sp_across_cluster);
                assert(ds_for_write->owner_compute_node_id_ == rdma_mg->node_id);
#else
                std::shared_lock<std::shared_mutex> lck(delta_map_mtx);
                for (auto iter = delta_sections.begin(); iter != delta_sections.end(); iter++){
                    if (iter->second->owner_compute_node_id_ == rdma_mg->node_id){
                        iter->second->GarbageCollectionBySnapshot(least_sp_across_cluster);
                    }
                }
#endif

            }
            // do garbage collection.
            usleep(5000); //todo: tune the sleep time or make the sleep time
        }
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
