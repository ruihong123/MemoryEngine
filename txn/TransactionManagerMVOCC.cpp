#if defined(MVOCC)
#include "TransactionManager.h"
#include "GlobalTimestamp.h"
namespace DSMEngine{
        WritableFile* TransactionManager::log_file = nullptr;
        std::shared_mutex TransactionManager::delta_map_mtx;
        std::map<GlobalAddress, DeltaSectionWrap*, std::greater<GlobalAddress>> TransactionManager::delta_sections;
        SpinMutex TransactionManager::pin_sp_mtx;
        std::map<uint64_t, uint16_t> TransactionManager::pined_snapshot_this_node;
//        uint64_t TransactionManager::last_broadcasted_sp = 0;
        SpinMutex TransactionManager::c_l_mtx;
        std::map<uint16_t, uint64_t > TransactionManager::cluster_least_sp_;
        bool TransactionManager::AllocateNewRecord(TxnContext *context, size_t table_id, Cache::Handle *&handle,
                                                   GlobalAddress &tuple_gaddr, Record*& tuple) {
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
        if (access_type == READ_ONLY) {
//                uint64_t wts = record->GetWTS();
            default_gallocator->SELCC_Shared_Lock(page_buff, page_gaddr, handle);

        } else  {
            if (pure_read_txn){
                pure_read_txn = false;
            }
            //Read_Write, Delete_Only, Insert_Only
            // There is no actual update on the record temporarily, so we can use shared lock.
            default_gallocator->SELCC_Shared_Lock(page_buff, page_gaddr, handle);

        }
        assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
        tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);

//        record->Set_Handle(handle);

        Access* access = access_list_.NewAccess();
        access->access_type_ = access_type;
        access->access_global_record_ = new Record(schema_ptr, tuple_buffer);
        record = new Record(schema_ptr);
        record->CopyFrom(access->access_global_record_);
        uint64_t ts = record->GetWTS();
        while (ts > snapshot_ts){
            // TODO: ROll back old version of the data.
            MetaColumn meta = record->GetMeta();
            GlobalAddress prev_delta = meta.prev_delta_;
            assert(prev_delta != GlobalAddress::Null());
            // implement a mechanism to detect whether the local copy of delta section is up to date.
            // if not, we need to fetch the latest version of the delta section.
//            record->roll_back()
            DeltaSectionWrap* delta_section = nullptr;
//            uint64_t largest_ds_timestamp = 0;
            uint64_t ds_head = 0;
            uint64_t ds_tail = 0;
            uint64_t ds_epoch = 0;
            {
                std::shared_lock<std::shared_mutex> l(delta_map_mtx);
                auto iter = delta_sections.lower_bound(prev_delta);
                delta_section = iter->second;
//                largest_ds_timestamp = delta_section->GetMaxTimestamp();
                ds_head = delta_section->GetHead();
                ds_tail = delta_section->GetTail();
                ds_epoch = delta_section->GetEpoch();
                assert(iter != delta_sections.end());
                assert(iter->first.nodeID == prev_delta.nodeID);
                assert(iter->first.offset - prev_delta.offset <= ds_for_write->seg_real_size_);
            }
//            uint64_t delta_offset = prev_delta.offset - delta_section->seg_addr_.offset;
            // check whether the prev delta is the latest version. check whether the prev_delta is within the head and tail plus checking
            // whether the epoch is the same.


            //check whether the delta_offset is within the ring buffer by tail and head.


            if ( ds_epoch != meta.prev_delta_epoch_ || !delta_section->inner_section->is_empty_ || !delta_section->isOffsetInValidRing(prev_delta)){
                // fetch the latest version of the delta section.
                // todo: sync the delta section.
                delta_section->PullUpdates();
            }else{
#ifndef NDEBUG
                long offset = prev_delta.offset - prev_delta.offset - STRUCT_OFFSET(DeltaSection, local_seg_addr_);
                if (ds_tail >= ds_head){
                    assert(ds_tail - offset > STRUCT_OFFSET(DeltaRecord, data_));
                }
#endif
            }
            std::shared_lock<std::shared_mutex> lck(delta_section->ds_mtx_);
            DeltaRecord* delta_record = (DeltaRecord*)(delta_section->inner_section->local_seg_addr_ + prev_delta.offset);
            record->roll_back(delta_record);
            ts = record->GetWTS();
        }
        access->txn_local_tuple_ = record;
        access->access_addr_ = tuple_gaddr;
        if (access_type == DELETE_ONLY) {
            record->PutWTS(UINT64_MAX);
            record->SetVisible(false);
        }
        if (access_type == READ_ONLY) {
//                uint64_t wts = record->GetWTS();
            default_gallocator->SELCC_Shared_UnLock(page_gaddr, handle);

        } else  {
            //Read_Write, Delete_Only, Insert_Only
            default_gallocator->SELCC_Shared_UnLock(page_gaddr, handle);

        }
        PROFILE_TIME_END(thread_id_, CC_SELECT);
        return true;
    }
    // Contain validation and commit stages.
    bool TransactionManager::CommitTransaction(TxnContext* context,
                                               TxnParam* param, CharArray& ret_str) {
        PROFILE_TIME_START(thread_id_, CC_COMMIT);

        assert(locked_handles_.empty());
        std::map<uint64_t, Access*> sorted_access;
        // lock the access list in order to avoid deadlock.
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            GlobalAddress g_addr = access->access_addr_;
            sorted_access.insert({g_addr, access});
        }
        // First let us check whether the transaciton need to abort. (validate stage)
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
        }
        // get the commit timestamp right before the commit phase.
        uint64_t commit_ts = GlobalTimestamp::FetchAddMonotoneTimestamp();
        // Then let us write the data and commit. (commit stage)
        for (size_t i = 0; i < access_list_.access_count_; ++i) {
            Access* access = access_list_.GetAccess(i);
            AccessType access_type = access->access_type_;
            if (access_type == READ_WRITE) {
                // TODO: Create a new delta_record in the delta section and update the prev_delta in the record's metadata.
                GlobalAddress delta_gadd = GlobalAddress::Null();
                size_t delta_size = 0;
                ds_for_write->fill_in_delta_record(access->txn_local_tuple_, access->access_global_record_, delta_gadd, delta_size);
                MetaColumn meta = access->txn_local_tuple_->GetMeta();
                meta.prev_delta_ = delta_gadd;
                meta.prev_delta_epoch_ = ds_for_write->GetEpoch();
                meta.prev_delta_size_ = delta_size;
                access->txn_local_tuple_->PutMeta(meta);
                access->access_global_record_->CopyFrom(access->txn_local_tuple_);
                access->access_global_record_->PutWTS(commit_ts);
            }else if(access_type == INSERT_ONLY){
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
        ReleaseSnapshot();
        is_first_access_ = false;
        is_first_access_ = true;
        pure_read_txn = true;
        snapshot_ts = 0;
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
            is_first_access_ = true;
            pure_read_txn = true;;
            snapshot_ts = 0;
            PROFILE_TIME_END(thread_id_, CC_ABORT);

        }
        void TransactionManager::GetSnapshot() {
            snapshot_ts = GlobalTimestamp::GetMonotoneTimestamp();
            // todo: there is a potential bug. If the snapshot is acquire but this thread is yield, then the global cluster may not detect that this snapshot number is pinned and the background thread may clean up
            // the old version for this snapshot number. We can make the timestamp acquire inside the spin lock, but it may cause the performance issue.
            // Another solution could be using another spin mutex to use a shared lock to block the garbage collector when we are calling Get snapshot function
            std::unique_lock<SpinMutex> psp_lck(pin_sp_mtx);

            if(pined_snapshot_this_node.count(snapshot_ts) == 0){
                pined_snapshot_this_node[snapshot_ts] = 1;
            }else{
                pined_snapshot_this_node[snapshot_ts]++;
            }
            psp_lck.unlock();
            // todo: may I insert this snapshot to the cluster_least_sp_?
        }
        void TransactionManager::ReleaseSnapshot() {
            std::unique_lock<SpinMutex> psp_lck(pin_sp_mtx);
            assert(pined_snapshot_this_node.count(snapshot_ts) != 0);
            bool least_sp_change = false;
            if (pined_snapshot_this_node.begin()->first == snapshot_ts && pined_snapshot_this_node.begin()->second == 1)
                least_sp_change = true;
            if(pined_snapshot_this_node[snapshot_ts] == 1){
                pined_snapshot_this_node.erase(snapshot_ts);
            }else{
                pined_snapshot_this_node[snapshot_ts]--;
                assert(pined_snapshot_this_node[snapshot_ts] > 0);
            };
//            psp_lck.unlock();
//            if (least_sp_change){
//                // I think current strategy to sync the global least sp for this node is too eager, we can do it lazily
//                auto rdma_mg = RDMA_Manager::Get_Instance();
//                std::unique_lock<SpinMutex> cl_lck(c_l_mtx);
//                cluster_least_sp_[rdma_mg->node_id] = pined_snapshot_this_node.begin()->first;
//            }
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
        auto* ds_for_write= new DeltaSectionWrap(compute_node_id, ds_gaddr, rdma_mg->delta_section_size, local_mr);
        {
            std::unique_lock<std::shared_mutex> lck(TransactionManager::delta_map_mtx);
            TransactionManager::delta_sections.insert(std::make_pair(ds_gaddr, ds_for_write));
        }
        delete receive_msg_buf;
    }
     void TransactionManager::ProcessDeltaPull(void* args){

        auto* rdma_mg = RDMA_Manager::Get_Instance();
        auto *receive_msg_buf = (RDMA_Request*)args;
        GlobalAddress ds_gaddr = receive_msg_buf->content.pull_ds.ds_gaddr;
        uint64_t old_head_ = receive_msg_buf->content.pull_ds.old_head;
        uint64_t old_tail_ = receive_msg_buf->content.pull_ds.old_tail;
        uint64_t old_max_ts = receive_msg_buf->content.pull_ds.old_max_ts;
        uint64_t old_epoch = receive_msg_buf->content.pull_ds.old_epoch;

        {
            std::unique_lock<std::shared_mutex> map_lck(TransactionManager::delta_map_mtx);
            auto it = TransactionManager::delta_sections.find(ds_gaddr);
            DeltaSectionWrap* ds_w = it->second;
            // RDMA write back the most updated delta section.
            std::shared_lock<std::shared_mutex> delta_lck(ds_w->ds_mtx_);
            ibv_mr* local_mr = ds_w->seg_local_mr_;
            int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
//                if (old_epoch < ds_w->inner_section->epoch){
            // the local copy is up to date.
            //todo: develop reply mechanism according to the old epoch, old head and old tail.
            uint8_t* polling_byte = (uint8_t*)((uint8_t*)local_mr->addr + rdma_mg->delta_section_size - 1);
            *polling_byte = 1;
            rdma_mg->RDMA_Write_xcompute(local_mr, receive_msg_buf->buffer, receive_msg_buf->rkey, rdma_mg->delta_section_size,
                                         ds_w->owner_compute_node_id_, qp_id, false, true);
//                }else if() {}


        }
        delete receive_msg_buf;
    }
    void TransactionManager::GarbageCollection(){
        auto rdma_mg = RDMA_Manager::Get_Instance();
        uint64_t last_broadcasted_sp = 0;
        uint64_t last_gc_ts = 0;
        while(1){
            //step 1: update the least sp of this node and broadcast.
            std::unique_lock<SpinMutex> psp_lck(pin_sp_mtx);
            uint64_t least_sp_this_node;

            if(pined_snapshot_this_node.empty()){
                least_sp_this_node = UINT64_MAX;
            }else{
                least_sp_this_node = pined_snapshot_this_node.begin()->first;
            }
            if (least_sp_this_node != last_broadcasted_sp){
                // todo: implement the garbage collection broadcast.
                BroadCastLeastSP(least_sp_this_node);
                last_broadcasted_sp = least_sp_this_node;

            }
            psp_lck.unlock();
            //step 2: do garbage collection according to the least sp across the cluster.
            std::unique_lock<SpinMutex> cl_lck(c_l_mtx);
            cluster_least_sp_[rdma_mg->node_id] = least_sp_this_node;
            uint64_t least_sp_across_cluster = UINT64_MAX;
            for(auto iter: cluster_least_sp_){
                if (iter.second < least_sp_across_cluster){
                    least_sp_across_cluster = iter.second;
                }
            }
            if (least_sp_across_cluster > last_gc_ts){
                // do garbage collection.
                last_gc_ts = least_sp_across_cluster;
                for (auto iter = delta_sections.begin(); iter != delta_sections.end();){
                    // todo: garbage collect the old delta file from the head.
                }
            }
            cl_lck.unlock();
                    // do garbage collection.
            usleep(1000);
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
