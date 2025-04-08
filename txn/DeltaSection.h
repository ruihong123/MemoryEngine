//
// Created by wang4996 on 12/27/24.
//

#ifndef SELCC_DELTASECTION_H
#define SELCC_DELTASECTION_H

#include <infiniband/verbs.h>
#include <pmmintrin.h>
#include <atomic>
#include "Common.h"
#include "Record.h"
#include "rdma.h"
#define SINGLE_DELTA_PER_NODE

namespace DSMEngine {
#if defined(MVOCC)
    class alignas(8) DeltaSection{
    public:
        uint64_t head_;
        alignas(8) std::atomic<uint64_t> tail_;
#ifdef SINGLE_DELTA_PER_NODE
        uint64_t tail_allocated;
#endif
        uint64_t epoch;
        uint64_t max_ts;// this may be depracated later
        bool is_empty_;
        char local_seg_addr_[1];

    }__attribute__((packed));

    class DeltaSectionWrap {
    public:
        // is_empty is necessary because we can not tell whether the ring  buffer is full or empty
        // merely by checking the head and tail pointer.
        uint8_t owner_compute_node_id_;
        GlobalAddress seg_addr_;
        ibv_mr *seg_local_mr_;
        size_t seg_real_size_; // not include the header size of inner delta section.
        RDMA_Manager *rdma_mg_;
        std::shared_mutex ds_mtx_; // todo: change it into spinlatch.
        std::condition_variable_any cv;

        DeltaSection* inner_section;
        DeltaSectionWrap(uint8_t compute_node_id, GlobalAddress seg_addr, size_t seg_size, ibv_mr *seg_local_mr) {
            seg_local_mr_ = seg_local_mr;
            inner_section = (DeltaSection *) seg_local_mr_->addr;
            assert(((uint64_t)seg_local_mr_->addr)%8 == 0);
            inner_section->head_ = 0;
            inner_section->tail_ = 0;
            inner_section->is_empty_ = true;
            owner_compute_node_id_ = compute_node_id;
            seg_addr_ = seg_addr;
            seg_real_size_ = seg_size - STRUCT_OFFSET(DeltaSection, local_seg_addr_) - 1; // 1 is for the RDMA write polling.
            seg_local_mr_ = seg_local_mr;

            rdma_mg_ = RDMA_Manager::Get_Instance();
            inner_section->max_ts = 0;
            inner_section->epoch = 0;
        }
        ~DeltaSectionWrap() {
            //TODO: need to deallocate the remote memory.
            rdma_mg_->Deallocate_Local_RDMA_Slot(seg_local_mr_->addr, DeltaChunk);
            delete seg_local_mr_;
        }
#ifdef SINGLE_DELTA_PER_NODE
        uint64_t AllocateDelta(size_t delta_size, size_t& prev_offset, size_t& next_offset) {
            std::unique_lock<std::shared_mutex> lck(ds_mtx_);
            uint64_t  old_head = inner_section->head_;
            uint64_t return_offset = 0;
            prev_offset = inner_section->tail_allocated;
            // we append new delta record to the tail.
            while (!inner_section->is_empty_ && (old_head + seg_real_size_ - inner_section->tail_allocated) % seg_real_size_ <= delta_size) {
                // wait until there is enough space for the new delta record.
                // if full then we clear the whole delta section. (will be changed later)
                old_head = inner_section->head_;
                //todo: wait for the signal of garbage collection.
                cv.wait(lck, [this, old_head, delta_size]{return ((old_head + seg_real_size_ - inner_section->tail_allocated) % seg_real_size_ > delta_size);});
            }

            if (seg_real_size_ - inner_section->tail_allocated < delta_size)
            {
                if (inner_section->tail_allocated < seg_real_size_){
                    //mark that the parser need to move to 0 postion of this ring buffer
                    *((char*)(inner_section->local_seg_addr_ + inner_section->tail_allocated)) = '^';
                }
                inner_section->tail_allocated = 0;
                inner_section->epoch++;
            }
            return_offset = inner_section->tail_allocated;
            inner_section->tail_allocated += delta_size;
            if (inner_section->is_empty_){
                inner_section->is_empty_ = false;
            }
            next_offset = inner_section->tail_allocated;
            return return_offset;

        }

        void fill_in_delta_record_single(Record *new_record, Record *old_record, GlobalAddress &delta_gadd, size_t &delta_size,
                                               uint64_t commit_ts) {
            delta_size = new_record->estimate_delta_size(); // delta size include both delta header and delta content.
            uint64_t prev_offset;
            uint64_t next_offset;
            uint64_t offset_to_write = AllocateDelta(delta_size, prev_offset, next_offset);
            assert(next_offset <= seg_real_size_);
            assert(offset_to_write <= seg_real_size_);
            //todo the max_ts need to be guarded by a mtx.
            MetaColumn meta_col = old_record->GetMeta();
            // update the max time stamp.
            if (inner_section->max_ts < meta_col.Wts_){
                inner_section->max_ts = meta_col.Wts_;
            }
            DeltaRecord * delta_record = new(inner_section->local_seg_addr_ + offset_to_write) DeltaRecord(
                    meta_col.Wts_, delta_size, meta_col.prev_delta_, commit_ts,
                    meta_col.prev_delta_epoch_, meta_col.prev_delta_data_size_ );
            old_record->dirty_col_ids = std::move(new_record->dirty_col_ids);
            old_record->serialize_to_delta(delta_record);
#ifndef NDEBUG
            if (next_offset < prev_offset){
                assert(*((char*)(inner_section->local_seg_addr_ + prev_offset)) == '^');
            }
#endif

            assert((char*)delta_record + delta_size <= (char*)seg_local_mr_->addr + seg_local_mr_->length);
            delta_gadd = seg_addr_;
            delta_gadd.offset += offset_to_write + STRUCT_OFFSET(DeltaSection, local_seg_addr_);
            while(!inner_section->tail_.compare_exchange_weak(prev_offset, next_offset, std::memory_order_seq_cst,
                                                             std::memory_order_seq_cst)){
                _mm_pause();
            };

        }
#endif
        // new_record is the local copy and the old_record is the global copy. Later the local copy will be written to the global copy.
        // and the global copy's modified columns should be written to the delta section.
        void fill_in_delta_record_thread_local(Record *new_record, Record *old_record, GlobalAddress &delta_gadd, size_t &delta_size,
                                               uint64_t commit_ts) {
            // todo: create a new function for fill in the delta records for mulitple tuple records.
            delta_size = new_record->estimate_delta_size(); // delta size include both delta header and delta content.
//            size_t delta_size_padding = delta_size;
            std::unique_lock<std::shared_mutex> lck(ds_mtx_);
            uint64_t  old_head = inner_section->head_;
            // we append new delta record to the tail.
            while (!inner_section->is_empty_ && (old_head + seg_real_size_ - inner_section->tail_) % seg_real_size_ <= delta_size) {
                // wait until there is enough space for the new delta record.
                // if full then we clear the whole delta section. (will be changed later)
                old_head = inner_section->head_;
                //todo: wait for the signal of garbage collection.
                cv.wait(lck, [this, old_head, delta_size]{return ((old_head + seg_real_size_ - inner_section->tail_) % seg_real_size_ > delta_size);});
            //     // fake garbage collecion code. should be cleared.
            //    inner_section->tail_ = inner_section->head_;
            //    inner_section->is_empty_ = true;
            //    inner_section->epoch++;
            }

            if (seg_real_size_ - inner_section->tail_ < delta_size)
            {
                if (inner_section->tail_ < seg_real_size_){
                    //mark that the parser need to move to 0 postion of this ring buffer
                    *((char*)(inner_section->local_seg_addr_ + inner_section->tail_)) = '^';
                }
                inner_section->tail_ = 0;
                inner_section->epoch++;
            }
            MetaColumn meta_col = old_record->GetMeta();
            // update the max time stamp.
            if (inner_section->max_ts < meta_col.Wts_){
                inner_section->max_ts = meta_col.Wts_;
            }
            DeltaRecord * delta_record = new(inner_section->local_seg_addr_ + inner_section->tail_) DeltaRecord(
                    meta_col.Wts_, delta_size, meta_col.prev_delta_, commit_ts,
                    meta_col.prev_delta_epoch_, meta_col.prev_delta_data_size_ );
            old_record->dirty_col_ids = std::move(new_record->dirty_col_ids);
            old_record->serialize_to_delta(delta_record);
#ifndef NDEBUG
               Record* record = new Record(new_record->schema_ptr_);
                record->roll_back(delta_record);
                delete record;
#endif
            assert((char*)delta_record + delta_size <= (char*)seg_local_mr_->addr + seg_local_mr_->length);
            delta_gadd = seg_addr_;
            delta_gadd.offset += inner_section->tail_ + STRUCT_OFFSET(DeltaSection, local_seg_addr_);
            assert(*((char*)inner_section + (delta_gadd.offset - seg_addr_.offset)) == '&');
            inner_section->tail_ += delta_size;
            if (inner_section->is_empty_){
                inner_section->is_empty_ = false;
            }
        }
        void GarbageCollectionBySnapshot(uint64_t snapshot){
            // todo: this garbage collection logic is not correct. We need to fix it.
            std::unique_lock<std::shared_mutex> lck(ds_mtx_);
            while(1){
                //todo: think about the single delta case, when should we mark empty?
                assert(inner_section->head_ <= seg_real_size_);
                if (inner_section->is_empty_ ){
#ifdef SINGLE_DELTA_PER_NODE
                    assert(inner_section->head_ == inner_section->tail_allocated);
#endif
                    break;
                }
                DeltaRecord* delta_record = (DeltaRecord*)(inner_section->local_seg_addr_ + inner_section->head_);
                if (delta_record->marker_ == '^'|| inner_section->head_ == seg_real_size_){
                    // move the head to 0 position. Reset the head and delta_record.
                    inner_section->head_ = 0;
                    delta_record = (DeltaRecord*)(inner_section->local_seg_addr_ + inner_section->head_);
                    assert(inner_section->tail_ > 0);
                }
                //todo: below code need verify.
                if (inner_section->head_ == inner_section->tail_ ){
                    // if reaching the tail we can stop the garbage collection here
                    break;
                }
                
                if (delta_record->next_delta_wts_ < snapshot){
                    inner_section->head_ += delta_record->current_record_data_size_;
#ifdef SINGLE_DELTA_PER_NODE

                    if (inner_section->head_ == inner_section->tail_allocated){
                        assert(inner_section->tail_ == inner_section->tail_allocated);
                        inner_section->is_empty_ = true;
                    }

#else
                    if (inner_section->head_ == inner_section->tail_){
                        inner_section->is_empty_ = true;
                    }
#endif

                }else{
                    break;
                }
            }
            cv.notify_all();
        }

        void recover_from_delta_record(Record *record, GlobalAddress& delta_gadd){
            std::shared_lock<std::shared_mutex> lck(ds_mtx_);
            DeltaRecord* delta_record = (DeltaRecord*)(inner_section->local_seg_addr_ + (delta_gadd.offset - seg_addr_.offset));
            record->roll_back(delta_record);
        }
        uint64_t GetMaxTimestamp(){
            return inner_section->max_ts;
        }
        uint64_t GetEpoch(){
            return inner_section->epoch;
        }
        uint64_t GetHead(){
            return inner_section->head_;
        }
        uint64_t GetTail(){
            return inner_section->tail_;
        }
        bool isOffsetValid(GlobalAddress gaddr, uint64_t epoch)
        {   
            const uint64_t & head = inner_section->head_;
            const uint64_t & tail = inner_section->tail_;
            long offset = gaddr.offset - seg_addr_.offset - STRUCT_OFFSET(DeltaSection, local_seg_addr_);
            assert(offset >= 0);
            // strictly speaking this logic is not correct. A smaller epoch does not mean the offset is invalid.
            // but a larger epoch means the offset is invalid.
            if (epoch > inner_section->epoch){
                return false;
            }

            // Case 1: The buffer is not wrapped
            // Occupied region is [head, tail)
            if (tail >= head) {
                return (offset >= head && offset < tail);
            }
            // Case 2: The buffer is wrapped
            // Occupied region is [head, capacity) U [0, tail)
            else {
                // offset is valid if it is in [head, capacity) OR [0, tail)
                return ((offset >= head && offset < seg_real_size_) ||
                        (offset >= 0    && offset < tail));
            }
        }
        void PullUpdates(){
            RDMA_Manager *rdma_mg = RDMA_Manager::Get_Instance();
            // pull the updates from the remote node.
            RDMA_Request* send_pointer;
            ibv_mr* send_mr = rdma_mg->Get_local_send_message_mr();
            ibv_mr* recv_mr = seg_local_mr_;
            char* tuple_buffer = (char*)recv_mr->addr;
            send_pointer = (RDMA_Request*)send_mr->addr;
            send_pointer->command = pull_delta_section;
            send_pointer->content.pull_ds.ds_gaddr = seg_addr_;
            send_pointer->content.pull_ds.old_head = inner_section->head_;
            send_pointer->content.pull_ds.old_tail = inner_section->tail_;
            send_pointer->content.pull_ds.old_max_ts = inner_section->max_ts;
            send_pointer->content.pull_ds.old_epoch = inner_section->epoch;
            send_pointer->content.pull_ds.requester_node_id = rdma_mg->node_id;
            send_pointer->buffer = recv_mr->addr;
            send_pointer->rkey = recv_mr->rkey;

            uint8_t * receive_pointer = (uint8_t*)((uint8_t*)recv_mr->addr + rdma_mg->delta_section_size - 1);
            //Clear the reply buffer for the polling.
            *receive_pointer = 0;
//            memset((void*)recv_mr->addr, 0, rdma_mg->delta_section_size);
//        *receive_pointer = {};

            int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
            assert(owner_compute_node_id_ != rdma_mg->node_id);
            rdma_mg->post_send_xcompute(send_mr, owner_compute_node_id_, qp_id, sizeof(RDMA_Request));
            ibv_wc wc[2] = {};
            assert(send_pointer->command!= create_qp_);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
            volatile uint8_t * check_byte = (uint8_t*)receive_pointer;
            size_t poll_num = 0;
            while(!*check_byte){
                poll_num++;
                _mm_clflush((const void *) check_byte);
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
            }
            assert(*check_byte == 5);
            assert(((DeltaSection*)recv_mr->addr)->local_seg_addr_[inner_section->head_] == '&' || ((DeltaSection*)recv_mr->addr)->local_seg_addr_[inner_section->head_] == '^');
            assert(((DeltaSection*)recv_mr->addr)->tail_!=0);
//            printf("Successfully pull the updates for %p delta section, pollnum is %d \n", seg_addr_, poll_num);
//            fflush(stdout);


        }
        void CalculateWriteBoundaries(std::vector<std::pair<size_t, size_t>> & boundaries, uint64_t old_h, uint64_t old_t, uint64_t old_epoch){
            //todo: the logic need carefully proofread.
#ifdef SINGLE_DELTA_PER_NODE
            while(inner_section->tail_ != inner_section->tail_allocated){
                assert(inner_section->tail_ <= inner_section->tail_allocated);
                // no ops
                _mm_pause();
            }
#endif
            assert(inner_section->head_!= inner_section->tail_ || inner_section->is_empty_);
            uint64_t merge_thre = 4096;
            uint64_t start = 0;
            uint64_t end = 0;

            if(UNLIKELY(old_epoch < inner_section->epoch )){
                if (inner_section->tail_ > inner_section->head_) {
                    end = STRUCT_OFFSET(DeltaSection, local_seg_addr_);
                    boundaries.push_back(std::make_pair(start, end));

                    start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->head_;
                    end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
                    boundaries.push_back(std::make_pair(start, end));

                    start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_;
                    end = start + 1;
                    assert(end == rdma_mg_->delta_section_size);
                    boundaries.push_back(std::make_pair(start, end));

                    // prune the result.
                    if (boundaries[2].first - boundaries[1].second <= merge_thre){
                        // merge the third and the second one.
                        boundaries[1].second = boundaries[2].second;
                        boundaries.pop_back();
                    }
                    if (boundaries[1].first - boundaries[0].second <= merge_thre){
                        // merge the second and the first one.
                        boundaries[0].second = boundaries[1].second;
                        boundaries.erase(boundaries.begin() + 1);
                    }
                    return;

//                    if (inner_section->head_ <= merge_thre && seg_real_size_ - inner_section->tail_ <= merge_thre) {
//                        // transfer the whole delta section.
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_;
//                        assert(end - start == rdma_mg_->delta_section_size);
//                        return;
//                    }
//                    if (inner_section->head_ > merge_thre && seg_real_size_ - inner_section->tail_ > merge_thre){
//                        // transfer by three parts.
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_);
//                        boundaries.push_back(std::make_pair(start, end));
//
//                        start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->head_;
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
//                        boundaries.push_back(std::make_pair(start, end));
//
//                        start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_ - 1;
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_;
//                        boundaries.push_back(std::make_pair(start, end));
//                        return;
//                    }
//                    if(inner_section->head_ > merge_thre){
//                        assert(seg_real_size_ - inner_section->tail_ <= merge_thre);
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->head_;
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
//                        boundaries.push_back(std::make_pair(start, end));
//
//                        start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_;
//                        boundaries.push_back(std::make_pair(start, end));
//                        return;
//                    }
//                    if (seg_real_size_ - inner_section->tail_ > merge_thre){
//                        assert(inner_section->head_ <= merge_thre);
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
//                        boundaries.push_back(std::make_pair(start, end));
//
//                        start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_ - 1;
//                        end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_;
//                        boundaries.push_back(std::make_pair(start, end));
//                        return;
//                    }
                }else{
                    // write the header and first half.
                    end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
                    assert(end - start < rdma_mg_->delta_section_size);
                    boundaries.push_back(std::make_pair(start, end));
                    if (old_epoch == inner_section->epoch - 1){
                        // if the epoch is the previous epoch, we may reduce the data size that we need to transfer.
                        start = old_t > inner_section->head_? (STRUCT_OFFSET(DeltaSection, local_seg_addr_) + old_t):(STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->head_);
                    }else{
                        start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->head_;
                    }
                    end = rdma_mg_->delta_section_size;
                    boundaries.push_back(std::make_pair(start, end));
                    // pruning the result.
                    if (boundaries[1].first - boundaries[0].second <= merge_thre){
                        boundaries[0].second = boundaries[1].second;
                        boundaries.pop_back();
                        assert(boundaries[0].second == STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_ + 1);
                    }
                    return;
                }

            }else{
                if (inner_section->tail_ > inner_section->head_) {
                    end = STRUCT_OFFSET(DeltaSection, local_seg_addr_);
                    boundaries.push_back(std::make_pair(start, end));
                    if (old_t > inner_section->head_){
                        start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + old_t;
                    }else{
                        start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->head_;
                    }
                    start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->head_;
                    end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
                    boundaries.push_back(std::make_pair(start, end));

                    start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_;
                    end = start + 1;
                    boundaries.push_back(std::make_pair(start, end));
                    assert(end == rdma_mg_->delta_section_size);

                    // prune the result.
                    if (boundaries[2].first - boundaries[1].second <= merge_thre){
                        // merge the third and the second one.
                        boundaries[1].second = boundaries[2].second;
                        boundaries.pop_back();
                    }
                    if (boundaries[1].first - boundaries[0].second <= merge_thre){
                        // merge the second and the first one.
                        boundaries[0].second = boundaries[1].second;
                        boundaries.erase(boundaries.begin() + 1);
                    }
                    return;
                }else{
                    assert(old_h <= inner_section->head_);
                    // write the header and first half.
                    end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
                    assert(end - start < rdma_mg_->delta_section_size);
                    boundaries.push_back(std::make_pair(start, end));

                    // we may reduce the data size that we need to transfer.
                    start = (STRUCT_OFFSET(DeltaSection, local_seg_addr_) + old_t);
                    end = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + inner_section->tail_;
                    boundaries.push_back(std::make_pair(start, end));

                    // The third part only contain the polling byte, because the old head has to smaller than the current head.
                    // There is no need to transfer the data for the second half of the delta section.
                    start = STRUCT_OFFSET(DeltaSection, local_seg_addr_) + seg_real_size_;
                    end = start + 1;
                    assert(end == rdma_mg_->delta_section_size);
                    boundaries.push_back(std::make_pair(start, end));

                    // pruning the result.
                    // prune the result.
                    if (boundaries[2].first - boundaries[1].second <= merge_thre){
                        // merge the third and the second one.
                        boundaries[1].second = boundaries[2].second;
                        boundaries.pop_back();
                    }
                    if (boundaries[1].first - boundaries[0].second <= merge_thre){
                        // merge the second and the first one.
                        boundaries[0].second = boundaries[1].second;
                        boundaries.erase(boundaries.begin() + 1);
                    }
                    return;
                }
            }
        }

    };
#endif
}

#endif //SELCC_DELTASECTION_H
