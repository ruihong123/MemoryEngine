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
#include "port/port_posix.h"
#include "Timer.h"
#define SINGLE_DELTA_PER_NODE

namespace DSMEngine {
    // Forward declaration for callback (only used by benchmarks)
    extern void (*g_delta_pull_time_callback)(uint64_t time_us);

#ifdef MVCC_STORAGE_BENCH
    // Define the callback pointer only for mvcc_storage_bench
    void (*g_delta_pull_time_callback)(uint64_t time_us) = nullptr;
#endif

#if defined(MVOCC)
    class alignas(8) DeltaSection {
    public:
        alignas(8) std::atomic<uint64_t> danger_size; // used for the concurrency control on the issuer side.
        alignas(8) std::atomic<uint64_t> epoch_;
        alignas(8) std::atomic<uint64_t> head_;
        alignas(8) std::atomic<uint64_t> tail_;
#ifdef SINGLE_DELTA_PER_NODE
        uint64_t tail_allocated;
#endif

        uint64_t max_ts; // this may be depracated later
        bool is_empty_;
        char local_addr_[1];
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
        RWSpinMutex shadow_mtx_; // todo: change it into spinlatch.
        RWSpinMutex main_mtx_; // Reader-prioritized RW latch for delta section updates
        std::condition_variable_any cv;
        #ifndef NDEBUG
        std::atomic<uint64_t> last_tail_;
        std::atomic<uint64_t> last_head_;
        std::atomic<uint64_t> last_epoch_;
        std::atomic<uint64_t> last_danger_size_;
        #endif

        DeltaSection *inner_section;

        DeltaSectionWrap(uint8_t compute_node_id, GlobalAddress seg_addr, size_t seg_size, ibv_mr *seg_local_mr)// Initialize as reader-prioritized
        {
            seg_local_mr_ = seg_local_mr;
            inner_section = (DeltaSection *) seg_local_mr_->addr;
            assert(((uint64_t)seg_local_mr_->addr)%64 == 0);
            inner_section->head_ = 0;
            inner_section->tail_ = 0;
            inner_section->is_empty_ = true;
            owner_compute_node_id_ = compute_node_id;
            seg_addr_ = seg_addr;
            seg_real_size_ = seg_size - STRUCT_OFFSET(DeltaSection, local_addr_) - 1;
            // 1 is for the RDMA write polling.
            seg_local_mr_ = seg_local_mr;

            rdma_mg_ = RDMA_Manager::Get_Instance();
            inner_section->max_ts = 0;
            inner_section->epoch_.store(0, std::memory_order_relaxed);
        }

        ~DeltaSectionWrap() {
            //TODO: need to deallocate the remote memory.
            rdma_mg_->Deallocate_Local_RDMA_Slot(seg_local_mr_->addr, DeltaChunk);
            delete seg_local_mr_;
        }
#ifdef SINGLE_DELTA_PER_NODE
        uint64_t AllocateDelta(size_t delta_size, size_t &prev_offset, size_t &next_offset) {
            std::unique_lock<RWSpinMutex> lck(main_mtx_);
            uint64_t old_head = inner_section->head_;
            uint64_t return_offset = 0;
#ifndef NDEBUG
            size_t old_epoch = inner_section->epoch_.load(std::memory_order_relaxed);
#endif

            // we append new delta record to the tail.
            while (!inner_section->is_empty_ && (old_head + seg_real_size_ - inner_section->tail_allocated) %
                   seg_real_size_ <= delta_size) {
                // wait until there is enough space for the new delta record.
                // if full then we clear the whole delta section. (will be changed later)

                //todo: wait for the signal of garbage collection.
                cv.wait(lck, [this, delta_size] {
                    uint64_t current_head = inner_section->head_;
                    return (inner_section->is_empty_ || (current_head + seg_real_size_ - inner_section->tail_allocated)
                            % seg_real_size_ > delta_size);
                });
                old_head = inner_section->head_;
            }

            prev_offset = inner_section->tail_allocated;
            if (seg_real_size_ - inner_section->tail_allocated < delta_size) {
                if (inner_section->tail_allocated < seg_real_size_) {
                    //mark that the parser need to move to 0 postion of this ring buffer
                    *((char *) (inner_section->local_addr_ + inner_section->tail_allocated)) = '^';
                    //                    printf("^ is writtern at %lu epoch is %d\n", inner_section->tail_allocated, inner_section->epoch);
                    //                    fflush(stdout);
                }
                inner_section->tail_allocated = 0;
            }
            return_offset = inner_section->tail_allocated;
            inner_section->tail_allocated += delta_size;
            if (inner_section->is_empty_) {
                inner_section->is_empty_ = false;
            }
            next_offset = inner_section->tail_allocated;
            return return_offset;
        }

        void fill_in_delta_record_single(Record *new_record, Record *old_record, GlobalAddress &delta_gadd,
                                         size_t &delta_size,
                                         uint64_t commit_ts) {
            // std::unique_lock<RWSpinMutex> lck(main_mtx_);
            delta_size = new_record->estimate_delta_size(); // delta size include both delta header and delta content.
            assert(delta_size <10000);
            uint64_t prev_offset;
            uint64_t next_offset;
            uint64_t offset_to_write = AllocateDelta(delta_size, prev_offset, next_offset);
            assert(next_offset <= seg_real_size_);
            assert(offset_to_write <= seg_real_size_);
            //todo the max_ts need to be guarded by a mtx.
            MetaColumn meta_col = old_record->GetMeta();
            // update the max time stamp.
            if (inner_section->max_ts < meta_col.Wts_) {
                inner_section->max_ts = meta_col.Wts_;
            }
            DeltaRecord *delta_record = new(inner_section->local_addr_ + offset_to_write) DeltaRecord(
                meta_col.Wts_, delta_size, meta_col.prev_version_, commit_ts,
                meta_col.prev_delta_epoch_, meta_col.prev_delta_data_size_);
            old_record->dirty_col_ids = std::move(new_record->dirty_col_ids);
            old_record->serialize_to_delta(delta_record);
#ifndef NDEBUG
            if (next_offset < prev_offset && prev_offset < seg_real_size_) {
                assert(*((char *) (inner_section->local_addr_ + prev_offset)) == '^');
            }
#endif

            assert((char *) delta_record + delta_size <= (char *) seg_local_mr_->addr + seg_local_mr_->length);
            delta_gadd = seg_addr_;
            delta_gadd.offset += offset_to_write + STRUCT_OFFSET(DeltaSection, local_addr_);
            {
                //                std::unique_lock<std::shared_mutex> lck(shadow_mtx_);
                uint64_t tail_shot_before = inner_section->tail_;
                //todo: need to understand why CAS method for updating the tail is not working.

                size_t expect = prev_offset;
                //                size_t old_next_offset = next_offset;
                assert(next_offset > prev_offset || prev_offset - next_offset > 100000);
                // Memory barrier: ensure serialize_to_delta writes are visible before tail update
                // This prevents RDMA readers from seeing updated tail but uninitialized buffer
                std::atomic_thread_fence(std::memory_order_seq_cst);
                if (next_offset < prev_offset) {
                    assert(prev_offset <= seg_real_size_);
                    inner_section->epoch_.fetch_add(1, std::memory_order_release);
                }
                // NOTE: epoch increment was removed here - epoch should only increment when tail wraps around to 0
                // See line 192 for the correct epoch increment logic


                while (!inner_section->tail_.compare_exchange_weak(expect, next_offset, std::memory_order_seq_cst,
                                                                   std::memory_order_seq_cst)) {
                    // the expect will be changed by the compare exchange, so wee need to update it.
                    expect = prev_offset;
                    //                    assert(next_offset > expect || expect - next_offset > 100000);
                    _mm_pause();
                };
            }
        }
#endif
        // new_record is the local copy and the old_record is the global copy. Later the local copy will be written to the global copy.
        // and the global copy's modified columns should be written to the delta section.
        void fill_in_delta_record_thread_local(Record *new_record, Record *old_record, GlobalAddress &delta_gadd,
                                               size_t &delta_size,
                                               uint64_t commit_ts) {
            // todo: create a new function for fill in the delta records for mulitple tuple records.
            delta_size = new_record->estimate_delta_size(); // delta size include both delta header and delta content.
            //            size_t delta_size_padding = delta_size;
            std::unique_lock<RWSpinMutex> lck(main_mtx_);
            uint64_t old_head = inner_section->head_;
            // we append new delta record to the tail.
            while (!inner_section->is_empty_ && (old_head + seg_real_size_ - inner_section->tail_) % seg_real_size_ <=
                   delta_size) {
                // wait until there is enough space for the new delta record.
                // if full then we clear the whole delta section. (will be changed later)
                old_head = inner_section->head_;
                //todo: wait for the signal of garbage collection.
                cv.wait(lck, [this, old_head, delta_size] {
                    return ((old_head + seg_real_size_ - inner_section->tail_) % seg_real_size_ > delta_size);
                });
                //     // fake garbage collecion code. should be cleared.
                //    inner_section->tail_ = inner_section->head_;
                //    inner_section->is_empty_ = true;
                //    inner_section->epoch++;
            }

            if (seg_real_size_ - inner_section->tail_ < delta_size) {
                if (inner_section->tail_ < seg_real_size_) {
                    //mark that the parser need to move to 0 postion of this ring buffer
                    *((char *) (inner_section->local_addr_ + inner_section->tail_)) = '^';
                }
                inner_section->tail_ = 0;
                inner_section->epoch_.fetch_add(1, std::memory_order_release);
            }
            MetaColumn meta_col = old_record->GetMeta();
            // update the max time stamp.
            if (inner_section->max_ts < meta_col.Wts_) {
                inner_section->max_ts = meta_col.Wts_;
            }
            DeltaRecord *delta_record = new(inner_section->local_addr_ + inner_section->tail_) DeltaRecord(
                meta_col.Wts_, delta_size, meta_col.prev_version_, commit_ts,
                meta_col.prev_delta_epoch_, meta_col.prev_delta_data_size_);
            old_record->dirty_col_ids = std::move(new_record->dirty_col_ids);
            old_record->serialize_to_delta(delta_record);
#ifndef NDEBUG
            Record *record = new Record(new_record->schema_ptr_);
            record->roll_back(delta_record);
            delete record;
#endif
            assert((char*)delta_record + delta_size <= (char*)seg_local_mr_->addr + seg_local_mr_->length);
            delta_gadd = seg_addr_;
            delta_gadd.offset += inner_section->tail_ + STRUCT_OFFSET(DeltaSection, local_addr_);
            assert(*((char*)inner_section + (delta_gadd.offset - seg_addr_.offset)) == '&');

            // Memory barrier: ensure serialize_to_delta writes are visible before tail update
            // This prevents RDMA readers from seeing updated tail but uninitialized buffer
            std::atomic_thread_fence(std::memory_order_release);

            inner_section->tail_ += delta_size;
            if (inner_section->is_empty_) {
                inner_section->is_empty_ = false;
            }
        }

        void GarbageCollectionBySnapshot(uint64_t snapshot) {
            // Start timing
            uint64_t start_time = Timer::get_time_ns();

            // todo: this garbage collection logic is not correct. We need to fix it.
            std::unique_lock<RWSpinMutex> lck(main_mtx_);

            while (1) {
                //todo: think about the single delta case, when should we mark empty?
                assert(inner_section->head_ <= seg_real_size_);
                if (inner_section->is_empty_) {
#ifdef SINGLE_DELTA_PER_NODE
                    assert(inner_section->head_ == inner_section->tail_allocated);
#endif
                    break;
                }
                DeltaRecord *delta_record = (DeltaRecord *) (inner_section->local_addr_ + inner_section->head_);
                if (delta_record->marker_ == '^' || inner_section->head_ == seg_real_size_) {
                    // move the head to 0 position. Reset the head and delta_record.
                    inner_section->head_ = 0;
                    delta_record = (DeltaRecord *) (inner_section->local_addr_ + inner_section->head_);
                    assert(inner_section->tail_ > 0);
                }
                //todo: below code need verify.
                if (inner_section->head_ == inner_section->tail_) {
                    // if reaching the tail we can stop the garbage collection here
                    break;
                }

                if (delta_record->next_delta_wts_ < snapshot) {
                    int record_size = delta_record->current_record_data_size_;
#ifndef NDEBUG
                    memset(inner_section->local_addr_ + inner_section->head_, 2, record_size);
#endif
                    std::atomic_thread_fence(std::memory_order_seq_cst);
                    inner_section->head_ += record_size;

#ifdef SINGLE_DELTA_PER_NODE

                    if (inner_section->head_ == inner_section->tail_allocated) {
                        assert(inner_section->tail_ == inner_section->tail_allocated);
                        inner_section->is_empty_ = true;
                    }

#else
                    if (inner_section->head_ == inner_section->tail_) {
                        inner_section->is_empty_ = true;
                    }
#endif
                } else {
                    break;
                }
            }

            // End timing
            uint64_t end_time = Timer::get_time_ns();
            uint64_t elapsed_us = (end_time - start_time) / 1000;

            // Track GC runtime statistics
            static std::atomic<uint64_t> gc_runs(0);
            static std::atomic<uint64_t> total_time_us(0);
            static std::atomic<uint64_t> min_time_us(UINT64_MAX);
            static std::atomic<uint64_t> max_time_us(0);

            uint64_t runs = gc_runs.fetch_add(1) + 1;
            total_time_us.fetch_add(elapsed_us);

            // Update min
            uint64_t current_min = min_time_us.load();
            while (elapsed_us < current_min && !min_time_us.compare_exchange_weak(current_min, elapsed_us)) {
                current_min = min_time_us.load();
            }

            // Update max
            uint64_t current_max = max_time_us.load();
            while (elapsed_us > current_max && !max_time_us.compare_exchange_weak(current_max, elapsed_us)) {
                current_max = max_time_us.load();
            }

            // Print GC runtime statistics
            // printf("[GC] GC #%lu took %lu us, avg=%lu us, min=%lu us, max=%lu us\n",
            //        runs, elapsed_us, total_time_us.load() / runs,
            //        min_time_us.load(), max_time_us.load());
            // fflush(stdout);

            cv.notify_all();
        }

        uint64_t GetMaxTimestamp() {
            return inner_section->max_ts;
        }

        uint64_t GetEpoch() {
            return inner_section->epoch_.load(std::memory_order_acquire);
        }

        uint64_t GetHead() {
#ifdef SINGLE_DELTA_PER_NODE
            return inner_section->head_.load();
#else
            return inner_section->head_;

#endif
        }

        uint64_t GetTail() {
#ifdef SINGLE_DELTA_PER_NODE
            return inner_section->tail_.load(std::memory_order_seq_cst);
#else
            return inner_section->tail_.load(std::memory_order_seq_cst);
#endif
        }
#ifdef SINGLE_DELTA_PER_NODE
        uint64_t GetTailAllocate() {
            return inner_section->tail_allocated;
        }
#endif
        bool isOffsetDangerous(long offset, uint64_t epoch) {
            uint64_t danger_size = inner_section->danger_size.load(std::memory_order_relaxed);
            if (danger_size == 0) {
                return false;
            }

            assert(offset >= 0);
            uint64_t pos = static_cast<uint64_t>(offset);
            uint64_t head = inner_section->head_.load(std::memory_order_relaxed);
            uint64_t tail = inner_section->tail_.load(std::memory_order_relaxed);
            uint64_t capacity = seg_real_size_;

            assert(danger_size < capacity);

            if (tail >= head) {
                uint64_t danger_start = (danger_size >= tail) ? 0 : tail - danger_size;
                return pos >= danger_start && pos <= tail;
            }

            if (danger_size <= tail) {
                uint64_t lower_start = tail - danger_size;
                return pos >= lower_start && pos <= tail;
            }

            if (pos <= tail) {
                return true;
            }

            uint64_t upper_span = danger_size - tail;
            assert(upper_span < capacity);
            uint64_t upper_start = capacity - upper_span;
            return pos >= upper_start && pos < capacity;
        }

        bool isOffsetValid(long offset, uint64_t epoch) {
            // Use acquire to observe committed values (writers publish with release)
            uint64_t head = inner_section->head_.load(std::memory_order_relaxed);
            uint64_t tail = inner_section->tail_.load(std::memory_order_relaxed);
            // Check epoch first: a larger epoch means the offset is invalid
            // Epoch is published under locks; acquire is sufficient here
            uint64_t current_epoch = inner_section->epoch_.load(std::memory_order_relaxed);
            assert(offset >= 0);

            if (epoch > current_epoch) {
                return false;
            }

            // Note: We don't need to check offset < head because garbage collection
            // guarantees that the head will never be recycled if the snapshot (epoch)
            // is still being used. We only need to check the tail boundary.

            // Case 1: The buffer is not wrapped
            // Occupied region is [head, tail)
            if (tail >= head) {
                return (offset < tail);
            }
            // Case 2: The buffer is wrapped
            // Occupied region is [head, capacity) U [0, tail)
            else {
                // offset is valid if it is in [head, capacity) OR [0, tail)
                return ((offset >= head && offset < seg_real_size_) ||
                        (offset >= 0 && offset < tail));
            }
        }

        // Combined fast path: validate offset and ensure it is not in the dangerous region
        // Reduces atomic loads by snapshotting head/tail/epoch/danger_size once.
        bool isvalidandnotdangerours(long offset, uint64_t epoch) {
            assert(offset >= 0);
            // Snapshot shared state (acquire pairs with release updates)
            uint64_t head = inner_section->head_.load(std::memory_order_relaxed);
            uint64_t tail = inner_section->tail_.load(std::memory_order_relaxed);
            uint64_t current_epoch = inner_section->epoch_.load(std::memory_order_relaxed);
            uint64_t danger_sz = inner_section->danger_size.load(std::memory_order_relaxed);

            // Epoch check: larger epoch means offset invalid
            if (epoch > current_epoch) {
                return false;
            }

            // Validity check (same as isOffsetValid)
            bool valid;
            if (tail >= head) {
                // Occupied: [head, tail)
                valid = (offset < tail);
            } else {
                // Wrapped: [head, N) U [0, tail)
                valid = ((offset >= head && offset < seg_real_size_) || (offset >= 0 && offset < tail));
            }
            if (!valid) return false;

            // Danger check (same as isOffsetDangerous), but inverted result
            if (danger_sz == 0) return true;

            uint64_t pos = static_cast<uint64_t>(offset);
            uint64_t capacity = seg_real_size_;

            assert(danger_sz < capacity);

            if (tail >= head) {
                uint64_t danger_start = (danger_sz >= tail) ? 0 : tail - danger_sz;
                if (pos >= danger_start && pos <= tail) return false;
            } else {
                if (danger_sz <= tail) {
                    uint64_t lower_start = tail - danger_sz;
                    if (pos >= lower_start && pos <= tail) return false;
                } else {
                    if (pos <= tail) return false;
                    uint64_t upper_span = danger_sz - tail;
                    assert(upper_span < capacity);
                    uint64_t upper_start = capacity - upper_span;
                    if (pos >= upper_start && pos < capacity) return false;
                }
            }
            return true;
        }

        void PullUpdates() {
            #ifndef NDEBUG
            last_head_.store(inner_section->head_, std::memory_order_relaxed);
            last_tail_.store(inner_section->tail_, std::memory_order_relaxed);
            last_epoch_.store(inner_section->epoch_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            #endif
            // Start timing
            uint64_t start_time = Timer::get_time_ns();

            RDMA_Manager *rdma_mg = RDMA_Manager::Get_Instance();
            // pull the updates from the remote node.
            RDMA_Request *send_pointer;
            ibv_mr *send_mr = rdma_mg->Get_local_send_message_mr();
            ibv_mr *recv_mr = seg_local_mr_;
            char *tuple_buffer = (char *) recv_mr->addr;
            send_pointer = (RDMA_Request *) send_mr->addr;
            send_pointer->command = pull_delta_section;
            send_pointer->content.pull_ds.ds_gaddr = seg_addr_;
            send_pointer->content.pull_ds.old_head = inner_section->head_;
            send_pointer->content.pull_ds.old_tail = inner_section->tail_;
            send_pointer->content.pull_ds.old_max_ts = inner_section->max_ts;
            send_pointer->content.pull_ds.old_epoch = inner_section->epoch_.load(std::memory_order_acquire);
            send_pointer->content.pull_ds.requester_node_id = rdma_mg->node_id;
            send_pointer->buffer = recv_mr->addr;
            send_pointer->rkey = recv_mr->rkey;

            assert(inner_section->tail_ == last_tail_);

            uint8_t *receive_pointer = (uint8_t *) ((uint8_t *) recv_mr->addr + rdma_mg->delta_section_size - 1);
            //Clear the reply buffer for the polling.
            *receive_pointer = 0;
            //            memset((void*)recv_mr->addr, 0, rdma_mg->delta_section_size);
            //        *receive_pointer = {};

            int qp_id = rdma_mg->GetQPForDeltaPull();
            assert(owner_compute_node_id_ != rdma_mg->node_id);
            rdma_mg->post_send_xcompute(send_mr, owner_compute_node_id_, qp_id, sizeof(RDMA_Request));
            ibv_wc wc[2] = {};
            assert(send_pointer->command!= create_qp_);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
            volatile uint8_t *check_byte = (uint8_t *) receive_pointer;
            size_t poll_num = 0;
            while (!*check_byte) {
                poll_num++;
                _mm_clflush((const void *) check_byte);
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
            }
            assert(*check_byte == 5);
            
            // Reset danger_size after pull completes
            // Use release semantics to ensure all RDMA writes are visible before danger_size is reset
            std::atomic_thread_fence(std::memory_order_seq_cst);
#ifndef NDEBUG
            last_danger_size_.store(inner_section->danger_size.load(std::memory_order_relaxed), std::memory_order_relaxed);
            assert(last_danger_size_.load() >0);
            if (last_epoch_.load() == inner_section->epoch_.load(std::memory_order_relaxed) && last_head_.load()  < last_tail_.load()){
                assert(last_tail_.load() + inner_section->danger_size.load(std::memory_order_relaxed) == inner_section->tail_.load());
             }
#endif
            inner_section->danger_size.store(0, std::memory_order_release);
            _mm_clflush((const void *) &inner_section->danger_size);
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
#ifndef NDEBUG
            char* head_buff = inner_section->local_addr_ + inner_section->head_;
            // assert(*head_buff == '&' || *head_buff == '^');
            // assert(((DeltaSection*)recv_mr->addr)->tail_!=0);
#endif
            // End timing and print statistics
            uint64_t end_time = Timer::get_time_ns();
            uint64_t elapsed_us = (end_time - start_time) / 1000;

            // Update global statistics
            static std::atomic<uint64_t> total_pulls(0);
            static std::atomic<uint64_t> total_time_us(0);
            static std::atomic<uint64_t> min_time_us(UINT64_MAX);
            static std::atomic<uint64_t> max_time_us(0);

            uint64_t pulls = total_pulls.fetch_add(1) + 1;
            uint64_t total = total_time_us.fetch_add(elapsed_us) + elapsed_us;

            // Update min
            uint64_t current_min = min_time_us.load();
            while (elapsed_us < current_min && !min_time_us.compare_exchange_weak(current_min, elapsed_us)) {
                current_min = min_time_us.load();
            }

            // Update max
            uint64_t current_max = max_time_us.load();
            while (elapsed_us > current_max && !max_time_us.compare_exchange_weak(current_max, elapsed_us)) {
                current_max = max_time_us.load();
            }

            // // Print individual delta pull timing
            // printf("[Delta Pull] Pull #%lu took %lu us (poll_num=%lu), avg=%lu us, min=%lu us, max=%lu us\n",
            //        pulls, elapsed_us, poll_num, total / pulls,
            //        min_time_us.load(), max_time_us.load());
            // fflush(stdout);

            // Print summary after every 10 pulls or at specific intervals
            // if (pulls % 1000 == 0 || elapsed_us > 10000) {
            //     // Every 10 pulls or if time > 10ms
            //     uint64_t avg = total / pulls;
            //     printf("[Delta Pull Summary] Total pulls: %lu, Avg time: %lu us, Min: %lu us, Max: %lu us\n",
            //            pulls, avg, min_time_us.load(), max_time_us.load());
            //     fflush(stdout);
            // }

            // Export this pull's timing via a callback function pointer
            // This allows the benchmark to collect statistics without tight coupling
#ifdef MVCC_STORAGE_BENCH
            if (g_delta_pull_time_callback != nullptr) {
                g_delta_pull_time_callback(elapsed_us);
            }
#endif
        }

        // Helper function to split boundaries that exceed BIGPAGESIZE
        void SplitBoundariesByBigPage(std::vector<std::pair<size_t, size_t> > &boundaries) {
            std::vector<std::pair<size_t, size_t> > new_boundaries;

            for (auto &boundary: boundaries) {
                size_t start = boundary.first;
                size_t end = boundary.second;
                size_t size = end - start;

                if (size <= BIGPAGESIZE) {
                    // Boundary is within BigPage size, keep as is
                    new_boundaries.push_back(boundary);
                } else {
                    size_t current_start = start;
                    while (current_start < end) {
                        size_t current_end = std::min(current_start + BIGPAGESIZE, end);
                        new_boundaries.push_back(std::make_pair(current_start, current_end));
                        current_start = current_end;
                    }
                }
            }

            boundaries = std::move(new_boundaries);
        }

        uint64_t CalculateWriteBoundaries(std::vector<std::pair<size_t, size_t> > &boundaries, uint64_t old_h,
                                      uint64_t old_t, uint64_t old_epoch) {
            //todo: the logic need carefully proofread.
#ifdef SINGLE_DELTA_PER_NODE
            while (inner_section->tail_ != inner_section->tail_allocated) {
                // the assertion is not correct, if we do not use the CAS method to update the tail.
                //                assert(inner_section->tail_ <= inner_section->tail_allocated);
                // no ops
                _mm_pause();
            }
#endif
            assert(inner_section->head_!= inner_section->tail_ || inner_section->is_empty_);
            
            // Create consistent snapshot of head, tail, and epoch to ensure boundary calculation consistency
            uint64_t snapshot_head = inner_section->head_;
            uint64_t snapshot_tail = inner_section->tail_;
            uint64_t snapshot_epoch = inner_section->epoch_.load(std::memory_order_acquire);
            
            uint64_t merge_thre = 2048;
            uint64_t start = 0;
            uint64_t end = 0;
            if (UNLIKELY(old_epoch < snapshot_epoch)) {
                if (snapshot_tail > snapshot_head) {
                    end = STRUCT_OFFSET(DeltaSection, local_addr_);
                    boundaries.push_back(std::make_pair(start, end));

                    start = STRUCT_OFFSET(DeltaSection, local_addr_) + snapshot_head;
                    end = STRUCT_OFFSET(DeltaSection, local_addr_) + snapshot_tail;
                    boundaries.push_back(std::make_pair(start, end));

                    start = STRUCT_OFFSET(DeltaSection, local_addr_) + seg_real_size_;
                    end = start + 1;
                    assert(end == rdma_mg_->delta_section_size);
                    boundaries.push_back(std::make_pair(start, end));

                    // prune the result.
                    if (boundaries[2].first - boundaries[1].second <= merge_thre) {
                        // merge the third and the second one.
                        boundaries[1].second = boundaries[2].second;
                        boundaries.pop_back();
                    }
                    if (boundaries[1].first - boundaries[0].second <= merge_thre) {
                        // merge the second and the first one.
                        boundaries[0].second = boundaries[1].second;
                        boundaries.erase(boundaries.begin() + 1);
                    }
                } else {
                    // write the header and first half.
                    end = STRUCT_OFFSET(DeltaSection, local_addr_) ;
                    assert(end - start < rdma_mg_->delta_section_size);
                    boundaries.push_back(std::make_pair(start, end));
                    start = end;
                    end = STRUCT_OFFSET(DeltaSection, local_addr_) + snapshot_tail;
                    boundaries.push_back(std::make_pair(start, end));
                    if (old_epoch == snapshot_epoch - 1) {
                        // if the epoch is the previous epoch, we may reduce the data size that we need to transfer.
                        start = old_t > snapshot_head
                                    ? (STRUCT_OFFSET(DeltaSection, local_addr_) + old_t)
                                    : (STRUCT_OFFSET(DeltaSection, local_addr_) + snapshot_head);
                    } else {
                        start = STRUCT_OFFSET(DeltaSection, local_addr_) + snapshot_head;
                    }
                    end = rdma_mg_->delta_section_size;
                    boundaries.push_back(std::make_pair(start, end));
                    // Keep header separate from data - do not merge
                }
            } else {
                if (snapshot_tail > snapshot_head) {
                    end = STRUCT_OFFSET(DeltaSection, local_addr_);
                    boundaries.push_back(std::make_pair(start, end));
                    if (old_t > snapshot_head) {
                        start = STRUCT_OFFSET(DeltaSection, local_addr_) + old_t;
                    } else {
                        start = STRUCT_OFFSET(DeltaSection, local_addr_) + snapshot_head;
                    }
                    end = STRUCT_OFFSET(DeltaSection, local_addr_) + snapshot_tail;
                    boundaries.push_back(std::make_pair(start, end));

                    start = STRUCT_OFFSET(DeltaSection, local_addr_) + seg_real_size_;
                    end = start + 1;
                    boundaries.push_back(std::make_pair(start, end));
                    assert(end == rdma_mg_->delta_section_size);

                    // prune the result.
                    if (boundaries[2].first - boundaries[1].second <= merge_thre) {
                        // merge the third and the second one.
                        boundaries[1].second = boundaries[2].second;
                        boundaries.pop_back();
                    }
                    if (boundaries[1].first - boundaries[0].second <= merge_thre) {
                        // merge the second and the first one.
                        boundaries[0].second = boundaries[1].second;
                        boundaries.erase(boundaries.begin() + 1);
                    }
                } else {
                    assert(old_h <= snapshot_head);
                    // write the header and first half.
                    end = STRUCT_OFFSET(DeltaSection, local_addr_);
                    assert(end - start < rdma_mg_->delta_section_size);
                    boundaries.push_back(std::make_pair(start, end));

                    // we may reduce the data size that we need to transfer.
                    start = (STRUCT_OFFSET(DeltaSection, local_addr_) + old_t);
                    end = STRUCT_OFFSET(DeltaSection, local_addr_) + snapshot_tail;
                    boundaries.push_back(std::make_pair(start, end));

                    // The third part only contain the polling byte, because the old head has to smaller than the current head.
                    // There is no need to transfer the data for the second half of the delta section.
                    start = STRUCT_OFFSET(DeltaSection, local_addr_) + seg_real_size_;
                    end = start + 1;
                    assert(end == rdma_mg_->delta_section_size);
                    boundaries.push_back(std::make_pair(start, end));

                    // pruning the result.
                    // prune the result.
                    if (boundaries[2].first - boundaries[1].second <= merge_thre) {
                        // merge the third and the second one.
                        boundaries[1].second = boundaries[2].second;
                        boundaries.pop_back();
                    }
                    if (boundaries[1].first - boundaries[0].second <= merge_thre) {
                        // merge the second and the first one.
                        boundaries[0].second = boundaries[1].second;
                        boundaries.erase(boundaries.begin() + 1);
                    }
                }
            }


            // Split boundaries by BigPage size and return (common for all cases)
            SplitBoundariesByBigPage(boundaries);
            // if (old_epoch == snapshot_epoch) {
            //     assert(boundaries.size() <= 3);
            // }
            
            // Calculate danger_size: the size of new deltas written since old_tail
            // This represents the region that might be modified during the RDMA write
            uint64_t calculated_danger_size;
            if (snapshot_epoch > old_epoch) {
                // Epoch changed - tail has wrapped around, need to account for buffer wraparound
                calculated_danger_size = snapshot_tail + seg_real_size_ - old_t;
            } else {
                // Normal case - just the difference between current and old tail
                calculated_danger_size = snapshot_tail - old_t;
            }
            
            #ifndef NDEBUG
            if (old_epoch == snapshot_epoch && old_t > old_h) {
                assert(calculated_danger_size == snapshot_tail - old_t);
            }
            #endif
            
            return calculated_danger_size;
        }
    };
#endif
}

#endif //SELCC_DELTASECTION_H
