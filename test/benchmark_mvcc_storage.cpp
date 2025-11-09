//
// Multi-Version Storage Benchmark for SELCC
// Comparing three MVCC storage strategies:
// 1. Dedicated Delta Section (current MVOCC implementation)
// 2. PostgreSQL-like Version Chains (new tuples allocated for each version)
// 3. Delta in GCL (Global Coherence Layer - undo logs in regular pages)
//
// Key features:
// - Uses disaggregated B-tree index for tuple discovery (all nodes share the index)
// - Fuzzy local timestamps with opportunistic synchronization
// - Configurable workloads: uniform/Zipfian, pure read/write/mixed
//

// *** COMPILE-TIME CHECK: MVOCC macro must be defined ***
#ifndef MVOCC
#error "MVOCC macro must be defined to compile this benchmark! Use -DMVOCC flag."
#endif

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <map>
#include <algorithm>
#include <deque>
#include <memory>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "Common.h"
#include "DDSM.h"
#include "Timer.h"
#include "storage/Meta.h"
#include "storage/Record.h"
#include "storage/Records.h"
#include "storage/Table.h"
#include "storage/page.h"
#include "storage/rdma.h"
#include "txn/GlobalTimestamp.h"
#include "txn/DeltaSection.h"
#include "utils/mutexlock.h"
#include "utils/random.h"
#include "zipf.h"

using namespace DSMEngine;

// Define external symbols needed by Table.h
namespace DSMEngine {
    extern DDSM *default_gallocator;
    DDSM *default_gallocator = nullptr;
} // namespace DSMEngine

// Benchmark Configuration
const int kMaxThreads = 32;
uint64_t kNumTuples = 100000000; // Number of tuples to work with
uint64_t kSnapshotLag = 1000000; // Max snapshot lag (read snapshot in [current_ts - kSnapshotLag, current_ts])
int kBenchmarkDurationSec = 30;
int kWarmupDurationSec = 10; // Warmup duration (same as normal run)

// Benchmark Parameters (set via command line)
int kNumThreads = 8; // Total number of worker threads
int kReadRatio = 50; // Read ratio 0-100 (for mixed workload mode)
bool kMixedWorkload = true; // true=mixed read/write, false=separate writers/readers
int kNumWriters = 2; // Only used when kMixedWorkload=false
int kNumReaders = 2; // Only used when kMixedWorkload=false
int kStorageType = 1; // 1=DeltaSection, 2=VersionChain, 3=DeltaInGCL
int kWorkloadType = 0; // 0=uniform, 1=zipfian
double kZipfianTheta = 0.99; // Zipfian skew parameter (0.0 = uniform, 0.99 = highly skewed)
uint16_t ThisNodeID = 0;
uint16_t tcp_port = 19843;
uint64_t kCacheSize = 8; // GB

// Statistics
std::atomic<uint64_t> write_count{0};
std::atomic<uint64_t> read_count{0};
std::atomic<uint64_t> write_latency_sum{0};
std::atomic<uint64_t> read_latency_sum{0};
std::atomic<uint64_t> version_chain_traversals{0};
std::atomic<uint64_t> delta_applications{0};
std::atomic<uint64_t> delta_pull_count{0};
std::atomic<uint64_t> delta_pull_time_sum_us{0}; // Sum of all delta pull times in microseconds
std::atomic<uint64_t> delta_rollback_traversal_count{0}; // Number of delta rollback traversals
std::atomic<uint64_t> delta_rollback_time_sum_ns{0}; // Sum of all delta rollback traversal times in nanoseconds
std::atomic<bool> benchmark_running{true};
std::atomic<bool> benchmark_ready{false};
// warmup_phase variable removed - warmup now behaves exactly like normal run

// Delta Section Management (similar to TransactionManager)
RWSpinMutex g_delta_map_mtx;
std::map<GlobalAddress, DeltaSectionWrap *, std::greater<GlobalAddress> > g_delta_sections;
DeltaSectionWrap *g_ds_for_write = nullptr;
std::thread *g_gc_thread = nullptr;
std::atomic<uint64_t> *g_local_snapshot_ptr = nullptr; // Pointer to local_snapshot_ts_ for GC
std::atomic<bool> gc_thread_running{true}; // Separate flag for GC thread

// External cache statistics
extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];

// Callback function to collect delta pull timing
void RecordDeltaPullTime(uint64_t time_us) {
    delta_pull_time_sum_us.fetch_add(time_us);
}

// Forward declarations
class MVCCBenchmark;

// Delta Section Handler Functions (similar to TransactionManager)
void ProcessDeltaCreate(void *args) {
    auto *rdma_mg = RDMA_Manager::Get_Instance();
    auto *receive_msg_buf = (RDMA_Request *) args;
    GlobalAddress ds_gaddr = receive_msg_buf->content.create_ds.ds_gaddr;
    uint8_t compute_node_id = receive_msg_buf->content.create_ds.compute_node_id;

    ibv_mr *local_mr = new ibv_mr{};
    rdma_mg->Allocate_Local_RDMA_Slot(*local_mr, DeltaChunk);

    auto *ds = new DeltaSectionWrap(compute_node_id, ds_gaddr, rdma_mg->delta_section_size, local_mr);
    {
        std::unique_lock<RWSpinMutex> lck(g_delta_map_mtx);
        g_delta_sections.insert(std::make_pair(ds_gaddr, ds));
    }

    printf("Node %d: Created delta section for node %d at %p\n",
           rdma_mg->node_id, compute_node_id, ds_gaddr.val);

    delete receive_msg_buf;
}

void ProcessDeltaPull(void *args) {
    auto *rdma_mg = RDMA_Manager::Get_Instance();
    auto *receive_msg_buf = (RDMA_Request *) args;
    assert(receive_msg_buf->command == pull_delta_section);

    GlobalAddress ds_gaddr = receive_msg_buf->content.pull_ds.ds_gaddr;
    uint64_t old_head_ = receive_msg_buf->content.pull_ds.old_head;
    uint64_t old_tail_ = receive_msg_buf->content.pull_ds.old_tail;
    uint64_t old_max_ts = receive_msg_buf->content.pull_ds.old_max_ts;
    uint64_t old_epoch = receive_msg_buf->content.pull_ds.old_epoch;
    uint8_t requester_node_id = receive_msg_buf->content.pull_ds.requester_node_id;

    DeltaSectionWrap *ds_w = nullptr;
    uint64_t calculated_danger_size = 0;
    
    {
        std::shared_lock<RWSpinMutex> map_lck(g_delta_map_mtx);
        auto it = g_delta_sections.find(ds_gaddr);
        map_lck.unlock();

        if (it == g_delta_sections.end()) {
            delete receive_msg_buf;
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

        ibv_mr local_mr = *ds_w->seg_local_mr_;
        char *remote_addr = (char *) receive_msg_buf->buffer;

        int qp_id = rdma_mg->GetQPForDeltaPull();
        uint8_t *polling_byte = (uint8_t *) ((uint8_t *) local_mr.addr + rdma_mg->delta_section_size - 1);
        assert(ds_w->inner_section->tail_ != ds_w->inner_section->head_ || ds_w->inner_section->is_empty_);
        *polling_byte = 5;

        std::vector<std::pair<uint64_t, uint64_t> > boundaries;
        // Calculate boundaries and danger_size without modifying delta section (holding shared lock)
        calculated_danger_size = ds_w->CalculateWriteBoundaries(boundaries, old_head_, old_tail_, old_epoch);

        // // Print boundaries information
        // printf("[Delta Pull Handler] Node %d -> Node %d: old_head=%lu, old_tail=%lu, old_epoch=%lu, "
        //        "current_head=%lu, current_tail=%lu, current_epoch=%lu, danger_size=%lu, num_boundaries=%lu\n",
        //        rdma_mg->node_id, requester_node_id, old_head_, old_tail_, old_epoch,
        //        ds_w->GetHead(), ds_w->GetTail(), ds_w->GetEpoch(), 
        //        calculated_danger_size, boundaries.size());
        // for (size_t i = 0; i < boundaries.size(); i++) {
        //   printf("  Boundary %lu: start=%lu, end=%lu, size=%lu bytes\n",
        //          i, boundaries[i].first, boundaries[i].second,
        //          boundaries[i].second - boundaries[i].first);
        // }
        // fflush(stdout);
        
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
            }else{
              local_mr = *ds_w->seg_local_mr_;
              remote_addr = (char *) receive_msg_buf->buffer;
              uint64_t start = pair.first;
              uint64_t end = pair.second;
              size_t write_size = end - start;
              bool async = true;

              local_mr.addr = (void *) ((char *) local_mr.addr + start);
              remote_addr += start;
              std::atomic_thread_fence(std::memory_order_release);
              //todo: may be use RDMA atomic operation can help?
              rdma_mg->RDMA_Write_xcompute_localcopy(&local_mr, remote_addr, receive_msg_buf->rkey,
                write_size, requester_node_id, qp_id, true,  nullptr);

            }
            count++;
        }
        

    }

    delete receive_msg_buf;
}

// Simplified GC thread - runs independently of benchmark
void GarbageCollectionThread() {
    while (gc_thread_running.load()) {
        // Simplified GC: gc_threshold = current_local_sp - 4*kSnapshotLag
        if (g_local_snapshot_ptr != nullptr) {
            uint64_t local_sp = g_local_snapshot_ptr->load(std::memory_order_relaxed);
            uint64_t effective_snapshot_lag = std::max<uint64_t>(kSnapshotLag, 10000);
            uint64_t gc_threshold = (local_sp > 2 * effective_snapshot_lag) ? (local_sp - 2 * effective_snapshot_lag) : 0;

            if (g_ds_for_write != nullptr && gc_threshold > 0) {
                g_ds_for_write->GarbageCollectionBySnapshot(gc_threshold);
            }
        } else {
            printf("ERROR: g_local_snapshot_ptr is nullptr!\n");
            assert(false);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    printf("GC thread exiting...\n");
}

// Storage Strategy Enum
enum StorageStrategy {
    DELTA_SECTION = 1, // Current MVOCC with dedicated delta section
    VERSION_CHAIN = 2, // PostgreSQL-like version chains
    DELTA_IN_GCL = 3 // Delta in GCL (Global Coherence Layer) - regular SELCC pages
};

// VERSION_CHAIN: No separate metadata needed - everything stored in MetaColumn

// Delta Record for DELTA_IN_GCL strategy (stored in GCL pages)
struct GCLDelta {
    uint64_t tuple_key;
    uint64_t old_wts;
    uint64_t old_value; // Only 1 value field
    GlobalAddress next_delta; // Pointer to next delta (older)

    GCLDelta()
        : tuple_key(0), old_wts(0), old_value(0),
          next_delta(GlobalAddress::Null()) {
    }
} __attribute__((packed));

// MVCC Benchmark Class
class MVCCBenchmark {
public:
    DDSM *ddsm_;
    Cache *cache_;
    Table *table_;
    RecordSchema *schema_;
    StorageStrategy strategy_;

    // Fuzzy snapshot management
    std::atomic<uint64_t> local_snapshot_ts_;

    // Zipfian distribution generator - ONE PER THREAD for thread safety
    std::vector<std::unique_ptr<struct zipf_gen_state> > thread_zipf_states_;
    bool use_zipfian_;

    // Thread-local random number generators (one per thread)
    std::vector<std::unique_ptr<Random64> > thread_randoms_;

    // For VERSION_CHAIN strategy: we use the primary index to find the latest version
    // No additional data structures needed - all metadata stored in MetaColumn

    // For DELTA_IN_GCL strategy: delta storage with centralized page pool and GC
    RecordSchema *delta_schema_;  // Schema for delta records (tuple_key, old_wts, old_value, next_delta)

    struct DeltaPageInfo {
        GlobalAddress page_addr{GlobalAddress::Null()};
        std::atomic<uint32_t> live_records{0};
        std::atomic<bool> sealed{false};
    };

    struct DeltaHistoryEntry {
        uint64_t old_wts{0};
        GlobalAddress delta_addr{GlobalAddress::Null()};
        std::shared_ptr<DeltaPageInfo> page_info;
    };

    // Centralized delta page pool and metadata
    std::deque<GlobalAddress> free_delta_pages_;
    std::unique_ptr<SpinMutex> delta_page_pool_mutex_{new SpinMutex()};
    std::unordered_map<uint64_t, std::shared_ptr<DeltaPageInfo> > delta_page_infos_;
    std::unique_ptr<SpinMutex> delta_page_info_mutex_{new SpinMutex()};

    // Per-thread state for delta allocation and history tracking
    std::vector<std::deque<DeltaHistoryEntry> > thread_delta_histories_;
    std::vector<std::unique_ptr<SpinMutex> > thread_history_mutexes_;

    // Background garbage collector for delta pages
    std::atomic<bool> delta_gc_running_{false};
    std::unique_ptr<std::thread> delta_gc_thread_;

    // Per-thread stats to avoid atomics in hot paths
    std::vector<uint64_t> per_thread_delta_pulls_;
    std::vector<uint64_t> per_thread_delta_apps_;

    MVCCBenchmark(DDSM *ddsm, Cache *cache, StorageStrategy strategy)
        : ddsm_(ddsm), cache_(cache), strategy_(strategy), local_snapshot_ts_(0), use_zipfian_(false) {
        // Set default gallocator for Table operations
        default_gallocator = ddsm_;

        // Create schema for benchmark table
        // key (8B) + value (8B) + padding (496B) + meta = 512B data + meta
        schema_ = new RecordSchema(0);
        std::vector<ColumnInfo *> columns;
        columns.push_back(new ColumnInfo("key", ValueType::UINT64));
        columns.push_back(new ColumnInfo("value", ValueType::UINT64));
        columns.push_back(new ColumnInfo("padding", ValueType::FIXCHAR, 496)); // 496 bytes padding
        columns.push_back(new ColumnInfo("meta", ValueType::META)); // MetaColumn for MVCC
        schema_->BulkloadColumns(columns);
        size_t column_ids[1] = {0};
        schema_->SetPrimaryColumns(column_ids, 1);

        // Create table with primary index
        table_ = new Table();
        table_->Init(0, schema_, ddsm_);

        if (strategy_ == DELTA_SECTION) {
            delta_schema_ = nullptr;
            // Register delta section message handlers
            auto *rdma_mg = RDMA_Manager::Get_Instance();
            if (rdma_mg->message_handling_funcs_map.count(DeltaCreate) == 0) {
                rdma_mg->Set_message_handling_func(ProcessDeltaCreate, DeltaCreate);
                rdma_mg->Set_message_handling_func(ProcessDeltaPull, DeltaPull);
            }

            // Allocate remote delta section using DeltaChunk
            // For benchmark, allocate on a memory node (if available)
            uint8_t target_node_id = 2 * ((rdma_mg->node_id / 2) % rdma_mg->GetLogicalMemNodeNum()) + 1;
            if (rdma_mg->GetLogicalMemNodeNum() == 0) {
                assert(false);
            }
            GlobalAddress delta_seg_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Chunk_type::DeltaChunk, target_node_id);

            // Allocate local memory for writable delta section
            ibv_mr *local_mr = new ibv_mr{};
            rdma_mg->Allocate_Local_RDMA_Slot(*local_mr, DeltaChunk);

            // Create writable DeltaSectionWrap (one per node)
            std::unique_lock<RWSpinMutex> lck(g_delta_map_mtx);
            if (g_ds_for_write == nullptr) {
                g_ds_for_write = new DeltaSectionWrap(rdma_mg->node_id, delta_seg_addr,
                                                      rdma_mg->delta_section_size, local_mr);
                g_delta_sections.insert(std::make_pair(delta_seg_addr, g_ds_for_write));

                printf("Node %d: Initialized writable Delta Section: gaddr=%p, size=%lu MB\n",
                       rdma_mg->node_id, delta_seg_addr.val, rdma_mg->delta_section_size / (1024 * 1024));
            }
            lck.unlock();
        } else if (strategy_ == VERSION_CHAIN) {
            // No additional setup needed - using primary index and MetaColumn
            printf("Node %d: VERSION_CHAIN strategy - using primary index for version tracking\n",
                   RDMA_Manager::Get_Instance()->node_id);
            delta_schema_ = nullptr;
        } else if (strategy_ == DELTA_IN_GCL) {
            // Create delta schema matching GCLDelta structure
            // tuple_key (8B) + old_wts (8B) + old_value (8B) + next_delta (8B) = 32B
            delta_schema_ = new RecordSchema(1);  // Use table_id 1 for delta table
            std::vector<ColumnInfo *> delta_columns;
            delta_columns.push_back(new ColumnInfo("tuple_key", ValueType::UINT64));
            delta_columns.push_back(new ColumnInfo("old_wts", ValueType::UINT64));
            delta_columns.push_back(new ColumnInfo("old_value", ValueType::UINT64));
            delta_columns.push_back(new ColumnInfo("next_delta", ValueType::UINT64));  // GlobalAddress as UINT64
            delta_schema_->BulkloadColumns(delta_columns);
            // No primary key needed for delta table (but we need to set one for table initialization)
            size_t delta_column_ids[1] = {0};
            delta_schema_->SetPrimaryColumns(delta_column_ids, 1);
            
            printf("Node %d: Initialized delta schema for GCL storage (schema size=%lu bytes)\n",
                   RDMA_Manager::Get_Instance()->node_id, delta_schema_->GetRecordTotalSize());
        } else {
            delta_schema_ = nullptr;
        }

        // Initialize zipfian generators (one per thread for thread safety)
        use_zipfian_ = (kWorkloadType == 1);
        // Calculate max threads needed (for mixed or separate writer/reader modes)
        int max_threads = kMixedWorkload ? kNumThreads : std::max(kNumWriters, kNumReaders);
        thread_zipf_states_.resize(max_threads);
        thread_randoms_.resize(max_threads);
        per_thread_delta_pulls_.assign(max_threads, 0);
        per_thread_delta_apps_.assign(max_threads, 0);
        
        for (int i = 0; i < max_threads; i++) {
            uint64_t seed = i * 12345 + ThisNodeID * 67890;
            thread_randoms_[i] = std::make_unique<Random64>(seed);
            
            if (use_zipfian_) {
                // Each thread gets its own zipfian generator with unique seed
                uint64_t zipf_seed = i * 987654321UL + ThisNodeID * 123456789UL;
                thread_zipf_states_[i] = std::make_unique<struct zipf_gen_state>();
                mehcached_zipf_init(thread_zipf_states_[i].get(), kNumTuples, kZipfianTheta, zipf_seed);
            }
        }

        if (strategy_ == DELTA_IN_GCL) {
            InitializeDeltaInGCLState(static_cast<size_t>(max_threads));
        }
        
        if (use_zipfian_) {
            printf("Initialized %d thread-local Zipfian generators with theta=%.2f, n=%lu\n", 
                   max_threads, kZipfianTheta, kNumTuples);
        }

        const char *strategy_name =
                (strategy_ == DELTA_SECTION)
                    ? "Delta Section"
                    : (strategy_ == VERSION_CHAIN)
                          ? "Version Chain"
                          : "Delta In Page";
        printf("Initialized MVCC Benchmark with storage strategy: %s\n",
               strategy_name);
    }

    ~MVCCBenchmark() {
        // ShutdownDeltaInGCL();
        delete table_;
        delete schema_;
        if (delta_schema_ != nullptr) {
            delete delta_schema_;
        }
    }

    // Aggregate per-thread counters into global atomics for reporting
    void AggregateThreadLocalStats() {
        uint64_t pulls = 0, apps = 0;
        for (size_t i = 0; i < per_thread_delta_pulls_.size(); ++i) {
            pulls += per_thread_delta_pulls_[i];
            apps += per_thread_delta_apps_[i];
        }
        delta_pull_count.store(pulls);
        delta_applications.store(apps);
    }

    // Sync delta sections across all compute nodes
    void SyncDeltaSectionsMeta() {
        if (strategy_ != DELTA_SECTION) return;

        auto *rdma_mg = RDMA_Manager::Get_Instance();

        // Broadcast our delta section to other compute nodes
        if (g_ds_for_write != nullptr) {
            rdma_mg->Sync_Create_Delta_Section_RPC(g_ds_for_write->seg_addr_, rdma_mg->node_id);
            printf("Node %d: Broadcasted delta section metadata to other nodes\n", rdma_mg->node_id);
        }
    }

    // Initialize tuples
    void InitializeTuples() {
        printf("Node %d: Initializing %lu tuples...\n", ThisNodeID, kNumTuples);

        // All nodes participate in creating tuples (distributed B-tree index)
        CreateTuplesWithIndex();

        printf("Node %d: Tuple initialization complete\n", ThisNodeID);
    }

private:
    void CreateTuplesWithIndex() {
        // Distribute tuple creation across all compute nodes
        auto *rdma_mg = RDMA_Manager::Get_Instance();
        int compute_num = rdma_mg->GetComputeNodeNum();
        int my_compute_rank = ThisNodeID / 2; // Compute nodes are 0, 2, 4, ...

        uint64_t tuples_per_node = kNumTuples / compute_num;
        uint64_t start_key = tuples_per_node * my_compute_rank;
        uint64_t end_key = (my_compute_rank == compute_num - 1) ? kNumTuples : (start_key + tuples_per_node);

        printf("Node %d: Creating tuples %lu to %lu (total %lu)\n",
               ThisNodeID, start_key, end_key - 1, end_key - start_key);

        for (uint64_t key = start_key; key < end_key; key++) {
            char tuple_buffer[schema_->GetRecordTotalSize()];
            memset(tuple_buffer, 0, schema_->GetRecordTotalSize());

            // Set key and initial value
            *(uint64_t *) (tuple_buffer) = key;
            *(uint64_t *) (tuple_buffer + 8) = key * 2; // Initial value
            // Padding bytes (496) are already zeroed by memset

            GlobalAddress tuple_gaddr;
            Cache::Handle *handle;
            char *allocated_tuple;

            // Allocate tuple in the table
            bool ret = table_->AllocateNewTuple(allocated_tuple, tuple_gaddr, handle,
                                                ddsm_, nullptr);
            if (!ret) {
                printf("Failed to allocate tuple for key %lu\n", key);
                continue;
            }

            // Copy data
            memcpy(allocated_tuple, tuple_buffer, schema_->GetRecordTotalSize());

            // Initialize MVCC metadata
            Record *record = new Record(schema_, allocated_tuple);
            MetaColumn meta;
            meta.Wts_ = 0; // Initial timestamp
            meta.prev_version_ = GlobalAddress::Null();
            meta.prev_delta_epoch_ = 0;
            meta.prev_delta_data_size_ = 0;
            meta.is_visible_ = true; // Tuple is visible
            record->PutMeta(meta);
            delete record;

            // Insert into primary index (B-tree) - all nodes can now find this tuple
            RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
            DynamicCompoundKey primary_key((char *) &key, index_schema_ptr);
            table_->InsertPriIndex(primary_key, 1, tuple_gaddr);

            // Unlock page
            ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), handle);

            if ((key - start_key) % 1000000 == 0 && key > start_key) {
                printf("Node %d: Initialized %lu / %lu tuples\n", ThisNodeID, key - start_key, end_key - start_key);
            }
        }
    }

public:
    // Select key based on workload type (uniform or zipfian)
    uint64_t SelectKey(int thread_id) {
        // Safety check: ensure thread_id is within bounds
        assert(thread_id >= 0 && thread_id < (int)thread_randoms_.size() && "Invalid thread_id in SelectKey");
        
        if (use_zipfian_) {
            // Zipfian distribution using thread-local generator (thread-safe)
            assert(thread_zipf_states_[thread_id] != nullptr && "Zipfian generator not initialized for thread");
            uint64_t key = mehcached_zipf_next(thread_zipf_states_[thread_id].get());
            // Validate and clamp range: zipf generator should return [0, n-1] but ensure safety
            // (Due to floating point precision, rarely might return n or slightly above)
            if (key >= kNumTuples) {
                key = key % kNumTuples;
            }
            return key;
        } else {
            // Uniform distribution using thread-local random generator
            uint64_t key = thread_randoms_[thread_id]->Next() % kNumTuples;
            return key;
        }
    }

    // Write operation - creates new version
    void WriteOperation(int thread_id) {
        auto start = std::chrono::high_resolution_clock::now();

        try {
            // Select tuple key based on workload type
            uint64_t key = SelectKey(thread_id);

            // Get commit timestamp using fuzzy local timestamp (increment locally)
            uint64_t commit_ts = local_snapshot_ts_.fetch_add(1, std::memory_order_relaxed) + 1;

            if (strategy_ == VERSION_CHAIN) {
                WriteWithVersionChain(key, commit_ts, thread_id);
            } else if (strategy_ == DELTA_SECTION) {
                WriteWithDeltaSection(key, commit_ts, thread_id);
            } else if (strategy_ == DELTA_IN_GCL) {
                WriteWithDeltaInGCL(key, commit_ts, thread_id);
            }


            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

            write_count.fetch_add(1);
            write_latency_sum.fetch_add(duration.count());
        } catch (const std::exception &e) {
            printf("Write operation exception: %s\n", e.what());
        }
    }

    // Read operation - accesses old snapshot
    void ReadOperation(int thread_id) {
        auto start = std::chrono::high_resolution_clock::now();

        try {
            // Select tuple key based on workload type
            uint64_t key = SelectKey(thread_id);

            // Get snapshot timestamp randomly distributed in [current_ts - kSnapshotLag, current_ts]
            uint64_t current_ts = local_snapshot_ts_.load(std::memory_order_relaxed);
            uint64_t lag_range = (current_ts > kSnapshotLag) ? kSnapshotLag : current_ts;
            uint64_t random_lag = (lag_range > 0) ? (thread_randoms_[thread_id]->Next() % lag_range) : 0;
            uint64_t snapshot_ts = current_ts - random_lag;

            if (strategy_ == VERSION_CHAIN) {
                ReadWithVersionChain(key, snapshot_ts, thread_id);
            } else if (strategy_ == DELTA_SECTION) {
                ReadWithDeltaSection(key, snapshot_ts, thread_id);
            } else if (strategy_ == DELTA_IN_GCL) {
                ReadWithDeltaInGCL(key, snapshot_ts, thread_id);
            }

            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

            read_count.fetch_add(1);
            read_latency_sum.fetch_add(duration.count());
        } catch (const std::exception &e) {
            printf("Read operation exception: %s\n", e.what());
        }
    }

    void InitializeDeltaInGCLState(size_t max_threads) {
        assert(delta_schema_ != nullptr);
        thread_delta_histories_.resize(max_threads);
        thread_history_mutexes_.resize(max_threads);
        for (size_t i = 0; i < max_threads; ++i) {
            thread_history_mutexes_[i] = std::make_unique<SpinMutex>();
        }

        size_t pages_to_prealloc = std::max<size_t>(max_threads * 16, 128);
        PreallocateDeltaPages(pages_to_prealloc);
        delta_gc_running_.store(true, std::memory_order_release);
        delta_gc_thread_ = std::make_unique<std::thread>(&MVCCBenchmark::DeltaHistoryGCWorker, this);
    }

    void ShutdownDeltaInGCL() {
        if (strategy_ != DELTA_IN_GCL) {
            return;
        }
        delta_gc_running_.store(false, std::memory_order_release);
        if (delta_gc_thread_ && delta_gc_thread_->joinable()) {
            delta_gc_thread_->join();
            delta_gc_thread_.reset();
        }
        DrainDeltaHistories();
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    void PreallocateDeltaPages(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            GlobalAddress page_addr = AllocateAndInitializeDeltaPage();
            {
                std::lock_guard<SpinMutex> pool_lock(*delta_page_pool_mutex_);
                free_delta_pages_.push_back(page_addr);
            }
        }
    }

    GlobalAddress AllocateAndInitializeDeltaPage() {
        GlobalAddress page_addr = ddsm_->Allocate_Remote(Regular_Page);
        Cache::Handle *handle = nullptr;
        void *page_buffer = nullptr;
        ddsm_->SELCC_Exclusive_Lock(page_buffer, page_addr, handle);
        uint64_t cardinality = DataPage::calculate_cardinality(kLeafPageSize, delta_schema_->GetRecordTotalSize());
        new (page_buffer) DataPage(page_addr, cardinality, 1);
        ddsm_->SELCC_Exclusive_UnLock(page_addr, handle);

        auto page_info = std::make_shared<DeltaPageInfo>();
        page_info->page_addr = page_addr;
        page_info->live_records.store(0, std::memory_order_relaxed);
        page_info->sealed.store(false, std::memory_order_relaxed);

        {
            std::lock_guard<SpinMutex> map_lock(*delta_page_info_mutex_);
            delta_page_infos_[page_addr.val] = page_info;
        }
        return page_addr;
    }

    GlobalAddress AcquireDeltaPage() {
        GlobalAddress page_addr = GlobalAddress::Null();
        {
            std::lock_guard<SpinMutex> pool_lock(*delta_page_pool_mutex_);
            if (!free_delta_pages_.empty()) {
                page_addr = free_delta_pages_.front();
                free_delta_pages_.pop_front();
            }
        }
        if (page_addr == GlobalAddress::Null()) {
            page_addr = AllocateAndInitializeDeltaPage();
        }

        auto page_info = GetOrCreateDeltaPageInfo(page_addr);
        page_info->sealed.store(false, std::memory_order_release);
        page_info->live_records.store(0, std::memory_order_release);
        return page_addr;
    }

    std::shared_ptr<DeltaPageInfo> GetOrCreateDeltaPageInfo(GlobalAddress page_addr) {
        uint64_t key = page_addr.val;
        {
            std::lock_guard<SpinMutex> map_lock(*delta_page_info_mutex_);
            auto iter = delta_page_infos_.find(key);
            if (iter != delta_page_infos_.end()) {
                return iter->second;
            }
            auto page_info = std::make_shared<DeltaPageInfo>();
            page_info->page_addr = page_addr;
            delta_page_infos_[key] = page_info;
            return page_info;
        }
    }

    void SealDeltaPage(const std::shared_ptr<DeltaPageInfo> &page_info) {
        if (!page_info) {
            return;
        }
        page_info->sealed.store(true, std::memory_order_release);
    }

    void AppendDeltaHistory(int thread_id, uint64_t old_wts, GlobalAddress delta_addr,
                            const std::shared_ptr<DeltaPageInfo> &page_info) {
        if (!page_info) {
            return;
        }
        if (thread_id < 0 || static_cast<size_t>(thread_id) >= thread_delta_histories_.size()) {
            return;
        }
        DeltaHistoryEntry entry;
        entry.old_wts = old_wts;
        entry.delta_addr = delta_addr;
        entry.page_info = page_info;

        auto &mutex_ptr = thread_history_mutexes_[thread_id];
        if (mutex_ptr) {
            std::lock_guard<SpinMutex> lock(*mutex_ptr);
            thread_delta_histories_[thread_id].push_back(std::move(entry));
        }
    }

    void ReleaseDeltaHistoryEntry(DeltaHistoryEntry &entry) {
        if (!entry.page_info) {
            return;
        }
        uint32_t prev = entry.page_info->live_records.fetch_sub(1, std::memory_order_acq_rel);
        assert(prev > 0);
        if (prev == 1) {
            if (entry.page_info->sealed.load(std::memory_order_acquire)) {
                RecycleDeltaPage(entry.page_info);
            }
        }
    }

    void RecycleDeltaPage(const std::shared_ptr<DeltaPageInfo> &page_info) {
        if (!page_info) {
            return;
        }
        Cache::Handle *handle = nullptr;
        void *page_buffer = nullptr;
        ddsm_->SELCC_Exclusive_Lock(page_buffer, page_info->page_addr, handle);
        uint64_t cardinality = DataPage::calculate_cardinality(kLeafPageSize, delta_schema_->GetRecordTotalSize());
        new (page_buffer) DataPage(page_info->page_addr, cardinality, 1);
        ddsm_->SELCC_Exclusive_UnLock(page_info->page_addr, handle);

        page_info->sealed.store(false, std::memory_order_release);
        page_info->live_records.store(0, std::memory_order_release);

        {
            std::lock_guard<SpinMutex> pool_lock(*delta_page_pool_mutex_);
            free_delta_pages_.push_back(page_info->page_addr);
        }
    }

    void DeltaHistoryGCWorker() {
        while (delta_gc_running_.load(std::memory_order_acquire)) {
            uint64_t local_ts = local_snapshot_ts_.load(std::memory_order_relaxed);
            uint64_t lower_bound = (local_ts > 2 * kSnapshotLag) ? (local_ts - 2 * kSnapshotLag) : 0;

            for (size_t tid = 0; tid < thread_delta_histories_.size(); ++tid) {
                auto &mutex_ptr = thread_history_mutexes_[tid];
                if (!mutex_ptr) {
                    continue;
                }

                std::unique_lock<SpinMutex> history_lock(*mutex_ptr);
                auto &history = thread_delta_histories_[tid];
                while (!history.empty() && history.front().old_wts < lower_bound) {
                    DeltaHistoryEntry entry = std::move(history.front());
                    history.pop_front();
                    history_lock.unlock();
                    ReleaseDeltaHistoryEntry(entry);
                    history_lock.lock();
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    void DrainDeltaHistories() {
        if (strategy_ != DELTA_IN_GCL) {
            return;
        }
        for (size_t tid = 0; tid < thread_delta_histories_.size(); ++tid) {
            auto &mutex_ptr = thread_history_mutexes_[tid];
            if (!mutex_ptr) {
                continue;
            }
            std::unique_lock<SpinMutex> history_lock(*mutex_ptr);
            auto &history = thread_delta_histories_[tid];
            while (!history.empty()) {
                DeltaHistoryEntry entry = std::move(history.front());
                history.pop_front();
                history_lock.unlock();
                ReleaseDeltaHistoryEntry(entry);
                history_lock.lock();
            }
        }

    }

private:
    // ==================== VERSION CHAIN IMPLEMENTATION ====================
    // PostgreSQL-style version chains:
    // - Each write creates a new tuple in disaggregated memory
    // - Primary index always points to the latest version
    // - Old versions linked via prev_version_ in MetaColumn
    // - Reads start from latest version, follow chain backwards if needed
    // - CRITICAL: Every write must exclusively lock the old tuple and update
    //   next_version_ts_ before creating the new version to maintain chain integrity
    // - No garbage collection in this benchmark (simplified)

    void WriteWithVersionChain(uint64_t key, uint64_t commit_ts, int thread_id) {
        // Look up current tuple (latest version) via primary index
        RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
        DynamicCompoundKey primary_key((char *) &key, index_schema_ptr);
        GlobalAddress old_tuple_gaddr = table_->SearchPriIndex(primary_key);

        if (old_tuple_gaddr == GlobalAddress::Null()) {
            printf("WriteWithVersionChain: Key %lu not found in index\n", key);
            return;
        }

        // Step 1: Read old version into local buffer (with EXCLUSIVE lock for VERSION_CHAIN)
        // Use std::vector for portability (VLAs are not standard C++)
        std::vector<char> local_buffer(schema_->GetRecordTotalSize());

        Cache::Handle *old_handle;
        void *old_page_buffer;
        ddsm_->SELCC_Exclusive_Lock(old_page_buffer, TOPAGE(old_tuple_gaddr), old_handle);
        char *old_tuple_ptr = (char *) old_page_buffer +
                              (old_tuple_gaddr.offset - TOPAGE(old_tuple_gaddr).offset);

        // Copy old tuple to local buffer
        memcpy(local_buffer.data(), old_tuple_ptr, schema_->GetRecordTotalSize());

        // Update the next_version_ts_ in the old tuple before releasing the lock
        Record old_record(schema_, old_tuple_ptr);
        MetaColumn old_meta = old_record.GetMeta();
        old_meta.next_version_ts_ = commit_ts; // Set timestamp for the next version
        old_record.PutMeta(old_meta);

        ddsm_->SELCC_Exclusive_UnLock(TOPAGE(old_tuple_gaddr), old_handle);

        // Step 2: Modify value in local buffer (increment value)
        *(uint64_t *) (local_buffer.data() + 8) += 1;

        // Step 3: Allocate new tuple (now safe - no locks held)
        GlobalAddress new_tuple_gaddr;
        Cache::Handle *new_handle;
        char *new_tuple_buffer;

        bool ret = table_->AllocateNewTuple(new_tuple_buffer, new_tuple_gaddr,
                                            new_handle, ddsm_, nullptr);
        if (!ret) {
            printf("WriteWithVersionChain: Failed to allocate new tuple for key %lu\n", key);
            return;
        }


        // Step 5: Set metadata in new version
        Record new_record(schema_, new_tuple_buffer);
        MetaColumn meta = new_record.GetMeta();


        // Fuzzy snapshot: update local snapshot if we see a higher timestamp from SELCC layer
        // Use CAS to avoid over-incrementing when multiple threads see the same high timestamp
        uint64_t record_ts = meta.Wts_;
        if (record_ts > commit_ts) {
            commit_ts = record_ts + 1;
        }
        uint64_t expected = local_snapshot_ts_.load(std::memory_order_relaxed);
        while (expected < record_ts) {
            if (local_snapshot_ts_.compare_exchange_weak(expected, record_ts + 1,
                                                         std::memory_order_relaxed,
                                                         std::memory_order_relaxed)) {
                break; // Successfully updated
            }
            // expected was updated by compare_exchange_weak, retry if still needed
            expected = local_snapshot_ts_.load(std::memory_order_relaxed);
        }

        // Step 4: Copy updated data from local buffer to new tuple
        memcpy(new_tuple_buffer, local_buffer.data(), schema_->GetRecordTotalSize());
        meta.Wts_ = commit_ts;
        meta.prev_version_ = old_tuple_gaddr; // Link to previous version
        meta.prev_delta_epoch_ = 0;
        meta.prev_delta_data_size_ = 0;
        meta.next_version_ts_ = 0; // Initialize next version timestamp (no next version yet)
        meta.is_visible_ = true;
        new_record.PutMeta(meta);

        // Unlock new tuple page
        ddsm_->SELCC_Exclusive_UnLock(TOPAGE(new_tuple_gaddr), new_handle);

        // Update primary index to point to new version
        // Note: InsertPriIndex should overwrite the existing entry for the same key
        table_->InsertPriIndex(primary_key, 1, new_tuple_gaddr);
    }

    void ReadWithVersionChain(uint64_t key, uint64_t snapshot_ts, int thread_id) {
        // Look up latest version via primary index
        RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
        DynamicCompoundKey primary_key((char *) &key, index_schema_ptr);
        GlobalAddress current_gaddr = table_->SearchPriIndex(primary_key);

        if (current_gaddr == GlobalAddress::Null()) {
            printf("ReadWithVersionChain: Key %lu not found in index\n", key);
            return;
        }

        int traversal_count = 0;

        // Traverse version chain backwards to find visible version
        while (current_gaddr != GlobalAddress::Null()) {
            Cache::Handle *handle;
            void *page_buffer;
            ddsm_->SELCC_Shared_Lock(page_buffer, TOPAGE(current_gaddr), handle);

            char *tuple_ptr = (char *) page_buffer +
                              (current_gaddr.offset - TOPAGE(current_gaddr).offset);
            Record record(schema_, tuple_ptr);

            MetaColumn meta = record.GetMeta();
            uint64_t version_ts = meta.Wts_;
            GlobalAddress prev_version = meta.prev_version_;
            // Fuzzy snapshot: update local snapshot if we see a higher timestamp from SELCC layer
            // Use CAS to avoid over-incrementing when multiple threads see the same high timestamp
            uint64_t expected = local_snapshot_ts_.load(std::memory_order_relaxed);
            while (expected < version_ts) {
                if (local_snapshot_ts_.compare_exchange_weak(expected, version_ts,
                                                             std::memory_order_relaxed,
                                                             std::memory_order_relaxed)) {
                    break; // Successfully updated
                }
                // expected was updated by compare_exchange_weak, retry if still needed
                expected = local_snapshot_ts_.load(std::memory_order_relaxed);
            }
            if (version_ts <= snapshot_ts) {
                // Found visible version - read the value
                uint64_t value = *(uint64_t *) (tuple_ptr + 8);
                (void) value; // Suppress unused warning

                ddsm_->SELCC_Shared_UnLock(TOPAGE(current_gaddr), handle);

                version_chain_traversals.fetch_add(traversal_count);
                return;
            }

            ddsm_->SELCC_Shared_UnLock(TOPAGE(current_gaddr), handle);

            // Move to previous version
            current_gaddr = prev_version;
            traversal_count++;
        }

        // This should never happen: we should always find at least the initial version (Wts=0)
        // which is visible to any snapshot_ts >= 0
        printf(
            "ERROR: ReadWithVersionChain key=%lu snapshot_ts=%lu - reached end of chain without finding visible version!\n",
            key, snapshot_ts);
        assert(false && "Version chain traversal failed - no visible version found");
    }

    // ==================== DELTA SECTION IMPLEMENTATION ====================

    void WriteWithDeltaSection(uint64_t key, uint64_t commit_ts, int thread_id) {
        // Look up tuple address via primary index
        RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
        DynamicCompoundKey primary_key((char *) &key, index_schema_ptr);
        GlobalAddress tuple_gaddr = table_->SearchPriIndex(primary_key);

        Cache::Handle *handle;
        void *page_buffer;
        ddsm_->SELCC_Exclusive_Lock(page_buffer, TOPAGE(tuple_gaddr), handle);

        char *tuple_ptr =
                (char *) page_buffer + (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
        // todo: we may develop a new api for deltasectio to directly prepare the delta by values rather than tuples.
        // Create record objects using stack allocation (old values)
        Record global_record(schema_, tuple_ptr);

        // Fuzzy timestamp sync: if tuple has higher timestamp, sync up local timestamp
        uint64_t tuple_ts = global_record.GetWTS();
        if (tuple_ts >= commit_ts) {
            uint64_t expected = local_snapshot_ts_.load(std::memory_order_relaxed);
            while (expected < tuple_ts) {
                if (local_snapshot_ts_.compare_exchange_weak(expected, tuple_ts + 1,
                                                             std::memory_order_relaxed,
                                                             std::memory_order_relaxed)) {
                    break; // Successfully updated
                }
                expected = local_snapshot_ts_.load(std::memory_order_relaxed);
                // expected was updated by compare_exchange_weak, retry if still needed
            }
            commit_ts = tuple_ts + 1;
        }

        // Create new record with updated value for delta creation (stack allocation)
        Record new_record(schema_);
        new_record.CopyFrom(&global_record);

        // Update value field in new_record (column index 1)
        uint64_t old_value = *(uint64_t *) (new_record.data_ptr_ + 8);
        *(uint64_t *) (new_record.data_ptr_ + 8) = old_value + 1;
        new_record.dirty_col_ids.insert(1); // Mark value column as dirty

        // Create delta record (compares new_record vs global_record, saves old values)
        GlobalAddress delta_gaddr = GlobalAddress::Null();
        size_t delta_size = 0;

        g_ds_for_write->fill_in_delta_record_single(&new_record, &global_record,
                                                    delta_gaddr, delta_size, commit_ts);

        // Update metadata with delta pointer
        MetaColumn meta = new_record.GetMeta();
        meta.prev_version_ = delta_gaddr;
        meta.prev_delta_epoch_ = g_ds_for_write->GetEpoch();
        meta.prev_delta_data_size_ = delta_size;
        meta.Wts_ = commit_ts;
        assert(meta.prev_version_.nodeID < 100);
        new_record.PutMeta(meta);

        // Copy new values to global_record (updates page buffer)
        global_record.CopyFrom(&new_record);
        ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), handle);

        // Note: local_snapshot_ts_ is already incremented in WriteOperation
    }

    void ReadWithDeltaSection(uint64_t key, uint64_t snapshot_ts, int thread_id) {
        // Look up tuple address via primary index
        RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
        DynamicCompoundKey primary_key((char *) &key, index_schema_ptr);
        GlobalAddress tuple_gaddr = table_->SearchPriIndex(primary_key);

        Cache::Handle *handle;
        void *page_buffer;
        ddsm_->SELCC_Shared_Lock(page_buffer, TOPAGE(tuple_gaddr), handle);

        char *tuple_ptr =
                (char *) page_buffer + (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);

        // Use stack allocation for records
        Record global_record(schema_, tuple_ptr);
        Record record(schema_);
        record.CopyFrom(&global_record);

        uint64_t current_ts = record.GetWTS();

        // Release SELCC lock early - we've copied the data and delta rollback
        // only accesses the dedicated delta section, not this page
        ddsm_->SELCC_Shared_UnLock(TOPAGE(tuple_gaddr), handle);

        // Fuzzy snapshot: update local snapshot if we see a higher timestamp from SELCC layer
        // Use CAS to avoid over-incrementing when multiple threads see the same high timestamp
        uint64_t expected = local_snapshot_ts_.load(std::memory_order_relaxed);
        while (expected < current_ts) {
            if (local_snapshot_ts_.compare_exchange_weak(expected, current_ts,
                                                         std::memory_order_relaxed,
                                                         std::memory_order_relaxed)) {
                break; // Successfully updated
            }
            // expected was updated by compare_exchange_weak, retry if still needed
            expected = local_snapshot_ts_.load(std::memory_order_relaxed);
        }

        // Apply deltas if current version is newer than snapshot
        auto rollback_start = std::chrono::high_resolution_clock::now();
        int delta_count = 0;
        int pull_wait_count = 0;
        while (current_ts > snapshot_ts) {
            MetaColumn meta = record.GetMeta();
            GlobalAddress prev_delta = meta.prev_version_;
            assert(meta.prev_version_.nodeID <100);
            if (prev_delta == GlobalAddress::Null()) {
                // No older version available
                printf("ERROR: No older version available for tuple %lu!\n", key);
                fflush(stdout);
                break;
            }

            // Find the delta section that contains this delta
            DeltaSectionWrap *delta_section = nullptr;
            {
                // std::shared_lock<RWSpinMutex> lck(g_delta_map_mtx);
                auto iter = g_delta_sections.lower_bound(prev_delta);
                if (iter == g_delta_sections.end()) {
                    break; // Delta section not found
                }
                delta_section = iter->second;
                assert(iter->first.nodeID == prev_delta.nodeID);
            }
            // Use double-checked locking to avoid conflict
            // Check if delta section is stale and pull updates if needed
            // Calculate offset from data_ in the delta section
            long offset = prev_delta.offset - delta_section->seg_addr_.offset - STRUCT_OFFSET(
                              DeltaSection, local_addr_);
            
            // Sequential consistency fence ensures we see the latest state before checking validity/danger
            // This prevents reordering between multiple checks that need to be consistent
            // std::atomic_thread_fence(std::memory_order_seq_cst);
            
            // bool is_ok = delta_section->isvalidandnotdangerours(offset, meta.prev_delta_epoch_);
            // {
            //     // std::shared_lock<RWSpinMutex> slck(delta_section->shadow_mtx_);
            //     if (delta_section->inner_section->is_empty_ || !is_ok) {
            //         // slck.unlock();
            //         std::unique_lock<RWSpinMutex> lck(delta_section->shadow_mtx_);
            //         pull_wait_count++;
            //         if(pull_wait_count > 8){
            //           printf("Pull wait count > 8 impossible\n");
            //           assert(false);
            //           fflush(stdout);
            //           exit(1);
            //         };
            //         if (delta_section->inner_section->is_empty_ ||
            //             !delta_section->isOffsetValid(offset, meta.prev_delta_epoch_)) {
            //             // Pull updates from remote node
            //             if (delta_section->owner_compute_node_id_ != RDMA_Manager::Get_Instance()->node_id) {
            //                 delta_section->PullUpdates();

            //                 // Update global statistics
            //                 per_thread_delta_pulls_[thread_id]++;
            //             }
            //         }
            //     }
            // }
            {   
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

                            // Update global statistics
                            per_thread_delta_pulls_[thread_id]++;
                        }
                    }
                }else{
                  while (delta_section->isOffsetDangerous(offset, meta.prev_delta_epoch_)) {
                    _mm_pause();
                  }  
                }
            }

            // Access delta from delta section
            DeltaRecord *delta_record = (DeltaRecord *) ((char *) delta_section->seg_local_mr_->addr +
                                                         (prev_delta.offset - delta_section->seg_addr_.offset));
            
            // Apply delta to roll back to previous version
            record.roll_back(delta_record);

            current_ts = record.GetWTS();
            delta_count++;
            // per_thread_delta_apps_[thread_id]++;
        }

        // Record delta rollback traversal time statistics
        if (delta_count > 0) {
            auto rollback_end = std::chrono::high_resolution_clock::now();
            auto rollback_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(rollback_end - rollback_start);
            delta_rollback_traversal_count.fetch_add(1);
            delta_rollback_time_sum_ns.fetch_add(rollback_duration.count());
        }

        if (current_ts <= snapshot_ts) {
            // Found visible version - read the value
            uint64_t value = *(uint64_t *) (record.data_ptr_ + 8);
            (void) value; // Suppress unused warning
        }
        // Note: SELCC lock already released early (after copying record)
    }

    // ==================== DELTA IN GCL IMPLEMENTATION ====================
    // Delta in GCL (Global Coherence Layer):
    // - Stores delta records in regular SELCC pages (not dedicated delta section)
    // - Each write creates a delta in GCL pages and updates tuple in place
    // - Delta pointer stored in MetaColumn (prev_version_)
    // - Causes SELCC invalidations when delta pages are modified (key difference from DELTA_SECTION)
    // - Reads must traverse delta chain in GCL pages to reconstruct old versions

    // Helper function to allocate delta record from per-thread delta page
    bool AllocateDeltaRecord(int thread_id, GlobalAddress &delta_gaddr, Cache::Handle *&delta_handle,
                             char *&delta_record_buffer, std::shared_ptr<DeltaPageInfo> &page_info_out) {
        (void) thread_id;
        GlobalAddress current_page = AcquireDeltaPage();
        page_info_out = GetOrCreateDeltaPageInfo(current_page);
        void *delta_page_buffer = nullptr;
        DataPage *delta_page = nullptr;
        ddsm_->SELCC_Exclusive_Lock(delta_page_buffer, current_page, delta_handle);
        delta_page = reinterpret_cast<DataPage *>(delta_page_buffer);

        int cnt = 0;
        bool alloc_success = delta_page->AllocateRecord(cnt, delta_schema_, delta_gaddr, delta_record_buffer);

        if (!alloc_success) {
            // Page is full before allocation – seal it and retry with a new page
            ddsm_->SELCC_Exclusive_UnLock(current_page, delta_handle);
            if (page_info_out) {
                SealDeltaPage(page_info_out);
                RecycleDeltaPage(page_info_out);
            }
            page_info_out.reset();
            return false;
        }

        page_info_out->live_records.store(1, std::memory_order_release);

        return true;
    }

    void WriteWithDeltaInGCL(uint64_t key, uint64_t commit_ts, int thread_id) {
        // Look up tuple address via primary index
        RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
        DynamicCompoundKey primary_key((char *) &key, index_schema_ptr);
        GlobalAddress tuple_gaddr = table_->SearchPriIndex(primary_key);

        // Step 1: Lock tuple page and read current values
        Cache::Handle *tuple_handle;
        void *tuple_page_buffer;
        ddsm_->SELCC_Exclusive_Lock(tuple_page_buffer, TOPAGE(tuple_gaddr), tuple_handle);

        char *tuple_ptr = (char *) tuple_page_buffer +
                          (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
        Record tuple_record(schema_, tuple_ptr);

        // Read old metadata and values
        MetaColumn old_meta = tuple_record.GetMeta();
        uint64_t old_wts = old_meta.Wts_;
        uint64_t old_value = *(uint64_t *) (tuple_ptr + 8);
        GlobalAddress old_prev_delta = old_meta.prev_version_;

        // Fuzzy timestamp sync: if tuple has higher timestamp, sync up local timestamp
        if (old_wts >= commit_ts) {
            uint64_t expected = local_snapshot_ts_.load(std::memory_order_relaxed);
            while (expected < old_wts) {
                if (local_snapshot_ts_.compare_exchange_weak(expected, old_wts + 1,
                                                             std::memory_order_relaxed,
                                                             std::memory_order_relaxed)) {
                    break; // Successfully updated
                }
                expected = local_snapshot_ts_.load(std::memory_order_relaxed);
            }
            commit_ts = old_wts + 1;
        }

        // Release tuple lock to avoid deadlock (will re-acquire)
        ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), tuple_handle);

        // Step 2: Allocate and write delta record using centralized delta page pool
        GlobalAddress delta_gaddr;
        Cache::Handle *delta_handle;
        char *delta_record_buffer;

        std::shared_ptr<DeltaPageInfo> page_info;
        bool alloc_success = AllocateDeltaRecord(thread_id, delta_gaddr, delta_handle, delta_record_buffer, page_info);
        if (!alloc_success) {
            printf("ERROR: Failed to allocate delta record for key %lu\n", key);
            return;
        }

        // Fill in delta record using Record API (while holding page lock)
        Record delta_record(delta_schema_, delta_record_buffer);
        delta_record.SetColumn(0, &key);  // tuple_key
        delta_record.SetColumn(1, &old_wts);  // old_wts
        delta_record.SetColumn(2, &old_value);  // old_value
        uint64_t next_delta_val = old_prev_delta.val;
        delta_record.SetColumn(3, &next_delta_val);  // next_delta (GlobalAddress as UINT64)

        // Unlock delta page - THIS CAUSES SELCC INVALIDATIONS!
        ddsm_->SELCC_Exclusive_UnLock(TOPAGE(delta_gaddr), delta_handle);

        SealDeltaPage(page_info);

        // Track delta record for garbage collection
        AppendDeltaHistory(thread_id, old_wts, delta_gaddr, page_info);

        // Step 3: Re-acquire tuple lock and update tuple with new values
        ddsm_->SELCC_Exclusive_Lock(tuple_page_buffer, TOPAGE(tuple_gaddr), tuple_handle);
        tuple_ptr = (char *) tuple_page_buffer + (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);

        // Update tuple value (increment)
        *(uint64_t *) (tuple_ptr + 8) = old_value + 1;

        // Update metadata with new delta pointer
        Record updated_record(schema_, tuple_ptr);
        MetaColumn new_meta = updated_record.GetMeta();
        new_meta.Wts_ = commit_ts;
        new_meta.prev_version_ = delta_gaddr; // Point to the delta we just created
        new_meta.prev_delta_epoch_ = 0;
        new_meta.prev_delta_data_size_ = delta_schema_->GetRecordTotalSize();
        new_meta.is_visible_ = true;
        updated_record.PutMeta(new_meta);

        // Unlock tuple page - causes invalidations!
        ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), tuple_handle);
    }

    void ReadWithDeltaInGCL(uint64_t key, uint64_t snapshot_ts, int thread_id) {
        // Look up tuple address via primary index
        RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
        DynamicCompoundKey primary_key((char *) &key, index_schema_ptr);
        GlobalAddress tuple_gaddr = table_->SearchPriIndex(primary_key);

        // Step 1: Read current tuple value and metadata
        Cache::Handle *tuple_handle;
        void *tuple_page_buffer;
        ddsm_->SELCC_Shared_Lock(tuple_page_buffer, TOPAGE(tuple_gaddr), tuple_handle);

        char *tuple_ptr = (char *) tuple_page_buffer +
                          (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);

        // Copy tuple to local buffer for delta application
        std::vector<char> local_buffer(schema_->GetRecordTotalSize());
        memcpy(local_buffer.data(), tuple_ptr, schema_->GetRecordTotalSize());

        // Release tuple lock early - we've copied the data
        ddsm_->SELCC_Shared_UnLock(TOPAGE(tuple_gaddr), tuple_handle);

        // Work with local copy
        Record local_record(schema_, local_buffer.data());
        MetaColumn meta = local_record.GetMeta();
        uint64_t current_ts = meta.Wts_;
        GlobalAddress current_delta_gaddr = meta.prev_version_;

        // Fuzzy snapshot: update local snapshot if we see a higher timestamp
        // Use CAS to avoid over-incrementing when multiple threads see the same high timestamp
        {
            uint64_t expected = local_snapshot_ts_.load(std::memory_order_relaxed);
            while (expected < current_ts) {
                if (local_snapshot_ts_.compare_exchange_weak(expected, current_ts,
                                                             std::memory_order_relaxed,
                                                             std::memory_order_relaxed)) {
                    break; // Successfully updated
                }
                expected = local_snapshot_ts_.load(std::memory_order_relaxed);
            }
        }

        // Step 2: Traverse delta chain if needed
        int delta_count = 0;
        while (current_ts > snapshot_ts && current_delta_gaddr != GlobalAddress::Null()) {
            // Lock delta page - THIS CAUSES SELCC INVALIDATIONS!
            Cache::Handle *delta_handle;
            void *delta_page_buffer;
            ddsm_->SELCC_Shared_Lock(delta_page_buffer, TOPAGE(current_delta_gaddr), delta_handle);

            // Read delta record from GCL page using Record API
            char *delta_ptr = (char *) delta_page_buffer +
                              (current_delta_gaddr.offset - TOPAGE(current_delta_gaddr).offset);
            Record delta_record(delta_schema_, delta_ptr);

            // Read delta fields using Record API
            uint64_t delta_tuple_key = *(uint64_t *) (delta_record.data_ptr_ + delta_schema_->GetColumnOffset(0));
            uint64_t delta_old_wts = *(uint64_t *) (delta_record.data_ptr_ + delta_schema_->GetColumnOffset(1));
            uint64_t delta_old_value = *(uint64_t *) (delta_record.data_ptr_ + delta_schema_->GetColumnOffset(2));
            uint64_t next_delta_val = *(uint64_t *) (delta_record.data_ptr_ + delta_schema_->GetColumnOffset(3));
            GlobalAddress next_delta;
            next_delta.val = next_delta_val;

            // Verify this is the correct delta
            if (delta_tuple_key != key) {
                printf("ERROR: Delta mismatch! Expected key %lu, got %lu\n", key, delta_tuple_key);
                ddsm_->SELCC_Shared_UnLock(TOPAGE(current_delta_gaddr), delta_handle);
                break;
            }

            // Apply delta (roll back to previous version)
            *(uint64_t *) (local_buffer.data() + 8) = delta_old_value;
            current_ts = delta_old_wts;

            // Unlock delta page
            ddsm_->SELCC_Shared_UnLock(TOPAGE(current_delta_gaddr), delta_handle);

            // Move to next delta in chain
            current_delta_gaddr = next_delta;
            delta_count++;
            per_thread_delta_apps_[thread_id]++;
        }

        // Step 3: Read the value from the reconstructed version
        if (current_ts <= snapshot_ts) {
            uint64_t value = *(uint64_t *) (local_buffer.data() + 8);
            (void) value; // Suppress unused warning
        } else {
            printf("WARNING: ReadWithDeltaInGCL key=%lu snapshot_ts=%lu - no visible version found after %d deltas\n",
                   key, snapshot_ts, delta_count);
        }
    }

};

// ==================== WORKER THREADS ====================

// Mixed workload thread (decides read/write based on read_ratio)
void MixedWorkloadThread(MVCCBenchmark *benchmark, int thread_id) {
    bindCore(thread_id);
    Random64 rand(thread_id * 54321 + ThisNodeID * 98765);

    // Wait for benchmark to be ready
    while (!benchmark_ready.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // printf("Mixed workload thread %d started (read_ratio=%d%%)\n", thread_id, kReadRatio);

    while (benchmark_running.load()) {
        // Both warmup and normal operation use the same logic
        int random_val = rand.Next() % 100;
        if (random_val < kReadRatio) {
            benchmark->ReadOperation(thread_id);
        } else {
            benchmark->WriteOperation(thread_id);
        }
    }

    printf("Mixed workload thread %d finished\n", thread_id);
}

void WriterThread(MVCCBenchmark *benchmark, int thread_id) {
    bindCore(thread_id);

    // Wait for benchmark to be ready
    while (!benchmark_ready.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    printf("Writer thread %d started\n", thread_id);

    while (benchmark_running.load()) {
        // Both warmup and normal operation: only writes
        benchmark->WriteOperation(thread_id);
    }

    printf("Writer thread %d finished\n", thread_id);
}

void ReaderThread(MVCCBenchmark *benchmark, int thread_id) {
    bindCore(kNumWriters + thread_id);

    // Wait for benchmark to be ready
    while (!benchmark_ready.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    printf("Reader thread %d started\n", thread_id);

    while (benchmark_running.load()) {
        // Readers always do reads (both warmup and normal)
        benchmark->ReadOperation(thread_id);
    }

    printf("Reader thread %d finished\n", thread_id);
}

// ==================== MAIN ====================

void ParseArgs(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            kNumThreads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--read_ratio") == 0 && i + 1 < argc) {
            kReadRatio = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--mixed_workload") == 0 && i + 1 < argc) {
            kMixedWorkload = atoi(argv[++i]) != 0;
        } else if (strcmp(argv[i], "--writers") == 0 && i + 1 < argc) {
            kNumWriters = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--readers") == 0 && i + 1 < argc) {
            kNumReaders = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--storage_type") == 0 && i + 1 < argc) {
            kStorageType = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--workload_type") == 0 && i + 1 < argc) {
            kWorkloadType = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--zipfian_theta") == 0 && i + 1 < argc) {
            kZipfianTheta = atof(argv[++i]);
        } else if (strcmp(argv[i], "--snapshot_lag") == 0 && i + 1 < argc) {
            kSnapshotLag = atol(argv[++i]);
        } else if (strcmp(argv[i], "--warmup_duration") == 0 && i + 1 < argc) {
            kWarmupDurationSec = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--node_id") == 0 && i + 1 < argc) {
            ThisNodeID = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--tcp_port") == 0 && i + 1 < argc) {
            tcp_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--num_tuples") == 0 && i + 1 < argc) {
            kNumTuples = atol(argv[++i]);
        } else if (strcmp(argv[i], "--duration") == 0 && i + 1 < argc) {
            kBenchmarkDurationSec = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--cache_size") == 0 && i + 1 < argc) {
            kCacheSize = atol(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            fprintf(stderr,
                    "MVCC Storage Benchmark\n\n"
                    "Usage: %s [options]\n\n"
                    "Workload Options:\n"
                    "  --mixed_workload 0|1       Use mixed workload (default: 1)\n"
                    "  --threads N                Number of threads for mixed workload (default: 8)\n"
                    "  --read_ratio 0-100         Read percentage for mixed workload (default: 50)\n"
                    "  --writers N                Number of writers for separate mode (default: 2)\n"
                    "  --readers N                Number of readers for separate mode (default: 2)\n\n"
                    "Access Pattern:\n"
                    "  --workload_type 0|1        0=Uniform, 1=Zipfian (default: 0)\n"
                    "  --zipfian_theta 0.0-0.99   Zipfian skew parameter (default: 0.99)\n\n"
                    "Storage Strategy:\n"
                    "  --storage_type 1|2|3       1=Delta Section, 2=Version Chain, 3=Delta In GCL (default: 1)\n\n"
                    "Benchmark Configuration:\n"
                    "  --num_tuples N             Number of tuples (default: 100000)\n"
                    "  --snapshot_lag N           Max snapshot lag (default: 10000)\n"
                    "  --warmup_duration N        Warmup duration in seconds (same as normal run, default: 10)\n"
                    "  --duration N               Benchmark duration in seconds (default: 30)\n"
                    "  --cache_size N             Cache size in GB (default: 2)\n\n"
                    "System Configuration:\n"
                    "  --node_id N                Compute node ID (default: 0)\n"
                    "  --tcp_port N               TCP port (default: 19843)\n\n",
                    argv[0]);
            fflush(stderr);
            exit(0);
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            fprintf(stderr, "Use --help for usage information\n");
            fflush(stderr);
            exit(1);
        }
    }

    printf("Configuration:\n");
    if (kMixedWorkload) {
        printf("  Workload Mode: Mixed (read_ratio=%d%%)\n", kReadRatio);
        printf("  Threads: %d\n", kNumThreads);
    } else {
        printf("  Workload Mode: Separate Writers/Readers\n");
        printf("  Writers: %d\n", kNumWriters);
        printf("  Readers: %d\n", kNumReaders);
    }
    printf("  Storage Type: %d", kStorageType);
    const char *storage_name = (kStorageType == DELTA_SECTION)
                                   ? " (Delta Section)"
                                   : (kStorageType == VERSION_CHAIN)
                                         ? " (Version Chain)"
                                         : " (Delta In GCL)";
    printf("%s\n", storage_name);

    printf("  Access Pattern: %s", kWorkloadType == 0 ? "Uniform" : "Zipfian");
    if (kWorkloadType == 1) {
        printf(" (theta=%.2f)", kZipfianTheta);
    }
    printf("\n");
    printf("  Snapshot Lag: %lu (randomized)\n", kSnapshotLag);
    printf("  Warmup Duration: %d seconds\n", kWarmupDurationSec);
    printf("  Node ID: %d\n", ThisNodeID);
    printf("  Num Tuples: %lu\n", kNumTuples);
    printf("  Benchmark Duration: %d seconds\n", kBenchmarkDurationSec);
    printf("  Cache Size: %lu GB\n", kCacheSize);
}

int main(int argc, char *argv[]) {
    ParseArgs(argc, argv);

    // Initialize RDMA and storage
    struct config_t config = {
        NULL, /* dev_name */
        NULL, /* server_name */
        tcp_port, /* tcp_port */
        1, /* ib_port */
        1, /* gid_idx */
        4 * 10 * 1024 *
        1024, /* initial local buffer size */
        ThisNodeID
    };

    RDMA_Manager *rdma_mg = RDMA_Manager::Get_Instance(&config);
    Cache *cache_ptr = NewLRUCache(kCacheSize * 1024ULL * 1024 * 1024);
    rdma_mg->set_page_cache(cache_ptr);

    DDSM ddsm(cache_ptr, rdma_mg);
    // need sync with other computenodes
    rdma_mg->sync_with_computes_Cside();

    printf("\n========================================\n");
    printf("     MVCC Storage Benchmark\n");
    printf("========================================\n");
    printf("Storage Type: ");
    switch (kStorageType) {
        case DELTA_SECTION:
            printf("Dedicated Delta Section (Current MVOCC)\n");
            break;
        case VERSION_CHAIN:
            printf("Version Chain (PostgreSQL-like)\n");
            break;
        case DELTA_IN_GCL:
            printf("Delta in GCL (Global Coherence Layer)\n");
            break;
        default:
            printf("Unknown (%d)\n", kStorageType);
            return 1;
    }
    printf("========================================\n\n");

    // Initialize global timestamp
    GlobalTimestamp::rdma_mg = rdma_mg;

    // Set the callback pointer for delta pull timing
    DSMEngine::g_delta_pull_time_callback = RecordDeltaPullTime;

    // Create benchmark
    MVCCBenchmark benchmark(&ddsm, cache_ptr,
                            static_cast<StorageStrategy>(kStorageType));

    // Set global pointer for GC thread to access local_snapshot_ts_
    g_local_snapshot_ptr = &(benchmark.local_snapshot_ts_);

    // Initialize tuples
    benchmark.InitializeTuples();

    // Sync delta sections across nodes (for DELTA_SECTION strategy)
    benchmark.SyncDeltaSectionsMeta();

    // Synchronize all nodes
    printf("Node %d: Synchronizing with other nodes...\n", ThisNodeID);
    rdma_mg->sync_with_computes_Cside();
    printf("Node %d: Synchronization complete\n", ThisNodeID);

    // Start GC thread for DELTA_SECTION strategy
    if (kStorageType == DELTA_SECTION && g_ds_for_write != nullptr) {
        g_gc_thread = new std::thread(GarbageCollectionThread);
        printf("Node %d: Started GC thread (using local_snapshot_ts_ for tracking)\n", ThisNodeID);
    }

    // Reset cache statistics
    for (int i = 0; i < MAX_APP_THREAD; i++) {
        cache_invalidation[i] = 0;
        for (int j = 0; j < 8; j++) {
            cache_hit_valid[i][j] = 0;
        }
    }

    // Start threads based on workload mode
    std::vector<std::thread> threads;

    if (kMixedWorkload) {
        // Mixed workload mode: all threads do both reads and writes based on read_ratio
        for (int i = 0; i < kNumThreads; i++) {
            threads.emplace_back(MixedWorkloadThread, &benchmark, i);
        }
    } else {
        // Separate writer/reader mode: dedicated writer and reader threads
        for (int i = 0; i < kNumWriters; i++) {
            threads.emplace_back(WriterThread, &benchmark, i);
        }

        for (int i = 0; i < kNumReaders; i++) {
            threads.emplace_back(ReaderThread, &benchmark, i);
        }
    }

    // Give threads a moment to initialize
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Start warmup phase
    if (kWarmupDurationSec > 0) {
        printf("\n========================================\n");
        printf("Starting warmup phase (pure reads for %d seconds)...\n", kWarmupDurationSec);
        printf("========================================\n");
        benchmark_ready.store(true);

        std::this_thread::sleep_for(std::chrono::seconds(kWarmupDurationSec));

        // End warmup, reset statistics
        write_count.store(0);
        read_count.store(0);
        write_latency_sum.store(0);
        read_latency_sum.store(0);
        version_chain_traversals.store(0);
        delta_applications.store(0);
        delta_pull_count.store(0);
        delta_rollback_traversal_count.store(0);
        delta_rollback_time_sum_ns.store(0);

        for (int i = 0; i < MAX_APP_THREAD; i++) {
            cache_invalidation[i] = 0;
            for (int j = 0; j < 8; j++) {
                cache_hit_valid[i][j] = 0;
            }
        }

        printf("Warmup complete. Waiting for all compute nodes to finish warmup...\n");

        // *** CRITICAL BARRIER: Synchronize all compute nodes after warmup ***
        rdma_mg->sync_with_computes_Cside();
        printf("Node %d: All nodes finished warmup. Starting actual benchmark...\n\n", ThisNodeID);
    } else {
        benchmark_ready.store(true);
    }

    // Start actual benchmark
    printf("========================================\n");
    printf("Running benchmark for %d seconds...\n", kBenchmarkDurationSec);
    printf("========================================\n");
    Timer bench_timer;
    bench_timer.begin();

    // Run benchmark with progress updates every 5 seconds
    int elapsed_sec = 0;
    int print_interval = 5; // Print every 5 seconds

    while (elapsed_sec < kBenchmarkDurationSec) {
        std::this_thread::sleep_for(std::chrono::seconds(print_interval));
        elapsed_sec += print_interval;

        if (elapsed_sec < kBenchmarkDurationSec) {
            uint64_t current_writes = write_count.load();
            uint64_t current_reads = read_count.load();
            double current_throughput = (current_writes + current_reads) / (double) elapsed_sec;

            printf("[%3d/%3d sec] Writes: %lu, Reads: %lu, Throughput: %.2f ops/sec\n",
                   elapsed_sec, kBenchmarkDurationSec, current_writes, current_reads, current_throughput);
        }
    }

    // Sleep any remaining time
    if (elapsed_sec < kBenchmarkDurationSec) {
        std::this_thread::sleep_for(std::chrono::seconds(kBenchmarkDurationSec - elapsed_sec));
    }

    uint64_t actual_duration_ns = bench_timer.end();
    double actual_duration_sec = actual_duration_ns / 1e9;

    printf("\n[COMPLETE] Benchmark finished after %.2f seconds\n", actual_duration_sec);
    fflush(stdout);

    // Stop threads
    benchmark_running.store(false);

    for (auto &thread: threads) {
        thread.join();
    }

    // Stop GC thread
    if (g_gc_thread != nullptr) {
        gc_thread_running.store(false); // Signal GC thread to stop
        g_gc_thread->join();
        delete g_gc_thread;
        g_gc_thread = nullptr;
        printf("Node %d: GC thread stopped\n", ThisNodeID);
    }

    benchmark.ShutdownDeltaInGCL();

    // sync all nodes after stop the GC thread for storage type 3
    RDMA_Manager::Get_Instance()->sync_with_computes_Cside();

    // Aggregate per-thread stats into global counters for reporting
    benchmark.AggregateThreadLocalStats();

    // Collect cache invalidation statistics
    uint64_t total_invalidations = 0;
    uint64_t total_cache_hits = 0;
    for (int i = 0; i < MAX_APP_THREAD; i++) {
        total_invalidations += cache_invalidation[i];
        total_cache_hits += cache_hit_valid[i][0];
    }

    // Print delta pull count summary
    uint64_t total_pulls = delta_pull_count.load();
    printf("[DELTA_PULL_SUMMARY] Node %d: %lu delta pulls performed\n",
           RDMA_Manager::Get_Instance()->node_id, total_pulls);

    // Print results
    printf("\n========================================\n");
    printf("               RESULTS\n");
    printf("========================================\n");
    printf("Actual Duration: %.2f seconds\n", actual_duration_sec);
    printf("\nThroughput:\n");
    printf("  Total Writes: %lu\n", write_count.load());
    printf("  Total Reads: %lu\n", read_count.load());
    printf("  Write Throughput: %.2f ops/sec\n",
           write_count.load() / actual_duration_sec);
    printf("  Read Throughput: %.2f ops/sec\n",
           read_count.load() / actual_duration_sec);
    printf("  Total Throughput: %.2f ops/sec\n",
           (write_count.load() + read_count.load()) / actual_duration_sec);

    printf("\nLatency:\n");
    if (write_count.load() > 0) {
        printf("  Avg Write Latency: %lu ns (%.2f us)\n",
               write_latency_sum.load() / write_count.load(),
               (write_latency_sum.load() / write_count.load()) / 1000.0);
    }
    if (read_count.load() > 0) {
        printf("  Avg Read Latency: %lu ns (%.2f us)\n",
               read_latency_sum.load() / read_count.load(),
               (read_latency_sum.load() / read_count.load()) / 1000.0);
    }

    printf("\nStorage-Specific Metrics:\n");
    if (kStorageType == VERSION_CHAIN) {
        if (read_count.load() > 0) {
            printf("  Avg Version Chain Traversals: %.2f\n",
                   version_chain_traversals.load() / (double) read_count.load());
        }
    } else if (kStorageType == DELTA_SECTION || kStorageType == DELTA_IN_GCL) {
        printf("  Delta Applications: %lu\n", delta_applications.load());
        printf("  Delta Pulls: %lu\n", delta_pull_count.load());
        if (delta_applications.load() > 0) {
            uint64_t local_delta_hits = delta_applications.load() - delta_pull_count.load();
            double delta_hit_rate = (double) local_delta_hits / delta_applications.load() * 100.0;
            printf("  Local Delta Hits: %lu\n", local_delta_hits);
            printf("  Delta Hit Rate: %.2f%%\n", delta_hit_rate);
        }
        if (kStorageType == DELTA_SECTION && delta_rollback_traversal_count.load() > 0) {
            uint64_t avg_rollback_time_ns = delta_rollback_time_sum_ns.load() / delta_rollback_traversal_count.load();
            printf("  Delta Rollback Traversals: %lu\n", delta_rollback_traversal_count.load());
            printf("  Avg Delta Rollback Time: %lu ns (%.2f us)\n", 
                   avg_rollback_time_ns, avg_rollback_time_ns / 1000.0);
            if (delta_applications.load() > 0) {
                printf("  Avg Deltas per Rollback: %.2f\n",
                       delta_applications.load() / (double) delta_rollback_traversal_count.load());
            }
        }
    }

    printf("\nCache Statistics:\n");
    printf("  Cache Invalidations: %lu\n", total_invalidations);
    printf("  Cache Hits (Valid): %lu\n", total_cache_hits);
    if (read_count.load() + write_count.load() > 0) {
        printf("  Invalidation Rate: %.4f%%\n",
               100.0 * total_invalidations /
               (read_count.load() + write_count.load()));
    }

    printf("========================================\n");

    // Prepare and store local results to memcached
    struct BenchmarkResults {
        uint64_t node_id;
        uint64_t write_count;
        uint64_t read_count;
        uint64_t write_latency_sum;
        uint64_t read_latency_sum;
        uint64_t total_invalidations;
        uint64_t total_cache_hits;
        uint64_t delta_apps;
        uint64_t delta_pulls;
        uint64_t version_chain_traversals;
        uint64_t delta_rollback_traversals;
        uint64_t delta_rollback_time_sum_ns;
    } local_results;

    local_results.node_id = ThisNodeID;
    local_results.write_count = write_count.load();
    local_results.read_count = read_count.load();
    local_results.write_latency_sum = write_latency_sum.load();
    local_results.read_latency_sum = read_latency_sum.load();
    local_results.total_invalidations = total_invalidations;
    local_results.total_cache_hits = total_cache_hits;
    local_results.delta_apps = delta_applications.load();
    local_results.delta_pulls = delta_pull_count.load();
    local_results.version_chain_traversals = version_chain_traversals.load();
    local_results.delta_rollback_traversals = delta_rollback_traversal_count.load();
    local_results.delta_rollback_time_sum_ns = delta_rollback_time_sum_ns.load();

    // Store local results to memcached
    char benchmark_end_key[64];
    snprintf(benchmark_end_key, sizeof(benchmark_end_key), "benchmark_end_node_%d", ThisNodeID);
    ddsm.memSet(benchmark_end_key, strlen(benchmark_end_key),
                (const char *) &local_results, sizeof(BenchmarkResults));

    printf("Node %d: Stored local results to memcached (key='%s')\n", ThisNodeID, benchmark_end_key);

    // Only node 0 fetches and prints aggregated results
    if (ThisNodeID != 0) {
        printf("Node %d: Benchmark complete, exiting.\n", ThisNodeID);
        return 0;
    }

    // Fetch results from all compute nodes and aggregate
    int compute_num = rdma_mg->GetComputeNodeNum();
    uint64_t total_writes = 0;
    uint64_t total_reads = 0;
    uint64_t total_write_latency = 0;
    uint64_t total_read_latency = 0;
    uint64_t total_invalidations_all = 0;
    uint64_t total_cache_hits_all = 0;
    uint64_t total_delta_apps = 0;
    uint64_t total_delta_pulls = 0;
    uint64_t total_version_traversals = 0;
    uint64_t total_rollback_traversals = 0;
    uint64_t total_rollback_time_sum_ns = 0;

    printf("\nFetching results from all %d compute nodes...\n", compute_num);

    for (int i = 0; i < compute_num; i++) {
        snprintf(benchmark_end_key, sizeof(benchmark_end_key), "benchmark_end_node_%d", i * 2);
        size_t len = 0;

        BenchmarkResults *node_results = (BenchmarkResults *) ddsm.memGet(benchmark_end_key,
                                                                          strlen(benchmark_end_key), &len);

        if (node_results != nullptr && len == sizeof(BenchmarkResults)) {
            printf("  Node %lu: writes=%lu, reads=%lu, invalidations=%lu, cache_hits=%lu, deltas=%lu, pulls=%lu\n",
                   node_results->node_id, node_results->write_count, node_results->read_count,
                   node_results->total_invalidations, node_results->total_cache_hits,
                   node_results->delta_apps, node_results->delta_pulls);

            total_writes += node_results->write_count;
            total_reads += node_results->read_count;
            total_write_latency += node_results->write_latency_sum;
            total_read_latency += node_results->read_latency_sum;
            total_invalidations_all += node_results->total_invalidations;
            total_cache_hits_all += node_results->total_cache_hits;
            total_delta_apps += node_results->delta_apps;
            total_delta_pulls += node_results->delta_pulls;
            total_version_traversals += node_results->version_chain_traversals;
            total_rollback_traversals += node_results->delta_rollback_traversals;
            total_rollback_time_sum_ns += node_results->delta_rollback_time_sum_ns;

            free(node_results);
        }
    }

    // Print aggregated statistics
    printf("\n========================================\n");
    printf("     AGGREGATED RESULTS (ALL NODES)\n");
    printf("========================================\n");

    printf("\nBenchmark Configuration:\n");
    printf("  Compute Nodes: %d\n", compute_num);
    if (kMixedWorkload) {
        printf("  Workload Mode: Mixed (read_ratio=%d%%)\n", kReadRatio);
        printf("  Threads per Node: %d\n", kNumThreads);
    } else {
        printf("  Workload Mode: Separate Writers/Readers\n");
        printf("  Writers per Node: %d, Readers per Node: %d\n", kNumWriters, kNumReaders);
    }
    const char *storage_name = (kStorageType == DELTA_SECTION)
                                   ? "Delta Section"
                                   : (kStorageType == VERSION_CHAIN)
                                         ? "Version Chain"
                                         : "Delta In GCL";
    printf("  Storage Strategy: %s\n", storage_name);

    const char *workload_name = (kWorkloadType == 0) ? "Uniform" : "Zipfian";
    printf("  Access Pattern: %s", workload_name);
    if (kWorkloadType == 1) {
        printf(" (theta=%.2f)", kZipfianTheta);
    }
    printf("\n");
    printf("  Num Tuples: %lu\n", kNumTuples);
    printf("  Snapshot Lag: %lu (randomized)\n", kSnapshotLag);
    printf("  Warmup Duration: %d seconds\n", kWarmupDurationSec);
    printf("  Benchmark Duration: %d seconds\n", kBenchmarkDurationSec);
    printf("  Actual Duration: %.2f seconds\n", actual_duration_sec);

    printf("\nAggregated Throughput:\n");
    printf("  Total Writes (all nodes): %lu\n", total_writes);
    printf("  Total Reads (all nodes): %lu\n", total_reads);
    printf("  Total Operations: %lu\n", total_writes + total_reads);
    printf("  Aggregate Write Throughput: %.2f ops/sec\n", total_writes / actual_duration_sec);
    printf("  Aggregate Read Throughput: %.2f ops/sec\n", total_reads / actual_duration_sec);
    printf("  Aggregate Total Throughput: %.2f ops/sec\n",
           (total_writes + total_reads) / actual_duration_sec);

    printf("\nAverage Latency (across all nodes):\n");
    if (total_writes > 0) {
        uint64_t avg_write_lat = total_write_latency / total_writes;
        printf("  Avg Write Latency: %lu ns (%.2f us)\n", avg_write_lat, avg_write_lat / 1000.0);
    }
    if (total_reads > 0) {
        uint64_t avg_read_lat = total_read_latency / total_reads;
        printf("  Avg Read Latency: %lu ns (%.2f us)\n", avg_read_lat, avg_read_lat / 1000.0);
    }

    printf("\nStorage-Specific Metrics (aggregated):\n");
    if (kStorageType == VERSION_CHAIN) {
        if (total_reads > 0) {
            printf("  Total Version Chain Traversals: %lu\n", total_version_traversals);
            printf("  Avg Version Chain Traversals: %.2f per read\n",
                   total_version_traversals / (double) total_reads);
        }
    } else if (kStorageType == DELTA_SECTION || kStorageType == DELTA_IN_GCL) {
        printf("  Total Delta Applications: %lu\n", total_delta_apps);
        printf("  Total Delta Pulls: %lu\n", total_delta_pulls);
        if (total_delta_pulls > 0) {
            // Calculate average delta pull time from this node
            uint64_t local_delta_pull_sum = delta_pull_time_sum_us.load();
            double avg_delta_pull_time = local_delta_pull_sum / (double) total_delta_pulls;
            printf("  Avg Delta Pull Time: %.2f us\n", avg_delta_pull_time);
        }
        if (total_delta_apps > 0) {
            uint64_t local_delta_hits = total_delta_apps - total_delta_pulls;
            double delta_hit_rate = (double) local_delta_hits / total_delta_apps * 100.0;
            printf("  Local Delta Hits: %lu\n", local_delta_hits);
            printf("  Delta Hit Rate: %.2f%%\n", delta_hit_rate);
        }
        if (total_reads > 0) {
            printf("  Avg Delta Applications: %.2f per read\n", total_delta_apps / (double) total_reads);
        }
        if (kStorageType == DELTA_SECTION && total_rollback_traversals > 0) {
            uint64_t avg_rollback_time_ns = total_rollback_time_sum_ns / total_rollback_traversals;
            printf("  Total Delta Rollback Traversals: %lu\n", total_rollback_traversals);
            printf("  Avg Delta Rollback Time (aggregated): %lu ns (%.2f us)\n",
                   avg_rollback_time_ns, avg_rollback_time_ns / 1000.0);
            if (total_delta_apps > 0) {
                printf("  Avg Deltas per Rollback (aggregated): %.2f\n",
                       total_delta_apps / (double) total_rollback_traversals);
            }
        }
    }

    printf("\nCache Statistics (aggregated):\n");
    printf("  Total Cache Invalidations: %lu\n", total_invalidations_all);
    printf("  Total Cache Hits (Valid): %lu\n", total_cache_hits_all);
    if (total_reads + total_writes > 0) {
        printf("  Invalidation Rate: %.4f%%\n",
               100.0 * total_invalidations_all / (total_reads + total_writes));
        printf("  Cache Hit Rate: %.4f%%\n",
               100.0 * total_cache_hits_all / (total_reads + total_writes));
    }

    printf("========================================\n");

    // Sleep briefly to allow memory servers to detect completion and exit
    printf("\nNode 0: Sleeping for 2 seconds to allow memory servers to process exit signal...\n");
    sleep(2);

    printf("Node 0: All results aggregated, exiting.\n");

    return 0;
}
