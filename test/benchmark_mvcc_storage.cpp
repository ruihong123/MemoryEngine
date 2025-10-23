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
uint64_t kNumTuples = 100000; // Number of tuples to work with
uint64_t kSnapshotLag = 10000; // Max snapshot lag (read snapshot in [current_ts - kSnapshotLag, current_ts])
int kBenchmarkDurationSec = 30;
int kWarmupDurationSec = 10;  // Warmup duration (pure reads)

// Benchmark Parameters (set via command line)
int kNumThreads = 8;  // Total number of worker threads
int kReadRatio = 50;  // Read ratio 0-100 (for mixed workload mode)
bool kMixedWorkload = true;  // true=mixed read/write, false=separate writers/readers
int kNumWriters = 2;  // Only used when kMixedWorkload=false
int kNumReaders = 2;  // Only used when kMixedWorkload=false
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
std::atomic<bool> benchmark_running{true};
std::atomic<bool> benchmark_ready{false};
std::atomic<bool> warmup_phase{true};

// Delta Section Management (similar to TransactionManager)
std::shared_mutex g_delta_map_mtx;
std::map<GlobalAddress, DeltaSectionWrap*, std::greater<GlobalAddress>> g_delta_sections;
DeltaSectionWrap* g_ds_for_write = nullptr;
std::thread* g_gc_thread = nullptr;
std::atomic<uint64_t>* g_local_snapshot_ptr = nullptr;  // Pointer to local_snapshot_ts_ for GC
std::atomic<bool> gc_thread_running{true};  // Separate flag for GC thread

// External cache statistics
extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];

// Forward declarations
class MVCCBenchmark;

// Delta Section Handler Functions (similar to TransactionManager)
void ProcessDeltaCreate(void* args) {
  auto* rdma_mg = RDMA_Manager::Get_Instance();
  auto *receive_msg_buf = (RDMA_Request*)args;
  GlobalAddress ds_gaddr = receive_msg_buf->content.create_ds.ds_gaddr;
  uint8_t compute_node_id = receive_msg_buf->content.create_ds.compute_node_id;
  
  ibv_mr* local_mr = new ibv_mr{};
  rdma_mg->Allocate_Local_RDMA_Slot(*local_mr, DeltaChunk);
  
  auto* ds = new DeltaSectionWrap(compute_node_id, ds_gaddr, rdma_mg->delta_section_size, local_mr);
  {
    std::unique_lock<std::shared_mutex> lck(g_delta_map_mtx);
    g_delta_sections.insert(std::make_pair(ds_gaddr, ds));
  }
  
  printf("Node %d: Created delta section for node %d at %p\n", 
         rdma_mg->node_id, compute_node_id, ds_gaddr.val);
  
  delete receive_msg_buf;
}

void ProcessDeltaPull(void* args) {
  auto* rdma_mg = RDMA_Manager::Get_Instance();
  auto *receive_msg_buf = (RDMA_Request*)args;
  assert(receive_msg_buf->command == pull_delta_section);
  
  GlobalAddress ds_gaddr = receive_msg_buf->content.pull_ds.ds_gaddr;
  uint64_t old_head_ = receive_msg_buf->content.pull_ds.old_head;
  uint64_t old_tail_ = receive_msg_buf->content.pull_ds.old_tail;
  uint64_t old_max_ts = receive_msg_buf->content.pull_ds.old_max_ts;
  uint64_t old_epoch = receive_msg_buf->content.pull_ds.old_epoch;
  uint8_t requester_node_id = receive_msg_buf->content.pull_ds.requester_node_id;
  
  printf("[DEBUG] ProcessDeltaPull: Node %d received delta pull request from node %d, ds_gaddr=%p, old_head=%lu, old_tail=%lu, old_max_ts=%lu, old_epoch=%lu\n", 
         rdma_mg->node_id, requester_node_id, ds_gaddr.val, old_head_, old_tail_, old_max_ts, old_epoch);
  fflush(stdout);
  
  {
    std::shared_lock<std::shared_mutex> map_lck(g_delta_map_mtx);
    auto it = g_delta_sections.find(ds_gaddr);
    map_lck.unlock();
    
    if (it == g_delta_sections.end()) {
      delete receive_msg_buf;
      return;
    }
    
    DeltaSectionWrap* ds_w = it->second;
    std::shared_lock<std::shared_mutex> delta_lck(ds_w->main_mtx_);
    assert(!ds_w->inner_section->is_empty_);
    
    ibv_mr local_mr = *ds_w->seg_local_mr_;
    char* remote_addr = (char*)receive_msg_buf->buffer;
    
    int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
    uint8_t* polling_byte = (uint8_t*)((uint8_t*)local_mr.addr + rdma_mg->delta_section_size - 1);
    assert(ds_w->inner_section->tail_ != ds_w->inner_section->head_ || ds_w->inner_section->is_empty_);
    *polling_byte = 5;
    
    std::vector<std::pair<uint64_t, uint64_t>> boundaries;
    ds_w->CalculateWriteBoundaries(boundaries, old_head_, old_tail_, old_epoch);
    
    for(auto pair : boundaries){
      qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
      local_mr = *ds_w->seg_local_mr_;
      remote_addr = (char*)receive_msg_buf->buffer;
      
      uint64_t start = pair.first;
      uint64_t end = pair.second;
      size_t write_size = end - start;
      bool async = write_size >= BIGPAGESIZE;
      
      local_mr.addr = (void*)((char*)local_mr.addr + start);
      remote_addr += start;
      
      rdma_mg->RDMA_Write_xcompute(&local_mr, remote_addr, receive_msg_buf->rkey,
                                    write_size, requester_node_id, qp_id, async);
    }
  }
  
  delete receive_msg_buf;
}

// Simplified GC thread - runs independently of benchmark
void GarbageCollectionThread() {
  while(gc_thread_running.load()) {
    // Simplified GC: gc_threshold = current_local_sp - 2*kSnapshotLag
    if (g_local_snapshot_ptr != nullptr) {
      uint64_t local_sp = g_local_snapshot_ptr->load(std::memory_order_relaxed);
      uint64_t gc_threshold = (local_sp > 2 * kSnapshotLag) ? (local_sp - 2 * kSnapshotLag) : 0;
      
      if (g_ds_for_write != nullptr && gc_threshold > 0) {
        g_ds_for_write->GarbageCollectionBySnapshot(gc_threshold);
      }
    }else {
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
  DELTA_IN_GCL = 3   // Delta in GCL (Global Coherence Layer) - regular SELCC pages
};

// VERSION_CHAIN: No separate metadata needed - everything stored in MetaColumn

// Delta Record for DELTA_IN_GCL strategy (stored in GCL pages)
struct GCLDelta {
  uint64_t tuple_key;
  uint64_t old_wts;
  uint64_t old_value;  // Only 1 value field
  GlobalAddress next_delta; // Pointer to next delta (older)

  GCLDelta()
      : tuple_key(0), old_wts(0), old_value(0),
        next_delta(GlobalAddress::Null()) {}
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
  
  // Zipfian distribution generator (shared across threads)
  struct zipf_gen_state zipf_gen_state_;
  bool use_zipfian_;
  
  // Thread-local random number generators (one per thread)
  std::vector<std::unique_ptr<Random64>> thread_randoms_;

  // For VERSION_CHAIN strategy: we use the primary index to find the latest version
  // No additional data structures needed - all metadata stored in MetaColumn

  // For DELTA_IN_GCL strategy: delta storage pages in GCL (Global Coherence Layer)
  std::vector<GlobalAddress> delta_pages_;
  std::atomic<int> delta_page_index_{0};
  std::vector<std::unique_ptr<SpinMutex>> delta_page_locks_;
  std::vector<uint64_t> delta_page_offsets_;
  std::vector<std::unique_ptr<SpinMutex>> delta_offset_locks_;

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
    columns.push_back(new ColumnInfo("padding", ValueType::FIXCHAR, 496));  // 496 bytes padding
    columns.push_back(new ColumnInfo("meta", ValueType::META));  // MetaColumn for MVCC
    schema_->BulkloadColumns(columns);
    size_t column_ids[1] = {0};
    schema_->SetPrimaryColumns(column_ids, 1);

    // Create table with primary index
    table_ = new Table();
    table_->Init(0, schema_, ddsm_);

    if (strategy_ == DELTA_SECTION) {
      // Register delta section message handlers
      auto* rdma_mg = RDMA_Manager::Get_Instance();
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
      ibv_mr* local_mr = new ibv_mr{};
      rdma_mg->Allocate_Local_RDMA_Slot(*local_mr, DeltaChunk);
      
      // Create writable DeltaSectionWrap (one per node)
      std::unique_lock<std::shared_mutex> lck(g_delta_map_mtx);
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
    } else if (strategy_ == DELTA_IN_GCL) {
      // Pre-allocate delta pages in GCL
      int num_delta_pages = std::max(10, kNumWriters * 2);
      for (int i = 0; i < num_delta_pages; i++) {
        GlobalAddress delta_page = ddsm_->Allocate_Remote(Regular_Page);
        delta_pages_.push_back(delta_page);
        delta_page_offsets_.push_back(STRUCT_OFFSET(DataPage, data_));
        delta_page_locks_.emplace_back(new SpinMutex());
        delta_offset_locks_.emplace_back(new SpinMutex());
      }
    }

    // Initialize zipfian generator if needed
    if (kWorkloadType == 1) {
      uint64_t rand_seed = ThisNodeID * 123456789UL + 987654321UL;
      mehcached_zipf_init(&zipf_gen_state_, kNumTuples, kZipfianTheta, rand_seed);
      use_zipfian_ = true;
      printf("Initialized Zipfian generator with theta=%.2f, n=%lu\n", kZipfianTheta, kNumTuples);
    }
    
    // Initialize thread-local random number generators
    thread_randoms_.resize(kNumThreads);
    for (int i = 0; i < kNumThreads; i++) {
      uint64_t seed = i * 12345 + ThisNodeID * 67890;
      thread_randoms_[i] = std::make_unique<Random64>(seed);
      printf("Node %d Thread %d: Initialized random generator with seed %lu\n", ThisNodeID, i, seed);
    }

    const char *strategy_name =
        (strategy_ == DELTA_SECTION)
            ? "Delta Section"
            : (strategy_ == VERSION_CHAIN) ? "Version Chain" : "Delta In Page";
    printf("Initialized MVCC Benchmark with storage strategy: %s\n",
           strategy_name);
  }

  ~MVCCBenchmark() {
    delete table_;
    delete schema_;
  }
  
  // Sync delta sections across all compute nodes
  void SyncDeltaSectionsMeta() {
    if (strategy_ != DELTA_SECTION) return;
    
    auto* rdma_mg = RDMA_Manager::Get_Instance();
    
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
    auto* rdma_mg = RDMA_Manager::Get_Instance();
    int compute_num = rdma_mg->GetComputeNodeNum();
    int my_compute_rank = ThisNodeID / 2;  // Compute nodes are 0, 2, 4, ...
    
    uint64_t tuples_per_node = kNumTuples / compute_num;
    uint64_t start_key = tuples_per_node * my_compute_rank;
    uint64_t end_key = (my_compute_rank == compute_num - 1) ? kNumTuples : (start_key + tuples_per_node);
    
    printf("Node %d: Creating tuples %lu to %lu (total %lu)\n", 
           ThisNodeID, start_key, end_key - 1, end_key - start_key);
    
    for (uint64_t key = start_key; key < end_key; key++) {
      char tuple_buffer[schema_->GetRecordTotalSize()];
      memset(tuple_buffer, 0, schema_->GetRecordTotalSize());

      // Set key and initial value
      *(uint64_t *)(tuple_buffer) = key;
      *(uint64_t *)(tuple_buffer + 8) = key * 2;  // Initial value
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
      meta.Wts_ = 0;  // Initial timestamp
      meta.prev_version_ = GlobalAddress::Null();
      meta.prev_delta_epoch_ = 0;
      meta.prev_delta_data_size_ = 0;
      meta.is_visible_ = true;  // Tuple is visible
      record->PutMeta(meta);
      delete record;

      // Insert into primary index (B-tree) - all nodes can now find this tuple
      RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
      DynamicCompoundKey primary_key((char*)&key, index_schema_ptr);
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
    if (use_zipfian_) {
      // Zipfian distribution (thread-safe as each call modifies local state)
      return mehcached_zipf_next(&zipf_gen_state_);
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
      
      printf("[DATA_ACCESS] WRITE: Node %d Thread %d - Key %lu committing at timestamp %lu\n", 
             ThisNodeID, thread_id, key, commit_ts);
      fflush(stdout);

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
      
      printf("[DATA_ACCESS] READ: Node %d Thread %d accessing key %lu at snapshot_ts %lu (current_ts %lu, lag %lu)\n", 
             ThisNodeID, thread_id, key, snapshot_ts, current_ts, random_lag);
      fflush(stdout);

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

private:
  // ==================== VERSION CHAIN IMPLEMENTATION ====================
  // PostgreSQL-style version chains:
  // - Each write creates a new tuple in disaggregated memory
  // - Primary index always points to the latest version
  // - Old versions linked via prev_version_ in MetaColumn
  // - Reads start from latest version, follow chain backwards if needed
  // - No garbage collection in this benchmark (simplified)

  void WriteWithVersionChain(uint64_t key, uint64_t commit_ts, int thread_id) {
    // Look up current tuple (latest version) via primary index
    RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
    DynamicCompoundKey primary_key((char*)&key, index_schema_ptr);
    GlobalAddress old_tuple_gaddr = table_->SearchPriIndex(primary_key);
    
    if (old_tuple_gaddr == GlobalAddress::Null()) {
      printf("WriteWithVersionChain: Key %lu not found in index\n", key);
      return;
    }

    // Step 1: Read old version into local buffer (with shared lock)
    // Use std::vector for portability (VLAs are not standard C++)
    std::vector<char> local_buffer(schema_->GetRecordTotalSize());
    
    Cache::Handle *old_handle;
    void *old_page_buffer;
    ddsm_->SELCC_Shared_Lock(old_page_buffer, TOPAGE(old_tuple_gaddr), old_handle);
    char *old_tuple_ptr = (char *)old_page_buffer + 
                          (old_tuple_gaddr.offset - TOPAGE(old_tuple_gaddr).offset);
    
    // Copy old tuple to local buffer
    memcpy(local_buffer.data(), old_tuple_ptr, schema_->GetRecordTotalSize());
    
    ddsm_->SELCC_Shared_UnLock(TOPAGE(old_tuple_gaddr), old_handle);

    // Step 2: Modify value in local buffer (increment value)
    *(uint64_t *)(local_buffer.data() + 8) += 1;

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
      if (local_snapshot_ts_.compare_exchange_weak(expected, record_ts,
                                                     std::memory_order_relaxed,
                                                     std::memory_order_relaxed)) {
        break;  // Successfully updated
                                                     }
      // expected was updated by compare_exchange_weak, retry if still needed
      expected = local_snapshot_ts_.load(std::memory_order_relaxed);
    }

    // Step 4: Copy updated data from local buffer to new tuple
    memcpy(new_tuple_buffer, local_buffer.data(), schema_->GetRecordTotalSize());
    meta.Wts_ = commit_ts;
    meta.prev_version_ = old_tuple_gaddr;  // Link to previous version
    meta.prev_delta_epoch_ = 0;
    meta.prev_delta_data_size_ = 0;
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
    DynamicCompoundKey primary_key((char*)&key, index_schema_ptr);
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

      char *tuple_ptr = (char *)page_buffer +
                        (current_gaddr.offset - TOPAGE(current_gaddr).offset);
      Record record(schema_, tuple_ptr);

      MetaColumn meta = record.GetMeta();
      uint64_t version_ts = meta.Wts_;
      GlobalAddress prev_version = meta.prev_version_;

      if (version_ts <= snapshot_ts) {
        // Found visible version - read the value
        uint64_t value = *(uint64_t *)(tuple_ptr + 8);
        (void)value; // Suppress unused warning

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
    printf("ERROR: ReadWithVersionChain key=%lu snapshot_ts=%lu - reached end of chain without finding visible version!\n",
           key, snapshot_ts);
    assert(false && "Version chain traversal failed - no visible version found");
  }

  // ==================== DELTA SECTION IMPLEMENTATION ====================

  void WriteWithDeltaSection(uint64_t key, uint64_t commit_ts, int thread_id) {
    // Look up tuple address via primary index
    RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
    DynamicCompoundKey primary_key((char*)&key, index_schema_ptr);
    GlobalAddress tuple_gaddr = table_->SearchPriIndex(primary_key);

    Cache::Handle *handle;
    void *page_buffer;
    ddsm_->SELCC_Exclusive_Lock(page_buffer, TOPAGE(tuple_gaddr), handle);

    char *tuple_ptr =
        (char *)page_buffer + (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
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
          printf("Node %d thread %d has updated local snapshot ts from %lu to %lu\n", RDMA_Manager::node_id, RDMA_Manager::thread_id, expected, tuple_ts + 1);
          fflush(stdout);
          break;  // Successfully updated
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
    uint64_t old_value = *(uint64_t *)(new_record.data_ptr_ + 8);
    *(uint64_t *)(new_record.data_ptr_ + 8) = old_value + 1;
    new_record.dirty_col_ids.insert(1);  // Mark value column as dirty
    
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
    DynamicCompoundKey primary_key((char*)&key, index_schema_ptr);
    GlobalAddress tuple_gaddr = table_->SearchPriIndex(primary_key);

    Cache::Handle *handle;
    void *page_buffer;
    ddsm_->SELCC_Shared_Lock(page_buffer, TOPAGE(tuple_gaddr), handle);

    char *tuple_ptr =
        (char *)page_buffer + (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
    
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
        break;  // Successfully updated
      }
      // expected was updated by compare_exchange_weak, retry if still needed
      expected = local_snapshot_ts_.load(std::memory_order_relaxed);
    }

    // Apply deltas if current version is newer than snapshot
    int delta_count = 0;
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
        std::shared_lock<std::shared_mutex> lck(g_delta_map_mtx);
        auto iter = g_delta_sections.lower_bound(prev_delta);
        if (iter == g_delta_sections.end()) {
          break;  // Delta section not found
        }
        delta_section = iter->second;
        assert(iter->first.nodeID == prev_delta.nodeID);
      }
      
      // Check if delta section is stale and pull updates if needed
      {
        std::shared_lock<RWSpinMutex> slck(delta_section->shadow_mtx_);
        if (delta_section->inner_section->is_empty_ ||
            !delta_section->isOffsetValid(prev_delta, meta.prev_delta_epoch_)) {
          slck.unlock();
          
          // Use double-checked locking to avoid conflict
          std::unique_lock<RWSpinMutex> lck(delta_section->shadow_mtx_);
          if (delta_section->inner_section->is_empty_ ||
              !delta_section->isOffsetValid(prev_delta, meta.prev_delta_epoch_)) {
            
            // Pull updates from remote node
            if (delta_section->owner_compute_node_id_ != RDMA_Manager::Get_Instance()->node_id) {
              printf("[DEBUG] Issuing DeltaPull: Node %d requesting delta updates from owner node %d, ds_gaddr=%p, key=%lu\n",
                     RDMA_Manager::Get_Instance()->node_id, delta_section->owner_compute_node_id_,
                     delta_section->seg_addr_.val, key);
              fflush(stdout);
              delta_section->PullUpdates();
            }
          }
        }
      }
      
      // Access delta from delta section
      DeltaRecord *delta_record = (DeltaRecord *)((char *)delta_section->seg_local_mr_->addr +
                                                   (prev_delta.offset - delta_section->seg_addr_.offset));
      
      // Apply delta to roll back to previous version
      record.roll_back(delta_record);
      
      current_ts = record.GetWTS();
      delta_count++;
      delta_applications.fetch_add(1);
    }

    if (current_ts <= snapshot_ts) {
      // Found visible version - read the value
      uint64_t value = *(uint64_t *)(record.data_ptr_ + 8);
      (void)value;  // Suppress unused warning
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

  void WriteWithDeltaInGCL(uint64_t key, uint64_t commit_ts, int thread_id) {
    // Look up tuple address via primary index
    RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
    DynamicCompoundKey primary_key((char*)&key, index_schema_ptr);
    GlobalAddress tuple_gaddr = table_->SearchPriIndex(primary_key);

    // Step 1: Lock tuple page and read current values
    Cache::Handle *tuple_handle;
    void *tuple_page_buffer;
    ddsm_->SELCC_Exclusive_Lock(tuple_page_buffer, TOPAGE(tuple_gaddr), tuple_handle);

    char *tuple_ptr = (char *)tuple_page_buffer +
                      (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
    Record tuple_record(schema_, tuple_ptr);

    // Read old metadata and values
    MetaColumn old_meta = tuple_record.GetMeta();
    uint64_t old_wts = old_meta.Wts_;
    uint64_t old_value = *(uint64_t *)(tuple_ptr + 8);
    GlobalAddress old_prev_delta = old_meta.prev_version_;

    // Release tuple lock to avoid deadlock (will re-acquire)
    ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), tuple_handle);

    // Step 2: Allocate and write delta record in GCL page
    int delta_idx = (key % delta_pages_.size());
    GlobalAddress delta_page_gaddr = delta_pages_[delta_idx];
    
    Cache::Handle *delta_handle;
    void *delta_page_buffer;
    // Lock delta page - THIS CAUSES SELCC INVALIDATIONS!
    ddsm_->SELCC_Exclusive_Lock(delta_page_buffer, delta_page_gaddr, delta_handle);

    // Allocate space in delta page
    delta_offset_locks_[delta_idx]->lock();
    uint64_t delta_offset = delta_page_offsets_[delta_idx];
    delta_page_offsets_[delta_idx] += sizeof(GCLDelta);

    // Check if delta page is full
    if (delta_offset + sizeof(GCLDelta) > kLeafPageSize) {
      // Reset to beginning (circular) - overwrite old deltas
      delta_page_offsets_[delta_idx] = STRUCT_OFFSET(DataPage, data_) + sizeof(GCLDelta);
      delta_offset = STRUCT_OFFSET(DataPage, data_);
    }
    delta_offset_locks_[delta_idx]->unlock();

    // Write delta record to delta page
    GlobalAddress delta_gaddr = delta_page_gaddr;
    delta_gaddr.offset = delta_offset;
    
    char *delta_ptr = (char *)delta_page_buffer + delta_offset;
    GCLDelta *delta = new (delta_ptr) GCLDelta();
    delta->tuple_key = key;
    delta->old_wts = old_wts;
    delta->old_value = old_value;
    delta->next_delta = old_prev_delta;  // Link to previous delta (forms chain)

    // Unlock delta page
    ddsm_->SELCC_Exclusive_UnLock(delta_page_gaddr, delta_handle);

    // Step 3: Re-acquire tuple lock and update tuple with new values
    ddsm_->SELCC_Exclusive_Lock(tuple_page_buffer, TOPAGE(tuple_gaddr), tuple_handle);
    tuple_ptr = (char *)tuple_page_buffer + (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
    
    // Update tuple value (increment)
    *(uint64_t *)(tuple_ptr + 8) = old_value + 1;
    
    // Update metadata with new delta pointer
    Record updated_record(schema_, tuple_ptr);
    MetaColumn new_meta = updated_record.GetMeta();
    new_meta.Wts_ = commit_ts;
    new_meta.prev_version_ = delta_gaddr;  // Point to the delta we just created
    new_meta.prev_delta_epoch_ = 0;
    new_meta.prev_delta_data_size_ = sizeof(GCLDelta);
    new_meta.is_visible_ = true;
    updated_record.PutMeta(new_meta);

    // Unlock tuple page - causes invalidations!
    ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), tuple_handle);
  }

  void ReadWithDeltaInGCL(uint64_t key, uint64_t snapshot_ts, int thread_id) {
    // Look up tuple address via primary index
    RecordSchema *index_schema_ptr = table_->GetPrimaryIndexSchema();
    DynamicCompoundKey primary_key((char*)&key, index_schema_ptr);
    GlobalAddress tuple_gaddr = table_->SearchPriIndex(primary_key);

    // Step 1: Read current tuple value and metadata
    Cache::Handle *tuple_handle;
    void *tuple_page_buffer;
    ddsm_->SELCC_Shared_Lock(tuple_page_buffer, TOPAGE(tuple_gaddr), tuple_handle);

    char *tuple_ptr = (char *)tuple_page_buffer +
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

    // Step 2: Traverse delta chain if needed
    int delta_count = 0;
    while (current_ts > snapshot_ts && current_delta_gaddr != GlobalAddress::Null()) {
      // Lock delta page - THIS CAUSES SELCC INVALIDATIONS!
      Cache::Handle *delta_handle;
      void *delta_page_buffer;
      ddsm_->SELCC_Shared_Lock(delta_page_buffer, TOPAGE(current_delta_gaddr), delta_handle);

      // Read delta record from GCL page
      char *delta_ptr = (char *)delta_page_buffer + 
                        (current_delta_gaddr.offset - TOPAGE(current_delta_gaddr).offset);
      GCLDelta *delta = (GCLDelta *)delta_ptr;
      
      // Verify this is the correct delta
      if (delta->tuple_key != key) {
        printf("ERROR: Delta mismatch! Expected key %lu, got %lu\n", key, delta->tuple_key);
        ddsm_->SELCC_Shared_UnLock(TOPAGE(current_delta_gaddr), delta_handle);
        break;
      }

      // Apply delta (roll back to previous version)
      *(uint64_t *)(local_buffer.data() + 8) = delta->old_value;
      current_ts = delta->old_wts;
      GlobalAddress next_delta = delta->next_delta;
      
      // Unlock delta page
      ddsm_->SELCC_Shared_UnLock(TOPAGE(current_delta_gaddr), delta_handle);
      
      // Move to next delta in chain
      current_delta_gaddr = next_delta;
      delta_count++;
      delta_applications.fetch_add(1);
    }

    // Step 3: Read the value from the reconstructed version
    if (current_ts <= snapshot_ts) {
      uint64_t value = *(uint64_t *)(local_buffer.data() + 8);
      (void)value;  // Suppress unused warning
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

  printf("Mixed workload thread %d started (read_ratio=%d%%)\n", thread_id, kReadRatio);

  while (benchmark_running.load()) {
    if (warmup_phase.load()) {
      // During warmup, only do reads to warm up cache
      benchmark->ReadOperation(thread_id);
    } else {
      // Normal operation: decide read or write based on read ratio
      int random_val = rand.Next() % 100;
      if (random_val < kReadRatio) {
        benchmark->ReadOperation(thread_id);
      } else {
        benchmark->WriteOperation(thread_id);
      }
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
    if (warmup_phase.load()) {
      // During warmup, writers also do reads to warm up cache
      benchmark->ReadOperation(thread_id);
    } else {
      // Normal operation: only writes
      benchmark->WriteOperation(thread_id);
    }
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
              "  --warmup_duration N        Warmup duration in seconds (default: 10)\n"
              "  --duration N               Benchmark duration in seconds (default: 30)\n"
              "  --cache_size N             Cache size in GB (default: 2)\n\n"
              "System Configuration:\n"
              "  --node_id N                Compute node ID (default: 0)\n"
              "  --tcp_port N               TCP port (default: 19843)\n\n",
              argv[0]);
      exit(0);
    } else {
      fprintf(stderr, "Unknown argument: %s\n", argv[i]);
      fprintf(stderr, "Use --help for usage information\n");
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
  const char *storage_name = (kStorageType == DELTA_SECTION) ? " (Delta Section)" :
                              (kStorageType == VERSION_CHAIN) ? " (Version Chain)" : " (Delta In GCL)";
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
  struct config_t config = {NULL,     /* dev_name */
                            NULL,     /* server_name */
                            tcp_port, /* tcp_port */
                            1,        /* ib_port */
                            1,        /* gid_idx */
                            4 * 10 * 1024 *
                                1024, /* initial local buffer size */
                            ThisNodeID};

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
    warmup_phase.store(true);
    benchmark_ready.store(true);
    
    std::this_thread::sleep_for(std::chrono::seconds(kWarmupDurationSec));
    
    // End warmup, reset statistics
    warmup_phase.store(false);
    write_count.store(0);
    read_count.store(0);
    write_latency_sum.store(0);
    read_latency_sum.store(0);
    version_chain_traversals.store(0);
    delta_applications.store(0);
    
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
      double current_throughput = (current_writes + current_reads) / (double)elapsed_sec;
      
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

  // Stop threads
  benchmark_running.store(false);

  for (auto &thread : threads) {
    thread.join();
  }
  
  // Stop GC thread
  if (g_gc_thread != nullptr) {
    gc_thread_running.store(false);  // Signal GC thread to stop
    g_gc_thread->join();
    delete g_gc_thread;
    g_gc_thread = nullptr;
    printf("Node %d: GC thread stopped\n", ThisNodeID);
  }

  // Collect cache invalidation statistics
  uint64_t total_invalidations = 0;
  uint64_t total_cache_hits = 0;
  for (int i = 0; i < MAX_APP_THREAD; i++) {
    total_invalidations += cache_invalidation[i];
    total_cache_hits += cache_hit_valid[i][0];
  }
  
  // Print data access summary
  printf("[DATA_ACCESS_SUMMARY] Node %d: Write operations: %lu, Read operations: %lu, Delta applications: %lu\n", 
         RDMA_Manager::Get_Instance()->node_id, write_count.load(), read_count.load(), delta_applications.load());

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
             version_chain_traversals.load() / (double)read_count.load());
    }
  } else if (kStorageType == DELTA_SECTION || kStorageType == DELTA_IN_GCL) {
    printf("  Delta Applications: %lu\n", delta_applications.load());
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
    uint64_t version_chain_traversals;
  } local_results;
  
  local_results.node_id = ThisNodeID;
  local_results.write_count = write_count.load();
  local_results.read_count = read_count.load();
  local_results.write_latency_sum = write_latency_sum.load();
  local_results.read_latency_sum = read_latency_sum.load();
  local_results.total_invalidations = total_invalidations;
  local_results.total_cache_hits = total_cache_hits;
  local_results.delta_apps = delta_applications.load();
  local_results.version_chain_traversals = version_chain_traversals.load();
  
  // Store local results to memcached
  char benchmark_end_key[64];
  snprintf(benchmark_end_key, sizeof(benchmark_end_key), "benchmark_end_node_%d", ThisNodeID);
  ddsm.memSet(benchmark_end_key, strlen(benchmark_end_key),
              (const char*)&local_results, sizeof(BenchmarkResults));
  
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
  uint64_t total_version_traversals = 0;
  
  printf("\nFetching results from all %d compute nodes...\n", compute_num);
  
  for (int i = 0; i < compute_num; i++) {
    snprintf(benchmark_end_key, sizeof(benchmark_end_key), "benchmark_end_node_%d", i * 2);
    size_t len = 0;
    
    BenchmarkResults* node_results = (BenchmarkResults*)ddsm.memGet(benchmark_end_key, 
                                                                     strlen(benchmark_end_key), &len);
    
    if (node_results != nullptr && len == sizeof(BenchmarkResults)) {
      printf("  Node %lu: writes=%lu, reads=%lu, invalidations=%lu, cache_hits=%lu, deltas=%lu\n",
             node_results->node_id, node_results->write_count, node_results->read_count,
             node_results->total_invalidations, node_results->total_cache_hits, 
             node_results->delta_apps);
      
      total_writes += node_results->write_count;
      total_reads += node_results->read_count;
      total_write_latency += node_results->write_latency_sum;
      total_read_latency += node_results->read_latency_sum;
      total_invalidations_all += node_results->total_invalidations;
      total_cache_hits_all += node_results->total_cache_hits;
      total_delta_apps += node_results->delta_apps;
      total_version_traversals += node_results->version_chain_traversals;
      
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
  const char *storage_name = (kStorageType == DELTA_SECTION) ? "Delta Section" :
                              (kStorageType == VERSION_CHAIN) ? "Version Chain" : "Delta In GCL";
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
             total_version_traversals / (double)total_reads);
    }
  } else if (kStorageType == DELTA_SECTION || kStorageType == DELTA_IN_GCL) {
    printf("  Total Delta Applications: %lu\n", total_delta_apps);
    if (total_reads > 0) {
      printf("  Avg Delta Applications: %.2f per read\n", total_delta_apps / (double)total_reads);
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
