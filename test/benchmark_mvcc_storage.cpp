//
// Multi-Version Storage Benchmark for SELCC
// Comparing three MVCC storage strategies:
// 1. Dedicated Delta Section (current MVOCC implementation)
// 2. PostgreSQL-like Version Chains (new tuples allocated for each version)
// 3. Delta in SELCC Pages (undo logs in regular pages, causing invalidations)
//

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
#include "utils/random.h"

using namespace DSMEngine;

// Define external symbols needed by Table.h
namespace DSMEngine {
extern DDSM *default_gallocator;
DDSM *default_gallocator = nullptr;
} // namespace DSMEngine

// Benchmark Configuration
const int kMaxThreads = 32;
uint64_t kNumTuples = 100000; // Number of tuples to work with
const uint64_t kSnapshotLag =
    10000; // Read snapshot = current_ts - kSnapshotLag
int kBenchmarkDurationSec = 30;

// Benchmark Parameters (set via command line)
int kNumWriters = 4;
int kNumReaders = 2;
int kStorageType = 1; // 1=DeltaSection, 2=VersionChain, 3=DeltaInPage
uint16_t ThisNodeID = 0;
uint16_t tcp_port = 19843;
uint64_t kCacheSize = 2; // GB

// Statistics
std::atomic<uint64_t> write_count{0};
std::atomic<uint64_t> read_count{0};
std::atomic<uint64_t> write_latency_sum{0};
std::atomic<uint64_t> read_latency_sum{0};
std::atomic<uint64_t> version_chain_traversals{0};
std::atomic<uint64_t> delta_applications{0};
std::atomic<bool> benchmark_running{true};
std::atomic<bool> benchmark_ready{false};

// External cache statistics
extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];

// Storage Strategy Enum
enum StorageStrategy {
  DELTA_SECTION = 1, // Current MVOCC with dedicated delta section
  VERSION_CHAIN = 2, // PostgreSQL-like version chains
  DELTA_IN_PAGE = 3  // Delta in regular SELCC pages
};

// Version Chain Metadata - stored separately to avoid MetaColumn dependency
// issues
struct VersionChainMeta {
  uint64_t version_ts;
  GlobalAddress prev_version; // Points to previous version (older)

  VersionChainMeta() : version_ts(0), prev_version(GlobalAddress::Null()) {}
} __attribute__((packed));

// Delta Record for DELTA_IN_PAGE strategy (stored in regular pages)
struct InPageDelta {
  uint64_t tuple_key;
  uint64_t old_wts;
  uint64_t old_value1;
  uint64_t old_value2;
  uint64_t old_value3;
  GlobalAddress next_delta; // Pointer to next delta (older)

  InPageDelta()
      : tuple_key(0), old_wts(0), old_value1(0), old_value2(0), old_value3(0),
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

  // Storage for initial tuple addresses (indexed by key)
  std::vector<GlobalAddress> tuple_addresses_;
  SpinMutex tuple_addresses_mutex_;

  // For VERSION_CHAIN strategy: maintain version chain heads
  std::vector<GlobalAddress> version_chain_heads_;
  std::vector<std::unique_ptr<SpinMutex>> version_chain_locks_;
  // Store version chain metadata separately (timestamp + prev pointer)
  std::unordered_map<uint64_t, VersionChainMeta> version_chain_meta_;

  // For DELTA_IN_PAGE strategy: delta storage pages
  std::vector<GlobalAddress> delta_pages_;
  std::atomic<int> delta_page_index_{0};
  std::vector<std::unique_ptr<SpinMutex>> delta_page_locks_;
  std::vector<uint64_t> delta_page_offsets_;
  std::vector<std::unique_ptr<SpinMutex>> delta_offset_locks_;

  MVCCBenchmark(DDSM *ddsm, Cache *cache, StorageStrategy strategy)
      : ddsm_(ddsm), cache_(cache), strategy_(strategy) {

    // Set default gallocator for Table operations
    default_gallocator = ddsm_;

    // Create schema for benchmark table
    schema_ = new RecordSchema(0);
    std::vector<ColumnInfo *> columns;
    columns.push_back(new ColumnInfo("key", ValueType::UINT64));
    columns.push_back(new ColumnInfo("value1", ValueType::UINT64));
    columns.push_back(new ColumnInfo("value2", ValueType::UINT64));
    columns.push_back(new ColumnInfo("value3", ValueType::UINT64));
    schema_->BulkloadColumns(columns);
    size_t column_ids[1] = {0};
    schema_->SetPrimaryColumns(column_ids, 1);

    // Create table
    table_ = new Table();
    table_->Init(0, schema_, ddsm_);

    // Initialize storage structures
    tuple_addresses_.resize(kNumTuples);

    if (strategy_ == VERSION_CHAIN) {
      version_chain_heads_.resize(kNumTuples);
      for (uint64_t i = 0; i < kNumTuples; i++) {
        version_chain_locks_.emplace_back(new SpinMutex());
      }
    } else if (strategy_ == DELTA_IN_PAGE) {
      // Pre-allocate delta pages
      int num_delta_pages = std::max(10, kNumWriters * 2);
      for (int i = 0; i < num_delta_pages; i++) {
        GlobalAddress delta_page = ddsm_->Allocate_Remote(Regular_Page);
        delta_pages_.push_back(delta_page);
        delta_page_offsets_.push_back(STRUCT_OFFSET(DataPage, data_));
        delta_page_locks_.emplace_back(new SpinMutex());
        delta_offset_locks_.emplace_back(new SpinMutex());
      }
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

  // Initialize tuples
  void InitializeTuples() {
    printf("Node %d: Initializing %lu tuples...\n", ThisNodeID, kNumTuples);

    // Only node 0 creates tuples
    if (ThisNodeID != 0) {
      return;
    }

    for (uint64_t key = 0; key < kNumTuples; key++) {
      char tuple_buffer[schema_->GetRecordTotalSize()];
      memset(tuple_buffer, 0, schema_->GetRecordTotalSize());

      // Set key and initial values
      *(uint64_t *)(tuple_buffer) = key;
      *(uint64_t *)(tuple_buffer + 8) = key * 2;
      *(uint64_t *)(tuple_buffer + 16) = key * 3;
      *(uint64_t *)(tuple_buffer + 24) = key * 4;

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

      // Initialize version metadata
      Record *record = new Record(schema_, allocated_tuple);
      record->PutWTS(0); // Initial timestamp
      delete record;

      // Store tuple address
      tuple_addresses_[key] = tuple_gaddr;

      if (strategy_ == VERSION_CHAIN) {
        version_chain_heads_[key] = tuple_gaddr;
      }

      // Unlock page
      ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), handle);

      if (key % 10000 == 0 && key > 0) {
        printf("Initialized %lu / %lu tuples\n", key, kNumTuples);
      }
    }

    printf("Node %d: Tuple initialization complete\n", ThisNodeID);
  }

  // Write operation - creates new version
  void WriteOperation(int thread_id) {
    Random64 rand(thread_id * 12345 + ThisNodeID * 67890);
    auto start = std::chrono::high_resolution_clock::now();

    try {
      // Select random tuple
      uint64_t key = rand.Next() % kNumTuples;

      // Get current timestamp
      uint64_t commit_ts = GlobalTimestamp::FetchAddMonotoneTimestamp();

      if (strategy_ == VERSION_CHAIN) {
        WriteWithVersionChain(key, commit_ts, thread_id);
      } else if (strategy_ == DELTA_SECTION) {
        WriteWithDeltaSection(key, commit_ts, thread_id);
      } else if (strategy_ == DELTA_IN_PAGE) {
        WriteWithDeltaInPage(key, commit_ts, thread_id);
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
    Random64 rand(thread_id * 54321 + ThisNodeID * 98765);
    auto start = std::chrono::high_resolution_clock::now();

    try {
      // Select random tuple
      uint64_t key = rand.Next() % kNumTuples;

      // Get old snapshot (current_ts - lag)
      uint64_t current_ts = GlobalTimestamp::GetMonotoneTimestamp();
      uint64_t snapshot_ts =
          (current_ts > kSnapshotLag) ? (current_ts - kSnapshotLag) : 0;

      if (strategy_ == VERSION_CHAIN) {
        ReadWithVersionChain(key, snapshot_ts, thread_id);
      } else if (strategy_ == DELTA_SECTION) {
        ReadWithDeltaSection(key, snapshot_ts, thread_id);
      } else if (strategy_ == DELTA_IN_PAGE) {
        ReadWithDeltaInPage(key, snapshot_ts, thread_id);
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

  void WriteWithVersionChain(uint64_t key, uint64_t commit_ts, int thread_id) {
    version_chain_locks_[key]->lock();

    try {
      // Get current head of version chain
      GlobalAddress old_head = version_chain_heads_[key];

      // Allocate new tuple for new version
      GlobalAddress new_tuple_gaddr;
      Cache::Handle *new_handle;
      char *new_tuple_buffer;

      bool ret = table_->AllocateNewTuple(new_tuple_buffer, new_tuple_gaddr,
                                          new_handle, ddsm_, nullptr);
      if (!ret) {
        version_chain_locks_[key]->unlock();
        return;
      }

      // Read old version
      Cache::Handle *old_handle;
      void *old_page_buffer;
      ddsm_->SELCC_Shared_Lock(old_page_buffer, TOPAGE(old_head), old_handle);
      char *old_tuple_ptr =
          (char *)old_page_buffer + (old_head.offset - TOPAGE(old_head).offset);

      // Copy data from old version
      memcpy(new_tuple_buffer, old_tuple_ptr, schema_->GetRecordTotalSize());

      ddsm_->SELCC_Shared_UnLock(TOPAGE(old_head), old_handle);

      // Modify value (increment value1)
      *(uint64_t *)(new_tuple_buffer + 8) += 1;

      // Set timestamp in tuple
      Record *new_record = new Record(schema_, new_tuple_buffer);
      new_record->PutWTS(commit_ts);
      delete new_record;

      // Store version chain metadata separately
      VersionChainMeta meta;
      meta.version_ts = commit_ts;
      meta.prev_version = old_head;
      version_chain_meta_[new_tuple_gaddr.val] = meta;

      // Update version chain head (index needs to point to new version)
      version_chain_heads_[key] = new_tuple_gaddr;

      ddsm_->SELCC_Exclusive_UnLock(TOPAGE(new_tuple_gaddr), new_handle);

    } catch (...) {
      version_chain_locks_[key]->unlock();
      throw;
    }

    version_chain_locks_[key]->unlock();
  }

  void ReadWithVersionChain(uint64_t key, uint64_t snapshot_ts, int thread_id) {
    GlobalAddress current_gaddr = version_chain_heads_[key];
    int traversal_count = 0;

    // Traverse version chain to find visible version
    while (current_gaddr != GlobalAddress::Null()) {
      Cache::Handle *handle;
      void *page_buffer;
      ddsm_->SELCC_Shared_Lock(page_buffer, TOPAGE(current_gaddr), handle);

      char *tuple_ptr = (char *)page_buffer +
                        (current_gaddr.offset - TOPAGE(current_gaddr).offset);
      Record *record = new Record(schema_, tuple_ptr);

      uint64_t version_ts = record->GetWTS();
      delete record;

      // Get version chain metadata
      GlobalAddress prev_version = GlobalAddress::Null();
      auto meta_it = version_chain_meta_.find(current_gaddr.val);
      if (meta_it != version_chain_meta_.end()) {
        prev_version = meta_it->second.prev_version;
      }

      if (version_ts <= snapshot_ts) {
        // Found visible version - read the value
        uint64_t value1 = *(uint64_t *)(tuple_ptr + 8);
        (void)value1; // Suppress unused warning

        ddsm_->SELCC_Shared_UnLock(TOPAGE(current_gaddr), handle);

        version_chain_traversals.fetch_add(traversal_count);
        return;
      }

      ddsm_->SELCC_Shared_UnLock(TOPAGE(current_gaddr), handle);

      // Move to previous version
      current_gaddr = prev_version;
      traversal_count++;
    }

    version_chain_traversals.fetch_add(traversal_count);
  }

  // ==================== DELTA SECTION IMPLEMENTATION ====================

  void WriteWithDeltaSection(uint64_t key, uint64_t commit_ts, int thread_id) {
    GlobalAddress tuple_gaddr = tuple_addresses_[key];

    Cache::Handle *handle;
    void *page_buffer;
    ddsm_->SELCC_Exclusive_Lock(page_buffer, TOPAGE(tuple_gaddr), handle);

    char *tuple_ptr =
        (char *)page_buffer + (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
    Record *record = new Record(schema_, tuple_ptr);

    // Update value
    uint64_t old_value = *(uint64_t *)(tuple_ptr + 8);
    *(uint64_t *)(tuple_ptr + 8) = old_value + 1;

    // Mark columns as dirty (for delta creation)
    record->dirty_col_ids.insert(1);

    // Update timestamp
    // In real MVOCC, delta record would be created in dedicated delta section
    // For benchmark: just update WTS to simulate
    record->PutWTS(commit_ts);

    delete record;
    ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), handle);
  }

  void ReadWithDeltaSection(uint64_t key, uint64_t snapshot_ts, int thread_id) {
    GlobalAddress tuple_gaddr = tuple_addresses_[key];

    Cache::Handle *handle;
    void *page_buffer;
    ddsm_->SELCC_Shared_Lock(page_buffer, TOPAGE(tuple_gaddr), handle);

    char *tuple_ptr =
        (char *)page_buffer + (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
    Record *record = new Record(schema_, tuple_ptr);

    uint64_t current_ts = record->GetWTS();

    if (current_ts <= snapshot_ts) {
      // Version is visible
      uint64_t value = *(uint64_t *)(tuple_ptr + 8);
      (void)value;
    } else {
      // Would need to apply deltas from delta section
      // Simulate delta application
      delta_applications.fetch_add(1);
    }

    delete record;
    ddsm_->SELCC_Shared_UnLock(TOPAGE(tuple_gaddr), handle);
  }

  // ==================== DELTA IN PAGE IMPLEMENTATION ====================

  void WriteWithDeltaInPage(uint64_t key, uint64_t commit_ts, int thread_id) {
    GlobalAddress tuple_gaddr = tuple_addresses_[key];

    // Select a delta page (round-robin per key to distribute load)
    int delta_idx = (key % delta_pages_.size());
    GlobalAddress delta_page_gaddr = delta_pages_[delta_idx];

    // Lock both tuple page and delta page (causes contention!)
    Cache::Handle *tuple_handle;
    Cache::Handle *delta_handle;
    void *tuple_page_buffer;
    void *delta_page_buffer;

    // Lock tuple page
    ddsm_->SELCC_Exclusive_Lock(tuple_page_buffer, TOPAGE(tuple_gaddr),
                                tuple_handle);

    // Lock delta page - THIS CAUSES SELCC INVALIDATIONS!
    ddsm_->SELCC_Exclusive_Lock(delta_page_buffer, delta_page_gaddr,
                                delta_handle);

    char *tuple_ptr = (char *)tuple_page_buffer +
                      (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
    Record *record = new Record(schema_, tuple_ptr);

    // Save old values to delta page
    uint64_t old_wts = record->GetWTS();
    uint64_t old_value1 = *(uint64_t *)(tuple_ptr + 8);
    uint64_t old_value2 = *(uint64_t *)(tuple_ptr + 16);
    uint64_t old_value3 = *(uint64_t *)(tuple_ptr + 24);

    // Allocate space in delta page
    delta_offset_locks_[delta_idx]->lock();
    uint64_t delta_offset = delta_page_offsets_[delta_idx];
    delta_page_offsets_[delta_idx] += sizeof(InPageDelta);

    // Check if delta page is full
    if (delta_offset + sizeof(InPageDelta) > kLeafPageSize) {
      // Reset to beginning (circular)
      delta_page_offsets_[delta_idx] =
          STRUCT_OFFSET(DataPage, data_) + sizeof(InPageDelta);
      delta_offset = STRUCT_OFFSET(DataPage, data_);
    }
    delta_offset_locks_[delta_idx]->unlock();

    // Write delta record to delta page
    char *delta_ptr = (char *)delta_page_buffer + delta_offset;
    InPageDelta *delta = new (delta_ptr) InPageDelta();
    delta->tuple_key = key;
    delta->old_wts = old_wts;
    delta->old_value1 = old_value1;
    delta->old_value2 = old_value2;
    delta->old_value3 = old_value3;
    delta->next_delta = GlobalAddress::Null(); // Could link to previous delta

    // Update tuple with new values
    *(uint64_t *)(tuple_ptr + 8) = old_value1 + 1;
    record->PutWTS(commit_ts);

    delete record;

    // Unlock in order - generates SELCC invalidation messages!
    ddsm_->SELCC_Exclusive_UnLock(delta_page_gaddr, delta_handle);
    ddsm_->SELCC_Exclusive_UnLock(TOPAGE(tuple_gaddr), tuple_handle);
  }

  void ReadWithDeltaInPage(uint64_t key, uint64_t snapshot_ts, int thread_id) {
    GlobalAddress tuple_gaddr = tuple_addresses_[key];

    // Read tuple
    Cache::Handle *tuple_handle;
    void *tuple_page_buffer;
    ddsm_->SELCC_Shared_Lock(tuple_page_buffer, TOPAGE(tuple_gaddr),
                             tuple_handle);

    char *tuple_ptr = (char *)tuple_page_buffer +
                      (tuple_gaddr.offset - TOPAGE(tuple_gaddr).offset);
    Record *record = new Record(schema_, tuple_ptr);

    uint64_t current_ts = record->GetWTS();

    if (current_ts > snapshot_ts) {
      // Need to access delta page - THIS ALSO CAUSES INVALIDATIONS!
      int delta_idx = (key % delta_pages_.size());
      GlobalAddress delta_page_gaddr = delta_pages_[delta_idx];

      Cache::Handle *delta_handle;
      void *delta_page_buffer;
      ddsm_->SELCC_Shared_Lock(delta_page_buffer, delta_page_gaddr,
                               delta_handle);

      // Search for matching delta (simplified - would traverse chain)
      // For benchmark: just simulate reading delta
      delta_applications.fetch_add(1);

      ddsm_->SELCC_Shared_UnLock(delta_page_gaddr, delta_handle);
    } else {
      // Current version is visible
      uint64_t value = *(uint64_t *)(tuple_ptr + 8);
      (void)value;
    }

    delete record;
    ddsm_->SELCC_Shared_UnLock(TOPAGE(tuple_gaddr), tuple_handle);
  }
};

// ==================== WORKER THREADS ====================

void WriterThread(MVCCBenchmark *benchmark, int thread_id) {
  bindCore(thread_id);

  // Wait for benchmark to be ready
  while (!benchmark_ready.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  printf("Writer thread %d started\n", thread_id);

  while (benchmark_running.load()) {
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
    benchmark->ReadOperation(thread_id);
  }

  printf("Reader thread %d finished\n", thread_id);
}

// ==================== MAIN ====================

void ParseArgs(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--writers") == 0 && i + 1 < argc) {
      kNumWriters = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--readers") == 0 && i + 1 < argc) {
      kNumReaders = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--storage_type") == 0 && i + 1 < argc) {
      kStorageType = atoi(argv[++i]);
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
    } else {
      fprintf(stderr, "Unknown argument: %s\n", argv[i]);
      fprintf(stderr,
              "Usage: %s [--writers N] [--readers N] [--storage_type 1|2|3] "
              "[--node_id N] [--tcp_port N] [--num_tuples N] [--duration N] "
              "[--cache_size N]\n",
              argv[0]);
    }
  }

  printf("Configuration:\n");
  printf("  Writers: %d\n", kNumWriters);
  printf("  Readers: %d\n", kNumReaders);
  printf("  Storage Type: %d\n", kStorageType);
  printf("  Node ID: %d\n", ThisNodeID);
  printf("  Num Tuples: %lu\n", kNumTuples);
  printf("  Duration: %d seconds\n", kBenchmarkDurationSec);
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
  case DELTA_IN_PAGE:
    printf("Delta in SELCC Pages\n");
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

  // Initialize tuples
  benchmark.InitializeTuples();

  // Synchronize all nodes
  printf("Node %d: Synchronizing with other nodes...\n", ThisNodeID);
  rdma_mg->sync_with_computes_Cside();
  printf("Node %d: Synchronization complete\n", ThisNodeID);

  // Reset cache statistics
  for (int i = 0; i < MAX_APP_THREAD; i++) {
    cache_invalidation[i] = 0;
    for (int j = 0; j < 8; j++) {
      cache_hit_valid[i][j] = 0;
    }
  }

  // Start threads
  std::vector<std::thread> threads;

  for (int i = 0; i < kNumWriters; i++) {
    threads.emplace_back(WriterThread, &benchmark, i);
  }

  for (int i = 0; i < kNumReaders; i++) {
    threads.emplace_back(ReaderThread, &benchmark, i);
  }

  // Give threads a moment to initialize
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Start benchmark
  printf("\nStarting benchmark for %d seconds...\n", kBenchmarkDurationSec);
  benchmark_ready.store(true);

  Timer bench_timer;
  bench_timer.begin();

  // Run benchmark
  std::this_thread::sleep_for(std::chrono::seconds(kBenchmarkDurationSec));

  uint64_t actual_duration_ns = bench_timer.end();
  double actual_duration_sec = actual_duration_ns / 1e9;

  // Stop threads
  benchmark_running.store(false);

  for (auto &thread : threads) {
    thread.join();
  }

  // Collect cache invalidation statistics
  uint64_t total_invalidations = 0;
  uint64_t total_cache_hits = 0;
  for (int i = 0; i < MAX_APP_THREAD; i++) {
    total_invalidations += cache_invalidation[i];
    total_cache_hits += cache_hit_valid[i][0];
  }

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
  } else if (kStorageType == DELTA_SECTION || kStorageType == DELTA_IN_PAGE) {
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

  return 0;
}
