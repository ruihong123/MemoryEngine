#include "BenchmarkArguments.h"
#include "ClusterHelper.h"
#include "ClusterSync.h"
#include "Meta.h"
#include "TpccConstants.h"
#include "TpccExecutor.h"
#include "TpccInitiator.h"
#include "TpccParams.h"
#include "TpccPopulator.h"
#include "TpccSource.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>

using namespace DSMEngine::TpccBenchmark;
using namespace DSMEngine;

void ExchPerfStatistics(ClusterConfig *config, ClusterSync *synchronizer,
                        PerfStatistics *s);


// Helper function to evict all pages for logical regions whose primary is on failed node
void HardInvalidatePagesForFailedNode(uint16_t failed_node) {
  auto rdma_mg = default_gallocator->rdma_mg;
  auto logical_regions = rdma_mg->GetLogicalRegionsWithPrimaryOnNode(failed_node);
  auto cache = rdma_mg->page_cache_;
  
  std::cout << "Evicting pages for " << logical_regions.size() 
            << " logical regions affected by failed node " << failed_node << std::endl;
  
  for (uint16_t logical_id : logical_regions) {
    // Evict all pages for this logical region without flushing
    cache->HardInvalidateByLogicalId(logical_id);
  }
  
  std::cout << "Finished evicting pages for failed node" << std::endl;
}

// Helper function to invalidate all pages
void hardInvalidateAllPages() {
  auto rdma_mg = default_gallocator->rdma_mg;
  auto cache = rdma_mg->page_cache_;
  
  std::cout << "Invalidating all cache entries" << std::endl;
  
  // Use the efficient single-pass invalidation API
  cache->HardInvalidateAll();
  
  std::cout << "Finished invalidating all pages" << std::endl;
}

extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];
extern uint64_t cache_miss[MAX_APP_THREAD][8];

//#if defined(MVOCC)
// extern uint64_t delta_pull_num[MAX_APP_THREAD];
//#endif
void clear_cache_statistics() {
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    cache_invalidation[i] = 0;
#if defined(MVOCC)
    delta_pull_num[i] = 0;
    roll_back_num[i] = 0;
    tuple_read_num[i] = 0;
#endif
    for (int j = 0; j < 8; ++j) {
      cache_hit_valid[i][j] = 0;
      cache_miss[i][j] = 0;
    }
  }
}
int main(int argc, char *argv[]) {
  ArgumentsParser(argc, argv);

  //  std::string my_host_name = ClusterHelper::GetLocalHostName();
  ClusterConfig config(my_host_name, conn_port, config_filename);
  ClusterSync synchronizer(&config);
  FillScaleParams(config);
  PrintScaleParams();

  TpccInitiator initiator(gThreadCount, &config);
  // initialize GAM storage layer
  initiator.InitGAllocator();
  // the RDMA Manager have a synchronization accross the nodes.
  if (enable_failure_recovery) {
    std::cout << "[FAILURE_RECOVERY] Setting replication mode to REPLICA_WRITE_ALL for population" << std::endl;
    default_gallocator->rdma_mg->SetReplicaType(REPLICA_WRITE_ALL);
  }
  // initialize benchmark data
  char *storage_addr = initiator.InitStorage();
  assert(storage_addr);
  char storage_key[16] = "Storage Key";
  //  default_gallocator->memSet(storage_key, 16, storage_addr,
  //  TableDirectory::GetSerializeSize());
  synchronizer.MasterBroadcast(storage_key, 16, storage_addr,
                               TableDirectory::GetSerializeSize());

  std::cout << "storage_addr=" << storage_addr << std::endl;
  TableDirectory storage_manager;
  storage_manager.Deserialize(storage_addr);
  default_gallocator->rdma_mg->sync_with_computes_Cside();
  
  // Failure recovery mode: set replication to ALL during population
  int original_replica_type = default_gallocator->rdma_mg->GetReplicaType();
  
  
  // Disable redo logging during database population
  bool original_enable_logging = enable_logging;
  enable_logging = false;
  std::cout << "[POPULATION] Disabling redo logging during database population" << std::endl;
  
  // populate database
  INIT_PROFILE_TIME(gThreadCount);
  TpccPopulator populator(&storage_manager, &tpcc_scale_params);
  populator.Start();
  REPORT_PROFILE_TIME(gThreadCount);
  
  // Restore logging setting after population
  enable_logging = original_enable_logging;
  if (enable_logging) {
    std::cout << "[POPULATION] Re-enabled redo logging after database population" << std::endl;
  }
  // TODO: it seems that FenceXComputes did not do the synchronization.
  synchronizer.FenceXComputes();
  
  // Failure recovery mode: flush all dirty pages after population
  if (enable_failure_recovery) {
    // Release cached root handles for all btree indexes before flushing
    std::cout << "[FAILURE_RECOVERY] Releasing cached root handles for all btree indexes..." << std::endl;
    for (size_t i = 0; i < storage_manager.GetTableCount(); ++i) {
      Table* table = storage_manager.tables_[i];
      if (table != nullptr) {
        Btr* primary_index = table->GetPrimaryIndex();
        if (primary_index != nullptr) {
          primary_index->release_cached_root_handle();
        }
      }
    }
    std::cout << "[FAILURE_RECOVERY] Flushing all dirty pages after population..." << std::endl;
    default_gallocator->rdma_mg->page_cache_->SoftFlushAllDirtyPages();
    synchronizer.FenceXComputes();
    std::cout << "[FAILURE_RECOVERY] Finished flushing dirty pages" << std::endl;
  }
  //    WORKLOAD_PATTERN == PARTITION_SOURCE
  if (TWOPHASECOMMIT) {
    auto func = std::bind(&TpccExecutor::ProcessQueryThread_2PC_Participant,
                          (void *)&storage_manager, std::placeholders::_1);
    default_gallocator->rdma_mg->Set_message_handling_func(func, TwoPC);
  }
  // generate workload
  IORedirector redirector1(gThreadCount);
  size_t sourcer1_txn_count = enable_failure_recovery ? (num_txn) : (num_txn/4);
  TpccSource sourcer1(&tpcc_scale_params, &redirector1, sourcer1_txn_count,
                      WORKLOAD_PATTERN, gThreadCount, dist_ratio,
                      config.GetMyPartitionId());
  sourcer1.Start();
  synchronizer.FenceXComputes();

  // Failure recovery mode: switch to PRIMARY_ONLY and enable logging for warm-up and testing
  if (enable_failure_recovery) {
    std::cout << "[FAILURE_RECOVERY] Setting replication mode to REPLICA_WRITE_PRIMARY_ONLY for warm-up and testing" << std::endl;
    default_gallocator->rdma_mg->SetReplicaType(REPLICA_WRITE_PRIMARY_ONLY);
    enable_logging = true;  // Enable redo logging for failure recovery test
  }

  {
    // warm up - use 8 threads for warmup phase
    constexpr size_t warmup_thread_count = 8;
    IORedirector warmup_redirector(warmup_thread_count);
    TpccSource warmup_sourcer(&tpcc_scale_params, &warmup_redirector, num_txn, WORKLOAD_PATTERN,
                              warmup_thread_count, dist_ratio, config.GetMyPartitionId());
    warmup_sourcer.Start();
    synchronizer.FenceXComputes();
    
    INIT_PROFILE_TIME(warmup_thread_count);
    TpccExecutor executor(&warmup_redirector, &storage_manager, warmup_thread_count, enable_failure_recovery ? true : false);
    executor.EnableProgressReporting(true);
    executor.Start();
    REPORT_PROFILE_TIME(warmup_thread_count);
  }
  synchronizer.FenceXComputes();
  // clear the cache statistics.
  clear_cache_statistics();
  
  if (enable_failure_recovery) {
    // Failure recovery test: run for 5 seconds, then trigger failure
    std::cout << "[FAILURE_RECOVERY] Starting failure recovery test..." << std::endl;
    if (failure_recovery_type == 0) {
      std::cout << "[FAILURE_RECOVERY] Will run for 5 seconds, then trigger memory node failure" << std::endl;
    } else {
      std::cout << "[FAILURE_RECOVERY] Will run for 5 seconds, then trigger compute node failure" << std::endl;
    }
    
    // Create executor for failure recovery test
    TpccExecutor executor(&redirector1, &storage_manager, gThreadCount, enable_logging,
                          enable_latency_recording);
    executor.EnableProgressReporting(true);
    
    // Set transaction type names for latency tracking (if enabled)
    if (enable_latency_recording) {
      std::map<size_t, std::string> txn_names;
      txn_names[DELIVERY] = "DELIVERY";
      txn_names[NEW_ORDER] = "NEW_ORDER";
      txn_names[PAYMENT] = "PAYMENT";
      txn_names[ORDER_STATUS] = "ORDER_STATUS";
      txn_names[STOCK_LEVEL] = "STOCK_LEVEL";
      executor.SetTxnTypeNames(txn_names);
    }
    executor.EnableHotTableScanner(enable_hot_table_scanner);
    
    // Start executor in a separate thread
    std::atomic<bool> executor_running(true);
    std::thread executor_thread([&executor, &executor_running]() {
      executor.Start();
      executor_running.store(false);
    });
    
    // Start dedicated throughput monitoring thread (runs independently, even when executor is paused)
    auto start_time = std::chrono::steady_clock::now();
    std::atomic<bool> monitoring_running(true);
    std::thread monitoring_thread([&executor, &monitoring_running, start_time]() {
      auto last_report_time = start_time;
      uint64_t last_txn_count = 0;
      const int report_interval_ms = 200;
      
      // Continue monitoring until explicitly stopped (runs independently of executor pause state)
      while (monitoring_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(report_interval_ms));
        
        auto now = std::chrono::steady_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_report_time).count();
        
        // Get current transaction count from executor's real-time finished counter
        // This counter is updated immediately when transactions finish (for failure recovery test)
        uint64_t current_txn_count = executor.GetRealtimeFinishedCount();
        uint64_t txn_delta = current_txn_count - last_txn_count;
        double throughput = (txn_delta * 1000.0) / elapsed_ms;  // transactions per second
        
        auto total_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
        bool is_paused = executor.IsPaused();
        printf("[FAILURE_RECOVERY] [%ld ms] Throughput: %.2f tps (total: %lu txns)%s\n", 
               total_elapsed, throughput, current_txn_count,
               is_paused ? " [PAUSED]" : "");
        fflush(stdout);
        
        last_report_time = now;
        last_txn_count = current_txn_count;
      }
    });
    
    // Wait for 5 seconds before triggering failure
    const int test_duration_sec = 5;
    std::this_thread::sleep_for(std::chrono::seconds(test_duration_sec));
    
    // Failure detection and notification: compute node 0 detects failure and broadcasts it
    uint16_t failed_node = 0; // not a real value, it will be replaced later.
    if (config.IsMaster()) {
      if (failure_recovery_type == 0) {
        // Memory node failure: find last memory node and mark it as failed
        failed_node = default_gallocator->rdma_mg->GetLastMemoryNodeId();
        std::cout << "\n[FAILURE_RECOVERY] Node 0: Detected failure of memory node " << failed_node << std::endl;
      } else {
        // Compute node failure: find last compute node and mark it as failed
        std::vector<uint16_t> compute_node_ids = default_gallocator->rdma_mg->GetAllComputeNodeIds();
        if (!compute_node_ids.empty()) {
          failed_node = *std::max_element(compute_node_ids.begin(), compute_node_ids.end());
        }
        std::cout << "\n[FAILURE_RECOVERY] Node 0: Detected failure of compute node " << failed_node << std::endl;
      }
      std::cout << "[FAILURE_RECOVERY] Node 0: Broadcasting failure notification to all compute nodes..." << std::endl;
    }
    
    // Broadcast failed node ID to all compute nodes via barrier
    synchronizer.MasterBroadcast<uint16_t>(&failed_node);
    
    // All compute nodes receive the failure notification
    if (!config.IsMaster()) {
      if (failure_recovery_type == 0) {
        std::cout << "\n[FAILURE_RECOVERY] Node " << config.GetMyPartitionId() 
                  << ": Received failure notification for memory node " << failed_node << std::endl;
      } else {
        std::cout << "\n[FAILURE_RECOVERY] Node " << config.GetMyPartitionId() 
                  << ": Received failure notification for compute node " << failed_node << std::endl;
      }
    }
    
    // All compute nodes pause their executors
    auto elapsed_pause = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    std::cout << "[FAILURE_RECOVERY] [" << elapsed_pause << " ms] Pausing transaction execution on all compute nodes..." << std::endl;
    executor.Pause();
    
    // Step 1: Force flush all remaining redo logs from local memory to remote memory
    // This is critical to ensure all logs are persisted before evicting pages
    auto elapsed_step1 = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    std::cout << "[FAILURE_RECOVERY] [" << elapsed_step1 << " ms] Step 1: Flushing all remaining redo logs to remote memory..." << std::endl;
    default_gallocator->GetRedoLogger(true)->FlushAllBuffers(false);
    synchronizer.FenceXComputes();// this memory fences are necessary to avoid dangling compute node at executor.Pause().
    
    if (failure_recovery_type == 0) {
      // Memory node failure recovery
      // Step 2: All compute nodes evict pages for failed node
      auto elapsed_step2 = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start_time).count();
      std::cout << "[FAILURE_RECOVERY] [" << elapsed_step2 << " ms] Step 2: Invalidate the cached GCLs for failed memory node..." << std::endl;
      HardInvalidatePagesForFailedNode(failed_node);
      
      // Step 3: Adjust logical groups to remove failed node and promote replica
      auto elapsed_step3 = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start_time).count();
      std::cout << "[FAILURE_RECOVERY] [" << elapsed_step3 << " ms] Step 3: Adjusting logical groups..." << std::endl;
      default_gallocator->rdma_mg->RemoveFailedMemoryNodeFromLogicalGroups(failed_node);
      
      // Step 4: Wait for all memory nodes to finish replaying logs
      auto elapsed_step4 = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start_time).count();
      std::cout << "[FAILURE_RECOVERY] [" << elapsed_step4 << " ms] Step 4: Waiting for all memory nodes to finish replaying logs..." << std::endl;
      default_gallocator->GetRedoLogger(true)->WaitForAllMemoryNodesReplayComplete();
    } else {
      // Compute node failure recovery
      // Step 2: Invalidate all pages for failed compute node (on local compute nodes)
      auto elapsed_step2 = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start_time).count();
      std::cout << "[FAILURE_RECOVERY] [" << elapsed_step2 << " ms] Step 2: Invalidating all pages for failed compute node..." << std::endl;
      hardInvalidateAllPages();
      
      // Step 3: Hard remove primary copy from all logical groups
      auto elapsed_step3 = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start_time).count();
      std::cout << "[FAILURE_RECOVERY] [" << elapsed_step3 << " ms] Step 3: Hard removing primary copy from all logical groups..." << std::endl;
      default_gallocator->rdma_mg->HardRemovePrimaryFromAllLogicalGroups();
      
      // Step 4: Wait for all memory nodes to finish replaying logs
      auto elapsed_step4 = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start_time).count();
      std::cout << "[FAILURE_RECOVERY] [" << elapsed_step4 << " ms] Step 4: Waiting for all memory nodes to finish replaying logs..." << std::endl;
      default_gallocator->GetRedoLogger(true)->WaitForAllMemoryNodesReplayComplete();
    }
    
    auto elapsed_complete = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    std::cout << "[FAILURE_RECOVERY] [" << elapsed_complete << " ms] Failure recovery complete. Resuming execution..." << std::endl;
    
    // All compute nodes resume their executors
    executor.Resume();
    // synchronizer.FenceXComputes();
    std::cout << "[FAILURE_RECOVERY] All compute nodes resumed transaction execution." << std::endl;
    
    // Wait for executor to finish (monitoring thread continues independently)
    executor_thread.join();
    
    // Stop monitoring thread
    monitoring_running.store(false);
    monitoring_thread.join();
    
    REPORT_PROFILE_TIME(gThreadCount);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ExchPerfStatistics(&config, &synchronizer, &executor.GetPerfStatistics());
    
    // Restore original replica type
    default_gallocator->rdma_mg->SetReplicaType(original_replica_type);
  } else {
    // Normal benchmark execution
    {
      // run workload
      INIT_PROFILE_TIME(gThreadCount);
      TpccExecutor executor(&redirector1, &storage_manager, gThreadCount, enable_logging,
                            enable_latency_recording);
      executor.EnableProgressReporting(true);

    // Set transaction type names for latency tracking (if enabled)
    if (enable_latency_recording) {
      std::map<size_t, std::string> txn_names;
      txn_names[DELIVERY] = "DELIVERY";
      txn_names[NEW_ORDER] = "NEW_ORDER";
      txn_names[PAYMENT] = "PAYMENT";
      txn_names[ORDER_STATUS] = "ORDER_STATUS";
      txn_names[STOCK_LEVEL] = "STOCK_LEVEL";
      executor.SetTxnTypeNames(txn_names);
    }
      executor.EnableHotTableScanner(enable_hot_table_scanner);

      executor.Start();
      REPORT_PROFILE_TIME(gThreadCount);
      ExchPerfStatistics(&config, &synchronizer, &executor.GetPerfStatistics());
    }
  }

  std::cout << "prepare to exit..." << std::endl;
  std::cout << "wait for all memory nodes to replay complete..." << std::endl;
  default_gallocator->GetRedoLogger(true)->FlushAllBuffers(false);
  default_gallocator->GetRedoLogger(true)->WaitForAllMemoryNodesReplayComplete();
  synchronizer.Fence_XALLNodes();
  default_gallocator->rdma_mg->join_all_handling_thread();
  std::cout << "over.." << std::endl;
  free(storage_addr);
  return 0;
}

void ExchPerfStatistics(ClusterConfig *config, ClusterSync *synchronizer,
                        PerfStatistics *s) {
  // Save local latency data before collecting stats across nodes
  // (latency_trackers_ cannot be serialized via memcpy)
  auto local_latency_trackers = s->latency_trackers_;
  auto local_txn_type_names = s->txn_type_names_;
  
  PerfStatistics *stats = new PerfStatistics[config->GetPartitionNum()];
  synchronizer->MasterCollect<PerfStatistics>(s, stats);
  synchronizer->MasterBroadcast<PerfStatistics>(stats);
  
  // Reconstruct corrupted map objects from memcpy using placement new
  // This overwrites the corrupted maps without trying to traverse/delete them
  for (size_t i = 0; i < config->GetPartitionNum(); ++i) {
    new (&stats[i].latency_trackers_) std::map<size_t, LatencyTracker>();
    new (&stats[i].txn_type_names_) std::map<size_t, std::string>();
  }
  
  for (size_t i = 0; i < config->GetPartitionNum(); ++i) {
    stats[i].Print();
    stats[0].Aggregate(stats[i]);
  }
  if (config->IsMaster()) {
    // Move (not copy) latency data from master node's local data
    stats[0].latency_trackers_ = std::move(local_latency_trackers);
    stats[0].txn_type_names_ = std::move(local_txn_type_names);
    // Calculate percentiles before printing
    stats[0].CalculateLatencyPercentiles();
    stats[0].PrintAgg();
  }
  delete[] stats;
  stats = nullptr;
}
