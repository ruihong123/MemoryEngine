#include "TransactionExecutor.h"
#include <chrono>
#include <atomic>
#include <memory>

namespace DSMEngine {
// Define static partition parameters
int TransactionExecutor::static_partition_start_ = 1;
int TransactionExecutor::static_partition_end_ = 1;
int TransactionExecutor::static_num_items_per_partition_ = 1;
int TransactionExecutor::static_partition_key_bits_ = 48;

void TransactionExecutor::StartHotTableScanner() {
  if (hot_scan_thread_started_) {
    StopHotTableScanner();
  }
  hot_scan_tasks_.clear();
  ConfigureHotTableScanner(hot_scan_tasks_);
  if (hot_scan_tasks_.empty()) {
    hot_scan_should_run_.store(false, std::memory_order_release);
    hot_scan_thread_started_ = false;
    return;
  }
  hot_scan_should_run_.store(true, std::memory_order_release);
  hot_scan_thread_ = std::thread(&TransactionExecutor::HotTableScannerMain, this);
  hot_scan_thread_started_ = true;
}

void TransactionExecutor::StopHotTableScanner() {
  hot_scan_should_run_.store(false, std::memory_order_release);
  if (hot_scan_thread_started_) {
    if (hot_scan_thread_.joinable()) {
      hot_scan_thread_.join();
    }
    hot_scan_thread_started_ = false;
  }
  hot_scan_tasks_.clear();
}

void TransactionExecutor::HotTableScannerMain() {
  bindCore(thread_count_ + 1);
  TransactionManager scanner_manager(
      storage_manager_, this->thread_count_, this->thread_count_, log_enabled_,
      TWOPHASECOMMIT, GetPartitionStart(), GetPartitionEnd(),
      GetNumItemsPerPartition(), GetPartitionKeyBits());

  while (hot_scan_should_run_.load(std::memory_order_acquire) && !is_begin_) {
    std::this_thread::yield();
  }

  size_t scan_transaction_count = 0;  // Only count successfully committed transactions
  size_t scan_abort_count = 0;  // Count aborted transactions
  uint64_t total_latency_ns = 0;  // Latency for ALL transactions (committed + aborted)
  
  // Run tasks in round-robin fashion - each task runs until a transaction completes
  while (hot_scan_should_run_.load(std::memory_order_acquire)) {
    for (auto &task : hot_scan_tasks_) {
      if (!hot_scan_should_run_.load(std::memory_order_acquire)) {
        break;
      }
      
      // Track transaction start time for latency measurement
      uint64_t txn_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch()).count();
      
      // Run the task - it contains its own loop and returns when a transaction completes
      // Returns true if committed, false if aborted
      bool committed = task.run(scanner_manager, hot_scan_should_run_);
      
      // Record latency for this transaction (both committed and aborted)
      auto txn_end = std::chrono::high_resolution_clock::now();
      auto txn_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          txn_end.time_since_epoch()).count();
      uint64_t txn_latency_ns = txn_end_ns - txn_start_ns;
      total_latency_ns += txn_latency_ns;
      
      // Count the transaction
      if (committed) {
        scan_transaction_count++;
      } else {
        scan_abort_count++;
      }
      
      // // Print hot table scan info
      // double txn_latency_us = txn_latency_ns / 1000.0;
      // size_t total_txns = scan_transaction_count + scan_abort_count;
      // double avg_latency_us = total_txns > 0 ? (total_latency_ns / 1000.0) / total_txns : 0.0;
      // std::cout << "[HotTableScan] Task: " << task.name 
      //           << ", Status: " << (committed ? "COMMITTED" : "ABORTED")
      //           << ", Latency: " << txn_latency_us << " us"
      //           << ", Total: " << scan_transaction_count << " committed, " 
      //           << scan_abort_count << " aborted"
      //           << ", Avg Latency: " << avg_latency_us << " us" << std::endl;
    }
  }
  
  // Commit any pending transaction before exiting
  if (scanner_manager.HasActiveTransaction()) {
    auto txn_start = std::chrono::high_resolution_clock::now();
    CharArray ret;
    bool committed = scanner_manager.CommitTransaction(ret);
    auto txn_end = std::chrono::high_resolution_clock::now();
    
    // Record latency for this final transaction
    auto txn_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        txn_start.time_since_epoch()).count();
    auto txn_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        txn_end.time_since_epoch()).count();
    uint64_t txn_latency_ns = txn_end_ns - txn_start_ns;
    total_latency_ns += txn_latency_ns;
    
    if (committed) {
      scan_transaction_count++;
    } else {
      scan_abort_count++;
    }
  }
  
  // Update hot scan statistics
  hot_scan_count_.fetch_add(scan_transaction_count, std::memory_order_relaxed);
  hot_scan_abort_count_.fetch_add(scan_abort_count, std::memory_order_relaxed);
  hot_scan_total_latency_ns_.fetch_add(total_latency_ns, std::memory_order_relaxed);
}

void TransactionExecutor::EnableHotTableScanner(bool enabled) {
  if (enable_hot_scan_ == enabled) {
    return;
  }
  enable_hot_scan_ = enabled;
  if (!enable_hot_scan_) {
    StopHotTableScanner();
  }
}
} // namespace DSMEngine
