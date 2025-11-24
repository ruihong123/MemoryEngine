#include "TransactionExecutor.h"

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

  while (hot_scan_should_run_.load(std::memory_order_acquire)) {
    for (auto &task : hot_scan_tasks_) {
      if (!hot_scan_should_run_.load(std::memory_order_acquire)) {
        break;
      }
      task.run_once(scanner_manager);
    }
  }
  
  // Commit any pending transaction before exiting
  if (scanner_manager.HasActiveTransaction()) {
    CharArray ret;
    scanner_manager.CommitTransaction(ret);
  }
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
