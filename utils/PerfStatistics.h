// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_UTILS_PERFORMANCE_STATISTICS_H__
#define __DATABASE_UTILS_PERFORMANCE_STATISTICS_H__

#include "BenchmarkArguments.h"
#include "LatencyTracker.h"
#include "TransactionManager.h"
#include <cstdio>
#include <iostream>
#include <map>
#include <string>
extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];
extern uint64_t cache_miss[MAX_APP_THREAD][8];
//#if defined(MVOCC)
// extern uint64_t delta_pull_num[MAX_APP_THREAD];
//#endif
namespace DSMEngine {
struct PerfStatistics {
  PerfStatistics() {
    total_count_ = 0;
    total_abort_count_ = 0;
    thread_count_ = 0;
    elapsed_time_ = 0;
    throughput_ = 0.0;

    hot_scan_count_ = 0;
    hot_scan_throughput_ = 0.0;
    hot_scan_abort_count_ = 0;
    hot_scan_avg_latency_ = 0.0;

    agg_total_count_ = 0;
    agg_thread_count_ = 0;
    agg_total_abort_count_ = 0;
    agg_elapsed_time_ = 0;
    agg_node_num_ = 0;
    longest_elapsed_time_ = 0;
    agg_throughput_ = 0.0;
    agg_hot_scan_count_ = 0;
    agg_hot_scan_throughput_ = 0.0;
    agg_hot_scan_abort_count_ = 0;
    agg_hot_scan_avg_latency_ = 0.0;
  }

  // Set transaction type names for latency reporting
  void SetTxnTypeNames(const std::map<size_t, std::string> &names) {
    txn_type_names_ = names;
  }

  // Merge latency data from per-thread tracker
  void MergeLatencyData(size_t txn_type, const LatencyTracker &tracker) {
    latency_trackers_[txn_type].Merge(tracker);
  }

  // Calculate all percentiles (call after all data is merged)
  void CalculateLatencyPercentiles() {
    for (auto &entry : latency_trackers_) {
      entry.second.CalculatePercentiles();
    }
  }

  // Print latency statistics
  void PrintLatencyStats() {
    if (latency_trackers_.empty()) {
      return;
    }

    std::cout
        << "\n==================== Latency Statistics ===================="
        << std::endl;
    for (auto &entry : latency_trackers_) {
      size_t txn_type = entry.first;
      auto it = txn_type_names_.find(txn_type);
      const char *txn_name =
          it != txn_type_names_.end() ? it->second.c_str() : "Unknown";
      entry.second.Print(txn_name);
    }
    std::cout
        << "============================================================\n"
        << std::endl;
  }
  void PrintAgg() {
    std::cout
        << "==================== perf statistics summary ===================="
        << std::endl;
    double abort_rate = agg_total_abort_count_ * 1.0 / (agg_total_count_ + 1);
    printf(
        "this node id: %hu, "
        "agg_total_count\t%lld\nagg_total_abort_count\t%lld\nabort_rate\t%lf\n",
        RDMA_Manager::node_id, agg_total_count_, agg_total_abort_count_,
        abort_rate);
    printf("FREQUENCY_DELIVERY is %d\nFREQUENCY_PAYMENT IS "
           "%d\nFREQUENCY_NEW_ORDER is %d\nFREQUENCY_ORDER_STATUS is "
           "%d\nFREQUENCY_STOCK_LEVEL is %d\n",
           FREQUENCY_DELIVERY, FREQUENCY_PAYMENT, FREQUENCY_NEW_ORDER,
           FREQUENCY_ORDER_STATUS, FREQUENCY_STOCK_LEVEL);
    printf("per_node_elapsed_time\t%lf\ntotal_throughput\t%lf\nper_node_"
           "throughput\t%lf\nper_core_throughput\t%lf\n",
           agg_elapsed_time_ * 1.0 / agg_node_num_, agg_throughput_,
           agg_throughput_ / agg_node_num_,
           agg_throughput_ / agg_thread_count_);
    if (agg_hot_scan_count_ > 0) {
      // Recalculate aggregated throughput from total count and average elapsed time
      double avg_elapsed_time = agg_elapsed_time_ * 1.0 / agg_node_num_;
      double agg_hot_scan_throughput = 0.0;
      if (avg_elapsed_time > 0) {
        agg_hot_scan_throughput = agg_hot_scan_count_ * 1.0 / avg_elapsed_time;
      }
      double hot_scan_abort_rate = agg_hot_scan_abort_count_ * 1.0 / (agg_hot_scan_count_ + agg_hot_scan_abort_count_);
      printf("hot_scan_total_count\t%lld\nhot_scan_throughput\t%lf\nhot_scan_abort_count\t%lld\nhot_scan_abort_rate\t%lf\nhot_scan_avg_latency\t%lf\n",
             agg_hot_scan_count_, agg_hot_scan_throughput, agg_hot_scan_abort_count_, hot_scan_abort_rate, agg_hot_scan_avg_latency_);
    }
    uint64_t invalidation_num = 0;
    uint64_t hit_valid_num = 0;
    uint64_t miss_num = 0;
#if defined(MVOCC)
    uint64_t delta_pull_count = 0;
    uint64_t roll_back_count = 0;
#endif
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      invalidation_num = cache_invalidation[i] + invalidation_num;
      hit_valid_num = cache_hit_valid[i][0] + hit_valid_num;
      miss_num = cache_miss[i][0] + miss_num;
#if defined(MVOCC)
      delta_pull_count = delta_pull_count + delta_pull_num[i];
      roll_back_count = roll_back_num[i] + roll_back_count;
#endif
    }
    printf("cache invalidation messages are %lu, cache hit numbers are %lu, "
           "cache miss numbers are %lu, avg latency is %lf\n",
           invalidation_num, hit_valid_num, miss_num,
           agg_thread_count_ /
               agg_throughput_); // agg_thread_count_/agg_throughput_
#if defined(MVOCC)
    printf("delta_pull count is %lu, roll back count is %lu \n",
           delta_pull_count, roll_back_count);
#endif

    /*std::cout << "agg_total_count=" << agg_total_count_ <<",
   agg_total_abort_count=" << agg_total_abort_count_ <<", abort_rate=" <<
   abort_rate << std::endl; std::cout << "per node elapsed time=" <<
   agg_elapsed_time_ * 1.0 / agg_node_num_ << "ms." << std::endl; std::cout <<
   "total throughput=" << agg_throughput_ << "K tps,per node throughput="
   << agg_throughput_ / agg_node_num_ << "K tps." << ",per core throughput=" <<
   agg_throughput_ / agg_thread_count_ << std::endl;*/
    std::cout << "==================== end ====================" << std::endl;
    fflush(stdout);

    // Print latency statistics
    PrintLatencyStats();
  }
  void Print() {
    std::cout << "total_count=" << total_count_
              << ",total_abort_count=" << total_abort_count_
              << ",throughput=" << throughput_
              << ",thread_count_=" << thread_count_
              << ",elapsed_time=" << elapsed_time_ << std::endl;
  }
  void Aggregate(const PerfStatistics &obj) {
    agg_total_count_ += obj.total_count_;
    agg_total_abort_count_ += obj.total_abort_count_;
    agg_throughput_ += obj.throughput_;
    agg_thread_count_ += obj.thread_count_;
    agg_elapsed_time_ += obj.elapsed_time_;
    agg_hot_scan_count_ += obj.hot_scan_count_;
    agg_hot_scan_abort_count_ += obj.hot_scan_abort_count_;
    // For average latency, simply average the averages (simpler but less accurate)
    if (agg_node_num_ == 0) {
      // First node: use its average directly
      agg_hot_scan_avg_latency_ = obj.hot_scan_avg_latency_;
    } else {
      // Subsequent nodes: average of averages
      agg_hot_scan_avg_latency_ = (agg_hot_scan_avg_latency_ * agg_node_num_ + obj.hot_scan_avg_latency_) / (agg_node_num_ + 1);
    }
    agg_node_num_++;
  }

  long long total_count_;
  long long total_abort_count_;
  long long thread_count_;
  long long elapsed_time_; // in milli seconds
  double throughput_;

  long long agg_total_count_;
  long long agg_total_abort_count_;
  double agg_throughput_;
  long long agg_thread_count_;
  long long agg_elapsed_time_;
  long long agg_node_num_;
  long long longest_elapsed_time_;
  
  // Hot scan transaction statistics
  long long hot_scan_count_;
  double hot_scan_throughput_;
  long long hot_scan_abort_count_;
  double hot_scan_avg_latency_;
  long long agg_hot_scan_count_;
  double agg_hot_scan_throughput_;
  long long agg_hot_scan_abort_count_;
  double agg_hot_scan_avg_latency_;

  // Latency tracking: recorded locally on each node, but only printed on master
  // Note: Not aggregated across nodes (complex containers cannot be serialized
  // by MasterCollect/MasterBroadcast)
  std::map<size_t, std::string> txn_type_names_;
  std::map<size_t, LatencyTracker> latency_trackers_;
};
} // namespace DSMEngine

#endif
