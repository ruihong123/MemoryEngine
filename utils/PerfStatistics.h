// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_UTILS_PERFORMANCE_STATISTICS_H__
#define __DATABASE_UTILS_PERFORMANCE_STATISTICS_H__

#include <iostream>
#include <cstdio>
#include "TpccSource.h"
extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];
extern uint64_t cache_miss[MAX_APP_THREAD][8];
namespace DSMEngine {
struct PerfStatistics {
  PerfStatistics() {
    total_count_ = 0;
    total_abort_count_ = 0;
    thread_count_ = 0;
    elapsed_time_ = 0;
    throughput_ = 0.0;

    agg_total_count_ = 0;
    agg_thread_count_ = 0;
    agg_total_abort_count_ = 0;
    agg_elapsed_time_ = 0;
    agg_node_num_ = 0;
    longest_elapsed_time_ = 0;
    agg_throughput_ = 0.0;
  }
  void PrintAgg() {
    std::cout
        << "==================== perf statistics summary ===================="
        << std::endl;
    double abort_rate = agg_total_abort_count_ * 1.0 / (agg_total_count_ + 1);
    printf(
        "this node id: %hu, agg_total_count\t%lld\nagg_total_abort_count\t%lld\nabort_rate\t%lf\n",
        RDMA_Manager::node_id, agg_total_count_, agg_total_abort_count_, abort_rate);
    printf("FREQUENCY_DELIVERY is %d\nFREQUENCY_PAYMENT IS %d\nFREQUENCY_NEW_ORDER is %d\nFREQUENCY_ORDER_STATUS is %d\nFREQUENCY_STOCK_LEVEL is %d\n",
                FREQUENCY_DELIVERY, FREQUENCY_PAYMENT, FREQUENCY_NEW_ORDER, FREQUENCY_ORDER_STATUS, FREQUENCY_STOCK_LEVEL);
    printf(
        "per_node_elapsed_time\t%lf\ntotal_throughput\t%lf\nper_node_throughput\t%lf\nper_core_throughput\t%lf\n",
        agg_elapsed_time_ * 1.0 / agg_node_num_, agg_throughput_,
        agg_throughput_ / agg_node_num_, agg_throughput_ / agg_thread_count_);
    uint64_t invalidation_num = 0;
    uint64_t hit_valid_num = 0;
    uint64_t miss_num = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        invalidation_num = cache_invalidation[i] + invalidation_num;
        hit_valid_num = cache_hit_valid[i][0] + hit_valid_num;
        miss_num = cache_miss[i][0] + miss_num;
    }
      printf("cache invalidation messages are %lu, cache hit numbers are %lu, cache miss numbers are %lu, avg latency is %lf\n", invalidation_num, hit_valid_num, miss_num, agg_thread_count_/agg_throughput_); //agg_thread_count_/agg_throughput_
    /*std::cout << "agg_total_count=" << agg_total_count_ <<", agg_total_abort_count=" << agg_total_abort_count_ <<", abort_rate=" << abort_rate << std::endl;
     std::cout << "per node elapsed time=" << agg_elapsed_time_ * 1.0 / agg_node_num_ << "ms." << std::endl;
     std::cout << "total throughput=" << agg_throughput_ << "K tps,per node throughput=" 
     << agg_throughput_ / agg_node_num_ << "K tps." << ",per core throughput=" << agg_throughput_ / agg_thread_count_ << std::endl;*/
    std::cout << "==================== end ====================" << std::endl;
  }
  void Print() {
    std::cout << "total_count=" << total_count_ << ",total_abort_count="
              << total_abort_count_ << ",throughput=" << throughput_
              << ",thread_count_=" << thread_count_ << ",elapsed_time="
              << elapsed_time_ << std::endl;
  }
  void Aggregate(const PerfStatistics& obj) {
    agg_total_count_ += obj.total_count_;
    agg_total_abort_count_ += obj.total_abort_count_;
    agg_throughput_ += obj.throughput_;
    agg_thread_count_ += obj.thread_count_;
    agg_elapsed_time_ += obj.elapsed_time_;
    agg_node_num_++;
  }

  long long total_count_;
  long long total_abort_count_;
  long long thread_count_;
  long long elapsed_time_;  // in milli seconds
  double throughput_;

  long long agg_total_count_;
  long long agg_total_abort_count_;
  double agg_throughput_;
  long long agg_thread_count_;
  long long agg_elapsed_time_;
  long long agg_node_num_;
  long long longest_elapsed_time_;
};
}

#endif
