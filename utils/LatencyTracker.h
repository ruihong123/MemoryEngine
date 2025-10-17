#ifndef __DATABASE_LATENCY_TRACKER_H__
#define __DATABASE_LATENCY_TRACKER_H__

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <vector>

namespace DSMEngine {

// Latency tracker for individual transaction types
class LatencyTracker {
public:
  LatencyTracker() : total_count_(0) {}

  // Record a latency sample (in nanoseconds)
  void RecordLatency(uint64_t latency_ns) {
    latencies_.push_back(latency_ns);
    total_count_++;
  }

  // Calculate percentiles after all samples are collected
  void CalculatePercentiles() {
    if (latencies_.empty())
      return;

    std::sort(latencies_.begin(), latencies_.end());

    p50_ = GetPercentile(0.50);
    p90_ = GetPercentile(0.90);
    p95_ = GetPercentile(0.95);
    p99_ = GetPercentile(0.99);
    p999_ = GetPercentile(0.999);

    // Calculate average
    uint64_t sum = 0;
    for (auto lat : latencies_) {
      sum += lat;
    }
    avg_ = latencies_.empty() ? 0 : sum / latencies_.size();
    min_ = latencies_.empty() ? 0 : latencies_.front();
    max_ = latencies_.empty() ? 0 : latencies_.back();
  }

  // Get specific percentile (must call CalculatePercentiles first)
  uint64_t GetP50() const { return p50_; }
  uint64_t GetP90() const { return p90_; }
  uint64_t GetP95() const { return p95_; }
  uint64_t GetP99() const { return p99_; }
  uint64_t GetP999() const { return p999_; }
  uint64_t GetAvg() const { return avg_; }
  uint64_t GetMin() const { return min_; }
  uint64_t GetMax() const { return max_; }
  uint64_t GetCount() const { return total_count_; }

  // Clear all data
  void Clear() {
    latencies_.clear();
    total_count_ = 0;
    p50_ = p90_ = p95_ = p99_ = p999_ = avg_ = min_ = max_ = 0;
  }

  // Merge data from another tracker
  void Merge(const LatencyTracker &other) {
    latencies_.insert(latencies_.end(), other.latencies_.begin(),
                      other.latencies_.end());
    total_count_ += other.total_count_;
  }

  // Print statistics
  void Print(const char *txn_name) const {
    if (total_count_ == 0) {
      printf("  %s: No transactions\n", txn_name);
      return;
    }
    printf("  %s (count=%lu):\n", txn_name, total_count_);
    printf("    Avg: %.2f us, Min: %.2f us, Max: %.2f us\n", avg_ / 1000.0,
           min_ / 1000.0, max_ / 1000.0);
    printf("    P50: %.2f us, P90: %.2f us, P95: %.2f us, P99: %.2f us, "
           "P999: %.2f us\n",
           p50_ / 1000.0, p90_ / 1000.0, p95_ / 1000.0, p99_ / 1000.0,
           p999_ / 1000.0);
  }

private:
  uint64_t GetPercentile(double percentile) const {
    if (latencies_.empty())
      return 0;
    size_t index = static_cast<size_t>(percentile * latencies_.size());
    if (index >= latencies_.size())
      index = latencies_.size() - 1;
    return latencies_[index];
  }

  std::vector<uint64_t> latencies_;
  uint64_t total_count_;
  uint64_t p50_, p90_, p95_, p99_, p999_;
  uint64_t avg_, min_, max_;
};

// Per-thread latency tracker for all transaction types
template <size_t MaxTxnTypes> class PerThreadLatencyTracker {
public:
  PerThreadLatencyTracker() {
    for (size_t i = 0; i < MaxTxnTypes; ++i) {
      trackers_[i] = LatencyTracker();
    }
  }

  // Record latency for a specific transaction type
  void RecordLatency(size_t txn_type, uint64_t latency_ns) {
    if (txn_type < MaxTxnTypes) {
      trackers_[txn_type].RecordLatency(latency_ns);
    }
  }

  // Get tracker for a specific transaction type
  LatencyTracker &GetTracker(size_t txn_type) { return trackers_[txn_type]; }

  const LatencyTracker &GetTracker(size_t txn_type) const {
    return trackers_[txn_type];
  }

  // Calculate percentiles for all transaction types
  void CalculateAllPercentiles() {
    for (size_t i = 0; i < MaxTxnTypes; ++i) {
      trackers_[i].CalculatePercentiles();
    }
  }

  // Merge data from another per-thread tracker
  void Merge(const PerThreadLatencyTracker &other) {
    for (size_t i = 0; i < MaxTxnTypes; ++i) {
      trackers_[i].Merge(other.trackers_[i]);
    }
  }

private:
  LatencyTracker trackers_[MaxTxnTypes];
};

} // namespace DSMEngine

#endif
