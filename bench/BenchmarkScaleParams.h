#ifndef __BENCHMARK_SCALE_PARAMS_H__
#define __BENCHMARK_SCALE_PARAMS_H__

namespace DSMEngine {

// Base class for benchmark-specific scale parameters
struct BenchmarkScaleParams {
  int partition_id_;
  int starting_partition_;
  int ending_partition_;
  int num_partitions_;
  double scale_factor_;

  BenchmarkScaleParams()
      : partition_id_(0), starting_partition_(1), ending_partition_(1),
        num_partitions_(1), scale_factor_(1.0) {}

  virtual ~BenchmarkScaleParams() {}
};

} // namespace DSMEngine

#endif
