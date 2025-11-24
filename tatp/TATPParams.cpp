#include "TATPParams.h"
#include <iostream>

namespace DSMEngine {
namespace TATPBenchmark {

TATPScaleParams tatp_scale_params;
int num_subscribers_per_partition = 0;

void FillScaleParams(ClusterConfig &config) {
  int num_partitions = config.GetPartitionNum();
  int total_subscribers = DEFAULT_NUM_SUBSCRIBERS;

  tatp_scale_params.num_subscribers_ = total_subscribers;
  tatp_scale_params.partition_id_ = config.GetMyPartitionId();
  tatp_scale_params.scale_factor_ = 1.0;

  num_subscribers_per_partition = total_subscribers / num_partitions;

  int partition_id = tatp_scale_params.partition_id_;
  tatp_scale_params.starting_subscriber_ =
      partition_id * num_subscribers_per_partition;
  tatp_scale_params.ending_subscriber_ =
      (partition_id + 1) * num_subscribers_per_partition - 1;

  if (partition_id == num_partitions - 1) {
    tatp_scale_params.ending_subscriber_ = total_subscribers - 1;
  }

  // Set base class fields
  tatp_scale_params.starting_partition_ =
      tatp_scale_params.starting_subscriber_;
  tatp_scale_params.ending_partition_ = tatp_scale_params.ending_subscriber_;
  tatp_scale_params.num_partitions_ = num_partitions;
}

void PrintScaleParams() {
  std::cout << "TATP Scale Parameters:" << std::endl;
  std::cout << "  Total Subscribers: " << tatp_scale_params.num_subscribers_
            << std::endl;
  std::cout << "  Partition ID: " << tatp_scale_params.partition_id_
            << std::endl;
  std::cout << "  Starting Subscriber: "
            << tatp_scale_params.starting_subscriber_ << std::endl;
  std::cout << "  Ending Subscriber: " << tatp_scale_params.ending_subscriber_
            << std::endl;
  std::cout << "  Subscribers per Partition: " << num_subscribers_per_partition
            << std::endl;
}

} // namespace TATPBenchmark
} // namespace DSMEngine
