#include "SmallBankParams.h"
#include <iostream>

namespace DSMEngine {
namespace SmallBankBenchmark {

SmallBankScaleParams smallbank_scale_params;
int num_accounts_per_partition = 0;

void FillScaleParams(ClusterConfig &config) {
  int num_partitions = config.GetPartitionNum();
  int total_accounts = DEFAULT_NUM_ACCOUNTS;

  smallbank_scale_params.num_accounts_ = total_accounts;
  smallbank_scale_params.partition_id_ = config.GetMyPartitionId();
  smallbank_scale_params.scale_factor_ = 1.0;

  num_accounts_per_partition = total_accounts / num_partitions;

  int partition_id = smallbank_scale_params.partition_id_;
  smallbank_scale_params.starting_account_ =
      partition_id * num_accounts_per_partition;
  smallbank_scale_params.ending_account_ =
      (partition_id + 1) * num_accounts_per_partition - 1;

  if (partition_id == num_partitions - 1) {
    smallbank_scale_params.ending_account_ = total_accounts - 1;
  }

  // Set base class fields
  smallbank_scale_params.starting_partition_ =
      smallbank_scale_params.starting_account_;
  smallbank_scale_params.ending_partition_ =
      smallbank_scale_params.ending_account_;
  smallbank_scale_params.num_partitions_ = num_partitions;
}

void PrintScaleParams() {
  std::cout << "SmallBank Scale Parameters:" << std::endl;
  std::cout << "  Total Accounts: " << smallbank_scale_params.num_accounts_
            << std::endl;
  std::cout << "  Partition ID: " << smallbank_scale_params.partition_id_
            << std::endl;
  std::cout << "  Starting Account: "
            << smallbank_scale_params.starting_account_ << std::endl;
  std::cout << "  Ending Account: " << smallbank_scale_params.ending_account_
            << std::endl;
  std::cout << "  Accounts per Partition: " << num_accounts_per_partition
            << std::endl;
}

} // namespace SmallBankBenchmark
} // namespace DSMEngine
