#include "TransactionExecutor.h"

namespace DSMEngine {
// Define static partition parameters
int TransactionExecutor::static_partition_start_ = 1;
int TransactionExecutor::static_partition_end_ = 1;
int TransactionExecutor::static_num_items_per_partition_ = 1;
int TransactionExecutor::static_partition_key_bits_ = 48;
} // namespace DSMEngine
