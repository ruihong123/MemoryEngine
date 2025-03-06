#ifndef __DATABASE_TPCC_PARAMS_H__
#define __DATABASE_TPCC_PARAMS_H__

#include "Meta.h"
#include "TpccConstants.h"
#include "ClusterConfig.h"
#include "BenchmarkArguments.h"

#include <cassert>

//THere are three macros for distributed txn: 1. whether the system run with sharding and 2 phase commit,
// 2. whether the generated workload is sharded, whether a single node only receive txn for a specific warehouse rather than radomly distributed.
// 3. for a specific txn, wther it contain product from other warehouse. this will result in cross-shard txn for sharded workload.
// NOte: never make Partitioned true with workload as random source.
#define LOGGING false
#define TWOPHASECOMMIT false
//#define WORKLOAD_PATTERN SourceType::PARTITION_SOURCE
#define WORKLOAD_PATTERN SourceType::RANDOM_SOURCE
namespace DSMEngine {
namespace TpccBenchmark {
struct TpccScaleParams {
    int num_warehouses_;
    int starting_warehouse_;
    int ending_warehouse_;
    int partition_id_;
    double scale_factor_;
    int num_items_;
    int num_districts_per_warehouse_;
    int num_customers_per_district_;
    int num_new_orders_per_district_;
};

// Global
//TpccScaleParams tpcc_scale_params = { 0, 0, 0, 0, 0.0, 0, 0, 0, 0 };
extern TpccScaleParams tpcc_scale_params;
extern int num_wh_per_par;

void FillScaleParams(ClusterConfig& config) ;
void PrintScaleParams();

}
}

#endif
