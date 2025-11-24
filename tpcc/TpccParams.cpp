//
// Created by wang4996 on 7/6/24.
//
#include "TpccParams.h"
namespace DSMEngine {
namespace TpccBenchmark {
TpccScaleParams tpcc_scale_params;
int num_wh_per_par = 0;
void FillScaleParams(ClusterConfig &config) {
  assert(factor_count == 2);
  tpcc_scale_params.num_warehouses_ = (int)scale_factors[0];
  tpcc_scale_params.partition_id_ = config.GetMyPartitionId();
  int partition_num = config.GetPartitionNum();
  num_wh_per_par =
      (tpcc_scale_params.num_warehouses_ + partition_num - 1) / partition_num;
  tpcc_scale_params.starting_warehouse_ =
      1 + num_wh_per_par * tpcc_scale_params.partition_id_;
  tpcc_scale_params.ending_warehouse_ =
      tpcc_scale_params.starting_warehouse_ + num_wh_per_par - 1;
  if (tpcc_scale_params.ending_warehouse_ > tpcc_scale_params.num_warehouses_) {
    tpcc_scale_params.ending_warehouse_ = tpcc_scale_params.num_warehouses_;
  }
  assert(tpcc_scale_params.starting_warehouse_ <=
         tpcc_scale_params.ending_warehouse_);
  tpcc_scale_params.scale_factor_ = scale_factors[1];
  tpcc_scale_params.num_items_ =
      static_cast<int>(NUM_ITEMS / tpcc_scale_params.scale_factor_);
  tpcc_scale_params.num_districts_per_warehouse_ = DISTRICTS_PER_WAREHOUSE;
  tpcc_scale_params.num_customers_per_district_ = static_cast<int>(
      CUSTOMERS_PER_DISTRICT / tpcc_scale_params.scale_factor_);
  tpcc_scale_params.num_new_orders_per_district_ = static_cast<int>(
      INITIAL_NEW_ORDERS_PER_DISTRICT / tpcc_scale_params.scale_factor_);

  // Set base class fields
  tpcc_scale_params.starting_partition_ = tpcc_scale_params.starting_warehouse_;
  tpcc_scale_params.ending_partition_ = tpcc_scale_params.ending_warehouse_;
  tpcc_scale_params.num_partitions_ = partition_num;
}

void PrintScaleParams() {
  std::cout << "============= tpcc_scale_params ===========" << std::endl;
  std::cout << "num_warehouses=" << tpcc_scale_params.num_warehouses_
            << ",starting_warehouse_=" << tpcc_scale_params.starting_warehouse_
            << ",ending_warehouse_=" << tpcc_scale_params.ending_warehouse_
            << std::endl;
  std::cout << "partition_id_" << tpcc_scale_params.partition_id_
            << "\nscale_factor_=" << tpcc_scale_params.scale_factor_
            << "\nnum_items_=" << tpcc_scale_params.num_items_
            << "\nnum_districts_per_warehouse_="
            << tpcc_scale_params.num_districts_per_warehouse_
            << "\nnum_customers_per_district_="
            << tpcc_scale_params.num_customers_per_district_
            << "\nnum_new_orders_per_district_="
            << tpcc_scale_params.num_new_orders_per_district_ << std::endl;
  std::cout << "dist_ratio=" << dist_ratio << ", gStandard=" << gStandard
            << ", gForceRandomAccess=" << gForceRandomAccess
            << ", gTimeLocality=" << gTimeLocality
            << ", gReadRatio=" << gReadRatio << std::endl;
  std::cout << "============= end ===========" << std::endl;
}
} // namespace TpccBenchmark
} // namespace DSMEngine
