#ifndef __DATABASE_TPCC_PARAMS_H__
#define __DATABASE_TPCC_PARAMS_H__

#include "BenchmarkArguments.h"
#include "BenchmarkScaleParams.h"
#include "ClusterConfig.h"
#include "Meta.h"
#include "TpccConstants.h"

#include <cassert>

namespace DSMEngine {
namespace TpccBenchmark {
struct TpccScaleParams : public BenchmarkScaleParams {
  int num_warehouses_;
  int starting_warehouse_;
  int ending_warehouse_;
  int num_items_;
  int num_districts_per_warehouse_;
  int num_customers_per_district_;
  int num_new_orders_per_district_;

  TpccScaleParams()
      : BenchmarkScaleParams(), num_warehouses_(0), starting_warehouse_(0),
        ending_warehouse_(0), num_items_(0), num_districts_per_warehouse_(0),
        num_customers_per_district_(0), num_new_orders_per_district_(0) {}
};

// Global
// TpccScaleParams tpcc_scale_params = { 0, 0, 0, 0, 0.0, 0, 0, 0, 0 };
extern TpccScaleParams tpcc_scale_params;
extern int num_wh_per_par;

void FillScaleParams(ClusterConfig &config);
void PrintScaleParams();

} // namespace TpccBenchmark
} // namespace DSMEngine

#endif
