#ifndef __DATABASE_SMALLBANK_PARAMS_H__
#define __DATABASE_SMALLBANK_PARAMS_H__

#include "BenchmarkArguments.h"
#include "BenchmarkScaleParams.h"
#include "ClusterConfig.h"
#include "Meta.h"
#include "SmallBankConstants.h"
#include <cassert>

// NOTE: LOGGING and TWOPHASECOMMIT are now defined in BenchmarkArguments.h

namespace DSMEngine {
namespace SmallBankBenchmark {

struct SmallBankScaleParams : public BenchmarkScaleParams {
  int num_accounts_;
  int starting_account_;
  int ending_account_;

  SmallBankScaleParams()
      : BenchmarkScaleParams(), num_accounts_(0), starting_account_(0),
        ending_account_(0) {}
};

extern SmallBankScaleParams smallbank_scale_params;
extern int num_accounts_per_partition;

void FillScaleParams(ClusterConfig &config);
void PrintScaleParams();

} // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
