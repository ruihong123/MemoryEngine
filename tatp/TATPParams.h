#ifndef __DATABASE_TATP_PARAMS_H__
#define __DATABASE_TATP_PARAMS_H__

#include "BenchmarkArguments.h"
#include "BenchmarkScaleParams.h"
#include "ClusterConfig.h"
#include "Meta.h"
#include "TATPConstants.h"
#include <cassert>

// NOTE: LOGGING and TWOPHASECOMMIT are now defined in BenchmarkArguments.h

namespace DSMEngine {
namespace TATPBenchmark {

struct TATPScaleParams : public BenchmarkScaleParams {
  int num_subscribers_;
  int starting_subscriber_;
  int ending_subscriber_;

  TATPScaleParams()
      : BenchmarkScaleParams(), num_subscribers_(0), starting_subscriber_(0),
        ending_subscriber_(0) {}
};

extern TATPScaleParams tatp_scale_params;
extern int num_subscribers_per_partition;

void FillScaleParams(ClusterConfig &config);
void PrintScaleParams();

} // namespace TATPBenchmark
} // namespace DSMEngine

#endif
