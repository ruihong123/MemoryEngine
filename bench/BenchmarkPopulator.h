// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_BENCHMARK_POPULATOR_H__
#define __DATABASE_BENCHMARK_POPULATOR_H__

#include <iostream>

#include "Meta.h"
#include "TableDirectory.h"
#include "TimeMeasurer.h"

namespace DSMEngine {
class BenchmarkPopulator {
 public:
  BenchmarkPopulator(TableDirectory *storage_manager)
      : storage_manager_(storage_manager) {
  }

  virtual ~BenchmarkPopulator() {
  }

  void Start() {
    std::cout << "start population" << std::endl;
    TimeMeasurer timer;
    timer.StartTimer();
    StartPopulate();
    timer.EndTimer();
    std::cout << "populate elapsed time=" << timer.GetElapsedMilliSeconds()
              << "ms" << std::endl;
  }

  virtual void StartPopulate() = 0;

 private:
  BenchmarkPopulator(const BenchmarkPopulator &);
  BenchmarkPopulator& operator=(const BenchmarkPopulator &);

 protected:
  TableDirectory *storage_manager_;
};
}

#endif
