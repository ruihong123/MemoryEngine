#include "BenchmarkArguments.h"
#include "ClusterHelper.h"
#include "ClusterSync.h"
#include "Meta.h"
#include "SmallBankConstants.h"
#include "SmallBankExecutor.h"
#include "SmallBankInitiator.h"
#include "SmallBankParams.h"
#include "SmallBankPopulator.h"
#include "SmallBankSource.h"
#include <iostream>

using namespace DSMEngine::SmallBankBenchmark;
using namespace DSMEngine;

void ExchPerfStatistics(ClusterConfig *config, ClusterSync *synchronizer,
                        PerfStatistics *s);

extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];
extern uint64_t cache_miss[MAX_APP_THREAD][8];

#if defined(MVOCC)
extern uint64_t delta_pull_num[MAX_APP_THREAD];
extern uint64_t roll_back_num[MAX_APP_THREAD];
#endif

void clear_cache_statistics() {
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    cache_invalidation[i] = 0;
#if defined(MVOCC)
    DSMEngine::delta_pull_num[i] = 0;
    DSMEngine::roll_back_num[i] = 0;
#endif
    for (int j = 0; j < 8; ++j) {
      cache_hit_valid[i][j] = 0;
      cache_miss[i][j] = 0;
    }
  }
}

int main(int argc, char *argv[]) {
  ArgumentsParser(argc, argv);

  ClusterConfig config(my_host_name, conn_port, config_filename);
  ClusterSync synchronizer(&config);
  FillScaleParams(config);
  PrintScaleParams();

  SmallBankInitiator initiator(gThreadCount, &config);

  // initialize GAM storage layer
  initiator.InitGAllocator();

  // initialize benchmark data
  char *storage_addr = initiator.InitStorage();
  assert(storage_addr);
  char storage_key[16] = "Storage Key";
  synchronizer.MasterBroadcast(storage_key, 16, storage_addr,
                               TableDirectory::GetSerializeSize());

  std::cout << "storage_addr=" << storage_addr << std::endl;
  TableDirectory storage_manager;
  storage_manager.Deserialize(storage_addr);
  default_gallocator->rdma_mg->sync_with_computes_Cside();

  // populate database
  INIT_PROFILE_TIME(gThreadCount);
  SmallBankPopulator populator(&storage_manager);
  populator.PopulateDatabase();
  REPORT_PROFILE_TIME(gThreadCount);

  synchronizer.FenceXComputes();

  if (TWOPHASECOMMIT) {
    auto func =
        std::bind(&SmallBankExecutor::ProcessQueryThread_2PC_Participant,
                  (void *)&storage_manager, std::placeholders::_1);
    default_gallocator->rdma_mg->Set_message_handling_func(func, TwoPC);
  }

  // generate workload
  IORedirector redirector(gThreadCount);
  SmallBankSource sourcer(&smallbank_scale_params, &redirector, num_txn, 0,
                          gThreadCount);
  sourcer.Start();

  IORedirector redirector1(gThreadCount);
  SmallBankSource sourcer1(&smallbank_scale_params, &redirector1, num_txn / 4,
                           0, gThreadCount);
  sourcer1.Start();

  synchronizer.FenceXComputes();

  {
    // warm up
    std::cout << "Warm up phase..." << std::endl;
    INIT_PROFILE_TIME(gThreadCount);
    SmallBankExecutor executor(&redirector, &storage_manager, gThreadCount,
                               false);
    executor.Start();
    REPORT_PROFILE_TIME(gThreadCount);
  }

  synchronizer.FenceXComputes();
  clear_cache_statistics();

  {
    // run workload
    std::cout << "Running benchmark..." << std::endl;
    INIT_PROFILE_TIME(gThreadCount);
    SmallBankExecutor executor(&redirector1, &storage_manager, gThreadCount,
                               LOGGING, enable_latency_recording);

    // Set transaction type names for latency tracking (if enabled)
    if (enable_latency_recording) {
      std::map<size_t, std::string> txn_names;
      txn_names[AMALGAMATE] = "AMALGAMATE";
      txn_names[BALANCE] = "BALANCE";
      txn_names[DEPOSIT_CHECKING] = "DEPOSIT_CHECKING";
      txn_names[SEND_PAYMENT] = "SEND_PAYMENT";
      txn_names[TRANSACT_SAVINGS] = "TRANSACT_SAVINGS";
      txn_names[WRITE_CHECK] = "WRITE_CHECK";
      executor.SetTxnTypeNames(txn_names);
    }

    executor.Start();
    REPORT_PROFILE_TIME(gThreadCount);
    ExchPerfStatistics(&config, &synchronizer, &executor.GetPerfStatistics());
  }

  std::cout << "prepare to exit..." << std::endl;
  synchronizer.Fence_XALLNodes();
  default_gallocator->rdma_mg->join_all_handling_thread();
  std::cout << "over.." << std::endl;
  return 0;
}

void ExchPerfStatistics(ClusterConfig *config, ClusterSync *synchronizer,
                        PerfStatistics *s) {
  PerfStatistics *stats = new PerfStatistics[config->GetPartitionNum()];
  synchronizer->MasterCollect<PerfStatistics>(s, stats);
  synchronizer->MasterBroadcast<PerfStatistics>(stats);
  for (size_t i = 0; i < config->GetPartitionNum(); ++i) {
    stats[i].Print();
    stats[0].Aggregate(stats[i]);
  }
  if (config->IsMaster()) {
    stats[0].PrintAgg();
  }
  delete[] stats;
  stats = nullptr;
}
