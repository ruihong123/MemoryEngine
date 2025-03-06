#include "TpccExecutor.h"
#include "TpccPopulator.h"
#include "TpccSource.h"
#include "TpccInitiator.h"
#include "TpccConstants.h"
#include "Meta.h"
#include "TpccParams.h"
#include "BenchmarkArguments.h"
#include "ClusterHelper.h"
#include "ClusterSync.h"
#include <iostream>

using namespace DSMEngine::TpccBenchmark;
using namespace DSMEngine;

void ExchPerfStatistics(ClusterConfig* config, 
    ClusterSync* synchronizer, PerfStatistics* s);
extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];
extern uint64_t cache_miss[MAX_APP_THREAD][8];
void clear_cache_statistics() {
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        cache_invalidation[i] = 0;
        for (int j = 0; j < 8; ++j) {
            cache_hit_valid[i][j] = 0;
            cache_miss[i][j] = 0;
        }
    }
}
int main(int argc, char* argv[]) {
  ArgumentsParser(argc, argv);

//  std::string my_host_name = ClusterHelper::GetLocalHostName();
  ClusterConfig config(my_host_name, conn_port, config_filename);
  ClusterSync synchronizer(&config);
  FillScaleParams(config);
  PrintScaleParams();

  TpccInitiator initiator(gThreadCount, &config);
  // initialize GAM storage layer
  initiator.InitGAllocator();
    // the RDMA Manager have a synchronization accross the nodes.

  // initialize benchmark data
  char* storage_addr = initiator.InitStorage();
    assert(storage_addr);
  char storage_key[16] = "Storage Key";
//  default_gallocator->memSet(storage_key, 16, storage_addr, StorageManager::GetSerializeSize());
    synchronizer.MasterBroadcast(storage_key, 16, storage_addr, StorageManager::GetSerializeSize());

    std::cout << "storage_addr=" << storage_addr << std::endl;
  StorageManager storage_manager;
  storage_manager.Deserialize(storage_addr);

  // populate database
  INIT_PROFILE_TIME(gThreadCount);
  TpccPopulator populator(&storage_manager, &tpcc_scale_params);
  populator.Start();
  REPORT_PROFILE_TIME(gThreadCount);
  //TODO: it seems that FenceXComputes did not do the synchronization.
    synchronizer.FenceXComputes();
//    WORKLOAD_PATTERN == PARTITION_SOURCE
    if (TWOPHASECOMMIT){
        auto func = std::bind(&TpccExecutor::ProcessQueryThread_2PC_Participant,  (void*)&storage_manager, std::placeholders::_1);
        default_gallocator->rdma_mg->Set_message_handling_func(func, "2PC");
    }
  // generate workload
  IORedirector redirector(gThreadCount);
  size_t access_pattern = 0;
  TpccSource sourcer(&tpcc_scale_params, &redirector, num_txn,
                     WORKLOAD_PATTERN, gThreadCount, dist_ratio,
                     config.GetMyPartitionId());
  sourcer.Start();

    IORedirector redirector1(gThreadCount);
    TpccSource sourcer1(&tpcc_scale_params, &redirector1, num_txn/4,
                       WORKLOAD_PATTERN, gThreadCount, dist_ratio,
                       config.GetMyPartitionId());
//    TpccSource sourcer1(&tpcc_scale_params, &redirector1, num_txn/1000,
//                        WORKLOAD_PATTERN, gThreadCount, dist_ratio,
//                        config.GetMyPartitionId());
    sourcer1.Start();
  synchronizer.FenceXComputes();

  {
    // warm up
    INIT_PROFILE_TIME(gThreadCount);
    TpccExecutor executor(&redirector, &storage_manager, gThreadCount, false);
    executor.Start();
    REPORT_PROFILE_TIME(gThreadCount);
  }
    synchronizer.FenceXComputes();
    // clear the cache statistics.
    clear_cache_statistics();
  {
    // run workload
    INIT_PROFILE_TIME(gThreadCount);
    TpccExecutor executor(&redirector1, &storage_manager, gThreadCount, LOGGING);
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

void ExchPerfStatistics(ClusterConfig* config, 
    ClusterSync* synchronizer, PerfStatistics* s) {
  PerfStatistics *stats = new PerfStatistics[config->GetPartitionNum()];
  synchronizer->MasterCollect<PerfStatistics>(
      s, stats);
  synchronizer->MasterBroadcast<PerfStatistics>(stats);
  for (size_t i = 0; i < config->GetPartitionNum(); ++i) {
    stats[i].Print();
    stats[0].Aggregate(stats[i]);
  }
    if (config->IsMaster()){
        stats[0].PrintAgg();
    }
  delete[] stats;
  stats = nullptr;
}



