#pragma once
#ifndef __CAVALIA_DATABASE_GLOBAL_TIMESTAMP_H__
#define __CAVALIA_DATABASE_GLOBAL_TIMESTAMP_H__

#include <cstdint>
#include <queue>
#include <atomic>
#include <thread>
#include <mutex>
#include <chrono>
#include "storage/rdma.h"
//#include "../Meta/MetaTypes.h"

#define BETTER_TS_ACQUIRE
namespace DSMEngine{
		class GlobalTimestamp{
		public:
			///////////////////////
	    static uint64_t FetchAddMonotoneTimestamp(){
                if (!rdma_mg){
                    rdma_mg = RDMA_Manager::Get_Instance();
                }
#ifdef BETTER_TS_ACQUIRE
#ifdef USE_SNAPSHOT_MANAGER
                // EnsureSnapshotThreadStarted();
                return local_ts_next.fetch_add(1, std::memory_order_acq_rel);
#else
                uint64_t ts_start = 0;
                uint64_t ts_end = 0;
            retry:
				if (!CTS_mtx.try_lock()){
                    if (ts_start == ts_end) {
                        goto retry;
                    }else{
                        return ts_start++;
                    }
		    }else {
					uint64_t ts_temp = rdma_mg->FetchAddNextTimestamp(8);
                    // Atomically update latest_timestamp if temp is larger
                    uint64_t current = latest_timestamp.load(std::memory_order_acquire);
                    while (ts_temp > current) {
                        if (latest_timestamp.compare_exchange_weak(
                                current, ts_temp,
                                std::memory_order_acq_rel,
                                std::memory_order_acquire)) {
                            break;
                        }
                    }
                    ts_start = ts_temp;
                    ts_end = ts_temp + 8;
                    CTS_mtx.unlock();
                    return ts_temp;
		    }
#endif
#else
                return rdma_mg->FetchAddNextTimestamp(1);

#endif

				// return rdma_mg->FetchAddNextTimestamp(1);

			}
            static uint64_t GetMonotoneTimestamp(){
                if (!rdma_mg){
                    rdma_mg = RDMA_Manager::Get_Instance();
                }
#ifdef USE_SNAPSHOT_MANAGER
                // EnsureSnapshotThreadStarted();
                // return global_read_snapshot.load(std::memory_order_acquire);
                return local_ts_next.load(std::memory_order_relaxed);
#else
#ifdef BETTER_TS_ACQUIRE
                // this optimization can reduce unnecessary RDMA read over the network.
                // uint64_t to_ret = 0;
                if (!RTS_mtx.try_lock()){
                    // latest_snapshot = rdma_mg->GetTimestamp();
                    RTS_mtx.lock();
                    // to_ret = latest_snapshot;
                    RTS_mtx.unlock();
                    return latest_timestamp.load(std::memory_order_relaxed);
                }else{
                    uint64_t temp = rdma_mg->GetTimestamp();
                    
                    // Atomically update latest_timestamp if temp is larger
                    uint64_t current = latest_timestamp.load(std::memory_order_acquire);
                    while (temp > current) {
                        if (latest_timestamp.compare_exchange_weak(
                                current, temp,
                                std::memory_order_acq_rel,
                                std::memory_order_acquire)) {
                            break;
                        }
                    }
                    RTS_mtx.unlock();
                    return latest_timestamp.load(std::memory_order_relaxed);
                }
                
#else
                return rdma_mg->GetTimestamp();
#endif
#endif
            }

		public:
            static RDMA_Manager* rdma_mg;
//			static std::atomic<uint64_t> monotone_timestamp_;
            static GlobalAddress time_stamp_gaddr;
            static std::atomic<uint64_t> latest_timestamp;
            static RWSpinMutex RTS_mtx;
			static RWSpinMutex CTS_mtx;
#ifdef USE_SNAPSHOT_MANAGER
            // Snapshot manager mode: commit IDs are allocated from ranges provided
            // by memory node 1. A per-node background thread keeps the range cache
            // fresh and updates the global read snapshot without issuing RDMA reads
            // on every transaction.
            static std::once_flag snapshot_thread_once;
            static std::atomic<bool> snapshot_thread_running;
            static std::atomic<uint64_t> local_ts_next;
            static std::atomic<uint64_t> global_read_snapshot;
            static constexpr uint64_t kSnapshotPollingIntervalUs = 5;
            static void EnsureSnapshotThreadStarted();
            static void SnapshotPollingLoop();
            static void EnsureCommitFloor(uint64_t minimum);
#else
            static void EnsureCommitFloor(uint64_t) {}
#endif
//			static std::atomic<uint64_t> *thread_timestamp_[kMaxThreadNum];
			static size_t thread_count_;
		};
}

#endif