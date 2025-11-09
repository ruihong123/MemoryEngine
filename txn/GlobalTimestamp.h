#pragma once
#ifndef __CAVALIA_DATABASE_GLOBAL_TIMESTAMP_H__
#define __CAVALIA_DATABASE_GLOBAL_TIMESTAMP_H__

#include <cstdint>
#include <queue>
#include <atomic>
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


				return rdma_mg->FetchAddNextTimestamp(1);
			}
            static uint64_t GetMonotoneTimestamp(){
                if (!rdma_mg){
                    rdma_mg = RDMA_Manager::Get_Instance();
                }
#ifdef BETTER_TS_ACQUIRE
                // this optimization can reduce unnecessary RDMA read over the network.
                uint64_t to_ret = 0;
                if (!time_stamp_mtx.try_lock()){
                    // latest_snapshot = rdma_mg->GetTimestamp();
                    time_stamp_mtx.lock();
                    to_ret = latest_snapshot;
                    time_stamp_mtx.unlock();
                    return to_ret;
                }else{
                    latest_snapshot = rdma_mg->GetTimestamp();
                    to_ret = latest_snapshot;
                }
                time_stamp_mtx.unlock();
                return to_ret;

#else
                return rdma_mg->GetTimestamp();
#endif
            }

//			static uint64_t GetBatchMonotoneTimestamp(){
//				return monotone_timestamp_.fetch_add(kBatchTsNum, std::memory_order_relaxed);
//			}
//			///////////////////////
//
//			///////////////////////
//			// for multiversion concurrency control, including snapshot isolation.
//			// the purpose is (1) to collect garbage for version maintenance; (2) generate a timestamp to retrieve consistent snapshot.
//
//			// for OCC or 2PL, we can use maximum timestamp to retrieve consistent snapshot.
//			// this is because the timestamp for OCC and 2PL is generated at the commit time, and new committed transactions must have larger timestamp.
//			static uint64_t GetMaxTimestamp(){
//				uint64_t res = *(thread_timestamp_[0]);
//				for (size_t i = 0; i < thread_count_; ++i){
//					if (*(thread_timestamp_[i]) > res){
//						res = *(thread_timestamp_[i]);
//					}
//				}
//				return res;
//			}
//
//			// for TO, we can use minimum timestamp to retrieve consistent snapshot.
//			// this is because the timestamp for TO is generated at the beginning of a transaction, and "staled" transactions can still commit.
//			static uint64_t GetMinTimestamp(){
//				uint64_t res = *(thread_timestamp_[0]);
//				for (size_t i = 1; i < thread_count_; ++i){
//					if (*(thread_timestamp_[i]) < res){
//						res = *(thread_timestamp_[i]);
//					}
//				}
//				return res;
//			}
//
//			static void SetThreadTimestamp(const size_t &thread_id, const uint64_t &timestamp){
//				*(thread_timestamp_[thread_id]) = timestamp;
//			}
			///////////////////////

		public:
            static RDMA_Manager* rdma_mg;
//			static std::atomic<uint64_t> monotone_timestamp_;
            static GlobalAddress time_stamp_gaddr;
            static uint64_t latest_snapshot;
            static RWSpinMutex time_stamp_mtx;
//			static std::atomic<uint64_t> *thread_timestamp_[kMaxThreadNum];
			static size_t thread_count_;
		};
}

#endif