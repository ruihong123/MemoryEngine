#pragma once
#ifndef __CAVALIA_DATABASE_GLOBAL_TIMESTAMP_H__
#define __CAVALIA_DATABASE_GLOBAL_TIMESTAMP_H__

#include <cstdint>
#include <queue>
#include <atomic>
#include "storage/rdma.h"
//#include "../Meta/MetaTypes.h"

namespace DSMEngine{
		class GlobalTimestamp{
		public:
			///////////////////////
			static uint64_t FetchAddMonotoneTimestamp(){
                static RDMA_Manager* rdma_mg = RDMA_Manager::Get_Instance();
				return rdma_mg->FetchAddNextTimestamp();
			}
            static uint64_t GetMonotoneTimestamp(){
                static RDMA_Manager* rdma_mg = RDMA_Manager::Get_Instance();
                return rdma_mg->GetTimestamp();
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
//			static std::atomic<uint64_t> *thread_timestamp_[kMaxThreadNum];
			static size_t thread_count_;
		};
}

#endif