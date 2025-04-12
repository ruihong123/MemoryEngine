#include "GlobalTimestamp.h"

namespace DSMEngine{
//		std::atomic<uint64_t> GlobalTimestamp::monotone_timestamp_(1);

//		std::atomic<uint64_t> *GlobalTimestamp::thread_timestamp_[kMaxThreadNum];
		size_t GlobalTimestamp::thread_count_ = 0;
    RDMA_Manager* GlobalTimestamp::rdma_mg = nullptr;
    uint64_t GlobalTimestamp::latest_snapshot = 0;
    RWSpinMutex GlobalTimestamp::time_stamp_mtx;
}
