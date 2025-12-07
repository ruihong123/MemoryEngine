#include "GlobalTimestamp.h"

#ifdef USE_SNAPSHOT_MANAGER
#include <algorithm>
#endif

namespace DSMEngine{
//		std::atomic<uint64_t> GlobalTimestamp::monotone_timestamp_(1);

//		std::atomic<uint64_t> *GlobalTimestamp::thread_timestamp_[kMaxThreadNum];
		size_t GlobalTimestamp::thread_count_ = 0;
    RDMA_Manager* GlobalTimestamp::rdma_mg = nullptr;
    std::atomic<uint64_t> GlobalTimestamp::latest_timestamp = 0;
    RWSpinMutex GlobalTimestamp::RTS_mtx;
    RWSpinMutex GlobalTimestamp::CTS_mtx;
#ifdef USE_SNAPSHOT_MANAGER
    std::once_flag GlobalTimestamp::snapshot_thread_once;
    std::atomic<bool> GlobalTimestamp::snapshot_thread_running{false};
    std::atomic<uint64_t> GlobalTimestamp::local_ts_next{1};
    std::atomic<uint64_t> GlobalTimestamp::global_read_snapshot{0};

    void GlobalTimestamp::EnsureSnapshotThreadStarted(){
        std::call_once(snapshot_thread_once, [](){
            snapshot_thread_running.store(true, std::memory_order_release);
            std::thread(SnapshotPollingLoop).detach();
        });
    }

    void GlobalTimestamp::SnapshotPollingLoop(){
        auto* manager = rdma_mg;
        while (snapshot_thread_running.load(std::memory_order_acquire)){
            if (manager == nullptr){
                // manager = rdma_mg;
                
                // std::this_thread::sleep_for(std::chrono::microseconds(kSnapshotPollingIntervalUs));
                continue;
            }
            SnapshotRangeReply reply{};
            uint64_t next = local_ts_next.load(std::memory_order_relaxed);
            if (manager->SyncSnapshotInfo(next, &reply)){
                if (reply.global_read_snapshot > 0){
                    global_read_snapshot.store(reply.global_read_snapshot, std::memory_order_release);
                }

                if (reply.forced_ts_next > 0){
                    uint64_t current = local_ts_next.load(std::memory_order_relaxed);
                    while (current < reply.forced_ts_next){
                        if (local_ts_next.compare_exchange_weak(
                                current, reply.forced_ts_next,
                                std::memory_order_acq_rel,
                                std::memory_order_relaxed)){
                            break;
                        }
                    }
                }

            }
            std::this_thread::sleep_for(std::chrono::microseconds(kSnapshotPollingIntervalUs));
        }
    }

    void GlobalTimestamp::EnsureCommitFloor(uint64_t minimum){
        if (minimum == 0){
            return;
        }
        // EnsureSnapshotThreadStarted();
        uint64_t current = local_ts_next.load(std::memory_order_relaxed);
        while (current < minimum){
            if (local_ts_next.compare_exchange_weak(
                    current, minimum,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)){
                return;
            }
        }
    }
#endif
}
