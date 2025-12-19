#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include "storage/rdma.h"
#include "storage/page.h"
#include "txn/RedoLogger.h"
#include "txn/LogCodec.h"
#include "utils/mutexlock.h"

namespace DSMEngine {

// Structure to track a log segment
struct LogSegment {
    GlobalAddress segment_addr;      // Address of the remote log segment
    size_t segment_size;              // Size of the segment
    uint64_t received_length;         // Total length of logs received in this segment
};

// Structure to track per-compute-node log stream state for a logical memory region
struct LogStreamState {
    uint16_t compute_node_id;         // Compute node ID
    uint16_t logical_region_id;       // Logical memory region ID
    
    // List of log segments for this stream
    std::list<LogSegment> segments;
    RWSpinMutex segments_mtx;          // Mutex for segments list
    
    // Tracking positions
    std::atomic<uint64_t> received_log_length{0};   // Total length of logs received
    std::atomic<uint64_t> replayed_log_length{0};   // Total length of logs replayed
    uint64_t recycled_prefix_bytes{0};              // Bytes already recycled from the head
    
    // Debugging: store the header of the record where this stream is stuck
    std::mutex stuck_record_mtx;  // Protects stuck_record_header
    bool has_stuck_record{false};
    RedoLogger::RecordHeader stuck_record_header{};  // Copy of the header of the stuck record
    
    LogStreamState(uint16_t compute_id, uint16_t region_id)
        : compute_node_id(compute_id), logical_region_id(region_id) {}
};

// Structure to manage one replayer thread per logical region
struct LogicalRegionReplayer {
    uint16_t logical_region_id;
    std::thread replayer_thread;
    std::atomic<bool> should_exit{false};
    
    // Condition variable for efficient waiting
    std::condition_variable new_data_cv;
    std::mutex cv_mtx;
    
    // Progress tracking for deadlock detection
    std::mutex progress_mtx;  // Protects progress tracking data
    uint32_t iterations_without_progress{0};
    static constexpr uint32_t MAX_ITERATIONS_WITHOUT_PROGRESS = 1000;  // Threshold for abort
    
    LogicalRegionReplayer(uint16_t region_id) : logical_region_id(region_id) {}
};

// Manager for log replayer threads
class LogReplayerManager {
public:
    LogReplayerManager(RDMA_Manager* rdma_mg) : rdma_mg_(rdma_mg) {
        // Initialize all stream initialization flags to false
        for (auto& flag : stream_initialized_) {
            flag.store(false, std::memory_order_relaxed);
        }
        
        // Initialize all region replayer flags to false
        for (auto& flag : region_replayer_initialized_) {
            flag.store(false, std::memory_order_relaxed);
        }
    }
    ~LogReplayerManager() { StopAllReplayers(); }
    
    // Handle log_segment_request RPC
    // If first request for this compute_node_id + logical_region_id, spawn replayer thread
    void HandleLogSegmentRequest(const LogSegmentRequest& request);
    
    // Handle RDMA write with imm completion
    // Increase received_log_length for the corresponding stream
    // compute_node_id: compute node that sent the write
    // logical_region_id: logical memory region ID (extracted from imm_data)
    // transferred_size: transferred size in bytes (extracted from imm_data)
    void HandleWriteWithImm(uint16_t compute_node_id, uint16_t logical_region_id, uint32_t transferred_size);
    
    // Find all streams for a given compute node (used when we only know compute_node_id)
    std::vector<uint16_t> GetLogicalRegionsForComputeNode(uint16_t compute_node_id);
    
    // Wait until all redo logs have been replayed (all streams caught up)
    // Blocks until replayed_log_length >= received_log_length for all initialized streams
    void WaitForAllLogsReplayed();
    
    // Stop all replayer threads (for cleanup)
    void StopAllReplayers();
    
private:
    RDMA_Manager* rdma_mg_;
    
    // Fixed-size array to avoid mutex overhead
    // Index: compute_node_id * MAX_LOGICAL_REGIONS + logical_region_id
    static constexpr size_t MAX_COMPUTE_NODES = 32;
    static constexpr size_t MAX_LOGICAL_REGIONS = 32;
    static constexpr size_t MAX_STREAMS = MAX_COMPUTE_NODES * MAX_LOGICAL_REGIONS;
    
    // Direct array of LogStreamState objects, indexed by (compute_node_id, logical_region_id)
    // Use aligned_storage to avoid default constructor issues
    std::array<std::aligned_storage_t<sizeof(LogStreamState), alignof(LogStreamState)>, MAX_STREAMS> stream_states_;
    
    // Track which streams are initialized (atomic flags)
    std::array<std::atomic<bool>, MAX_STREAMS> stream_initialized_;
    
    // One replayer thread per logical region
    std::array<std::unique_ptr<LogicalRegionReplayer>, MAX_LOGICAL_REGIONS> region_replayers_;
    std::array<std::atomic<bool>, MAX_LOGICAL_REGIONS> region_replayer_initialized_;
    
    // Map: segment base address -> (compute_node_id, logical_region_id, segment_size)
    // Used to identify which stream an RDMA write with imm belongs to
    // Key is segment base address (nodeID + base offset)
    struct SegmentInfo {
        uint16_t compute_node_id;
        uint16_t logical_region_id;
        size_t segment_size;
        uint64_t base_addr;  // Base address of the segment
    };
    std::unordered_map<uint64_t, SegmentInfo> segment_to_stream_;
    std::mutex segment_to_stream_mtx_;  // Mutex for segment_to_stream_ map
    
    // Condition variable and mutex for waiting until all logs are replayed
    std::condition_variable all_logs_replayed_cv_;
    std::mutex all_logs_replayed_mtx_;
    
    // Generate array index for stream_states_ array
    // Formula: compute_id * MAX_LOGICAL_REGIONS + region_id
    static size_t MakeStreamIndex(uint16_t compute_id, uint16_t region_id) {
        return static_cast<size_t>(compute_id) * MAX_LOGICAL_REGIONS + static_cast<size_t>(region_id);
    }
    
    // Get or initialize a LogStreamState at the given index
    LogStreamState* GetOrInitializeStreamState(uint16_t compute_id, uint16_t region_id) {
        size_t stream_idx = MakeStreamIndex(compute_id, region_id);
        if (stream_idx >= MAX_STREAMS) {
            return nullptr; // Invalid index
        }
        
        // Check if already initialized
        if (stream_initialized_[stream_idx].load(std::memory_order_acquire)) {
            return reinterpret_cast<LogStreamState*>(&stream_states_[stream_idx]);
        }
        
        // Try to initialize (atomic compare-and-swap to avoid race conditions)
        bool expected = false;
        if (stream_initialized_[stream_idx].compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            // We won the race, initialize the stream state
            printf("LogReplayerManager: Initializing stream state for compute_node=%u, logical_region=%u\n", compute_id, region_id);
            fflush(stdout);
            LogStreamState* state = reinterpret_cast<LogStreamState*>(&stream_states_[stream_idx]);
            new (state) LogStreamState(compute_id, region_id);
            return state;
        } else {
            // Someone else initialized it, just return the existing one
            return reinterpret_cast<LogStreamState*>(&stream_states_[stream_idx]);
        }
    }
    
    // Get or create a region replayer thread
    LogicalRegionReplayer* GetOrCreateRegionReplayer(uint16_t logical_region_id);
    
    // Replayer thread function (handles all streams for one logical region)
    static void ReplayerThreadFunc(LogicalRegionReplayer* region_replayer, 
                                   LogReplayerManager* manager, RDMA_Manager* rdma_mg);
    
    // Send RPC to compute node about recyclable segments
    static void SendSegmentRecycleRPC(LogStreamState* stream_state, 
                                      const std::vector<LogSegment>& recycled_segments,
                                      RDMA_Manager* rdma_mg);

    // Page-version-aware log replay logic
    void ReplayLogData(LogStreamState* stream_state, uint64_t available_bytes);
    bool ProcessLogRecord(const RedoLogger::RecordHeader& header, const uint8_t* payload, size_t payload_size);
    uint64_t GetCurrentPageVersion(GlobalAddress page_addr, DSMEngine::DataPage*& page_ptr);
    void SetCurrentPageVersion(GlobalAddress page_addr, uint64_t version);
    
    // Recycle fully replayed segments
    void RecycleSegments(LogStreamState* stream_state);
    
    
    // Pseudo replay logic for testing (deprecated)
    void PseudoReplayLogData(LogStreamState* stream_state, uint64_t available_bytes);
    void PseudoDecodeLogRecord(uint16_t compute_node_id, uint16_t logical_region_id,
                              uint32_t record_index, uint32_t payload_size);
};

} // namespace DSMEngine
