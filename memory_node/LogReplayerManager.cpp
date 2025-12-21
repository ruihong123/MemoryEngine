#include "memory_node/LogReplayerManager.h"
#include <cassert>
#include <cstdio>
#include <thread>
#include <chrono>

namespace DSMEngine {

#ifndef NDEBUG
// Helper function to get log type string (only in debug mode)
static const char* GetLogTypeString(RedoLogger::LogRecordType log_type) {
    switch (log_type) {
        case RedoLogger::LOG_UNKNOWN: return "LOG_UNKNOWN";
        case RedoLogger::LOG_DATA_PAGE_INIT: return "LOG_DATA_PAGE_INIT";
        case RedoLogger::LOG_DATA_PAGE_BITMAP_UPDATE: return "LOG_DATA_PAGE_BITMAP_UPDATE";
        case RedoLogger::LOG_DATA_PAGE_UPDATE: return "LOG_DATA_PAGE_UPDATE";
        case RedoLogger::LOG_INTERNAL_PAGE_STORE: return "LOG_INTERNAL_PAGE_STORE";
        case RedoLogger::LOG_LEAF_PAGE_STORE: return "LOG_LEAF_PAGE_STORE";
        case RedoLogger::LOG_LEAF_PAGE_DELETE: return "LOG_LEAF_PAGE_DELETE";
        case RedoLogger::LOG_INTERNAL_PAGE_SPLIT_OLD: return "LOG_INTERNAL_PAGE_SPLIT_OLD";
        case RedoLogger::LOG_INTERNAL_PAGE_SPLIT_NEW: return "LOG_INTERNAL_PAGE_SPLIT_NEW";
        case RedoLogger::LOG_LEAF_PAGE_SPLIT_OLD: return "LOG_LEAF_PAGE_SPLIT_OLD";
        case RedoLogger::LOG_LEAF_PAGE_SPLIT_NEW: return "LOG_LEAF_PAGE_SPLIT_NEW";
        case RedoLogger::LOG_NEW_ROOT_PAGE: return "LOG_NEW_ROOT_PAGE";
        case RedoLogger::LOG_INDEX_PAGE_CHANGE: return "LOG_INDEX_PAGE_CHANGE";
        case RedoLogger::LOG_INDEX_PAGE_HEADER_CHANGE: return "LOG_INDEX_PAGE_HEADER_CHANGE";
        case RedoLogger::LOG_INDEX_PAGE_CONTENT_CHANGE: return "LOG_INDEX_PAGE_CONTENT_CHANGE";
        default: return "LOG_UNKNOWN";
    }
}
#endif

void LogReplayerManager::HandleLogSegmentRequest(const LogSegmentRequest& request) {
    uint16_t compute_node_id = request.compute_node_id;
    uint16_t logical_region_id = request.logical_region_id;
    size_t stream_idx = MakeStreamIndex(compute_node_id, logical_region_id);
    
    if (stream_idx >= MAX_STREAMS) {
        fprintf(stderr, "LogReplayerManager: Invalid stream index %zu for compute_node=%u, logical_region=%u\n",
               stream_idx, compute_node_id, logical_region_id);
        return;
    }
    
    // Check if this is the first request for this stream
    bool is_new_stream = !stream_initialized_[stream_idx].load(std::memory_order_acquire);
    
    LogStreamState* stream_state = GetOrInitializeStreamState(compute_node_id, logical_region_id);
    if (stream_state == nullptr) {
        fprintf(stderr, "LogReplayerManager: Failed to get stream state for compute_node=%u, logical_region=%u\n",
               compute_node_id, logical_region_id);
        return;
    }
    
    // Add new segment
    LogSegment segment;
    segment.segment_addr = request.log_segment_addr;
    segment.segment_size = request.segment_size;
    segment.received_length = 0;
    
    // Calculate and cache the physical pointer to avoid recalculation during replay
    uint16_t logical_id = request.log_segment_addr.nodeID;
    uint16_t this_physical_id = rdma_mg_->node_id;
    segment.physical_ptr = rdma_mg_->TranslateLogicalToPhysicalAddress(
        logical_id, request.log_segment_addr.offset, this_physical_id);
    
    if (segment.physical_ptr == 0) {
        fprintf(stderr, "LogReplayerManager: Error - Could not translate segment address for logical_id=%u, segment=0x%lx\n",
               logical_id, request.log_segment_addr.val);
        return;
    }
    
    std::unique_lock<RWSpinMutex> segments_lk(stream_state->segments_mtx);
#ifndef NDEBUG
    // Before adding a new segment, verify all previous segments are filled to near capacity
    // (received_length >= segment_size - 1000)
    if (!stream_state->segments.empty()) {
        for (const auto& seg : stream_state->segments) {
            // Assert that all previous segments are filled to near capacity before adding a new segment
            assert(seg.received_length >= seg.segment_size - 1000 && 
                   "Previous segment must be filled to near capacity (within 1000 bytes) before adding new segment");
        }
    }
#endif
    
    stream_state->segments.push_back(segment);
    segments_lk.unlock();
    
    // Register segment address mapping (use GlobalAddress directly as key)
    {
        std::unique_lock<std::mutex> seg_lk(segment_to_stream_mtx_);
        uint64_t seg_key = request.log_segment_addr.val;
        SegmentInfo info;
        info.compute_node_id = compute_node_id;
        info.logical_region_id = logical_region_id;
        info.segment_size = request.segment_size;
        info.base_addr = seg_key;
        segment_to_stream_[seg_key] = info;
    }
    
    // Ensure region replayer thread exists for this logical region
    LogicalRegionReplayer* region_replayer = GetOrCreateRegionReplayer(logical_region_id);
    if (region_replayer != nullptr) {
        // Notify the region replayer that there's new data
        region_replayer->new_data_cv.notify_one();
    }
    
    if (is_new_stream) {
        printf("LogReplayerManager: Created new stream for compute_node=%u, logical_region=%u\n",
               compute_node_id, logical_region_id);
    } else {
        printf("LogReplayerManager: Added new segment for compute_node=%u, logical_region=%u\n",
               compute_node_id, logical_region_id);
    }
}

void LogReplayerManager::HandleWriteWithImm(uint16_t compute_node_id, uint16_t logical_region_id, 
                                           uint32_t transferred_size) {
    // transferred_size: bytes received in this RDMA write
    // compute_node_id and logical_region_id identify which stream this write belongs to
    
    size_t stream_idx = MakeStreamIndex(compute_node_id, logical_region_id);
    if (stream_idx >= MAX_STREAMS) {
        fprintf(stdout, "LogReplayerManager: Invalid stream index %zu for compute_node=%u, logical_region=%u\n",
               stream_idx, compute_node_id, logical_region_id);
               assert(false);
        return;
    }
    
    // Print RDMA write reception information
    printf("LogReplayerManager: Received RDMA write - memory_node_id=%u, logical_region_id=%u, compute_node_id=%u, received_size=%u bytes\n",
           rdma_mg_->node_id, logical_region_id, compute_node_id, transferred_size);
    fflush(stdout);
    
    if (stream_initialized_[stream_idx].load(std::memory_order_acquire)) {
        LogStreamState* stream_state = reinterpret_cast<LogStreamState*>(&stream_states_[stream_idx]);
        // Update received log length
        uint64_t old_length = stream_state->received_log_length.fetch_add(transferred_size);
        
        // Update the most recent segment's received_length
        // (writes are appended sequentially to the current segment)
        std::unique_lock<RWSpinMutex> segments_lk(stream_state->segments_mtx);
        if (!stream_state->segments.empty()) {
#ifndef NDEBUG
            // Before updating the last segment, ensure all previous segments are full
            // Check all segments except the last one
            if (stream_state->segments.size() > 1) {
                auto seg_it = stream_state->segments.begin();
                auto last_seg_it = --stream_state->segments.end();
                
                // Check all previous segments (excluding the last one)
                while (seg_it != last_seg_it) {
                    const LogSegment& seg = *seg_it;
                    // Ensure previous segments are filled to near capacity
                    // Allow 1000 bytes tolerance to account for partial writes
                    // Assert that all previous segments are full before writing to the new segment
                    assert(seg.received_length >= seg.segment_size - 1000 && 
                           "Previous segment must be filled to near capacity (within 1000 bytes) before writing to new segment");
                    ++seg_it;
                }
            }
#endif
            // Update the last segment's received_length
            stream_state->segments.back().received_length += transferred_size;
        }
        segments_lk.unlock();
        
        // Notify the region replayer thread that new data is available
        LogicalRegionReplayer* region_replayer = GetOrCreateRegionReplayer(logical_region_id);
        if (region_replayer != nullptr) {
            region_replayer->new_data_cv.notify_one();
        }
        
        // printf("LogReplayerManager: Updated received_log_length for compute_node=%u, "
        //        "logical_region=%u: +%u bytes (total=%lu)\n",
        //        compute_node_id, logical_region_id, imm_data, old_length + imm_data);
    }else{
        assert(false);
        printf("LogReplayerManager: Invalid stream index %zu for compute_node=%u, logical_region=%u\n",
               stream_idx, compute_node_id, logical_region_id);
    }
}

std::vector<uint16_t> LogReplayerManager::GetLogicalRegionsForComputeNode(uint16_t compute_node_id) {
    std::vector<uint16_t> regions;
    
    for (size_t i = 0; i < MAX_STREAMS; ++i) {
        if (stream_initialized_[i].load(std::memory_order_acquire)) {
            LogStreamState* state = reinterpret_cast<LogStreamState*>(&stream_states_[i]);
            if (state->compute_node_id == compute_node_id) {
                regions.push_back(state->logical_region_id);
            }
        }
    }
    
    return regions;
}

void LogReplayerManager::StopAllReplayers() {
    // Signal all region replayer threads to exit and notify them
    for (size_t i = 0; i < MAX_LOGICAL_REGIONS; ++i) {
        if (region_replayer_initialized_[i].load(std::memory_order_acquire)) {
            region_replayers_[i]->should_exit.store(true);
            region_replayers_[i]->new_data_cv.notify_one();
        }
    }
    
    // Wait for all region replayer threads to finish
    for (size_t i = 0; i < MAX_LOGICAL_REGIONS; ++i) {
        if (region_replayer_initialized_[i].load(std::memory_order_acquire)) {
            if (region_replayers_[i]->replayer_thread.joinable()) {
                region_replayers_[i]->replayer_thread.join();
            }
        }
    }
    
    // Clear all initialization flags
    for (auto& flag : stream_initialized_) {
        flag.store(false, std::memory_order_relaxed);
    }
    for (auto& flag : region_replayer_initialized_) {
        flag.store(false, std::memory_order_relaxed);
    }
}

void LogReplayerManager::WaitForAllLogsReplayed() {
    std::unique_lock<std::mutex> lk(all_logs_replayed_mtx_);
    
    // Wait until all initialized streams have replayed all received logs
    all_logs_replayed_cv_.wait(lk, [this] {
        // Check all initialized streams
        for (size_t i = 0; i < MAX_STREAMS; ++i) {
            if (stream_initialized_[i].load(std::memory_order_acquire)) {
                LogStreamState* stream_state = reinterpret_cast<LogStreamState*>(&stream_states_[i]);
                uint64_t received = stream_state->received_log_length.load(std::memory_order_acquire);
                uint64_t replayed = stream_state->replayed_log_length.load(std::memory_order_acquire);
                
                if (received > replayed) {
                    // At least one stream still has pending logs
                    return false;
                }
            }
        }
        // All streams are caught up
        return true;
    });
}

LogicalRegionReplayer* LogReplayerManager::GetOrCreateRegionReplayer(uint16_t logical_region_id) {
    if (logical_region_id >= MAX_LOGICAL_REGIONS) {
        return nullptr;
    }
    
    // Check if already initialized
    if (region_replayer_initialized_[logical_region_id].load(std::memory_order_acquire)) {
        return region_replayers_[logical_region_id].get();
    }
    
    // Try to initialize (atomic compare-and-swap to avoid race conditions)
    bool expected = false;
    if (region_replayer_initialized_[logical_region_id].compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        // We won the race, create the region replayer
        region_replayers_[logical_region_id] = std::make_unique<LogicalRegionReplayer>(logical_region_id);
        LogicalRegionReplayer* replayer = region_replayers_[logical_region_id].get();
        
        // Spawn the replayer thread
        replayer->replayer_thread = std::thread(ReplayerThreadFunc, replayer, this, rdma_mg_);
        
        printf("LogReplayerManager: Spawned region replayer thread for logical_region=%u\n", logical_region_id);
        return replayer;
    } else {
        // Someone else initialized it
        return region_replayers_[logical_region_id].get();
    }
}

void LogReplayerManager::ReplayerThreadFunc(LogicalRegionReplayer* region_replayer, 
                                           LogReplayerManager* manager, RDMA_Manager* rdma_mg) {
    printf("LogReplayer: Region thread started for logical_region=%u\n", region_replayer->logical_region_id);
    
    while (!region_replayer->should_exit.load()) {
        std::unique_lock<std::mutex> lk(region_replayer->cv_mtx);
        
        // Wait for new data or exit signal
        region_replayer->new_data_cv.wait(lk, [region_replayer, manager] {
            if (region_replayer->should_exit.load()) {
                return true;
            }
            
            // Check if any stream in this region has new data to replay
            uint16_t region_id = region_replayer->logical_region_id;
            for (uint16_t compute_id = 0; compute_id < manager->MAX_COMPUTE_NODES; ++compute_id) {
                size_t stream_idx = manager->MakeStreamIndex(compute_id, region_id);
                if (manager->stream_initialized_[stream_idx].load(std::memory_order_acquire)) {
                    LogStreamState* state = reinterpret_cast<LogStreamState*>(&manager->stream_states_[stream_idx]);
                    if (state->received_log_length.load() > state->replayed_log_length.load()) {
                        return true;
                    }
                }
            }
            return false;
        });
        
        if (region_replayer->should_exit.load()) {
            break;
        }
        
        lk.unlock();
        
        // Track progress to detect if we're bouncing between streams without making progress
        std::lock_guard<std::mutex> progress_lk(region_replayer->progress_mtx);
        bool has_streams_with_available_logs = false;
        bool any_stream_made_progress = false;
        
        // Process all streams in this logical region
        uint16_t region_id = region_replayer->logical_region_id;
        for (uint16_t compute_id = 0; compute_id < manager->MAX_COMPUTE_NODES; ++compute_id) {
            size_t stream_idx = manager->MakeStreamIndex(compute_id, region_id);
            if (manager->stream_initialized_[stream_idx].load(std::memory_order_acquire)) {
                LogStreamState* stream_state = reinterpret_cast<LogStreamState*>(&manager->stream_states_[stream_idx]);
                
                // Check if there's new data to replay for this stream
                uint64_t received = stream_state->received_log_length.load();
                uint64_t replayed_before = stream_state->replayed_log_length.load();
                
                // Check if there are available logs for this stream
                bool has_available_logs = (received > replayed_before);
                
                if (has_available_logs) {
                    has_streams_with_available_logs = true;
                    
                    // There's new data to replay using page-version-aware logic
                    // ReplayLogData now handles segment tracking and recycling internally
                    manager->ReplayLogData(stream_state, received - replayed_before);
                    
                    // Check if we made progress after replay
                    uint64_t replayed_after = stream_state->replayed_log_length.load();
                    
                    // Track if this stream made progress
                    if (replayed_after > replayed_before) {
                        any_stream_made_progress = true;
                    }
                }
            }
        }
        
#ifndef NDEBUG
        // Only abort if there are streams with available logs but NONE of them made progress
        bool should_abort = has_streams_with_available_logs && !any_stream_made_progress;
        
        if (should_abort) {
            region_replayer->iterations_without_progress++;
            
            // Check if we've exceeded the threshold
            if (region_replayer->iterations_without_progress >= LogicalRegionReplayer::MAX_ITERATIONS_WITHOUT_PROGRESS) {
                fprintf(stdout, "\n[LOG_REPLAY_DEADLOCK] Region %u: Detected log replay bouncing between streams without progress!\n", 
                       region_replayer->logical_region_id);
                fprintf(stdout, "[LOG_REPLAY_DEADLOCK] Iterations without progress: %u (threshold: %u)\n",
                       region_replayer->iterations_without_progress,
                       LogicalRegionReplayer::MAX_ITERATIONS_WITHOUT_PROGRESS);
                fprintf(stdout, "[LOG_REPLAY_DEADLOCK] Stream states for region %u:\n", region_replayer->logical_region_id);
                
                // Print detailed state for debugging
                for (uint16_t compute_id = 0; compute_id < manager->MAX_COMPUTE_NODES; ++compute_id) {
                    size_t stream_idx = manager->MakeStreamIndex(compute_id, region_id);
                    if (manager->stream_initialized_[stream_idx].load(std::memory_order_acquire)) {
                        LogStreamState* stream_state = reinterpret_cast<LogStreamState*>(&manager->stream_states_[stream_idx]);
                        uint64_t received = stream_state->received_log_length.load();
                        uint64_t replayed = stream_state->replayed_log_length.load();
                        fprintf(stdout, "  Stream (compute=%u, region=%u): received=%lu, replayed=%lu, pending=%lu",
                               compute_id, region_id, received, replayed, (received > replayed ? received - replayed : 0));
                        
                        // Print stuck record header if available
                        {
                            std::lock_guard<std::mutex> lk(stream_state->stuck_record_mtx);
                            if (stream_state->has_stuck_record) {
                                fprintf(stdout, " [STUCK at: page=0x%lx, page_version=%lu, payload_len=%u",
                                       stream_state->stuck_record_header.page_gaddr.val,
                                       stream_state->stuck_record_header.page_version,
                                       stream_state->stuck_record_header.payload_len);
#ifndef NDEBUG
                                fprintf(stdout, ", log_type=%u", static_cast<uint32_t>(stream_state->stuck_record_header.log_type));
#endif
                                fprintf(stdout, "]");
                            }
                        }
                        fprintf(stdout, "\n");
                    }
                }
                fflush(stdout);
                lk.lock();
                region_replayer->new_data_cv.wait(lk);
                // Abort for debugging
                // assert(false && "Log replay deadlock detected: bouncing between streams without progress");
            }
        } else {
            // Progress was made or no available logs, reset counter
            region_replayer->iterations_without_progress = 0;
        }
#endif
    }
    
    printf("LogReplayer: Region thread exiting for logical_region=%u\n", region_replayer->logical_region_id);
}

void LogReplayerManager::SendSegmentRecycleRPC(LogStreamState* stream_state,
                                               const std::vector<LogSegment>& recycled_segments,
                                               RDMA_Manager* rdma_mg) {
    // Send segments in batches (max 16 per RPC due to fixed array size)
    const size_t MAX_SEGMENTS_PER_RPC = 16;
    
    for (size_t i = 0; i < recycled_segments.size(); i += MAX_SEGMENTS_PER_RPC) {
        size_t batch_size = std::min(MAX_SEGMENTS_PER_RPC, recycled_segments.size() - i);
        
        // Get send buffer
        ibv_mr* send_mr = rdma_mg->Get_local_send_message_mr();
        RDMA_Request* send_pointer = (RDMA_Request*)send_mr->addr;
        
        send_pointer->command = log_segment_recycle;
        
        // Fill in the recycle request
        send_pointer->content.log_segment_recycle.memory_node_id = rdma_mg->node_id;
        send_pointer->content.log_segment_recycle.logical_region_id = stream_state->logical_region_id;
        send_pointer->content.log_segment_recycle.num_segments = static_cast<uint32_t>(batch_size);
        
        // Copy segment addresses
        for (size_t j = 0; j < batch_size; ++j) {
            send_pointer->content.log_segment_recycle.segment_addrs[j] = recycled_segments[i + j].segment_addr;
        }
        
        // Send RPC to compute node
        uint16_t target_compute_node = stream_state->compute_node_id;
        int rc = rdma_mg->post_send<RDMA_Request>(send_mr, target_compute_node, std::string("main"));
        if (rc) {
            fprintf(stderr, "LogReplayerManager: failed to send log_segment_recycle RPC to compute_node=%u (rc=%d)\n",
                   target_compute_node, rc);
            continue;
        }
        
        // Poll for send completion
        ibv_wc wc[2] = {};
        if (rdma_mg->poll_completion(wc, 1, std::string("main"), true, target_compute_node)) {
            fprintf(stderr, "LogReplayerManager: failed to poll send completion for log_segment_recycle RPC\n");
        } else {
            // Print debug information for each recycled segment
            for (size_t j = 0; j < batch_size; ++j) {
                const LogSegment& seg = recycled_segments[i + j];
                printf("LogReplayerManager: Sending recycle message - received_length=%lu, replayed_bytes=%lu, segment_size=%zu, logical_region=%u, compute_node=%u\n",
                       seg.received_length, stream_state->replayed_log_length.load(), seg.segment_size,
                       stream_state->logical_region_id, stream_state->compute_node_id);
            }
            printf("LogReplayerManager: Sent recycle notification for %zu segments to compute_node=%u, logical_region=%u\n",
                   batch_size, target_compute_node, stream_state->logical_region_id);
            fflush(stdout);
        }
    }
}

void LogReplayerManager::ReplayLogData(LogStreamState* stream_state, uint64_t available_bytes) {
    uint64_t total_processed_bytes = 0;
    uint32_t total_records_processed = 0;
    uint64_t replayed_bytes = stream_state->replayed_log_length.load(std::memory_order_relaxed);
    // todo: the log replay implementation below is not efficient, we need to parse the physical address for next log
    // in every loop. We can simply remember the physical pointer can move to next next time, if it did not go out of
    // bound of current segment.

    // (2) Replay log until we meet a future page version or use all available_bytes
    while (total_processed_bytes < available_bytes) {
        // (1) Find the current segment based on replayed_log_length
        std::unique_lock<RWSpinMutex> segments_lk(stream_state->segments_mtx);
        uint64_t offset_in_existing_segs = replayed_bytes - stream_state->recycled_prefix_bytes;
        
        auto seg_it = stream_state->segments.begin();
        uint64_t accumulated_bytes = 0;
        while (seg_it != stream_state->segments.end()) {
            if (offset_in_existing_segs < accumulated_bytes + seg_it->received_length) {
                break; // Found the current segment
            }
            accumulated_bytes += seg_it->received_length;
            seg_it++;
        }
        
        if (seg_it == stream_state->segments.end()) {
            segments_lk.unlock();
            break; // No more segments
        }
        
        // Calculate position within current segment
        uint64_t offset_in_segment = offset_in_existing_segs - accumulated_bytes;
        const LogSegment* current_seg = &(*seg_it);
        uint64_t remaining_in_segment = current_seg->received_length - offset_in_segment;
        
        // Use cached physical pointer instead of recalculating from replication metadata
        if (current_seg->physical_ptr == 0) {
            printf("LogReplayer: Error - Invalid physical pointer for segment=0x%lx\n",
                   current_seg->segment_addr.val);
            segments_lk.unlock();
            break;
        }
        
        // Calculate current physical address by adding offset to base physical pointer
        uint64_t physical_seg_addr = current_seg->physical_ptr + offset_in_segment;
        char* log_buffer = reinterpret_cast<char*>(physical_seg_addr);
        uint64_t bytes_to_process = std::min(available_bytes - total_processed_bytes, remaining_in_segment);
        segments_lk.unlock();
        
        // Process log records from this segment
        uint64_t processed_bytes = 0;
        
        while (processed_bytes < bytes_to_process) {
            // Check for segment close marker
            if (log_buffer[processed_bytes] == RedoLogger::SEGMENT_CLOSE_MARKER) {
                processed_bytes += 1; // Skip marker
                break; // Move to next segment
            }
            
            // Check if we have enough data for a record header
            if (processed_bytes + sizeof(RedoLogger::RecordHeader) > bytes_to_process) {
                assert(false);
                break; // Not enough data
            }
            
            // Parse record header
            RedoLogger::RecordHeader* header = reinterpret_cast<RedoLogger::RecordHeader*>(log_buffer + processed_bytes);
            
            // Validate header magic
            if (header->magic != 0x4C52) {
                assert(false);
                printf("LogReplayer: Invalid record magic 0x%04x at offset %lu\n", header->magic, processed_bytes);
                break;
            }
            
            uint32_t payload_size = header->payload_len;
            uint64_t record_size = sizeof(RedoLogger::RecordHeader) + payload_size;
            
            if (processed_bytes + record_size > bytes_to_process) {
                assert(false);
                break; // Not enough data for complete record
            }
            
            // Get payload pointer
            const uint8_t* payload = reinterpret_cast<const uint8_t*>(log_buffer + processed_bytes + sizeof(RedoLogger::RecordHeader));
            
            // Check page version ordering
            DSMEngine::DataPage* page_ptr = nullptr;
            uint64_t current_version = GetCurrentPageVersion(header->page_gaddr, page_ptr);
            
            if (header->page_version <= current_version) {
                // Records should never be skipped - this indicates a serious ordering bug
                assert(false && "Received stale log record - this indicates a serious ordering bug");
                // Store stuck record header for debugging
                {
                    std::lock_guard<std::mutex> lk(stream_state->stuck_record_mtx);
                    stream_state->has_stuck_record = true;
                    stream_state->stuck_record_header = *header;  // Copy the header
                }
                // Don't skip - abort to catch the bug
                return;
            } else if (header->page_version == current_version + 1) {
                // Apply the record
                if (ProcessLogRecord(*header, payload, payload_size)) {
                    SetCurrentPageVersion(header->page_gaddr, header->page_version);
                    printf("LogReplayer: Applied record (page_version=%lu) for page=0x%lx from compute_node=%u\n",
                           header->page_version, header->page_gaddr.val, stream_state->compute_node_id);
                    total_records_processed++;
                } else {
                    // ProcessLogRecord failed - this should not happen
                    assert(false && "ProcessLogRecord failed");
                    // Store stuck record header for debugging
                    {
                        std::lock_guard<std::mutex> lk(stream_state->stuck_record_mtx);
                        stream_state->has_stuck_record = true;
                        stream_state->stuck_record_header = *header;  // Copy the header
                    }
                    return;
                }
            } else {
                // Future record - move to another stream
                // printf("LogReplayer: Future record (page_version=%lu > current+1=%lu) for page=0x%lx - moving to next stream\n",
                //        header->page_version, current_version + 1, header->page_gaddr.val);
                // assert(false && "Future record");
                // Store stuck record header for debugging (this is where the stream stopped)
                {
                    std::lock_guard<std::mutex> lk(stream_state->stuck_record_mtx);
                    stream_state->has_stuck_record = true;
                    stream_state->stuck_record_header = *header;  // Copy the header
                }
                
                // Update state with what we've processed so far
                total_processed_bytes += processed_bytes;
                replayed_bytes += processed_bytes;
                stream_state->replayed_log_length.store(replayed_bytes, std::memory_order_release);
                
                // Check if this stream is now caught up and notify waiters
                uint64_t received = stream_state->received_log_length.load(std::memory_order_acquire);
                if (replayed_bytes >= received) {
                    all_logs_replayed_cv_.notify_all();
                }
                
                // Recycle segments only if we've replayed some logs
                if (total_records_processed > 0) {
                    RecycleSegments(stream_state);
                }
                if (total_records_processed > 0) {
                    // printf("LogReplayer: Processed %u records (%u applied) (%lu bytes) for compute_node=%u, logical_region=%u before moving to next stream\n",
                    //        total_records_processed, total_records_processed, total_processed_bytes,
                    //        stream_state->compute_node_id, stream_state->logical_region_id);
                    // fflush(stdout);
                }
                return;
            }
            
            processed_bytes += record_size;
        }
        
        // Update counters
        total_processed_bytes += processed_bytes;
        replayed_bytes += processed_bytes;
        stream_state->replayed_log_length.store(replayed_bytes, std::memory_order_release);
        
        // Check if this stream is now caught up and notify waiters
        uint64_t received = stream_state->received_log_length.load(std::memory_order_acquire);
        if (replayed_bytes >= received) {
            // This stream is caught up, notify waiters
            all_logs_replayed_cv_.notify_all();
        }
    }
    
    // (3) Recycle replayed log segments (only if we've replayed some logs)
    if (total_records_processed > 0) {
        RecycleSegments(stream_state);
    }
    
    // Final check: if this stream is caught up, notify waiters
    uint64_t final_replayed = stream_state->replayed_log_length.load(std::memory_order_acquire);
    uint64_t final_received = stream_state->received_log_length.load(std::memory_order_acquire);
    if (final_replayed >= final_received) {
        all_logs_replayed_cv_.notify_all();
    }
    
    if (total_records_processed > 0) {
        printf("LogReplayer: Processed %u records (%u applied) (%lu bytes) for compute_node=%u, logical_region=%u\n",
               total_records_processed, total_records_processed, total_processed_bytes,
               stream_state->compute_node_id, stream_state->logical_region_id);
        fflush(stdout);
    }
}

bool LogReplayerManager::ProcessLogRecord(const RedoLogger::RecordHeader& header, const uint8_t* payload, size_t payload_size) {
    // Translate logical address to physical address on this memory node
    uint16_t logical_id = header.page_gaddr.nodeID;
    uint16_t this_physical_id = rdma_mg_->node_id;
    
    // Get the physical address for this replica
    uint64_t physical_addr = rdma_mg_->TranslateLogicalToPhysicalAddress(
        logical_id, header.page_gaddr.offset, this_physical_id);
    
    if (physical_addr == 0) {
        printf("LogReplayer: Error - Could not translate address for logical_id=%u, page=0x%lx\n", 
               logical_id, header.page_gaddr.val);
        assert(false);
        return false;
    }
    
    // Get the actual physical page buffer
    void* page_buffer = reinterpret_cast<void*>(physical_addr);
    
#ifndef NDEBUG
    printf("    LogReplayer: Processing record for page=0x%lx, version=%lu, payload_size=%zu, log_type=%s\n",
           header.page_gaddr.val, header.page_version, payload_size, GetLogTypeString(header.log_type));
    fflush(stdout);
#endif
    // Use LogCodec to decode and apply operations to the actual page
    LogCodec::Decoder decoder(payload, payload_size);
    LogCodec::DecodedOp op;
    
    while (decoder.Next(op)) {
        switch (op.code) {
            case LogCodec::OpCode::UPDATE_BYTES: {
                // Apply UPDATE_BYTES operation to the physical page
                char* target_addr = static_cast<char*>(page_buffer) + op.u.update.offset;
                memcpy(target_addr, op.u.update.bytes, op.u.update.len);
#ifndef NDEBUG
                printf("      Applied UPDATE_BYTES: offset=%u, len=%u\n", 
                       op.u.update.offset, op.u.update.len);
#endif
                break;
            }
            case LogCodec::OpCode::SET_U64_LE: {
                // Apply SET_U64_LE operation to the physical page
                char* target_addr = static_cast<char*>(page_buffer) + op.u.set64.offset;
                *reinterpret_cast<uint64_t*>(target_addr) = op.u.set64.value;
#ifndef NDEBUG
                printf("      Applied SET_U64_LE: offset=%u, value=%lu\n", 
                       op.u.set64.offset, op.u.set64.value);
#endif
                break;
            }
            case LogCodec::OpCode::FILL_BYTES: {
                // Apply FILL_BYTES operation to the physical page
                char* target_addr = static_cast<char*>(page_buffer) + op.u.fill.offset;
                memset(target_addr, op.u.fill.value, op.u.fill.len);
#ifndef NDEBUG
                printf("      Applied FILL_BYTES: offset=%u, len=%u, value=0x%02x\n", 
                       op.u.fill.offset, op.u.fill.len, op.u.fill.value);
#endif
                break;
            }
            case LogCodec::OpCode::MEMMOVE_BYTES: {
                // Apply MEMMOVE_BYTES operation to the physical page
                char* page_base = static_cast<char*>(page_buffer);
                memmove(page_base + op.u.move.dst, page_base + op.u.move.src, op.u.move.len);
#ifndef NDEBUG
                printf("      Applied MEMMOVE_BYTES: dst=%u, src=%u, len=%u\n", 
                       op.u.move.dst, op.u.move.src, op.u.move.len);
#endif
                break;
            }
            default:
#ifndef NDEBUG
                printf("      Unknown opcode: %u\n", static_cast<uint8_t>(op.code));
#endif
                return false;
        }
    }
    
    if (!decoder.Good()) {
        printf("LogReplayer: Error - LogCodec decoder failed for page=0x%lx\n", header.page_gaddr.val);
        assert(false);
        return false;
    }
    
    return true; // Successfully processed all operations
}

uint64_t LogReplayerManager::GetCurrentPageVersion(GlobalAddress page_addr, DSMEngine::DataPage*& page_ptr) {
    // Translate logical address to physical address on this memory node
    uint16_t logical_id = page_addr.nodeID;
    uint16_t this_physical_id = rdma_mg_->node_id;
    
    // Get the physical address for this replica
    uint64_t physical_addr = rdma_mg_->TranslateLogicalToPhysicalAddress(
        logical_id, page_addr.offset, this_physical_id);
    
    if (physical_addr == 0) {
        printf("LogReplayer: Warning - Could not translate address for logical_id=%u, page=0x%lx\n", 
               logical_id, page_addr.val);
        page_ptr = nullptr;
        return 0;
    }
    
    // Get the page header and read version from the physical replica
    page_ptr = reinterpret_cast<DSMEngine::DataPage*>(physical_addr);
    
    // Return version and set page pointer for debugging
    return page_ptr->hdr.p_version;
}

void LogReplayerManager::SetCurrentPageVersion(GlobalAddress page_addr, uint64_t version) {
    // Translate logical address to physical address on this memory node
    uint16_t logical_id = page_addr.nodeID;
    uint16_t this_physical_id = rdma_mg_->node_id;
    
    // Get the physical address for this replica
    uint64_t physical_addr = rdma_mg_->TranslateLogicalToPhysicalAddress(
        logical_id, page_addr.offset, this_physical_id);
    
    if (physical_addr == 0) {
        printf("LogReplayer: Warning - Could not translate address for logical_id=%u, page=0x%lx\n", 
               logical_id, page_addr.val);
        return;
    }
    
    // Update the page header with new version
    DSMEngine::DataPage* data_page = reinterpret_cast<DSMEngine::DataPage*>(physical_addr);
    
    // TODO: Update actual page header with new version
    data_page->hdr.p_version = version;
    
    // printf("LogReplayer: Updated page version to %lu for page=0x%lx (physical_addr=0x%lx)\n", 
    //        version, page_addr.val, physical_addr);
}

void LogReplayerManager::RecycleSegments(LogStreamState* stream_state) {
    std::unique_lock<RWSpinMutex> segments_lk(stream_state->segments_mtx);
    
    uint64_t replayed_bytes = stream_state->replayed_log_length.load(std::memory_order_relaxed);
    if (replayed_bytes <= stream_state->recycled_prefix_bytes) {
        assert(replayed_bytes == stream_state->recycled_prefix_bytes);
        return; // Nothing to recycle
    }
    
    uint64_t recyclable_bytes = replayed_bytes - stream_state->recycled_prefix_bytes;
    std::vector<LogSegment> recycled_segments;
    
    while (!stream_state->segments.empty()) {
        const LogSegment& seg = stream_state->segments.front();
        
        // Skip if segment has no data or not enough bytes replayed
        if (seg.received_length == 0 || recyclable_bytes < seg.received_length) {
            break;
        }
        
        // Only check if recyclable_bytes equals received_length (all received bytes are replayed)
        // received_length INCLUDES the marker if it was written and flushed
        // So if recyclable_bytes == received_length, we've replayed all bytes including the marker
        bool can_recycle = false;
        if (recyclable_bytes == seg.received_length) {
            // Exactly all received bytes are replayed (including marker if present)
            // Check if segment reached its size limit
            if (seg.received_length >= seg.segment_size) {
                assert(seg.received_length == seg.segment_size);
                can_recycle = true;
            } else {
                // Check if the last byte (at position received_length-1) is SEGMENT_CLOSE_MARKER
                // received_length INCLUDES the marker, so marker is at position received_length-1
                // Use cached physical pointer instead of recalculating from replication metadata
                if (seg.physical_ptr == 0) {
                    printf("LogReplayer: Error - Invalid physical pointer for segment=0x%lx in RecycleSegments\n",
                           seg.segment_addr.val);
                    continue;
                }
                
                // Calculate the address to check using cached physical pointer + offset
                uint64_t check_physical_addr = seg.physical_ptr + seg.received_length - 1;
                
                // Use the cached physical pointer (no need to recalculate from replication metadata)
                // Directly read the byte from physical memory
                char* marker_ptr = reinterpret_cast<char*>(check_physical_addr);
                char marker = *marker_ptr;
                
                if (marker == RedoLogger::SEGMENT_CLOSE_MARKER) {
                    can_recycle = true;
                }
            }
        }
        
        if (!can_recycle) {
            break;
        }
        
        recyclable_bytes -= seg.received_length;
        stream_state->recycled_prefix_bytes += seg.received_length;
        recycled_segments.push_back(seg);
        stream_state->segments.pop_front();
    }
    
    segments_lk.unlock();
    
    // Send RPC to compute node about recyclable segments
    if (!recycled_segments.empty()) {
        SendSegmentRecycleRPC(stream_state, recycled_segments, rdma_mg_);
    }
}

void LogReplayerManager::PseudoReplayLogData(LogStreamState* stream_state, uint64_t available_bytes) {
    // Pseudo replay logic - simulate processing log records using LogCodec
    
    if (available_bytes < sizeof(RedoLogger::RecordHeader)) {
        return; // Not enough data for a complete record header
    }
    
    // Simulate decoding log records
    uint64_t processed_bytes = 0;
    uint32_t records_processed = 0;
    
    while (processed_bytes + sizeof(RedoLogger::RecordHeader) <= available_bytes) {
        // Simulate processing a log record header
        processed_bytes += sizeof(RedoLogger::RecordHeader);
        
        // Simulate variable payload size (32-512 bytes)
        uint32_t payload_size = 32 + (records_processed % 16) * 30;
        
        if (processed_bytes + payload_size > available_bytes) {
            break; // Not enough data for complete record
        }
        
        // Simulate LogCodec decoding
        PseudoDecodeLogRecord(stream_state->compute_node_id, 
                            stream_state->logical_region_id, 
                            records_processed, 
                            payload_size);
        
        processed_bytes += payload_size;
        records_processed++;
        
        // Don't process too many records at once
        if (records_processed >= 10) {
            break;
        }
    }
    
    if (records_processed > 0) {
        printf("LogReplayer: Pseudo-replayed %u records (%lu bytes) for compute_node=%u, logical_region=%u\n",
               records_processed, processed_bytes, 
               stream_state->compute_node_id, stream_state->logical_region_id);
    }
}

void LogReplayerManager::PseudoDecodeLogRecord(uint16_t compute_node_id, 
                                              uint16_t logical_region_id,
                                              uint32_t record_index,
                                              uint32_t payload_size) {
    // Simulate LogCodec decoding operations
    
    // Simulate different types of operations based on record index
    switch (record_index % 4) {
        case 0: {
            // Simulate UPDATE_BYTES operation
            uint32_t offset = record_index * 64;
            printf("  LogCodec: Pseudo UPDATE_BYTES at offset=%u, len=%u\n", offset, payload_size);
            break;
        }
        case 1: {
            // Simulate SET_U64_LE operation (timestamp update)
            uint32_t offset = record_index * 8;
            uint64_t timestamp = 1000000 + record_index;
            printf("  LogCodec: Pseudo SET_U64_LE at offset=%u, value=%lu\n", offset, timestamp);
            break;
        }
        case 2: {
            // Simulate FILL_BYTES operation
            uint32_t offset = record_index * 32;
            uint8_t fill_value = record_index % 256;
            printf("  LogCodec: Pseudo FILL_BYTES at offset=%u, len=%u, value=0x%02x\n", 
                   offset, payload_size, fill_value);
            break;
        }
        case 3: {
            // Simulate MEMMOVE_BYTES operation
            uint32_t dst = record_index * 16;
            uint32_t src = dst + 64;
            printf("  LogCodec: Pseudo MEMMOVE_BYTES dst=%u, src=%u, len=%u\n", 
                   dst, src, payload_size);
            break;
        }
    }
    
    // Simulate some processing delay
    // In real implementation, this would apply the operation to actual pages
    std::this_thread::sleep_for(std::chrono::microseconds(1));
}

} // namespace DSMEngine
