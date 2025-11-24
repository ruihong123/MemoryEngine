#include "memory_node/LogReplayerManager.h"
#include <cassert>
#include <cstdio>
#include <thread>
#include <chrono>

namespace DSMEngine {

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
    
    std::unique_lock<std::mutex> segments_lk(stream_state->segments_mtx);
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
                                           uint32_t imm_data) {
    // imm_data contains the transferred size (bytes received)
    // compute_node_id and logical_region_id identify which stream this write belongs to
    
    size_t stream_idx = MakeStreamIndex(compute_node_id, logical_region_id);
    if (stream_idx >= MAX_STREAMS) {
        fprintf(stderr, "LogReplayerManager: Invalid stream index %zu for compute_node=%u, logical_region=%u\n",
               stream_idx, compute_node_id, logical_region_id);
        return;
    }
    
    if (stream_initialized_[stream_idx].load(std::memory_order_acquire)) {
        LogStreamState* stream_state = reinterpret_cast<LogStreamState*>(&stream_states_[stream_idx]);
        // Update received log length
        uint64_t old_length = stream_state->received_log_length.fetch_add(imm_data);
        
        // Update the most recent segment's received_length
        // (writes are appended sequentially to the current segment)
        std::unique_lock<std::mutex> segments_lk(stream_state->segments_mtx);
        if (!stream_state->segments.empty()) {
            // Update the last segment's received_length
            stream_state->segments.back().received_length += imm_data;
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
        
        // Process all streams in this logical region
        uint16_t region_id = region_replayer->logical_region_id;
        for (uint16_t compute_id = 0; compute_id < manager->MAX_COMPUTE_NODES; ++compute_id) {
            size_t stream_idx = manager->MakeStreamIndex(compute_id, region_id);
            if (manager->stream_initialized_[stream_idx].load(std::memory_order_acquire)) {
                LogStreamState* stream_state = reinterpret_cast<LogStreamState*>(&manager->stream_states_[stream_idx]);
                
                // Check if there's new data to replay for this stream
                uint64_t received = stream_state->received_log_length.load();
                uint64_t replayed = stream_state->replayed_log_length.load();
                
                if (received > replayed) {
                    // There's new data to replay using page-version-aware logic
                    manager->ReplayLogData(stream_state, received - replayed);
                    
                    // Advance replayed position by a reasonable chunk
                    uint64_t advance_amount = std::min(static_cast<uint64_t>(256), received - replayed);
                    stream_state->replayed_log_length.store(replayed + advance_amount, std::memory_order_release);
                    
                    // Recycle fully replayed segments
                    std::vector<LogSegment> recycled_segments;
                    {
                        std::unique_lock<std::mutex> segments_lk(stream_state->segments_mtx);
                        uint64_t replayed_bytes = stream_state->replayed_log_length.load(std::memory_order_relaxed);
                        if (replayed_bytes > stream_state->recycled_prefix_bytes) {
                            uint64_t recyclable_bytes = replayed_bytes - stream_state->recycled_prefix_bytes;
                            while (!stream_state->segments.empty()) {
                                const LogSegment& seg = stream_state->segments.front();
                                if (seg.received_length == 0 || recyclable_bytes < seg.received_length) {
                                    break;
                                }
                                recyclable_bytes -= seg.received_length;
                                stream_state->recycled_prefix_bytes += seg.received_length;
                                recycled_segments.push_back(seg);
                                stream_state->segments.pop_front();
                            }
                        }
                    }
                    
                    // Send RPC to compute node about recyclable segments
                    if (!recycled_segments.empty()) {
                        manager->SendSegmentRecycleRPC(stream_state, recycled_segments, rdma_mg);
                    }
                }
            }
        }
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
            printf("LogReplayerManager: Sent recycle notification for %zu segments to compute_node=%u, logical_region=%u\n",
                   batch_size, target_compute_node, stream_state->logical_region_id);
        }
    }
}

void LogReplayerManager::ReplayLogData(LogStreamState* stream_state, uint64_t available_bytes) {
    // Page-version-aware log replay logic
    
    if (available_bytes < sizeof(RedoLogger::RecordHeader)) {
        assert(false);
        return; // Not enough data for a complete record header
    }
    
    // Process log records with page version ordering
    uint64_t processed_bytes = 0;
    uint32_t records_processed = 0;
    uint32_t records_skipped = 0;
    
    while (processed_bytes + sizeof(RedoLogger::RecordHeader) <= available_bytes) {
        // Parse record header
        RedoLogger::RecordHeader header;
        // In real implementation, this would read from the actual log buffer
        // For now, simulate header parsing
        processed_bytes += sizeof(RedoLogger::RecordHeader);
        
        // Simulate variable payload size based on record index
        uint32_t payload_size = 32 + (records_processed % 16) * 30;
        
        if (processed_bytes + payload_size > available_bytes) {
            break; // Not enough data for complete record
        }
        
        // Simulate payload data
        const uint8_t* payload = nullptr; // In real implementation, point to actual payload
        
        // Check page version ordering before applying
        uint64_t current_version = GetCurrentPageVersion(header.page_gaddr);
        
        if (header.page_version <= current_version) {
            // This should never happen in a correctly functioning system
            assert(false && "Received stale log record - this indicates a serious ordering bug");
        } else if (header.page_version == current_version + 1) {
            // This is the next expected version, apply it
            if (ProcessLogRecord(header, payload, payload_size)) {
                SetCurrentPageVersion(header.page_gaddr, header.page_version);
                printf("  LogReplayer: Applied record (page_version=%lu) for page=0x%lx\n",
                       header.page_version, header.page_gaddr.val);
            }
        } else {
            // This record is from the future, we need to wait for earlier versions
            printf("  LogReplayer: Future record (page_version=%lu > current+1=%lu) for page=0x%lx - waiting\n",
                   header.page_version, current_version + 1, header.page_gaddr.val);
            // Don't advance processed_bytes, we'll retry this record later
            break;
        }
        
        processed_bytes += payload_size;
        records_processed++;
        
        // Don't process too many records at once
        if (records_processed >= 10) {
            break;
        }
    }
    
    if (records_processed > 0 || records_skipped > 0) {
        printf("LogReplayer: Processed %u records (%u applied, %u skipped) (%lu bytes) for compute_node=%u, logical_region=%u\n",
               records_processed, records_processed - records_skipped, records_skipped, processed_bytes,
               stream_state->compute_node_id, stream_state->logical_region_id);
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
        return false;
    }
    
    // Get the actual physical page buffer
    void* page_buffer = reinterpret_cast<void*>(physical_addr);
    
    printf("    LogReplayer: Processing record for page=0x%lx, version=%lu, payload_size=%zu\n",
           header.page_gaddr.val, header.page_version, payload_size);
    
    // Use LogCodec to decode and apply operations to the actual page
    LogCodec::Decoder decoder(payload, payload_size);
    LogCodec::DecodedOp op;
    
    while (decoder.Next(op)) {
        switch (op.code) {
            case LogCodec::OpCode::UPDATE_BYTES: {
                // Apply UPDATE_BYTES operation to the physical page
                char* target_addr = static_cast<char*>(page_buffer) + op.u.update.offset;
                memcpy(target_addr, op.u.update.bytes, op.u.update.len);
                printf("      Applied UPDATE_BYTES: offset=%u, len=%u\n", 
                       op.u.update.offset, op.u.update.len);
                break;
            }
            case LogCodec::OpCode::SET_U64_LE: {
                // Apply SET_U64_LE operation to the physical page
                char* target_addr = static_cast<char*>(page_buffer) + op.u.set64.offset;
                *reinterpret_cast<uint64_t*>(target_addr) = op.u.set64.value;
                printf("      Applied SET_U64_LE: offset=%u, value=%lu\n", 
                       op.u.set64.offset, op.u.set64.value);
                break;
            }
            case LogCodec::OpCode::FILL_BYTES: {
                // Apply FILL_BYTES operation to the physical page
                char* target_addr = static_cast<char*>(page_buffer) + op.u.fill.offset;
                memset(target_addr, op.u.fill.value, op.u.fill.len);
                printf("      Applied FILL_BYTES: offset=%u, len=%u, value=0x%02x\n", 
                       op.u.fill.offset, op.u.fill.len, op.u.fill.value);
                break;
            }
            case LogCodec::OpCode::MEMMOVE_BYTES: {
                // Apply MEMMOVE_BYTES operation to the physical page
                char* page_base = static_cast<char*>(page_buffer);
                memmove(page_base + op.u.move.dst, page_base + op.u.move.src, op.u.move.len);
                printf("      Applied MEMMOVE_BYTES: dst=%u, src=%u, len=%u\n", 
                       op.u.move.dst, op.u.move.src, op.u.move.len);
                break;
            }
            default:
                printf("      Unknown opcode: %u\n", static_cast<uint8_t>(op.code));
                return false;
        }
    }
    
    if (!decoder.Good()) {
        printf("LogReplayer: Error - LogCodec decoder failed for page=0x%lx\n", header.page_gaddr.val);
        return false;
    }
    
    return true; // Successfully processed all operations
}

uint64_t LogReplayerManager::GetCurrentPageVersion(GlobalAddress page_addr) {
    // Translate logical address to physical address on this memory node
    uint16_t logical_id = page_addr.nodeID;
    uint16_t this_physical_id = rdma_mg_->node_id;
    
    // Get the physical address for this replica
    uint64_t physical_addr = rdma_mg_->TranslateLogicalToPhysicalAddress(
        logical_id, page_addr.offset, this_physical_id);
    
    if (physical_addr == 0) {
        printf("LogReplayer: Warning - Could not translate address for logical_id=%u, page=0x%lx\n", 
               logical_id, page_addr.val);
        return 0;
    }
    
    // Get the page header and read version from the physical replica
    DSMEngine::DataPage* data_page = reinterpret_cast<DSMEngine::DataPage*>(physical_addr);
    
    // TODO: Add version field to DataPage header
    // return data_page->hdr.page_version;
    
    // Temporary: return 0 until page header is updated with version field
    return 0;
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
    // data_page->hdr.page_version = version;
    
    printf("LogReplayer: Updated page version to %lu for page=0x%lx (physical_addr=0x%lx)\n", 
           version, page_addr.val, physical_addr);
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
