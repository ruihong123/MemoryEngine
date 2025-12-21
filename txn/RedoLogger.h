#pragma once
/**
 * RedoLogger (page-version aware):
 *   - Per-compute monotonic LSN (uint64_t).
 *   - Separate streams per (compute_node, memory_node).
 *   - Each log record carries the *target page* (GlobalAddress) and *page_version*.
 *   - Uses a single delta segment per stream with head/tail ring buffer (similar to DeltaSection).
 *   - Local buffer accumulates logs and flushes to remote segment until tail approaches head.
 *   - Optional file system logging (disabled by default).
 *
 * Zero-touch integration: header-only, no edits to existing code paths required.
 * If you later decide to persist page versions inside DataPage::Header (recommended),
 * the Append(...) call can pass that version directly; otherwise you can supply
 * the intended version computed by your caller (e.g., "new_version = old+1").
 */
#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <list>
#include <set>
#include <thread>
#include <chrono>

#include "Tools/env.h"
#include "Tools/slice.h"
#include "include/Common.h"
#include "storage/rdma.h"
#include "utils/mutexlock.h"

namespace DSMEngine {

    class RedoLogger {
    public:
        using LSN = uint64_t;
        
        // Closing marker for log segments (e.g., "$" indicates segment should be closed)
        static constexpr char SEGMENT_CLOSE_MARKER = '$';
        
        // Threshold for auto-flush: flush when unflushed data exceeds this size
        static constexpr size_t AUTO_FLUSH_THRESHOLD = 1024 * 1024;  // 64KB

        // Metadata structure (separate from data buffer, append-only)
        // For append-only logs, we track tail (write position) and flushed_tail (flushed position)
        // The replay thread reads sequentially from the beginning, no need to track head
        struct alignas(8) LogSegmentMetadata {
            alignas(8) std::atomic<uint64_t> tail_;         // Current write position in stage_mr (where next append goes)
            alignas(8) std::atomic<uint64_t> flushed_tail_;  // Position up to which data has been flushed to remote
        } __attribute__((packed));
        

        // Debug-only enum to identify log record types
#ifdef NDEBUG
        enum LogRecordType : uint8_t {
            LOG_UNKNOWN = 0
        };
#else
        enum LogRecordType : uint8_t {
            LOG_UNKNOWN = 0,
            LOG_DATA_PAGE_INIT,           // DataPage initialization
            LOG_DATA_PAGE_BITMAP_UPDATE,  // DataPage bitmap update (AllocateRecord)
            LOG_DATA_PAGE_UPDATE,         // DataPage record update (LogDataUpdateOperation)
            LOG_INTERNAL_PAGE_STORE,      // InternalPage::internal_page_store
            LOG_LEAF_PAGE_STORE,          // LeafPage::leaf_page_store
            LOG_LEAF_PAGE_DELETE,         // LeafPage::leaf_page_delete
            LOG_INTERNAL_PAGE_SPLIT_OLD,  // LogInternalPageSplit - old page
            LOG_INTERNAL_PAGE_SPLIT_NEW,  // LogInternalPageSplit - new page
            LOG_LEAF_PAGE_SPLIT_OLD,      // LogLeafPageSplit - old page
            LOG_LEAF_PAGE_SPLIT_NEW,      // LogLeafPageSplit - new page
            LOG_NEW_ROOT_PAGE,            // LogNewRootPage
            LOG_INDEX_PAGE_CHANGE,        // LogIndexPageChange (helper function)
            LOG_INDEX_PAGE_HEADER_CHANGE, // LogIndexPageHeaderChange (helper function)
            LOG_INDEX_PAGE_CONTENT_CHANGE // LogIndexPageContentChange (helper function)
        };
#endif

        struct RecordHeader {
            uint16_t magic;             // 0x4C52 ('R''L' little-endian)
            GlobalAddress page_gaddr;   // page the redo applies to
            uint64_t page_version;      // version of the page for last-writer-wins
            uint32_t payload_len;       // bytes following the header
#ifndef NDEBUG
            LogRecordType log_type;     // Debug: identifies which function created this log
#endif
        } __attribute__((packed));

        enum ReplicaWriteMode {
            WRITE_ONE_REPLICA = 0,    // Write to one replica only
            WRITE_ALL_REPLICAS = 1    // Write to all replicas
        };

        struct Options {
            std::string base_dir;
            size_t local_stage_bytes;
            Chunk_type remote_pool;
            bool fsync_on_flush;
            bool enable_file_system;  // If false, disable file system logging
            ReplicaWriteMode replica_write_mode;  // Write to one replica or all replicas

            Options()
                : base_dir("./logs"),
                  remote_pool(Chunk_type::DeltaChunk),
                  local_stage_bytes(1 << 20),
                  fsync_on_flush(false),
                  enable_file_system(false),  // Disabled by default
                  replica_write_mode(WRITE_ALL_REPLICAS) {}  // Write to all replicas by default
        };

        explicit RedoLogger(Env* env,
                            RDMA_Manager* rdma,
                            uint16_t compute_node_id,
                            const Options& opts = Options())
                : env_(env), rdma_(rdma),
                  compute_node_id_(compute_node_id),
                  opts_(opts), next_lsn_(1) {
            if (env_ && opts_.enable_file_system) {
                (void)env_->CreateDir(opts_.base_dir);
            }
        }

        ~RedoLogger() {
            try { FlushAll(opts_.fsync_on_flush); } catch (...) {}
        }

        // Append a redo record into the stream for 'logical_region_id'.
        // Appends to stage_mr at tail position, then increments tail.
        void Append(uint16_t logical_region_id,
                    GlobalAddress page_gaddr,
                    uint64_t page_version,
                    const void* payload,
                    uint32_t payload_len
#ifndef NDEBUG
                    , LogRecordType log_type = LOG_UNKNOWN
#endif
                    ) {
            const size_t need = sizeof(RecordHeader) + payload_len;

            auto& s = GetOrCreateStream(logical_region_id);

            RecordHeader hdr;
            hdr.magic = 0x4C52;
            hdr.page_gaddr = page_gaddr;
            hdr.page_version = page_version;
            hdr.payload_len = payload_len;
#ifndef NDEBUG
            hdr.log_type = log_type;
            // // Helper function to get log type string (only in debug mode)
            // auto GetLogTypeString = [](LogRecordType lt) -> const char* {
            //     switch (lt) {
            //         case LOG_UNKNOWN: return "LOG_UNKNOWN";
            //         case LOG_DATA_PAGE_INIT: return "LOG_DATA_PAGE_INIT";
            //         case LOG_DATA_PAGE_BITMAP_UPDATE: return "LOG_DATA_PAGE_BITMAP_UPDATE";
            //         case LOG_DATA_PAGE_UPDATE: return "LOG_DATA_PAGE_UPDATE";
            //         case LOG_INTERNAL_PAGE_STORE: return "LOG_INTERNAL_PAGE_STORE";
            //         case LOG_LEAF_PAGE_STORE: return "LOG_LEAF_PAGE_STORE";
            //         case LOG_LEAF_PAGE_DELETE: return "LOG_LEAF_PAGE_DELETE";
            //         case LOG_INTERNAL_PAGE_SPLIT_OLD: return "LOG_INTERNAL_PAGE_SPLIT_OLD";
            //         case LOG_INTERNAL_PAGE_SPLIT_NEW: return "LOG_INTERNAL_PAGE_SPLIT_NEW";
            //         case LOG_LEAF_PAGE_SPLIT_OLD: return "LOG_LEAF_PAGE_SPLIT_OLD";
            //         case LOG_LEAF_PAGE_SPLIT_NEW: return "LOG_LEAF_PAGE_SPLIT_NEW";
            //         case LOG_NEW_ROOT_PAGE: return "LOG_NEW_ROOT_PAGE";
            //         case LOG_INDEX_PAGE_CHANGE: return "LOG_INDEX_PAGE_CHANGE";
            //         case LOG_INDEX_PAGE_HEADER_CHANGE: return "LOG_INDEX_PAGE_HEADER_CHANGE";
            //         case LOG_INDEX_PAGE_CONTENT_CHANGE: return "LOG_INDEX_PAGE_CONTENT_CHANGE";
            //         default: return "LOG_UNKNOWN";
            //     }
            // };
            // printf("RedoLogger: Append record - logical_region_id=%u, page_gaddr=[nodeID=%u, offset=%lu, val=0x%lx], page_version=%lu, payload_len=%u, log_type=%s\n",
            //     logical_region_id, page_gaddr.nodeID, page_gaddr.offset, page_gaddr.val, page_version, payload_len, GetLogTypeString(log_type));
            //     fflush(stdout);

#endif
            
            std::lock_guard<SpinMutex> lk(s.mtx);

            // Get current tail and flushed_tail positions
            uint64_t current_tail = s.metadata.tail_.load(std::memory_order_acquire);
            uint64_t flushed_tail = s.metadata.flushed_tail_.load(std::memory_order_acquire);
            
            // Check if current record fits in the remaining segment capacity
            // If not, we need to allocate a new segment
            size_t remaining_capacity = s.stage_cap - current_tail;
            if (remaining_capacity < need) {
                // Current segment doesn't have enough space, allocate a new one
                // First, if there's remaining capacity, write closing marker to local buffer
                if (remaining_capacity > 0) {
                    // Write closing marker to indicate segment is ended
                    char* marker_pos = static_cast<char*>(s.stage_mr->addr) + current_tail;
                    *marker_pos = SEGMENT_CLOSE_MARKER;
                    current_tail += 1;
                    
                    // Update tail before flushing
                    std::atomic_thread_fence(std::memory_order_release);
                    s.metadata.tail_.store(current_tail, std::memory_order_release);
                }
                
                // Flush all pending data (including closing marker if written) to remote
                if (current_tail > flushed_tail) {
                    FlushLocked(s, opts_.fsync_on_flush);
                    current_tail = s.metadata.tail_.load(std::memory_order_acquire);
                }
                
                // Allocate new segment and notify remote (this resets tail to 0)
                AllocateNewSegmentAndNotify(s);
                current_tail = s.metadata.tail_.load(std::memory_order_acquire);
            }
            
            // Check if we need to auto-flush based on unflushed data threshold
            // Flush when unflushed data (tail - flushed_tail) exceeds threshold
            if (current_tail > flushed_tail) {
                size_t unflushed = current_tail - flushed_tail;
                if (unflushed >= AUTO_FLUSH_THRESHOLD) {
                    FlushLocked(s, opts_.fsync_on_flush);
                    current_tail = s.metadata.tail_.load(std::memory_order_acquire);
                }
            }

            // Serialize header + payload into staging buffer at tail position
            std::memcpy(static_cast<char*>(s.stage_mr->addr) + current_tail, &hdr, sizeof(hdr));
            current_tail += sizeof(hdr);
            if (payload_len) {
                std::memcpy(static_cast<char*>(s.stage_mr->addr) + current_tail, payload, payload_len);
                current_tail += payload_len;
            }

            // Update tail (memory barrier ensures writes are visible before tail update)
            std::atomic_thread_fence(std::memory_order_release);
            s.metadata.tail_.store(current_tail, std::memory_order_release);
        }

        void Flush(uint16_t logical_region_id, bool fsync) {
            std::shared_lock<RWSpinMutex> read_lk(streams_mtx_);
            auto it = streams_.find(logical_region_id);
            if (it == streams_.end()) return;
            std::lock_guard<SpinMutex> lk(it->second.mtx);
            FlushLocked(it->second, fsync);
        }

        void FlushAll(bool fsync) {
            std::shared_lock<RWSpinMutex> read_lk(streams_mtx_);
            for (auto& kv : streams_) {
                std::lock_guard<SpinMutex> lk(kv.second.mtx);
                FlushLocked(kv.second, fsync);
            }
        }
        
        // Flush all buffers for all streams (ensures all data is written to remote)
        void FlushAllBuffers(bool fsync = false) {
            FlushAll(fsync);
        }
        
        // Wait for all remote memory nodes to finish replaying all logs
        // This sends RPC queries to all memory nodes and waits for them to confirm replay is complete
        void WaitForAllMemoryNodesReplayComplete() {
            // Collect all unique logical region IDs (memory nodes) that we have streams for
            std::set<uint16_t> logical_region_ids;
            {
                std::shared_lock<RWSpinMutex> read_lk(streams_mtx_);
                for (const auto& kv : streams_) {
                    logical_region_ids.insert(kv.first);
                }
            }
            
            if (logical_region_ids.empty()) {
                return; // No streams, nothing to wait for
            }
            
            // Collect all unique physical memory nodes across all logical regions
            // Use a set to automatically deduplicate physical node IDs
            std::set<uint16_t> unique_physical_nodes;
            for (uint16_t logical_region_id : logical_region_ids) {
                const auto& replicas = rdma_->GetReplicaSet(logical_region_id);
                if (replicas.empty()) {
                    continue;
                }
                
                // Add all replica nodes (skip primary at index 0) to the set
                for (size_t i = 1; i < replicas.size(); ++i) {
                    unique_physical_nodes.insert(replicas[i].phys_id);
                }
            }
            
            // Send one RPC per unique physical memory node
            for (uint16_t physical_node_id : unique_physical_nodes) {
                // Allocate receive buffer for reply BEFORE sending request
                ibv_mr recv_mr;
                rdma_->Allocate_Local_RDMA_Slot(recv_mr, Message);
                RDMA_Reply* recv_pointer = reinterpret_cast<RDMA_Reply*>(recv_mr.addr);
                *recv_pointer = {};
                recv_pointer->received = false;
                
                // Send RPC query
                RDMA_Request* send_pointer;
                ibv_mr* send_mr = rdma_->Get_local_send_message_mr();
                send_pointer = (RDMA_Request*)send_mr->addr;
                
                send_pointer->command = log_replay_status_query;
                send_pointer->content.log_replay_status_query.compute_node_id = compute_node_id_;
                
                // Set the receive buffer address and rkey in the request
                send_pointer->buffer = recv_mr.addr;
                send_pointer->rkey = recv_mr.rkey;
                
                std::string qp_type_main("main");
                int rc = rdma_->post_send<RDMA_Request>(send_mr, physical_node_id, qp_type_main);
                if (rc) {
                    fprintf(stderr, "RedoLogger: failed to send log_replay_status_query RPC to physical_node_id=%u (rc=%d)\n", 
                           physical_node_id, rc);
                    rdma_->Deallocate_Local_RDMA_Slot(recv_mr.addr, Message);
                    continue;
                }
                
                // Poll for send completion
                ibv_wc wc[2] = {};
                if (rdma_->poll_completion(wc, 1, qp_type_main, true, physical_node_id)) {
                    fprintf(stderr, "RedoLogger: failed to poll send completion for log_replay_status_query RPC\n");
                    rdma_->Deallocate_Local_RDMA_Slot(recv_mr.addr, Message);
                    continue;
                }
                
                // Poll for reply using poll_reply_buffer (same pattern as Remote_Memory_Register)
                rdma_->poll_reply_buffer(recv_pointer);
                
                // Check if reply indicates all logs are replayed
                if (!recv_pointer->received || !recv_pointer->content.log_replay_status_reply.all_logs_replayed) {
                    fprintf(stderr, "RedoLogger: Unexpected reply from physical_node_id=%u\n", physical_node_id);
                }
                
                rdma_->Deallocate_Local_RDMA_Slot(recv_mr.addr, Message);
            }
        }

        // Expose the on-disk file path of a stream (useful for recovery tooling).
        std::string FilePath(uint16_t logical_region_id) const {
            return opts_.base_dir + "/redo_c" + std::to_string(compute_node_id_) +
                   "_m" + std::to_string(logical_region_id) + ".log";
        }

        size_t UnflushedBytes(uint16_t logical_region_id) const {
            std::shared_lock<RWSpinMutex> read_lk(streams_mtx_);
            auto it = streams_.find(logical_region_id);
            if (it == streams_.end()) return 0;
            uint64_t tail = it->second.metadata.tail_.load(std::memory_order_acquire);
            uint64_t flushed_tail = it->second.metadata.flushed_tail_.load(std::memory_order_acquire);
            return tail - flushed_tail;
        }

        // Handle log segment recycle RPC from memory nodes
        // This function processes recycle notifications and moves segments to the free list
        // when enough acknowledgments have been received (all replicas or one replica)
        void HandleLogSegmentRecycle(uint16_t logical_region_id,
                                     uint16_t memory_node_id,
                                     const std::vector<GlobalAddress>& segment_addrs) {
            std::shared_lock<RWSpinMutex> read_lk(streams_mtx_);
            auto it = streams_.find(logical_region_id);
            if (it == streams_.end()) {
                fprintf(stderr, "RedoLogger: HandleLogSegmentRecycle: stream not found for logical_region_id=%u\n", 
                       logical_region_id);
                return;
            }
            
            StreamState& s = it->second;
            std::lock_guard<SpinMutex> lk(s.mtx);
            
            // Get replica set to determine how many acknowledgments we need
            const auto& replicas = rdma_->GetReplicaSet(logical_region_id);
            if (replicas.empty()) {
                fprintf(stderr, "RedoLogger: HandleLogSegmentRecycle: no replicas found for logical_region_id=%u\n", 
                       logical_region_id);
                return;
            }
            
            // Determine required number of acknowledgments
            uint32_t required_acks;
            if (opts_.replica_write_mode == WRITE_ALL_REPLICAS) {
                // Need acknowledgments from all replicas (excluding primary)
                required_acks = static_cast<uint32_t>(replicas.size() - 1);
            } else {
                // WRITE_ONE_REPLICA: need acknowledgment from only one replica
                required_acks = 1;
            }
            
            // Process each segment in the recycle request
            for (const GlobalAddress& seg_addr : segment_addrs) {
                uint64_t seg_key = seg_addr.val;
                
                // Increment acknowledgment count for this segment
                auto ack_it = s.pending_recycle_acks.find(seg_key);
                if (ack_it == s.pending_recycle_acks.end()) {
                    // First acknowledgment for this segment
                    s.pending_recycle_acks[seg_key] = 1;
                } else {
                    // Increment existing count
                    ack_it->second++;
                }
                
                uint32_t current_acks = s.pending_recycle_acks[seg_key];
                
                // Check if we have enough acknowledgments to recycle
                if (current_acks >= required_acks) {
                    // Move segment to free list
                    s.free_segments.push_back(seg_addr);
                    // Remove from pending tracking (erase by key to handle case where required_acks=1)
                    s.pending_recycle_acks.erase(seg_key);
                    printf("RedoLogger: Recycled segment 0x%lx for logical_region_id=%u (acks=%u/%u)\n",
                           seg_addr.val, logical_region_id, current_acks, required_acks);
                } else {
                    printf("RedoLogger: Pending recycle for segment 0x%lx (acks=%u/%u)\n",
                           seg_addr.val, current_acks, required_acks);
                }
            }
        }


    private:
        struct StreamState {
            uint16_t logical_region_id = 0;
            std::unique_ptr<WritableFile> file;
            ibv_mr* stage_mr = nullptr;          // Local staging buffer (size equals remote segment size)
            size_t  stage_cap = 0;                // Capacity of stage_mr (equals remote segment size)
            
            // Current remote log data segment (append-only, allocate new segment when full)
            GlobalAddress current_segment_addr;      // Address of the current remote data segment
            size_t current_segment_size;              // Total size of current data segment
            
            // Local metadata (maintained only on compute node, synced to remote via RPC when needed)
            LogSegmentMetadata metadata;              // Local metadata structure
            
            // Free list of recycled segments (ready for reuse)
            std::list<GlobalAddress> free_segments;
            
            // Track pending recycle acknowledgments per segment
            // Maps segment address to count of received recycle messages
            std::unordered_map<uint64_t, uint32_t> pending_recycle_acks;
            
            SpinMutex mtx;
            
            // Make StreamState movable
            StreamState() = default;
            StreamState(const StreamState&) = delete;
            StreamState& operator=(const StreamState&) = delete;
            
            StreamState(StreamState&& other) noexcept 
                : logical_region_id(other.logical_region_id),
                  file(std::move(other.file)),
                  stage_mr(other.stage_mr),
                  stage_cap(other.stage_cap),
                  current_segment_addr(other.current_segment_addr),
                  current_segment_size(other.current_segment_size) {
                // Move atomic values manually
                metadata.tail_.store(other.metadata.tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
                metadata.flushed_tail_.store(other.metadata.flushed_tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
                
                other.stage_mr = nullptr;
                other.stage_cap = 0;
                // Note: SpinMutex is not moved, each StreamState gets its own mutex
            }
            
            StreamState& operator=(StreamState&& other) noexcept {
                if (this != &other) {
                    logical_region_id = other.logical_region_id;
                    file = std::move(other.file);
                    stage_mr = other.stage_mr;
                    stage_cap = other.stage_cap;
                    current_segment_addr = other.current_segment_addr;
                    current_segment_size = other.current_segment_size;
                    
                    // Move atomic values manually
                    metadata.tail_.store(other.metadata.tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
                    metadata.flushed_tail_.store(other.metadata.flushed_tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
                    
                    other.stage_mr = nullptr;
                    other.stage_cap = 0;
                    // Note: SpinMutex is not moved, each StreamState keeps its own mutex
                }
                return *this;
            }
        };

        StreamState& GetOrCreateStream(uint16_t logical_region_id) {
            // First check without lock (fast path)
            {
                std::shared_lock<RWSpinMutex> read_lk(streams_mtx_);
                auto it = streams_.find(logical_region_id);
                if (it != streams_.end()) return it->second;
            }
            
            // Need to create stream - acquire write lock
            std::unique_lock<RWSpinMutex> write_lk(streams_mtx_);
            
            // Double-check after acquiring write lock (another thread might have created it)
            auto it = streams_.find(logical_region_id);
            if (it != streams_.end()) return it->second;

            StreamState s;
            s.logical_region_id = logical_region_id;

            // Disk file (only if file system logging is enabled)
            if (env_ && opts_.enable_file_system) {
                std::string path = FilePath(logical_region_id);
                WritableFile* wf = nullptr;
                Status st = env_->NewWritableFile(path, &wf);
                if (!st.ok() || wf == nullptr) {
                    fprintf(stderr, "RedoLogger: cannot create %s (%s)\n",
                            path.c_str(), st.ToString().c_str());
                }
                s.file.reset(wf);
            }

            // Local staging MR: size equals remote segment size
            // We append to stage_mr at tail position and flush from flushed_tail to tail
            s.stage_mr = new ibv_mr{};
            rdma_->Allocate_Local_RDMA_Slot(*s.stage_mr, opts_.remote_pool);
            // stage_mr size should match remote segment size (chunk size)
            s.stage_cap = s.stage_mr->length;
            // Remove stage_used - we use metadata.tail_ instead

            // Initialize local metadata
            s.metadata.tail_.store(0, std::memory_order_relaxed);
            s.metadata.flushed_tail_.store(0, std::memory_order_relaxed);
            
            // Allocate first remote data segment (append-only, will allocate new segments when full)
            s.current_segment_addr = rdma_->Allocate_Remote_RDMA_Slot(opts_.remote_pool, logical_region_id);
            // Get chunk size for this pool type from name_to_chunksize
            s.current_segment_size = rdma_->Get_chunk_size(opts_.remote_pool);
            
            // Send RPC to remote memory node to create log stream and spawn replay thread
            SendLogSegmentRPC(logical_region_id, s.current_segment_addr, s.current_segment_size, true);

            auto [ins, _] = streams_.emplace(logical_region_id, std::move(s));
            return ins->second;
        }

        void FlushLocked(StreamState& s, bool fsync) {
            // Read current tail and flushed_tail from local metadata
            uint64_t current_tail = s.metadata.tail_.load(std::memory_order_acquire);
            uint64_t flushed_tail = s.metadata.flushed_tail_.load(std::memory_order_acquire);
            
            // Calculate how much data needs to be flushed (from flushed_tail to tail)
            size_t to_flush = current_tail - flushed_tail;
            if (to_flush == 0) return;  // Nothing to flush

            // Flush data from stage_mr[flushed_tail..tail] to remote segment
            // Since stage_mr size equals remote segment size, positions align
            // Note: By design, we always have space in the segment because Append() allocates
            // a new segment before writing if needed, so we can write all data in one go
            size_t local_offset = flushed_tail;
            
            // Remote segment position starts at flushed_tail (positions align)
            GlobalAddress remote_data_addr = s.current_segment_addr;
            remote_data_addr.offset += flushed_tail;

            // Create local view from stage_mr
            ibv_mr local_view = *s.stage_mr;
            local_view.addr = static_cast<char*>(s.stage_mr->addr) + local_offset;
            local_view.length = to_flush;

            // Write to replicas (skip primary, start from index 1)
            // Use RDMA write with imm, encoding logical_region_id in wr_id
            const auto& replicas = rdma_->GetReplicaSet(remote_data_addr.nodeID);
            assert(!replicas.empty() && "Replicas cannot be empty");
            
            // Determine which replicas to write to based on configuration
            size_t start_idx = 1;  // Skip primary (index 0)
            size_t end_idx = replicas.size();
            
            if (opts_.replica_write_mode == WRITE_ONE_REPLICA) {
                // Write to only one replica (first non-primary replica)
                end_idx = start_idx + 1;
            }
            // else: WRITE_ALL_REPLICAS - write to all replicas (end_idx = replicas.size())
            
            // Check if there are any replicas to write to (beyond the primary)
            // If there's only a primary (replicas.size() == 1), then start_idx (1) >= replicas.size() (1),
            // so no writes will be performed. Also ensure end_idx doesn't exceed replicas.size().
            if (start_idx >= replicas.size() || end_idx > replicas.size()) {
                // Only primary exists, no replicas to write to - return early
                // Update flushed_tail even though no remote write occurred
                std::atomic_thread_fence(std::memory_order_seq_cst);
                s.metadata.flushed_tail_.store(current_tail, std::memory_order_release);
                return;
            }
            
            // Get logical region ID from remote address
            uint16_t logical_region_id = remote_data_addr.nodeID;
            
            // Encode both logical_region_id and transferred_size into imm_data (32 bits)
            // Lower 8 bits: logical_region_id (guaranteed to be 0-255)
            // Upper 24 bits: transferred_size (to_flush)
            // Upper limit: transferred_size must fit in 24 bits (max 16,777,215 bytes ~16 MB)
            assert(logical_region_id <= UINT8_MAX && "logical_region_id must fit in 8 bits (0-255)");
            static constexpr size_t MAX_RDMA_WRITE_IMM_SIZE = (1UL << 24) - 1;  // 16,777,215 bytes
            assert(to_flush <= MAX_RDMA_WRITE_IMM_SIZE && 
                   "RDMA write with immediate data size exceeds 24-bit limit (16,777,215 bytes). "
                   "to_flush must be <= 16,777,215 to fit in imm_data encoding.");
            unsigned int imm_data = (static_cast<unsigned int>(to_flush) << 8) | static_cast<unsigned int>(logical_region_id);
            
            // Write to selected replica nodes (RDMA write with imm)
            for (size_t i = start_idx; i < end_idx; ++i) {
                uint16_t replica_phys_id = replicas[i].phys_id;
                uint64_t physical_addr = rdma_->TranslateLogicalToPhysicalAddress(
                    remote_data_addr.nodeID, remote_data_addr.offset, replica_phys_id);
                uint32_t rkey = rdma_->GetPhysicalRkey(remote_data_addr.nodeID, replica_phys_id);
                printf("RedoLogger: Compute node %u writing %zu bytes to replica %u for logical_region_id=%u at physical address 0x%lx with rkey 0x%x\n",
                       compute_node_id_, to_flush, replica_phys_id, logical_region_id, physical_addr, rkey);
                fflush(stdout);
                // RDMA write with imm, encoding both logical_region_id and transferred_size in imm_data
                int rc = rdma_->RDMA_Write_Imme(reinterpret_cast<void*>(physical_addr), rkey, 
                                                 &local_view, to_flush, "main", 
                                                 IBV_SEND_SIGNALED, 1, imm_data,
                                                 replica_phys_id);
                if (rc) {
                    fprintf(stdout, "RedoLogger: RDMA_Write_Imme to replica %u failed (rc=%d)\n", 
                           replica_phys_id, rc);
                           assert(false && "RDMA_Write_Imme failed");
                }
            }

            // After flushing, update flushed_tail to current_tail
            // Memory barrier: ensure data writes are visible before flushed_tail update
            std::atomic_thread_fence(std::memory_order_seq_cst);
            s.metadata.flushed_tail_.store(current_tail, std::memory_order_release);

            // TODO: Sync metadata to remote side via RPC if needed
            // For now, metadata is only maintained locally on compute node

            // Disk (only if file system logging is enabled)
            if (s.file) {
                Slice bytes(static_cast<const char*>(s.stage_mr->addr) + flushed_tail, to_flush);
                Status st = s.file->Append(bytes);
                if (!st.ok()) {
                    fprintf(stderr, "RedoLogger: file->Append() failed: %s\n", st.ToString().c_str());
                }
                if (fsync) {
                    st = s.file->Sync();
                    if (!st.ok()) {
                        fprintf(stderr, "RedoLogger: file->Sync() failed: %s\n", st.ToString().c_str());
                    }
                }
            }
        }

        // Allocate a new remote segment and notify the remote memory node
        // Resets local stage_mr for reuse with the new segment
        // First checks the free list for recycled segments before allocating a new one
        void AllocateNewSegmentAndNotify(StreamState& s) {
            // First check if there's a recycled segment available in the free list
            if (!s.free_segments.empty()) {
                // Reuse a recycled segment
                s.current_segment_addr = s.free_segments.front();
                s.free_segments.pop_front();
                s.current_segment_size = rdma_->Get_chunk_size(opts_.remote_pool);
                
                printf("RedoLogger: Reusing recycled segment 0x%lx for logical_region_id=%u\n",
                       s.current_segment_addr.val, s.logical_region_id);
            } else {
                // Allocate a new remote segment
                s.current_segment_addr = rdma_->Allocate_Remote_RDMA_Slot(opts_.remote_pool, s.logical_region_id);
                s.current_segment_size = rdma_->Get_chunk_size(opts_.remote_pool);
                
                printf("RedoLogger: Allocated new segment 0x%lx (size=%zu bytes) for compute_node=%u, logical_region_id=%u\n",
                       s.current_segment_addr.val, s.current_segment_size, compute_node_id_, s.logical_region_id);
                fflush(stdout);
            }
            
            // Notify remote memory node about the new segment (is_new_stream = false)
            SendLogSegmentRPC(s.logical_region_id, s.current_segment_addr, s.current_segment_size, false);
            
            // Reset stage_mr for reuse with new segment
            s.metadata.tail_.store(0, std::memory_order_release);
            s.metadata.flushed_tail_.store(0, std::memory_order_release);
        }

        // Unified RPC function for log segment requests (both new stream and new segment)
        void SendLogSegmentRPC(uint16_t logical_region_id,
                               GlobalAddress segment_addr,
                               size_t segment_size,
                               bool is_new_stream) {
            RDMA_Request* send_pointer;
            ibv_mr* send_mr = rdma_->Get_local_send_message_mr();
            send_pointer = (RDMA_Request*)send_mr->addr;
            
            send_pointer->command = log_segment_request;
            
            // Fill in the request content
            send_pointer->content.log_segment_request.log_segment_addr = segment_addr;
            send_pointer->content.log_segment_request.compute_node_id = compute_node_id_;
            send_pointer->content.log_segment_request.logical_region_id = logical_region_id;
            send_pointer->content.log_segment_request.segment_size = segment_size;
            send_pointer->content.log_segment_request.is_new_stream = is_new_stream;
            
            // Get all physical memory nodes that host replicas of this logical region
            const auto& replicas = rdma_->GetReplicaSet(logical_region_id);
            if (replicas.empty()) {
                fprintf(stderr, "RedoLogger: no replicas found for logical_region_id=%u\n", logical_region_id);
                return;
            }
            
            // Send RPC to replica nodes only (skip primary at index 0)
            for (size_t i = 1; i < replicas.size(); ++i) {
                uint16_t physical_node_id = replicas[i].phys_id;
                
                // Send RPC to this physical memory node
                printf("RedoLogger: Compute node %u sending log_segment_request RPC to physical_node_id=%u for logical_region_id=%u\n",
                       compute_node_id_, physical_node_id, logical_region_id);
                fflush(stdout);
                int rc = rdma_->post_send<RDMA_Request>(send_mr, physical_node_id, std::string("main"));
                if (rc) {
                    fprintf(stderr, "RedoLogger: failed to send log_segment_request RPC to physical_node_id=%u (rc=%d)\n", 
                           physical_node_id, rc);
                    continue;
                }
                
                // Poll for send completion
                ibv_wc wc[2] = {};
                if (rdma_->poll_completion(wc, 1, std::string("main"), true, physical_node_id)) {
                    fprintf(stderr, "RedoLogger: failed to poll send completion for log_segment_request RPC to physical_node_id=%u\n", 
                           physical_node_id);
                } else {
                    printf("RedoLogger: Successfully sent log_segment_request to physical_node_id=%u for logical_region_id=%u\n",
                           physical_node_id, logical_region_id);
                }
            }
            
            // TODO: Poll for reply if remote node sends one
            // The remote node will spawn a dedicated thread for replaying logs (if is_new_stream)
        }


    private:
        Env* env_;
        RDMA_Manager* rdma_;
        const uint16_t compute_node_id_;
        Options opts_;
        std::atomic<LSN> next_lsn_;
        std::unordered_map<uint16_t, StreamState> streams_;
        mutable RWSpinMutex streams_mtx_;  // Protects streams_ map from concurrent access
    };

} // namespace DSMEngine
