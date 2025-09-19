#pragma once
/**
 * RedoLogger (page-version aware):
 *   - Per-compute monotonic LSN (uint64_t).
 *   - Separate streams per (compute_node, memory_node).
 *   - Each log record carries the *target page* (GlobalAddress) and *page_version*.
 *   - Dual-sink flush: to remote (RDMA) and local disk.
 *
 * Zero-touch integration: header-only, no edits to existing code paths required.
 * If you later decide to persist page versions inside DataPage::Header (recommended),
 * the Append(...) call can pass that version directly; otherwise you can supply
 * the intended version computed by your caller (e.g., "new_version = old+1").
 */
#include <atomic>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

#include "Tools/env.h"
#include "Tools/slice.h"
#include "include/Common.h"
#include "storage/rdma.h"

namespace DSMEngine {

    class RedoLogger {
    public:
        using LSN = uint64_t;

        struct RecordHeader {
            uint16_t magic;             // 0x4C52 ('R''L' little-endian)
            uint16_t version;           // record format version (1)
            uint16_t compute_node_id;   // producing compute node id
            uint16_t memory_node_id;    // target memory node id (stream key)
            GlobalAddress page_gaddr;   // page the redo applies to
            uint64_t page_version;      // version of the page for last-writer-wins
            uint64_t lsn;               // per-compute-node LSN
            uint32_t payload_len;       // bytes following the header
        } __attribute__((packed));

        struct Options {
            std::string base_dir;
            size_t local_stage_bytes;
            Chunk_type remote_pool;
            bool fsync_on_flush;

            Options()
                : base_dir("./logs"),
                  remote_pool(Chunk_type::DeltaChunk),
                  local_stage_bytes(1 << 20),
                  fsync_on_flush(false) {}
        };

        explicit RedoLogger(Env* env,
                            RDMA_Manager* rdma,
                            uint16_t compute_node_id,
                            const Options& opts = Options())
                : env_(env), rdma_(rdma),
                  compute_node_id_(compute_node_id),
                  opts_(opts), next_lsn_(1) {
            if (env_) (void)env_->CreateDir(opts_.base_dir);
        }

        ~RedoLogger() {
            try { FlushAll(opts_.fsync_on_flush); } catch (...) {}
        }

        // Append a redo record into the stream for 'memory_node_id'.
        // Requires: header+payload <= local_stage_bytes (checked).
        void Append(uint16_t memory_node_id,
                    GlobalAddress page_gaddr,
                    uint64_t page_version,
                    const void* payload,
                    uint32_t payload_len,
                    LSN* out_lsn = nullptr) {
            const size_t need = sizeof(RecordHeader) + payload_len;
            if (need > opts_.local_stage_bytes) {
                // Defensive: refuse over-sized records to avoid ad-hoc MR allocations.
                // Increase Options::local_stage_bytes if this trips.
                fprintf(stderr, "RedoLogger::Append(): record too large (%zu > stage %zu)\n",
                        need, opts_.local_stage_bytes);
                abort();
            }

            auto& s = GetOrCreateStream(memory_node_id);

            RecordHeader hdr;
            hdr.magic = 0x4C52;
            hdr.version = 1;
            hdr.compute_node_id = compute_node_id_;
            hdr.memory_node_id  = memory_node_id;
            hdr.page_gaddr = page_gaddr;
            hdr.page_version = page_version;
            hdr.lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed);
            hdr.payload_len = payload_len;
            if (out_lsn) *out_lsn = hdr.lsn;

            std::unique_lock<std::mutex> lk(s.mtx);

            if (s.stage_used + need > s.stage_cap) {
                // Flush existing bytes first to preserve ordering.
                if (s.stage_used) FlushLocked(s, opts_.fsync_on_flush);
            }

            // Serialize header + payload into staging buffer
            std::memcpy(static_cast<char*>(s.stage_mr->addr) + s.stage_used, &hdr, sizeof(hdr));
            s.stage_used += sizeof(hdr);
            if (payload_len) {
                std::memcpy(static_cast<char*>(s.stage_mr->addr) + s.stage_used, payload, payload_len);
                s.stage_used += payload_len;
            }

            // Opportunistic flush if nearly full
            if (s.stage_used + 4096 > s.stage_cap) {
                FlushLocked(s, opts_.fsync_on_flush);
            }
        }

        void Flush(uint16_t memory_node_id, bool fsync) {
            auto it = streams_.find(memory_node_id);
            if (it == streams_.end()) return;
            std::unique_lock<std::mutex> lk(it->second.mtx);
            FlushLocked(it->second, fsync);
        }

        void FlushAll(bool fsync) {
            for (auto& kv : streams_) {
                std::unique_lock<std::mutex> lk(kv.second.mtx);
                FlushLocked(kv.second, fsync);
            }
        }

        // Expose the on-disk file path of a stream (useful for recovery tooling).
        std::string FilePath(uint16_t memory_node_id) const {
            return opts_.base_dir + "/redo_c" + std::to_string(compute_node_id_) +
                   "_m" + std::to_string(memory_node_id) + ".log";
        }

        size_t StagedBytes(uint16_t memory_node_id) const {
            auto it = streams_.find(memory_node_id);
            if (it == streams_.end()) return 0;
            return it->second.stage_used;
        }

    private:
        struct RemoteSlot {
            ibv_mr mr{};
            uint16_t memory_node_id = 0;
        };

        struct StreamState {
            uint16_t memory_node_id = 0;
            std::unique_ptr<WritableFile> file;
            ibv_mr* stage_mr = nullptr;
            size_t  stage_cap = 0;
            size_t  stage_used = 0;
            std::vector<RemoteSlot> remote_slots;
            std::mutex mtx;
        };

        StreamState& GetOrCreateStream(uint16_t memory_node_id) {
            auto it = streams_.find(memory_node_id);
            if (it != streams_.end()) return it->second;

            StreamState s;
            s.memory_node_id = memory_node_id;

            // Disk file
            if (env_) {
                std::string path = FilePath(memory_node_id);
                WritableFile* wf = nullptr;
                Status st = env_->NewWritableFile(path, &wf);
                if (!st.ok() || wf == nullptr) {
                    fprintf(stderr, "RedoLogger: cannot create %s (%s)\n",
                            path.c_str(), st.ToString().c_str());
                }
                s.file.reset(wf);
            }

            // Local staging MR: exactly opts_.local_stage_bytes
            // We use a pool chunk repeatedly; if the pool chunk is smaller, we simply cap the stage_cap to that size.
            s.stage_mr = new ibv_mr{};
            rdma_->Allocate_Local_RDMA_Slot(*s.stage_mr, opts_.remote_pool);
            s.stage_cap = std::min<size_t>(s.stage_mr->length, opts_.local_stage_bytes);
            s.stage_used = 0;

            auto [ins, _] = streams_.emplace(memory_node_id, std::move(s));
            return ins->second;
        }

        void FlushLocked(StreamState& s, bool fsync) {
            if (s.stage_used == 0) return;

            // Remote: write the whole staged buffer (may split into multiple slots)
            size_t remaining = s.stage_used;
            size_t offset = 0;
            while (remaining > 0) {
                RemoteSlot slot;
                rdma_->Allocate_Remote_RDMA_Slot(slot.mr, opts_.remote_pool, s.memory_node_id);
                size_t to_write = std::min(remaining, (size_t)slot.mr.length);

                // Use the same staging MR but adjust the local view via a temporary ibv_mr
                ibv_mr tmp_local = *s.stage_mr;
                tmp_local.addr = static_cast<char*>(s.stage_mr->addr) + offset;
                tmp_local.length = to_write;

                int rc = rdma_->RDMA_Write(&slot.mr, &tmp_local, to_write, IBV_SEND_SIGNALED,
                        /*poll_num=*/1, s.memory_node_id, "default");
                if (rc) {
                    fprintf(stderr, "RedoLogger: RDMA_Write failed (rc=%d)\n", rc);
                }
                s.remote_slots.emplace_back(std::move(slot));
                offset += to_write;
                remaining -= to_write;
            }

            // Disk
            if (s.file) {
                Slice bytes(static_cast<const char*>(s.stage_mr->addr), s.stage_used);
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

            s.stage_used = 0;
        }

    private:
        Env* env_;
        RDMA_Manager* rdma_;
        const uint16_t compute_node_id_;
        Options opts_;
        std::atomic<LSN> next_lsn_;
        std::unordered_map<uint16_t, StreamState> streams_;
    };

} // namespace DSMEngine
