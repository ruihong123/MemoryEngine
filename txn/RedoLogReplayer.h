#pragma once
/**
 * RedoLogReplayer:
 *  - Parses page-version-aware redo streams emitted by RedoLogger.
 *  - Replays records from disk files with user-supplied callbacks.
 *
 * Design:
 *  - The replayer is agnostic to the payload schema; you provide an
 *    Apply(...) functor that executes the physical change for the page.
 *  - For correctness under multiple producers and out-of-order streams,
 *    each record carries (page_gaddr, page_version). The replayer
 *    applies a record iff page_version > current_version(page_gaddr).
 *
 * Minimal dependency footprint; uses Env::SequentialFile to read files.
 */
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <cstdio>

#include "Tools/env.h"
#include "Tools/slice.h"
#include "txn/RedoLogger.h"  // for RecordHeader definition
#include "include/Common.h"

namespace DSMEngine {

class RedoLogReplayer {
 public:
  using Header = RedoLogger::RecordHeader;

  // Callbacks the embedding engine must supply.
  using GetPageVersionFn = std::function<uint64_t(GlobalAddress)>;
  using SetPageVersionFn = std::function<void(GlobalAddress, uint64_t)>;
  using ApplyFn = std::function<bool(const Header&, const char* payload, uint32_t len)>;

  explicit RedoLogReplayer(Env* env) : env_(env) {}

  // Replay a single file. Returns number of applied records.
  size_t ReplayFile(const std::string& path,
                    const GetPageVersionFn& get_version,
                    const SetPageVersionFn& set_version,
                    const ApplyFn& apply) {
    size_t applied = 0;
    if (!env_) return 0;
    SequentialFile* sf = nullptr;
    Status st = env_->NewSequentialFile(path, &sf);
    if (!st.ok() || !sf) {
      fprintf(stderr, "RedoLogReplayer: cannot open %s (%s)\n", path.c_str(), st.ToString().c_str());
      return 0;
    }
    std::unique_ptr<SequentialFile> file(sf);

    constexpr size_t kHeaderSz = sizeof(Header);
    std::vector<char> scratch(std::max<size_t>(kHeaderSz, 4096));
    Slice result;

    while (true) {
      // Read header
      st = file->Read(kHeaderSz, &result, scratch.data());
      if (!st.ok()) {
        fprintf(stderr, "RedoLogReplayer: read header error in %s: %s\n",
                path.c_str(), st.ToString().c_str());
        break;
      }
      if (result.size() == 0) {
        // EOF
        break;
      }
      if (result.size() < kHeaderSz) {
        fprintf(stderr, "RedoLogReplayer: truncated header in %s\n", path.c_str());
        break;
      }
      Header hdr;
      std::memcpy(&hdr, result.data(), kHeaderSz);
      if (hdr.magic != 0x4C52 || hdr.version != 1) {
        fprintf(stderr, "RedoLogReplayer: bad record header (magic=0x%x ver=%u) in %s\n",
                hdr.magic, hdr.version, path.c_str());
        break;
      }

      // Read payload (may require multiple reads)
      std::vector<char> payload(hdr.payload_len);
      size_t got = 0;
      while (got < hdr.payload_len) {
        size_t to_read = std::min<size_t>(payload.size() - got, scratch.size());
        st = file->Read(to_read, &result, scratch.data());
        if (!st.ok()) {
          fprintf(stderr, "RedoLogReplayer: payload read error in %s: %s\n",
                  path.c_str(), st.ToString().c_str());
          goto OUT;
        }
        if (result.size() == 0) {
          fprintf(stderr, "RedoLogReplayer: unexpected EOF while reading payload in %s\n", path.c_str());
          goto OUT;
        }
        std::memcpy(payload.data() + got, result.data(), result.size());
        got += result.size();
      }

      // Visibility check by page version
      const uint64_t cur_ver = get_version ? get_version(hdr.page_gaddr) : 0;
      if (hdr.page_version > cur_ver) {
        // Apply physical change
        const bool ok = apply(hdr, payload.data(), hdr.payload_len);
        if (ok && set_version) {
          set_version(hdr.page_gaddr, hdr.page_version);
        }
        if (ok) ++applied;
      } else {
        // Skip stale record
      }
    }

   OUT:
    return applied;
  }

 private:
  Env* env_;
};

} // namespace DSMEngine
