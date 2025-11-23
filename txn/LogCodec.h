#pragma once
/**
 * LogCodec.h — header-only payload codec for redo records.
 *
 * Purpose
 *   Provide a compact, self-describing byte format and helpers to encode/decode
 *   page-physiological operations as the payload inside a redo log record.
 *
 * Non-goals
 *   We do not prescribe specific page layouts. All operations are generic and
 *   can target arbitrary byte ranges on a page. The engine supplies the actual
 *   "apply" logic using the decoded operations.
 *
 * Format (little-endian where relevant; integers are ULEB128 unless noted)
 *   [u8 codec_version=1]
 *   repeated {
 *       [u8 opcode]
 *       opcode-specific fields (ULEB128 and/or raw bytes as documented below)
 *   } until EOF
 *
 * Supported operations (extendable without breaking older decoders):
 *   - UPDATE_BYTES:  opcode=0x01
 *       fields: [uvar32 offset][uvar32 len][len bytes raw]
 *       semantics: memcpy(page+offset, bytes, len)
 *
 *   - FILL_BYTES:    opcode=0x02
 *       fields: [uvar32 offset][uvar32 len][u8 value]
 *       semantics: memset(page+offset, value, len)
 *
 *   - MEMMOVE_BYTES: opcode=0x03
 *       fields: [uvar32 dst][uvar32 src][uvar32 len]
 *       semantics: memmove(page+dst, page+src, len)
 *
 *   - SET_U64_LE:    opcode=0x04
 *       fields: [uvar32 offset][uvar64 value]
 *       semantics: *(uint64_t*)(page+offset) = value (little-endian)
 *
 * Backward/forward compatibility:
 *   - Decoders skip unknown opcodes by failing gracefully; we recommend gating
 *     by logger version to avoid mixing incompatible codecs.
 *
 * Usage (encoding):
 *   LogCodec::Encoder enc;
 *   enc.AddUpdateBytes(off, ptr, len);
 *   enc.AddSetU64(off_hdr_lsn, lsn);
 *   auto& payload = enc.Buffer();   // std::vector<uint8_t>
 *
 * Usage (decoding):
 *   LogCodec::Decoder dec(payload.data(), payload.size());
 *   LogCodec::DecodedOp op;
 *   while (dec.Next(op)) {
 *     switch (op.code) {
 *       case OpCode::UPDATE_BYTES: apply_update(op.u.update.offset, op.u.update.len, op.u.update.bytes); break;
 *       case OpCode::FILL_BYTES:   apply_fill(op.u.fill.offset, op.u.fill.len, op.u.fill.value); break;
 *       case OpCode::MEMMOVE_BYTES:apply_memmove(op.u.move.dst, op.u.move.src, op.u.move.len); break;
 *       case OpCode::SET_U64_LE:   apply_set64(op.u.set64.offset, op.u.set64.value); break;
 *       default:  break;
 *     }
 *   }
 */

#include <cstdint>
#include <vector>
#include <cstring>
#include <stdexcept>
#include <type_traits>

namespace DSMEngine { namespace LogCodec {

// ---- Versioning ----
static constexpr uint8_t kCodecVersion = 1;

// ---- Opcodes ----
enum class OpCode : uint8_t {
  UPDATE_BYTES   = 0x01,
  FILL_BYTES     = 0x02,
  MEMMOVE_BYTES  = 0x03,
  SET_U64_LE     = 0x04,
};

// ---- Varint (ULEB128) helpers ----
inline void PutUVar(std::vector<uint8_t>& out, uint64_t v) {
  while (v >= 0x80) { out.push_back(static_cast<uint8_t>(v | 0x80)); v >>= 7; }
  out.push_back(static_cast<uint8_t>(v));
}
inline bool GetUVar(const uint8_t* buf, size_t len, size_t& off, uint64_t& v_out) {
  uint64_t v = 0; int shift = 0;
  while (off < len) {
    uint8_t b = buf[off++];
    v |= static_cast<uint64_t>(b & 0x7F) << shift;
    if ((b & 0x80) == 0) { v_out = v; return true; }
    shift += 7; if (shift > 63) return false; // overflow
  }
  return false; // truncated
}
inline void PutBytes(std::vector<uint8_t>& out, const void* p, size_t n) {
  if (n == 0) return;
  const uint8_t* b = static_cast<const uint8_t*>(p);
  out.insert(out.end(), b, b + n);
}
inline bool GetBytes(const uint8_t* buf, size_t len, size_t& off, void* dst, size_t n) {
  if (off + n > len) return false;
  std::memcpy(dst, buf + off, n);
  off += n;
  return true;
}

// ---- Decoded operation view ----
struct DecodedOp {
  OpCode code;
  union U {
    struct { uint32_t offset; uint32_t len; const uint8_t* bytes; } update;
    struct { uint32_t offset; uint32_t len; uint8_t value; } fill;
    struct { uint32_t dst;    uint32_t src; uint32_t len; } move;
    struct { uint32_t offset; uint64_t value; } set64;
    U() { std::memset(this, 0, sizeof(U)); }
  } u;
};

// ---- Encoder ----
class Encoder {
public:
  Encoder() { buf_.reserve(256); buf_.push_back(kCodecVersion); }

  void Clear() { buf_.clear(); buf_.push_back(kCodecVersion); }

  const std::vector<uint8_t>& Buffer() const { return buf_; }
  std::vector<uint8_t>& Buffer() { return buf_; }

  // UPDATE_BYTES
  void AddUpdateBytes(uint32_t offset, const void* src, uint32_t len) {
    buf_.push_back(static_cast<uint8_t>(OpCode::UPDATE_BYTES));
    PutUVar(buf_, offset);
    PutUVar(buf_, len);
    PutBytes(buf_, src, len);
  }

  // FILL_BYTES
  void AddFillBytes(uint32_t offset, uint32_t len, uint8_t value) {
    buf_.push_back(static_cast<uint8_t>(OpCode::FILL_BYTES));
    PutUVar(buf_, offset);
    PutUVar(buf_, len);
    buf_.push_back(value);
  }

  // MEMMOVE_BYTES
  void AddMemmove(uint32_t dst, uint32_t src, uint32_t len) {
    buf_.push_back(static_cast<uint8_t>(OpCode::MEMMOVE_BYTES));
    PutUVar(buf_, dst);
    PutUVar(buf_, src);
    PutUVar(buf_, len);
  }

  // SET_U64_LE
  void AddSetU64LE(uint32_t offset, uint64_t value) {
    buf_.push_back(static_cast<uint8_t>(OpCode::SET_U64_LE));
    PutUVar(buf_, offset);
    PutUVar(buf_, value);
  }

private:
  std::vector<uint8_t> buf_;
};

// ---- Decoder ----
class Decoder {
public:
  Decoder(const void* data, size_t n) : base_(static_cast<const uint8_t*>(data)), len_(n) {
    if (n == 0) { ok_ = false; return; }
    uint8_t ver = base_[0];
    if (ver != kCodecVersion) { ok_ = false; return; }
    off_ = 1; ok_ = true;
  }

  bool Good() const { return ok_; }

  // Parse next operation into 'op'. Returns false at EOF or on error.
  bool Next(DecodedOp& op) {
    if (!ok_ || off_ >= len_) return false;
    uint8_t opc = base_[off_++];
    switch (static_cast<OpCode>(opc)) {
      case OpCode::UPDATE_BYTES: {
        uint64_t off, ln;
        if (!GetUVar(base_, len_, off_, off)) return Fail();
        if (!GetUVar(base_, len_, off_, ln))  return Fail();
        op.code = OpCode::UPDATE_BYTES;
        op.u.update.offset = static_cast<uint32_t>(off);
        op.u.update.len    = static_cast<uint32_t>(ln);
        if (off_ + op.u.update.len > len_) return Fail();
        op.u.update.bytes  = base_ + off_;
        off_ += op.u.update.len;
        return true;
      }
      case OpCode::FILL_BYTES: {
        uint64_t off, ln;
        if (!GetUVar(base_, len_, off_, off)) return Fail();
        if (!GetUVar(base_, len_, off_, ln))  return Fail();
        if (off_ >= len_) return Fail();
        uint8_t v = base_[off_++];
        op.code = OpCode::FILL_BYTES;
        op.u.fill.offset = static_cast<uint32_t>(off);
        op.u.fill.len    = static_cast<uint32_t>(ln);
        op.u.fill.value  = v;
        return true;
      }
      case OpCode::MEMMOVE_BYTES: {
        uint64_t d, s, ln;
        if (!GetUVar(base_, len_, off_, d))  return Fail();
        if (!GetUVar(base_, len_, off_, s))  return Fail();
        if (!GetUVar(base_, len_, off_, ln)) return Fail();
        op.code = OpCode::MEMMOVE_BYTES;
        op.u.move.dst = static_cast<uint32_t>(d);
        op.u.move.src = static_cast<uint32_t>(s);
        op.u.move.len = static_cast<uint32_t>(ln);
        return true;
      }
      case OpCode::SET_U64_LE: {
        uint64_t off, v;
        if (!GetUVar(base_, len_, off_, off)) return Fail();
        if (!GetUVar(base_, len_, off_, v))   return Fail();
        op.code = OpCode::SET_U64_LE;
        op.u.set64.offset = static_cast<uint32_t>(off);
        op.u.set64.value  = v;
        return true;
      }
      default:
        return Fail(); // unknown opcode for this codec version
    }
  }

private:
  bool Fail() { ok_ = false; return false; }

  const uint8_t* base_{nullptr};
  size_t len_{0};
  size_t off_{0};
  bool ok_{false};
};

}} // namespace DSMEngine::LogCodec
