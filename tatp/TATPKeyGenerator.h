#ifndef __DATABASE_TATP_KEY_GENERATOR_H__
#define __DATABASE_TATP_KEY_GENERATOR_H__

#include "DynamicCompoundKey.h"
#include "Meta.h"
#include "TATPConstants.h"
#include <cstring>

// Note: COMPRESSED_TPCC_KEY is now defined in Meta.h for global configuration

namespace DSMEngine {
namespace TATPBenchmark {

class TATPKeyGenerator {
public:
#ifdef COMPRESSED_TPCC_KEY
  /******************** Compressed uint64_t Key Implementation
   * **********************/
  // Uses thread-local buffer for compatibility with multi-field API
  // Keys are packed into uint64_t using bit shifting

  // Generate key for Subscriber table
  static DynamicCompoundKey
  GenerateSubscriberKey(int64_t s_id,
                        RecordSchema *index_schema_ptr = nullptr) {
    static thread_local uint64_t key_buffer;
    key_buffer = static_cast<uint64_t>(s_id);
    return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                              index_schema_ptr);
  }

  // Generate key for AccessInfo table (s_id, ai_type)
  static DynamicCompoundKey
  GenerateAccessInfoKey(int64_t s_id, uint8_t ai_type,
                        RecordSchema *index_schema_ptr = nullptr) {
    static thread_local uint64_t key_buffer;
    key_buffer = (static_cast<uint64_t>(s_id) << 8) | ai_type;
    return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                              index_schema_ptr);
  }

  // Generate key for SpecialFacility table (s_id, sf_type)
  static DynamicCompoundKey
  GenerateSpecialFacilityKey(int64_t s_id, uint8_t sf_type,
                             RecordSchema *index_schema_ptr = nullptr) {
    static thread_local uint64_t key_buffer;
    key_buffer = (static_cast<uint64_t>(s_id) << 8) | sf_type;
    return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                              index_schema_ptr);
  }

  // Generate key for CallForwarding table (s_id, sf_type, start_time)
  static DynamicCompoundKey
  GenerateCallForwardingKey(int64_t s_id, uint8_t sf_type, uint8_t start_time,
                            RecordSchema *index_schema_ptr = nullptr) {
    static thread_local uint64_t key_buffer;
    key_buffer = (static_cast<uint64_t>(s_id) << 16) |
                 (static_cast<uint64_t>(sf_type) << 8) | start_time;
    return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                              index_schema_ptr);
  }

#else
  /******************** Multi-field DynamicCompoundKey Implementation
   * **********************/
  // Uses thread-local buffer

  // Generate key for Subscriber table
  static DynamicCompoundKey
  GenerateSubscriberKey(int64_t s_id,
                        RecordSchema *index_schema_ptr = nullptr) {
    static thread_local char prim_buffer[sizeof(int64_t)];
    memcpy(prim_buffer, &s_id, sizeof(int64_t));
    DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
    return ret;
  }

  // Generate key for AccessInfo table (s_id, ai_type)
  static DynamicCompoundKey
  GenerateAccessInfoKey(int64_t s_id, uint8_t ai_type,
                        RecordSchema *index_schema_ptr = nullptr) {
    static thread_local char prim_buffer[sizeof(int64_t) + sizeof(uint8_t)];
    memcpy(prim_buffer, &s_id, sizeof(int64_t));
    memcpy(prim_buffer + sizeof(int64_t), &ai_type, sizeof(uint8_t));
    DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
    return ret;
  }

  // Generate key for SpecialFacility table (s_id, sf_type)
  static DynamicCompoundKey
  GenerateSpecialFacilityKey(int64_t s_id, uint8_t sf_type,
                             RecordSchema *index_schema_ptr = nullptr) {
    static thread_local char prim_buffer[sizeof(int64_t) + sizeof(uint8_t)];
    memcpy(prim_buffer, &s_id, sizeof(int64_t));
    memcpy(prim_buffer + sizeof(int64_t), &sf_type, sizeof(uint8_t));
    DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
    return ret;
  }

  // Generate key for CallForwarding table (s_id, sf_type, start_time)
  static DynamicCompoundKey
  GenerateCallForwardingKey(int64_t s_id, uint8_t sf_type, uint8_t start_time,
                            RecordSchema *index_schema_ptr = nullptr) {
    static thread_local char
        prim_buffer[sizeof(int64_t) + sizeof(uint8_t) + sizeof(uint8_t)];
    memcpy(prim_buffer, &s_id, sizeof(int64_t));
    memcpy(prim_buffer + sizeof(int64_t), &sf_type, sizeof(uint8_t));
    memcpy(prim_buffer + sizeof(int64_t) + sizeof(uint8_t), &start_time,
           sizeof(uint8_t));
    DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
    return ret;
  }
#endif
};

} // namespace TATPBenchmark
} // namespace DSMEngine

#endif
