#ifndef __DATABASE_SMALLBANK_KEY_GENERATOR_H__
#define __DATABASE_SMALLBANK_KEY_GENERATOR_H__

#include "DynamicCompoundKey.h"
#include "Meta.h"
#include "SmallBankConstants.h"
#include <cstring>

// Note: COMPRESSED_TPCC_KEY is now defined in Meta.h for global configuration

namespace DSMEngine {
namespace SmallBankBenchmark {

class SmallBankKeyGenerator {
public:
#ifdef COMPRESSED_TPCC_KEY
  /******************** Compressed uint64_t Key Implementation
   * **********************/
  // Uses thread-local buffer for compatibility with multi-field API

  // Generate key for Accounts table
  static DynamicCompoundKey
  GenerateAccountsKey(int64_t custid,
                      RecordSchema *index_schema_ptr = nullptr) {
    static thread_local uint64_t key_buffer;
    key_buffer = static_cast<uint64_t>(custid);
    return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                              index_schema_ptr);
  }

  // Generate key for Savings table
  static DynamicCompoundKey
  GenerateSavingsKey(int64_t custid, RecordSchema *index_schema_ptr = nullptr) {
    static thread_local uint64_t key_buffer;
    key_buffer = static_cast<uint64_t>(custid);
    return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                              index_schema_ptr);
  }

  // Generate key for Checking table
  static DynamicCompoundKey
  GenerateCheckingKey(int64_t custid,
                      RecordSchema *index_schema_ptr = nullptr) {
    static thread_local uint64_t key_buffer;
    key_buffer = static_cast<uint64_t>(custid);
    return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                              index_schema_ptr);
  }

#else
  /******************** Multi-field DynamicCompoundKey Implementation
   * **********************/
  // Uses thread-local buffer

  // Generate key for Accounts table
  static DynamicCompoundKey
  GenerateAccountsKey(int64_t custid,
                      RecordSchema *index_schema_ptr = nullptr) {
    static thread_local char prim_buffer[sizeof(int64_t)];
    memcpy(prim_buffer, &custid, sizeof(int64_t));
    DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
    return ret;
  }

  // Generate key for Savings table
  static DynamicCompoundKey
  GenerateSavingsKey(int64_t custid, RecordSchema *index_schema_ptr = nullptr) {
    static thread_local char prim_buffer[sizeof(int64_t)];
    memcpy(prim_buffer, &custid, sizeof(int64_t));
    DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
    return ret;
  }

  // Generate key for Checking table
  static DynamicCompoundKey
  GenerateCheckingKey(int64_t custid,
                      RecordSchema *index_schema_ptr = nullptr) {
    static thread_local char prim_buffer[sizeof(int64_t)];
    memcpy(prim_buffer, &custid, sizeof(int64_t));
    DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
    return ret;
  }
#endif
};

} // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
