#ifndef __DATABASE_META_H__
#define __DATABASE_META_H__

#include <cstdint>
#include <cstring>
//#include "DDSM.h"

//#include "gallocator.h"

// Global key compression configuration
// When defined, all benchmarks use compressed uint64_t keys instead of
// multi-field keys This affects both Table.h index schema creation and all
// KeyGenerator implementations
#define COMPRESSED_TPCC_KEY

namespace DSMEngine {
typedef uint32_t HashcodeType;
typedef uint64_t IndexKey;
class DDSM;
enum LockType : size_t {
  NO_LOCK,
  READ_LOCK,
  WRITE_LOCK,
};
enum AccessType : size_t {
  READ_ONLY,   // prepageRead
  INSERT_ONLY, // prepageWrite
  DELETE_ONLY,
  READ_WRITE // prepageUpdate

};
enum SourceType : size_t { RANDOM_SOURCE, PARTITION_SOURCE };

// storage
const size_t kMaxTableNum = 16;
const size_t kMaxColumnNum = 64;
const size_t kMaxAttributeLength = 64; // largest attribute length 64Bytes.
const size_t kMaxSecondaryIndexNum = 5;
const uint64_t kHashIndexBucketHeaderNum = 1000007;
// txn
const size_t kTryLockLimit = 1;
const size_t kMaxAccessLimit = 256;

extern DDSM *default_gallocator;
extern DDSM **gallocators;
extern size_t gThreadCount;

// source
extern size_t gParamBatchSize;

} // namespace DSMEngine

#endif
