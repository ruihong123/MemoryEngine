// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN_TXN_ACCESS_H__
#define __DATABASE_TXN_TXN_ACCESS_H__

#include "Record.h"
#include "DDSM.h"
#include <vector>

namespace DSMEngine {
struct Access {
  Access()
      : access_global_record_(nullptr), access_addr_(GlobalAddress::Null()) {
  }
  AccessType access_type_;
  Record* access_global_record_ = nullptr;
  Record* txn_local_tuple_ = nullptr;
  GlobalAddress access_addr_; // tuple global address
};

class AccessList {
 public:
  AccessList()
      : access_count_(0) {
    accesses_.reserve(256); // Reserve initial capacity for performance
  }

  Access *NewAccess() {
    if (access_count_ >= accesses_.size()) {
      accesses_.emplace_back();
    }
    Access *ret = &(accesses_[access_count_]);
    ++access_count_;
    return ret;
  }

  Access *GetAccess(const size_t &index) {
    assert(index < access_count_);
    return &(accesses_[index]);
  }

  void Clear() {
    access_count_ = 0;
    // Optionally shrink if it grew too large (e.g., > 4x initial capacity)
    if (accesses_.capacity() > 1024 && accesses_.size() < 256) {
      accesses_.shrink_to_fit();
      accesses_.reserve(256);
    }
  }

 public:
  size_t access_count_;
 private:
  std::vector<Access> accesses_;
};
}

#endif
