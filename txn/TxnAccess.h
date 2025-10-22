// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN_TXN_ACCESS_H__
#define __DATABASE_TXN_TXN_ACCESS_H__

#include "Record.h"
#include "DDSM.h"

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

template<int N>
class AccessList {
 public:
  AccessList()
      : access_count_(0) {
  }

  Access *NewAccess() {
    assert(access_count_ < N);
    Access *ret = &(accesses_[access_count_]);
    ++access_count_;
    return ret;
  }

  Access *GetAccess(const size_t &index) {
    return &(accesses_[index]);
  }

  void Clear() {
    access_count_ = 0;
  }

 public:
  size_t access_count_;
 private:
  Access accesses_[N];
};
}

#endif
