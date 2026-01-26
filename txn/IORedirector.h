// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN_IO_REDIRECTOR_H__
#define __DATABASE_TXN_IO_REDIRECTOR_H__

#include <vector>
#include "TxnParam.h"

namespace DSMEngine {
class IORedirector {
 public:
  IORedirector(const size_t &thread_count)
      : thread_count_(thread_count),
        curr_thread_id_(0) {
    input_batches_ = new std::vector<ParamBatch*>[thread_count];
  }
  ~IORedirector() {
    if (input_batches_ != nullptr) {
      // Clean up all batches and their params for each thread
      for (size_t i = 0; i < thread_count_; ++i) {
        for (auto* batch : input_batches_[i]) {
          if (batch != nullptr) {
            // Delete all TxnParam objects in the batch
            for (size_t j = 0; j < batch->size(); ++j) {
              TxnParam* param = batch->get(j);
              if (param != nullptr) {
                delete param;
              }
            }
            // Delete the batch itself
            delete batch;
          }
        }
        // Clear the vector (batches are already deleted)
        input_batches_[i].clear();
      }
      // Delete the array of vectors
      delete[] input_batches_;
      input_batches_ = NULL;
    }
  }

  std::vector<ParamBatch*> *GetParameterBatches() {
    return input_batches_;
  }
  std::vector<ParamBatch*> *GetParameterBatches(const size_t &thread_id) {
    return &(input_batches_[thread_id]);
  }
  void PushParameterBatch(ParamBatch *tuples) {
    input_batches_[curr_thread_id_].push_back(tuples);
    curr_thread_id_ = (curr_thread_id_ + 1) % thread_count_;
  }

 private:
  IORedirector(const IORedirector &);
  IORedirector& operator=(const IORedirector &);

 protected:
  std::vector<ParamBatch*> *input_batches_;
  size_t thread_count_;
  size_t curr_thread_id_;
};
}

#endif
