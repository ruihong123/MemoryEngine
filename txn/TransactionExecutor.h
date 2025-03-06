// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE__TRANSACTION_EXECUTOR_H__
#define __DATABASE__TRANSACTION_EXECUTOR_H__

#include "StorageManager.h"
#include "IORedirector.h"
#include "Meta.h"
#include "TxnParam.h"
#include "TimeMeasurer.h"
#include "StoredProcedure.h"
#include "Profiler.h"
#include "PerfStatistics.h"
#include <iostream>
#include <unordered_map>
#include <boost/thread.hpp>
#include <atomic>
#include <xmmintrin.h>

namespace DSMEngine {
class TransactionExecutor {
 public:
  TransactionExecutor(IORedirector *const redirector, StorageManager *storage_manager, size_t thread_count,
                      bool log_enabled)
      :thread_count_(thread_count),
      storage_manager_(storage_manager),
      redirector_ptr_(redirector),
      log_enabled_(log_enabled) {
    is_begin_ = false;
    is_finish_ = false;
    total_count_ = 0;
    total_abort_count_ = 0;
    is_ready_ = new volatile bool[thread_count_];
    for (size_t i = 0; i < thread_count_; ++i) {
      is_ready_[i] = false;
    }
    memset(&time_lock_, 0, sizeof(time_lock_));
  }
  ~TransactionExecutor() {
    delete[] is_ready_;
    is_ready_ = NULL;
  }

  virtual void Start() {
    PrepareProcedures();
    ProcessQuery();
  }

  PerfStatistics &GetPerfStatistics() {
    return perf_statistics_;
  }
  static void ProcessQueryThread_2PC_Participant(void* storage_ptr, void* id_p) {
        uint32_t handler_id = *((uint32_t*)id_p);
        uint32_t dummy_thread_id = 0;
        bindCore(dummy_thread_id);
        StorageManager* storage_manager_ = (StorageManager*)storage_ptr;
        size_t thread_count = 0;
        TransactionManager *txn_manager = new TransactionManager(
                storage_manager_, thread_count, 0, LOGGING, false);
        auto rdma_mg = default_gallocator->rdma_mg;
        std::shared_lock<std::shared_mutex> read_lock(rdma_mg->user_df_map_mutex);
        if (rdma_mg->communication_queues.find(handler_id) == rdma_mg->communication_queues.end()){
            assert(false);
        }
        CharArray dummy;
        auto& communication_queue = rdma_mg->communication_queues.find(handler_id)->second;
        auto communication_mtx = rdma_mg->communication_mtxs.find(handler_id)->second;
        auto communication_cv = rdma_mg->communication_cvs.find(handler_id)->second;
        read_lock.unlock();
        uint16_t target_node_id = handler_id >> 16;
        ibv_mr* local_mr = rdma_mg->Get_local_read_mr();

      // wait for the signal on communicaiton buffer to process query.
        while (!rdma_mg->handler_is_finish.load()){
            std::unique_lock<std::mutex> lock(*communication_mtx);
//            communication_cv->wait(lock);
            communication_cv->wait(lock, [&] { return !communication_queue.empty() || rdma_mg->handler_is_finish.load(); });

            // The predicate below can be deleted.
            if (communication_queue.empty()){
                continue;
            }
//            printf("Thread waked up\n");
//            fflush(stdout);
            bool success = true;
            RDMA_Request received_rdma_request = communication_queue.front();
            communication_queue.pop();
            lock.unlock();
            if (received_rdma_request.content.tuple_info.log_enabled){
                txn_manager->EnableLog();
            } else{
                txn_manager->DisableLog();
            }
            Record* record;
            switch (received_rdma_request.command) {
                case tuple_read_2pc:
                    // process the request
                    success = txn_manager->SearchRecord(nullptr, received_rdma_request.content.tuple_info.table_id, received_rdma_request.content.tuple_info.primary_key, record, (DSMEngine::AccessType)received_rdma_request.content.tuple_info.access_type);
                    break;
                case prepare_2pc:
                    success = txn_manager->CoordinatorPrepare();
                    break;
                case commit_2pc:
                    txn_manager->WriteCommitLog();
                    break;
                case abort_2pc:
                    txn_manager->AbortTransaction();
                    break;
                default:
                    assert(false);
                    printf("Invalid command for 2pc processing\n");
                    exit(0);
            }
#ifndef NDEBUG
            memset(local_mr->addr, 0, local_mr->length);
#endif
            if(received_rdma_request.command == tuple_read_2pc){
                size_t tuple_size = received_rdma_request.content.tuple_info.tuple_size;
                if (success){
                    assert(record->data_size_ == tuple_size);
                    memcpy(local_mr->addr, record->data_ptr_, record->data_size_);
                }else{
                    memset(local_mr->addr, 0, tuple_size);
                }
                auto send_request_ptr = (RDMA_ReplyXCompute* )((char*)local_mr->addr+tuple_size);
                send_request_ptr->toPC_reply_type = success ? 1 : 2;
//                printf("Tuple Read Reply sent from node %u to node %u, the return type is %d\n", rdma_mg->node_id, target_node_id, send_request_ptr->toPC_reply_type);
//                fflush(stdout);
                int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
                rdma_mg->RDMA_Write_xcompute(local_mr, received_rdma_request.buffer, received_rdma_request.rkey,
                                             sizeof(RDMA_ReplyXCompute) + tuple_size, target_node_id, qp_id,
                                             false, false);

            }else if (received_rdma_request.command == prepare_2pc){
                ibv_mr* local_mr = rdma_mg->Get_local_send_message_mr();
                auto send_request_ptr = ((RDMA_ReplyXCompute* )(local_mr->addr));
//                printf("Prepare Reply sent from node %u to node %u, the return type is %d\n", rdma_mg->node_id, target_node_id, send_request_ptr->toPC_reply_type);
//                fflush(stdout);
                send_request_ptr->toPC_reply_type = success ? 1 : 2;
                int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
                rdma_mg->RDMA_Write_xcompute(local_mr, received_rdma_request.buffer, received_rdma_request.rkey,
                                             sizeof(RDMA_ReplyXCompute), target_node_id, qp_id, true, false);
            }

//            delete receive_msg_buf;


            success = true;
            delete (uint32_t *)id_p;
//            communication_queue->command = invalid_command_;
        }

  }
 private:
  virtual void PrepareProcedures() = 0;

  virtual void ProcessQuery() {
    std::cout << "start process query" << std::endl;
      boost::thread_group thread_group;
    for (size_t i = 0; i < thread_count_; ++i) {
      // can bind threads to cores here
      thread_group.create_thread(
          boost::bind(&TransactionExecutor::ProcessQueryThread, this, i));
    }
    //TODO: set the message handling function for the RDMA manager, need to develp an more elegant way.
    bool is_all_ready = true;
    while (1) {
      for (size_t i = 0; i < thread_count_; ++i) {
        if (is_ready_[i] == false) {
          is_all_ready = false;
          break;
        }
      }
      if (is_all_ready == true) {
        break;
      }
      is_all_ready = true;
    }
    // epoch generator.
    std::cout << "start processing..." << std::endl;
    is_begin_ = true;
    start_timestamp_ = timer_.GetTimePoint();
    thread_group.join_all();
    long long elapsed_time = timer_.CalcMilliSecondDiff(start_timestamp_,
                                                        end_timestamp_);
    double throughput = total_count_ * 1.0 / elapsed_time;
    double per_core_throughput = throughput / thread_count_;
    std::cout << "Node" << RDMA_Manager::Get_Instance()->node_id << "execute_count=" << total_count_ << ", abort_count="
              << total_abort_count_ << ", abort_rate="
              << total_abort_count_ * 1.0 / (total_count_ + 1) << std::endl;
    std::cout << "elapsed time=" << elapsed_time << "ms.\nthroughput="
              << throughput << "K tps.\nper-core throughput="
              << per_core_throughput << "K tps." << std::endl;

    perf_statistics_.total_count_ = total_count_;
    perf_statistics_.total_abort_count_ = total_abort_count_;
    perf_statistics_.thread_count_ = thread_count_;
    perf_statistics_.elapsed_time_ = elapsed_time;
    perf_statistics_.throughput_ = throughput;
  }

  virtual void ProcessQueryThread(const size_t& thread_id) {
      bindCore(thread_id + 1);
    //std::cout << "start thread " << thread_id << std::endl;
    std::vector<ParamBatch*> &execution_batches = 
      *(redirector_ptr_->GetParameterBatches(thread_id));

    TransactionManager *txn_manager = new TransactionManager(
            storage_manager_, this->thread_count_, thread_id, log_enabled_, TWOPHASECOMMIT);
    StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
    for (auto &entry : registers_) {
      procedures[entry.first] = entry.second();
      procedures[entry.first]->SetTransactionManager(txn_manager);
    }

    is_ready_[thread_id] = true;
    while (is_begin_ == false)
      ;
    int count = 0;
    int abort_count = 0;
    uint32_t backoff_shifts = 0;
    CharArray ret;
    ret.char_ptr_ = new char[1024];
    for (auto& tuples : execution_batches) {
      for (size_t idx = 0; idx < tuples->size(); ++idx) {
        TxnParam* tuple = tuples->get(idx);
        // begin txn
        PROFILE_TIME_START(thread_id, TXN_EXECUTE);
        ret.size_ = 0;
        if (procedures[tuple->type_]->Execute(tuple, ret) == false) {
//            assert(false);
          ret.size_ = 0;
          ++abort_count;
          if (is_finish_ == true) {
            total_count_.fetch_add(count);
            total_abort_count_.fetch_add(abort_count);
            PROFILE_TIME_END(thread_id, TXN_EXECUTE);
//            txn_manager->CleanUp();
            return;
          }PROFILE_TIME_START(thread_id, TXN_ABORT);
#if defined(BACKOFF)						
          if (backoff_shifts < 63) {
            ++backoff_shifts;
          }
          uint64_t spins = 1UL << backoff_shifts;

          spins *= 100;
          while (spins) {
            _mm_pause();
            --spins;
          }
#endif
          while (procedures[tuple->type_]->Execute(tuple, ret) == false) {

            ret.size_ = 0;
            ++abort_count;
            if (is_finish_ == true) {
                total_count_.fetch_add(count);
                total_abort_count_.fetch_add(abort_count);
              PROFILE_TIME_END(thread_id, TXN_ABORT);
              PROFILE_TIME_END(thread_id, TXN_EXECUTE);
              //txn_manager->CleanUp();
              return;
            }
#if defined(BACKOFF)
            uint64_t spins = 1UL << backoff_shifts;
            spins *= 100;
            while (spins) {
              _mm_pause();
              --spins;
            }
#endif
          }PROFILE_TIME_END(thread_id, TXN_ABORT);
        } else {
//            printf("Transaction finished for thread %zu\n", thread_id);
#if defined(BACKOFF)
          backoff_shifts >>= 1;
#endif
        }
        ++count;
        PROFILE_TIME_END(thread_id, TXN_EXECUTE);
        if(count % 10000 == 0){
            printf("Node %u Thread %zu finished %d\n", default_gallocator->rdma_mg->node_id, thread_id, count);
            fflush(stdout);
        }
        if (is_finish_ == true) {
          total_count_ += count;
          total_abort_count_ += abort_count;
          //txn_manager->CleanUp();
            return;
        }
      }
    }
    time_lock_.lock();
    end_timestamp_ = timer_.GetTimePoint();
    is_finish_ = true;
    time_lock_.unlock();
    total_count_ += count;
    total_abort_count_ += abort_count;
      printf("Thread %zu finished \n", thread_id);
      fflush(stdout);
    //txn_manager->CleanUp();
    return;
  }

 protected:
  size_t thread_count_;
  StorageManager *storage_manager_;
  IORedirector* const redirector_ptr_;
  std::unordered_map<size_t, std::function<StoredProcedure*()>> registers_;
  std::unordered_map<size_t, std::function<void(StoredProcedure*)>> deregisters_;

 private:
  // perf measurement
  TimeMeasurer timer_;
  system_clock::time_point start_timestamp_;
  system_clock::time_point end_timestamp_;
  boost::detail::spinlock time_lock_;
  // multi-thread util
  volatile bool *is_ready_;
  volatile bool is_begin_;
  volatile bool is_finish_;
  // profile count
  std::atomic<size_t> total_count_;
  std::atomic<size_t> total_abort_count_;

  PerfStatistics perf_statistics_;
  bool log_enabled_;
};
}

#endif
