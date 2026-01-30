#ifndef __DATABASE_TATP_EXECUTOR_H__
#define __DATABASE_TATP_EXECUTOR_H__

#include "TATPProcedure.h"
#include "TATPParams.h"
#include "TATPKeyGenerator.h"
#include "TransactionExecutor.h"
#include <cmath>
#include <atomic>
#include <memory>

namespace DSMEngine {
namespace TATPBenchmark {

class TATPExecutor : public TransactionExecutor {
public:
  TATPExecutor(IORedirector *const redirector, TableDirectory *storage_manager,
               size_t thread_count, bool log_enabled,
               bool enable_latency_recording = false)
      : TransactionExecutor(redirector, storage_manager, thread_count,
                            log_enabled, enable_latency_recording) {}

  virtual ~TATPExecutor() {}

  virtual void PrepareProcedures() {
    registers_[GET_SUBSCRIBER_DATA] = []() {
      return new GetSubscriberDataProcedure();
    };
    registers_[GET_NEW_DESTINATION] = []() {
      return new GetNewDestinationProcedure();
    };
    registers_[GET_ACCESS_DATA] = []() { return new GetAccessDataProcedure(); };
    registers_[UPDATE_SUBSCRIBER_DATA] = []() {
      return new UpdateSubscriberDataProcedure();
    };
    registers_[UPDATE_LOCATION] = []() {
      return new UpdateLocationProcedure();
    };
    registers_[INSERT_CALL_FORWARDING] = []() {
      return new InsertCallForwardingProcedure();
    };
    registers_[DELETE_CALL_FORWARDING] = []() {
      return new DeleteCallForwardingProcedure();
    };
    registers_[ANALYTICAL_SCAN] = []() {
      return new AnalyticalScanProcedure();
    };

    deregisters_[GET_SUBSCRIBER_DATA] = [](StoredProcedure *p) { delete p; };
    deregisters_[GET_NEW_DESTINATION] = [](StoredProcedure *p) { delete p; };
    deregisters_[GET_ACCESS_DATA] = [](StoredProcedure *p) { delete p; };
    deregisters_[UPDATE_SUBSCRIBER_DATA] = [](StoredProcedure *p) { delete p; };
    deregisters_[UPDATE_LOCATION] = [](StoredProcedure *p) { delete p; };
    deregisters_[INSERT_CALL_FORWARDING] = [](StoredProcedure *p) { delete p; };
    deregisters_[DELETE_CALL_FORWARDING] = [](StoredProcedure *p) { delete p; };
    deregisters_[ANALYTICAL_SCAN] = [](StoredProcedure *p) { delete p; };
  }

protected:
  virtual void
  ConfigureHotTableScanner(std::vector<HotTableScanTask> &tasks) override {
    using namespace TATPBenchmark;
    if (storage_manager_ == nullptr || storage_manager_->tables_.empty()) {
      return;
    }

    const int64_t start_sub = tatp_scale_params.starting_subscriber_;
    const int64_t end_sub = tatp_scale_params.ending_subscriber_;
    const int64_t global_total_subscribers = tatp_scale_params.num_subscribers_; // Global total across all partitions
    if (start_sub < 0 || end_sub < start_sub || global_total_subscribers <= 0) {
      return;
    }

    // Calculate number of users to scan per transaction (10% by default)
    // Based on global total, not just local partition
    const int64_t users_per_scan = std::max(static_cast<int64_t>(1), static_cast<int64_t>(std::ceil(global_total_subscribers * HOT_SCAN_USER_PERCENTAGE)));

    // Create scan task that scans all data for 10% of users per transaction
    auto access_it = storage_manager_->tables_.find(ACCESS_INFO_TABLE_ID);
    auto sf_it = storage_manager_->tables_.find(SPECIAL_FACILITY_TABLE_ID);
    if (access_it == storage_manager_->tables_.end() || 
        sf_it == storage_manager_->tables_.end()) {
      return;
    }
    auto access_schema = access_it->second->GetPrimaryIndexSchema();
    auto sf_schema = sf_it->second->GetPrimaryIndexSchema();
    
    // State tracking for scanning multiple users per transaction
    auto current_user_start = std::make_shared<int64_t>(start_sub); // Start of current user batch
    auto current_user = std::make_shared<int64_t>(start_sub); // Current user being scanned
    auto current_ai_type = std::make_shared<int>(AI_TYPE_MIN); // Current access info type
    auto current_sf_type = std::make_shared<int>(SF_TYPE_MIN); // Current special facility type
    auto scan_table = std::make_shared<int>(0); // 0=access_info, 1=special_facility
    auto in_scan = std::make_shared<bool>(false); // true if currently scanning
    
    tasks.push_back(
        HotTableScanTask("tatp_multi_user_scan",
                         [current_user_start, current_user, current_ai_type, current_sf_type,
                          access_schema, sf_schema, scan_table, in_scan, 
                          start_sub, global_total_subscribers, users_per_scan]
                         (TransactionManager &mgr, const std::atomic<bool> &should_run) -> bool {
                           Record *record = nullptr;
                           
                           // Loop until a transaction completes (commits or aborts)
                           while (should_run.load(std::memory_order_acquire)) {
                             if (!(*in_scan)) {
                               // Start a new scan batch
                               *in_scan = true;
                               *current_user = *current_user_start;
                               *current_ai_type = AI_TYPE_MIN;
                               *current_sf_type = SF_TYPE_MIN;
                               *scan_table = 0; // Start with access_info
                             }
                             
                             // Scan all data for users in current batch
                             switch (*scan_table) {
                               case 0: { // Scan access_info
                                 int64_t sub = *current_user;
                                 int ai_type = *current_ai_type;
                                 
                                 // Read access_info record
                                 DynamicCompoundKey key = TATPKeyGenerator::GenerateAccessInfoKey(sub, ai_type, access_schema);
                                 if (!mgr.SearchRecord(ACCESS_INFO_TABLE_ID, key, record, SCAN_READ)) {
                                   *in_scan = false;
                                   return false; // Transaction aborted
                                 }
#if defined(TO)
                                 Cache::Handle* held_handle = ((Cache::Handle*)record->Get_Handle());
                                 assert(held_handle->gptr != GlobalAddress::Null());
                                 mgr.ReleaseLatchForGCL(held_handle->gptr, held_handle);
#endif
                                 
                                 // Advance to next access_info type
                                 ++ai_type;
                                 if (ai_type > AI_TYPE_MAX) {
                                   // Finished all access_info for current user, move to next user
                                   ai_type = AI_TYPE_MIN;
                                   ++sub;
                                   // Check if we've exceeded the batch or global total
                                   int64_t batch_end = *current_user_start + users_per_scan - 1;
                                   if (sub > batch_end || sub >= global_total_subscribers) {
                                     // Finished all access_info for all users in batch, switch to special_facility
                                     *scan_table = 1;
                                     *current_user = *current_user_start;
                                     *current_sf_type = SF_TYPE_MIN;
                                   } else {
                                     *current_user = sub;
                                   }
                                   *current_ai_type = ai_type;
                                 } else {
                                   *current_ai_type = ai_type;
                                 }
                                 break;
                               }
                               case 1: { // Scan special_facility
                                 int64_t sub = *current_user;
                                 int sf_type = *current_sf_type;
                                 
                                 // Read special_facility record
                                 DynamicCompoundKey key = TATPKeyGenerator::GenerateSpecialFacilityKey(sub, sf_type, sf_schema);
                                 if (!mgr.SearchRecord(SPECIAL_FACILITY_TABLE_ID, key, record, SCAN_READ)) {
                                   *in_scan = false;
                                   return false; // Transaction aborted
                                 }
#if defined(TO)
                                 Cache::Handle* held_handle = ((Cache::Handle*)record->Get_Handle());
                                 assert(held_handle->gptr != GlobalAddress::Null());
                                 mgr.ReleaseLatchForGCL(held_handle->gptr, held_handle);
#endif
                                 
                                 // Advance to next special_facility type
                                 ++sf_type;
                                 if (sf_type > SF_TYPE_MAX) {
                                   // Finished all special_facility for current user, move to next user
                                   sf_type = SF_TYPE_MIN;
                                   ++sub;
                                   // Check if we've exceeded the batch or global total
                                   int64_t batch_end = *current_user_start + users_per_scan - 1;
                                   if (sub > batch_end || sub >= global_total_subscribers) {
                                     // Finished scanning all data for all users in batch
                                     // Commit transaction
                                     CharArray ret;
                                     bool committed = mgr.CommitTransaction(ret);
                                     if (committed) {
                                       *in_scan = false;
                                       // Move to next batch of users (round-robin globally)
                                       *current_user_start += users_per_scan;
                                       if (*current_user_start >= global_total_subscribers) {
                                         // Wrap around: start from 0 (first subscriber globally)
                                         *current_user_start = 0;
                                       }
                                       return true; // Transaction committed
                                     } else {
                                       *in_scan = false;
                                       return false; // Transaction aborted
                                     }
                                   } else {
                                     *current_user = sub;
                                   }
                                   *current_sf_type = sf_type;
                                 } else {
                                   *current_sf_type = sf_type;
                                 }
                                 break;
                               }
                             }
                           }
                           return false; // Should not reach here, but return false if loop exits
                         }));
  }
};

} // namespace TATPBenchmark
} // namespace DSMEngine

#endif
