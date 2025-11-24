#ifndef __DATABASE_TATP_EXECUTOR_H__
#define __DATABASE_TATP_EXECUTOR_H__

#include "TATPProcedure.h"
#include "TATPParams.h"
#include "TATPKeyGenerator.h"
#include "TransactionExecutor.h"
#include <chrono>
#include <xmmintrin.h>

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

    deregisters_[GET_SUBSCRIBER_DATA] = [](StoredProcedure *p) { delete p; };
    deregisters_[GET_NEW_DESTINATION] = [](StoredProcedure *p) { delete p; };
    deregisters_[GET_ACCESS_DATA] = [](StoredProcedure *p) { delete p; };
    deregisters_[UPDATE_SUBSCRIBER_DATA] = [](StoredProcedure *p) { delete p; };
    deregisters_[UPDATE_LOCATION] = [](StoredProcedure *p) { delete p; };
    deregisters_[INSERT_CALL_FORWARDING] = [](StoredProcedure *p) { delete p; };
    deregisters_[DELETE_CALL_FORWARDING] = [](StoredProcedure *p) { delete p; };
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
    if (start_sub <= 0 || end_sub < start_sub) {
      return;
    }

    // Create alternating scan between ACCESS_INFO and SPECIAL_FACILITY tables
    // Each node scans only its own partition (start_sub to end_sub) to avoid starvation
    // Each transaction scans ONE subscriber's records before committing (long-running transaction)
    auto access_it = storage_manager_->tables_.find(ACCESS_INFO_TABLE_ID);
    auto sf_it = storage_manager_->tables_.find(SPECIAL_FACILITY_TABLE_ID);
    if (access_it == storage_manager_->tables_.end() || 
        sf_it == storage_manager_->tables_.end()) {
      return;
    }
    auto access_schema = access_it->second->GetPrimaryIndexSchema();
    auto sf_schema = sf_it->second->GetPrimaryIndexSchema();
    
    // State tracking for subscriber-by-subscriber scans
    enum ScanTableType {
      SCAN_ACCESS_INFO = 0,
      SCAN_SPECIAL_FACILITY = 1
    };
    auto current_subscriber = std::make_shared<int64_t>(start_sub); // Current subscriber being scanned
    auto access_scan_state = std::make_shared<int>(AI_TYPE_MIN); // Current access info type
    auto sf_scan_state = std::make_shared<int>(SF_TYPE_MIN); // Current special facility type
    auto scan_table = std::make_shared<int>(SCAN_ACCESS_INFO); // Which table to scan (0=access_info, 1=special_facility)
    auto in_scan = std::make_shared<bool>(false); // true if currently scanning a subscriber
    
    tasks.push_back(
        HotTableScanTask{"tatp_access_info_special_facility_alternating_scan",
                         [current_subscriber, access_scan_state, sf_scan_state, 
                          access_schema, sf_schema, scan_table, in_scan, 
                          start_sub, end_sub](TransactionManager &mgr) {
                           Record *record = nullptr;
                           
                           if (!(*in_scan)) {
                             // Start a new subscriber scan
                             *in_scan = true;
                             switch (*scan_table) {
                               case SCAN_ACCESS_INFO:
                                 // Reset access info scan to beginning of current subscriber
                                 *access_scan_state = AI_TYPE_MIN;
                                 break;
                               case SCAN_SPECIAL_FACILITY:
                                 // Reset special facility scan to beginning of current subscriber
                                 *sf_scan_state = SF_TYPE_MIN;
                                 break;
                             }
                           }
                           
                           switch (*scan_table) {
                             case SCAN_ACCESS_INFO: {
                               // Scan all access info records for ONE subscriber in one transaction
                               int64_t sub = *current_subscriber;
                               int &ai_type = *access_scan_state;
                               
                               // Read current access info record
                               DynamicCompoundKey key =
                                   TATPKeyGenerator::GenerateAccessInfoKey(sub, ai_type, access_schema);
                               if (!mgr.SearchRecord(ACCESS_INFO_TABLE_ID, key, record, READ_ONLY)) {
                                 // If read fails, abort and retry
                                 mgr.AbortTransaction();
                                 *in_scan = false;
                                 return;
                               }
                               
                               // Advance to next access info type
                               ++ai_type;
                               if (ai_type > AI_TYPE_MAX) {
                                 // Finished scanning all access info records for current subscriber
                                 // Now commit
                                 CharArray ret;
                                 if (mgr.CommitTransaction(ret)) {
                                   *in_scan = false;
                                   *scan_table = SCAN_SPECIAL_FACILITY; // Switch to special facility next
                                   // Move to next subscriber (wrap around if needed)
                                   ++(*current_subscriber);
                                   if (*current_subscriber > end_sub) {
                                     *current_subscriber = start_sub;
                                   }
                                   // Break time after scan commit (500us)
                                   auto break_start = std::chrono::high_resolution_clock::now();
                                   while (std::chrono::duration_cast<std::chrono::microseconds>(
                                             std::chrono::high_resolution_clock::now() - break_start)
                                             .count() < 500) {
                                     _mm_pause(); // CPU pause hint for spin loop
                                   }
                                 } else {
                                   // Commit failed, abort and retry
                                   mgr.AbortTransaction();
                                   *in_scan = false;
                                 }
                                 return;
                               }
                               // Continue scanning (don't commit yet)
                               break;
                             }
                             case SCAN_SPECIAL_FACILITY: {
                               // Scan all special facility records for ONE subscriber in one transaction
                               int64_t sub = *current_subscriber;
                               int &sf_type = *sf_scan_state;
                               
                               // Read current special facility record
                               DynamicCompoundKey key =
                                   TATPKeyGenerator::GenerateSpecialFacilityKey(sub, sf_type, sf_schema);
                               if (!mgr.SearchRecord(SPECIAL_FACILITY_TABLE_ID, key, record, READ_ONLY)) {
                                 // If read fails, abort and retry
                                 mgr.AbortTransaction();
                                 *in_scan = false;
                                 return;
                               }
                               
                               // Advance to next special facility type
                               ++sf_type;
                               if (sf_type > SF_TYPE_MAX) {
                                 // Finished scanning all special facility records for current subscriber
                                 // Now commit
                                 CharArray ret;
                                 if (mgr.CommitTransaction(ret)) {
                                   *in_scan = false;
                                   *scan_table = SCAN_ACCESS_INFO; // Switch to access info next
                                   // Move to next subscriber (wrap around if needed)
                                   ++(*current_subscriber);
                                   if (*current_subscriber > end_sub) {
                                     *current_subscriber = start_sub;
                                   }
                                   // Break time after scan commit (500us)
                                   auto break_start = std::chrono::high_resolution_clock::now();
                                   while (std::chrono::duration_cast<std::chrono::microseconds>(
                                             std::chrono::high_resolution_clock::now() - break_start)
                                             .count() < 500) {
                                     _mm_pause(); // CPU pause hint for spin loop
                                   }
                                 } else {
                                   // Commit failed, abort and retry
                                   mgr.AbortTransaction();
                                   *in_scan = false;
                                 }
                                 return;
                               }
                               // Continue scanning (don't commit yet)
                               break;
                             }
                           }
                         }});
  }
};

} // namespace TATPBenchmark
} // namespace DSMEngine

#endif
