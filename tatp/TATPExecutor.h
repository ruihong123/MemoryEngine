#ifndef __DATABASE_TATP_EXECUTOR_H__
#define __DATABASE_TATP_EXECUTOR_H__

#include "TATPProcedure.h"
#include "TATPParams.h"
#include "TATPKeyGenerator.h"
#include "TransactionExecutor.h"

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

    struct SubscriberCursor {
      int64_t subscriber;
      int64_t start;
      int64_t end;
      void Advance() {
        ++subscriber;
        if (subscriber > end) {
          subscriber = start;
        }
      }
    };

    auto subscriber_it = storage_manager_->tables_.find(SUBSCRIBER_TABLE_ID);
    if (subscriber_it != storage_manager_->tables_.end()) {
      auto schema = subscriber_it->second->GetPrimaryIndexSchema();
      if (schema != nullptr) {
        auto cursor = std::make_shared<SubscriberCursor>(
            SubscriberCursor{start_sub, start_sub, end_sub});
        tasks.push_back(HotTableScanTask{
            "tatp_subscriber_scan",
            [cursor, schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key =
                  TATPKeyGenerator::GenerateSubscriberKey(cursor->subscriber,
                                                          schema);
              if (!mgr.SearchRecord(SUBSCRIBER_TABLE_ID, key, record,
                                    READ_ONLY)) {
                return;
              }
              CharArray ret;
              if (mgr.CommitTransaction(ret)) {
                cursor->Advance();
              }
            },
            10});
      }
    }

    struct AccessInfoCursor {
      int64_t subscriber;
      int ai_type;
      int64_t start;
      int64_t end;
      int max_type;
      void Advance() {
        ++ai_type;
        if (ai_type > max_type) {
          ai_type = AI_TYPE_MIN;
          ++subscriber;
          if (subscriber > end) {
            subscriber = start;
          }
        }
      }
    };

    auto access_it = storage_manager_->tables_.find(ACCESS_INFO_TABLE_ID);
    if (access_it != storage_manager_->tables_.end()) {
      auto schema = access_it->second->GetPrimaryIndexSchema();
      if (schema != nullptr) {
        auto cursor = std::make_shared<AccessInfoCursor>(
            AccessInfoCursor{start_sub, AI_TYPE_MIN, start_sub, end_sub,
                             ACCESS_TYPES_PER_SUBSCRIBER});
        tasks.push_back(HotTableScanTask{
            "tatp_access_info_scan",
            [cursor, schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key =
                  TATPKeyGenerator::GenerateAccessInfoKey(cursor->subscriber,
                                                          cursor->ai_type,
                                                          schema);
              if (!mgr.SearchRecord(ACCESS_INFO_TABLE_ID, key, record,
                                    READ_ONLY)) {
                return;
              }
              CharArray ret;
              if (mgr.CommitTransaction(ret)) {
                cursor->Advance();
              }
            },
            10});
      }
    }

    struct SpecialFacilityCursor {
      int64_t subscriber;
      int sf_type;
      int64_t start;
      int64_t end;
      int max_type;
      void Advance() {
        ++sf_type;
        if (sf_type > SF_TYPE_MAX) {
          sf_type = SF_TYPE_MIN;
          ++subscriber;
          if (subscriber > end) {
            subscriber = start;
          }
        }
      }
    };

    auto sf_it = storage_manager_->tables_.find(SPECIAL_FACILITY_TABLE_ID);
    if (sf_it != storage_manager_->tables_.end()) {
      auto schema = sf_it->second->GetPrimaryIndexSchema();
      if (schema != nullptr) {
        auto cursor = std::make_shared<SpecialFacilityCursor>(
            SpecialFacilityCursor{start_sub, SF_TYPE_MIN, start_sub, end_sub,
                                  SF_TYPES_PER_SUBSCRIBER});
        tasks.push_back(HotTableScanTask{
            "tatp_special_facility_scan",
            [cursor, schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key =
                  TATPKeyGenerator::GenerateSpecialFacilityKey(
                      cursor->subscriber, cursor->sf_type, schema);
              if (!mgr.SearchRecord(SPECIAL_FACILITY_TABLE_ID, key, record,
                                    READ_ONLY)) {
                return;
              }
              CharArray ret;
              if (mgr.CommitTransaction(ret)) {
                cursor->Advance();
              }
            },
            12});
      }
    }

    struct CallForwardingCursor {
      int64_t subscriber;
      int sf_type;
      int start_time;
      int64_t start;
      int64_t end;
      int max_sf;
      int max_start_time;
      void Advance() {
        ++start_time;
        if (start_time > max_start_time) {
          start_time = START_TIME_MIN;
          ++sf_type;
          if (sf_type > SF_TYPE_MAX) {
            sf_type = SF_TYPE_MIN;
            ++subscriber;
            if (subscriber > end) {
              subscriber = start;
            }
          }
        }
      }
    };

    auto cf_it = storage_manager_->tables_.find(CALL_FORWARDING_TABLE_ID);
    if (cf_it != storage_manager_->tables_.end()) {
      auto schema = cf_it->second->GetPrimaryIndexSchema();
      if (schema != nullptr) {
        auto cursor = std::make_shared<CallForwardingCursor>(
            CallForwardingCursor{start_sub, SF_TYPE_MIN, START_TIME_MIN,
                                 start_sub, end_sub, SF_TYPES_PER_SUBSCRIBER,
                                 START_TIME_MAX});
        tasks.push_back(HotTableScanTask{
            "tatp_call_forwarding_scan",
            [cursor, schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key =
                  TATPKeyGenerator::GenerateCallForwardingKey(
                      cursor->subscriber, cursor->sf_type, cursor->start_time,
                      schema);
              if (!mgr.SearchRecord(CALL_FORWARDING_TABLE_ID, key, record,
                                    READ_ONLY)) {
                return;
              }
              CharArray ret;
              if (mgr.CommitTransaction(ret)) {
                cursor->Advance();
              }
            },
            12});
      }
    }
  }
};

} // namespace TATPBenchmark
} // namespace DSMEngine

#endif
