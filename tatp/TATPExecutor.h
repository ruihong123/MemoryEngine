#ifndef __DATABASE_TATP_EXECUTOR_H__
#define __DATABASE_TATP_EXECUTOR_H__

#include "TATPProcedure.h"
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
};

} // namespace TATPBenchmark
} // namespace DSMEngine

#endif
