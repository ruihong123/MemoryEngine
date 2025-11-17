#ifndef __DATABASE_SMALLBANK_EXECUTOR_H__
#define __DATABASE_SMALLBANK_EXECUTOR_H__

#include "SmallBankConstants.h"
#include "SmallBankParams.h"
#include "SmallBankKeyGenerator.h"
#include "SmallBankProcedure.h"
#include "TransactionExecutor.h"

namespace DSMEngine {
namespace SmallBankBenchmark {

class SmallBankExecutor : public TransactionExecutor {
public:
  SmallBankExecutor(IORedirector *const redirector,
                    TableDirectory *storage_manager, size_t thread_count,
                    bool log_enabled, bool enable_latency_recording = false)
      : TransactionExecutor(redirector, storage_manager, thread_count,
                            log_enabled, enable_latency_recording) {}

  virtual ~SmallBankExecutor() {}

  virtual void PrepareProcedures() {
    registers_[AMALGAMATE] = []() { return new AmalProcedure(); };
    registers_[BALANCE] = []() { return new BalanceProcedure(); };
    registers_[DEPOSIT_CHECKING] = []() {
      return new DepositCheckingProcedure();
    };
    registers_[SEND_PAYMENT] = []() { return new SendPaymentProcedure(); };
    registers_[TRANSACT_SAVINGS] = []() {
      return new TransactSavingsProcedure();
    };
    registers_[WRITE_CHECK] = []() { return new WriteCheckProcedure(); };

    deregisters_[AMALGAMATE] = [](StoredProcedure *p) { delete p; };
    deregisters_[BALANCE] = [](StoredProcedure *p) { delete p; };
    deregisters_[DEPOSIT_CHECKING] = [](StoredProcedure *p) { delete p; };
    deregisters_[SEND_PAYMENT] = [](StoredProcedure *p) { delete p; };
    deregisters_[TRANSACT_SAVINGS] = [](StoredProcedure *p) { delete p; };
    deregisters_[WRITE_CHECK] = [](StoredProcedure *p) { delete p; };
  }

protected:
  virtual void
  ConfigureHotTableScanner(std::vector<HotTableScanTask> &tasks) override {
    using namespace SmallBankBenchmark;
    if (storage_manager_ == nullptr || storage_manager_->tables_.empty()) {
      return;
    }

    const int64_t start_cust = smallbank_scale_params.starting_account_;
    const int64_t end_cust = smallbank_scale_params.ending_account_;
    if (start_cust <= 0 || end_cust < start_cust) {
      return;
    }

    struct AccountCursor {
      int64_t custid;
      int64_t start;
      int64_t end;
      void Advance() {
        ++custid;
        if (custid > end) {
          custid = start;
        }
      }
    };

    auto accounts_it = storage_manager_->tables_.find(ACCOUNTS_TABLE_ID);
    if (accounts_it != storage_manager_->tables_.end()) {
      auto schema = accounts_it->second->GetPrimaryIndexSchema();
      if (schema != nullptr) {
        auto cursor = std::make_shared<AccountCursor>(
            AccountCursor{start_cust, start_cust, end_cust});
        tasks.push_back(HotTableScanTask{
            "smallbank_accounts_scan",
            [cursor, schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key =
                  SmallBankKeyGenerator::GenerateAccountsKey(cursor->custid,
                                                             schema);
              if (!mgr.SearchRecord(ACCOUNTS_TABLE_ID, key, record,
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

    auto savings_it = storage_manager_->tables_.find(SAVINGS_TABLE_ID);
    if (savings_it != storage_manager_->tables_.end()) {
      auto schema = savings_it->second->GetPrimaryIndexSchema();
      if (schema != nullptr) {
        auto cursor = std::make_shared<AccountCursor>(
            AccountCursor{start_cust, start_cust, end_cust});
        tasks.push_back(HotTableScanTask{
            "smallbank_savings_scan",
            [cursor, schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key =
                  SmallBankKeyGenerator::GenerateSavingsKey(cursor->custid,
                                                            schema);
              if (!mgr.SearchRecord(SAVINGS_TABLE_ID, key, record, READ_ONLY)) {
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

    auto checking_it = storage_manager_->tables_.find(CHECKING_TABLE_ID);
    if (checking_it != storage_manager_->tables_.end()) {
      auto schema = checking_it->second->GetPrimaryIndexSchema();
      if (schema != nullptr) {
        auto cursor = std::make_shared<AccountCursor>(
            AccountCursor{start_cust, start_cust, end_cust});
        tasks.push_back(HotTableScanTask{
            "smallbank_checking_scan",
            [cursor, schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key =
                  SmallBankKeyGenerator::GenerateCheckingKey(cursor->custid,
                                                             schema);
              if (!mgr.SearchRecord(CHECKING_TABLE_ID, key, record,
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
  }
};

} // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
