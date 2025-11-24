#ifndef __DATABASE_SMALLBANK_EXECUTOR_H__
#define __DATABASE_SMALLBANK_EXECUTOR_H__

#include "SmallBankConstants.h"
#include "SmallBankParams.h"
#include "SmallBankKeyGenerator.h"
#include "SmallBankProcedure.h"
#include "TransactionExecutor.h"
#include <chrono>
#include <xmmintrin.h>

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

    // Create alternating scan between SAVINGS and CHECKING tables
    // Each node scans only its own partition (start_cust to end_cust) to avoid starvation
    // Each transaction scans ONE customer's records before committing (long-running transaction)
    auto savings_it = storage_manager_->tables_.find(SAVINGS_TABLE_ID);
    auto checking_it = storage_manager_->tables_.find(CHECKING_TABLE_ID);
    if (savings_it == storage_manager_->tables_.end() || 
        checking_it == storage_manager_->tables_.end()) {
      return;
    }
    auto savings_schema = savings_it->second->GetPrimaryIndexSchema();
    auto checking_schema = checking_it->second->GetPrimaryIndexSchema();
    
    // State tracking for customer-by-customer scans
    enum ScanTableType {
      SCAN_SAVINGS = 0,
      SCAN_CHECKING = 1
    };
    auto current_customer = std::make_shared<int64_t>(start_cust); // Current customer being scanned
    auto scan_table = std::make_shared<int>(SCAN_SAVINGS); // Which table to scan (0=savings, 1=checking)
    auto in_scan = std::make_shared<bool>(false); // true if currently scanning a customer
    
    tasks.push_back(
        HotTableScanTask{"smallbank_savings_checking_alternating_scan",
                         [current_customer, savings_schema, checking_schema, 
                          scan_table, in_scan, start_cust, end_cust](TransactionManager &mgr) {
                           Record *record = nullptr;
                           
                           if (!(*in_scan)) {
                             // Start a new customer scan
                             *in_scan = true;
                           }
                           
                           switch (*scan_table) {
                             case SCAN_SAVINGS: {
                               // Scan savings record for ONE customer in one transaction
                               int64_t cust = *current_customer;
                               
                               // Read current savings record
                               DynamicCompoundKey key =
                                   SmallBankKeyGenerator::GenerateSavingsKey(cust, savings_schema);
                               if (!mgr.SearchRecord(SAVINGS_TABLE_ID, key, record, READ_ONLY)) {
                                 // If read fails, abort and retry
                                 mgr.AbortTransaction();
                                 *in_scan = false;
                                 return;
                               }
                               
                               // Finished scanning savings record for current customer
                               // Now commit
                               CharArray ret;
                               if (mgr.CommitTransaction(ret)) {
                                 *in_scan = false;
                                 *scan_table = SCAN_CHECKING; // Switch to checking next
                                 // Move to next customer (wrap around if needed)
                                 ++(*current_customer);
                                 if (*current_customer > end_cust) {
                                   *current_customer = start_cust;
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
                             case SCAN_CHECKING: {
                               // Scan checking record for ONE customer in one transaction
                               int64_t cust = *current_customer;
                               
                               // Read current checking record
                               DynamicCompoundKey key =
                                   SmallBankKeyGenerator::GenerateCheckingKey(cust, checking_schema);
                               if (!mgr.SearchRecord(CHECKING_TABLE_ID, key, record, READ_ONLY)) {
                                 // If read fails, abort and retry
                                 mgr.AbortTransaction();
                                 *in_scan = false;
                                 return;
                               }
                               
                               // Finished scanning checking record for current customer
                               // Now commit
                               CharArray ret;
                               if (mgr.CommitTransaction(ret)) {
                                 *in_scan = false;
                                 *scan_table = SCAN_SAVINGS; // Switch to savings next
                                 // Move to next customer (wrap around if needed)
                                 ++(*current_customer);
                                 if (*current_customer > end_cust) {
                                   *current_customer = start_cust;
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
                           }
                         }});
  }
};

} // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
