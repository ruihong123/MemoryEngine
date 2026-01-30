#ifndef __DATABASE_SMALLBANK_EXECUTOR_H__
#define __DATABASE_SMALLBANK_EXECUTOR_H__

#include "SmallBankConstants.h"
#include "SmallBankParams.h"
#include "SmallBankKeyGenerator.h"
#include "SmallBankProcedure.h"
#include "TransactionExecutor.h"
#include <cmath>
#include <atomic>
#include <memory>

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
    registers_[ANALYTICAL_SCAN] = []() { return new AnalyticalScanProcedure(); };

    deregisters_[AMALGAMATE] = [](StoredProcedure *p) { delete p; };
    deregisters_[BALANCE] = [](StoredProcedure *p) { delete p; };
    deregisters_[DEPOSIT_CHECKING] = [](StoredProcedure *p) { delete p; };
    deregisters_[SEND_PAYMENT] = [](StoredProcedure *p) { delete p; };
    deregisters_[TRANSACT_SAVINGS] = [](StoredProcedure *p) { delete p; };
    deregisters_[WRITE_CHECK] = [](StoredProcedure *p) { delete p; };
    deregisters_[ANALYTICAL_SCAN] = [](StoredProcedure *p) { delete p; };
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
    const int64_t global_total_accounts = smallbank_scale_params.num_accounts_; // Global total across all partitions
    if (start_cust < 0 || end_cust < start_cust || global_total_accounts <= 0) {
      return;
    }

    // Calculate number of users to scan per transaction (10% by default)
    // Based on global total, not just local partition
    const int64_t users_per_scan = std::max(static_cast<int64_t>(1), static_cast<int64_t>(std::ceil(global_total_accounts * HOT_SCAN_USER_PERCENTAGE)));

    // Create scan task that scans all data for 10% of users per transaction
    auto savings_it = storage_manager_->tables_.find(SAVINGS_TABLE_ID);
    auto checking_it = storage_manager_->tables_.find(CHECKING_TABLE_ID);
    if (savings_it == storage_manager_->tables_.end() || 
        checking_it == storage_manager_->tables_.end()) {
      return;
    }
    auto savings_schema = savings_it->second->GetPrimaryIndexSchema();
    auto checking_schema = checking_it->second->GetPrimaryIndexSchema();
    
    // State tracking for scanning multiple users per transaction
    auto current_user_start = std::make_shared<int64_t>(start_cust); // Start of current user batch
    auto current_user = std::make_shared<int64_t>(start_cust); // Current user being scanned
    auto scan_table = std::make_shared<int>(0); // 0=savings, 1=checking
    auto in_scan = std::make_shared<bool>(false); // true if currently scanning
    
    tasks.push_back(
        HotTableScanTask("smallbank_multi_user_scan",
                         [current_user_start, current_user, savings_schema, checking_schema, 
                          scan_table, in_scan, start_cust, global_total_accounts, users_per_scan]
                         (TransactionManager &mgr, const std::atomic<bool> &should_run) -> bool {
                           Record *record = nullptr;
                           
                           // Loop until a transaction completes (commits or aborts)
                           while (should_run.load(std::memory_order_acquire)) {
                             if (!(*in_scan)) {
                               // Start a new scan batch
                               *in_scan = true;
                               *current_user = *current_user_start;
                               *scan_table = 0; // Start with savings
                             }
                             
                             // Scan all data for users in current batch
                             switch (*scan_table) {
                               case 0: { // Scan savings
                                 int64_t cust = *current_user;
                                 
                                 // Read savings record
                                 DynamicCompoundKey key = SmallBankKeyGenerator::GenerateSavingsKey(cust, savings_schema);
                                 if (!mgr.SearchRecord(SAVINGS_TABLE_ID, key, record, SCAN_READ)) {
                                   *in_scan = false;
                                   return false; // Transaction aborted
                                 }
#if defined(TO)
                                 Cache::Handle* held_handle = ((Cache::Handle*)record->Get_Handle());
                                 assert(held_handle->gptr != GlobalAddress::Null());
                                 mgr.ReleaseLatchForGCL(held_handle->gptr, held_handle);
#endif
                                 
                                 // Advance to next user
                                 ++cust;
                                 // Check if we've exceeded the batch or global total
                                 int64_t batch_end = *current_user_start + users_per_scan - 1;
                                 if (cust > batch_end || cust >= global_total_accounts) {
                                   // Finished all savings for all users in batch, switch to checking
                                   *scan_table = 1;
                                   *current_user = *current_user_start;
                                 } else {
                                   *current_user = cust;
                                 }
                                 break;
                               }
                               case 1: { // Scan checking
                                 int64_t cust = *current_user;
                                 
                                 // Read checking record
                                 DynamicCompoundKey key = SmallBankKeyGenerator::GenerateCheckingKey(cust, checking_schema);
                                 if (!mgr.SearchRecord(CHECKING_TABLE_ID, key, record, SCAN_READ)) {
                                   *in_scan = false;
                                   return false; // Transaction aborted
                                 }
#if defined(TO)
                                 Cache::Handle* held_handle = ((Cache::Handle*)record->Get_Handle());
                                 assert(held_handle->gptr != GlobalAddress::Null());
                                 mgr.ReleaseLatchForGCL(held_handle->gptr, held_handle);
#endif
                                 
                                 // Advance to next user
                                 ++cust;
                                 // Check if we've exceeded the batch or global total
                                 int64_t batch_end = *current_user_start + users_per_scan - 1;
                                 if (cust > batch_end || cust >= global_total_accounts) {
                                   // Finished scanning all data for all users in batch
                                   // Commit transaction
                                   CharArray ret;
                                   bool committed = mgr.CommitTransaction(ret);
                                   if (committed) {
                                     *in_scan = false;
                                     // Move to next batch of users (round-robin globally)
                                     *current_user_start += users_per_scan;
                                     if (*current_user_start >= global_total_accounts) {
                                       // Wrap around: start from 0 (first account globally)
                                       *current_user_start = 0;
                                     }
                                     return true; // Transaction committed
                                   } else {
                                     *in_scan = false;
                                     return false; // Transaction aborted
                                   }
                                 } else {
                                   *current_user = cust;
                                 }
                                 break;
                               }
                             }
                           }
                           return false; // Should not reach here, but return false if loop exits
                         }));
  }
};

} // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
