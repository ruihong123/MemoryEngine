#ifndef __DATABASE_SMALLBANK_PROCEDURE_H__
#define __DATABASE_SMALLBANK_PROCEDURE_H__

#include "SmallBankConstants.h"
#include "SmallBankKeyGenerator.h"
#include "SmallBankTxnParams.h"
#include "StoredProcedure.h"
#include <iostream>

namespace DSMEngine {
    namespace SmallBankBenchmark {

        class AmalProcedure : public StoredProcedure {
        public:
            AmalProcedure() {
                context_.txn_type_ = AMALGAMATE;
            }

            virtual ~AmalProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                AmalgamateParam* amal_param = static_cast<AmalgamateParam*>(param);

                // Read savings and checking for account 0
                RecordSchema* savings_index_schema = transaction_manager_->GetPrimaryIndexSchema(SAVINGS_TABLE_ID);
                DynamicCompoundKey savings_key_0 = SmallBankKeyGenerator::GenerateSavingsKey(amal_param->custid_0_, savings_index_schema);
                Record* savings_record_0         = nullptr;
                DB_QUERY(SearchRecord(SAVINGS_TABLE_ID, savings_key_0, savings_record_0, READ_WRITE));

                RecordSchema* checking_index_schema = transaction_manager_->GetPrimaryIndexSchema(CHECKING_TABLE_ID);
                DynamicCompoundKey checking_key_0 = SmallBankKeyGenerator::GenerateCheckingKey(amal_param->custid_0_, checking_index_schema);
                Record* checking_record_0         = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key_0, checking_record_0, READ_WRITE));

                // Get balances
                double savings_bal = 0.0, checking_bal = 0.0;
                savings_record_0->GetColumn(1, &savings_bal);
                checking_record_0->GetColumn(1, &checking_bal);

                double total = savings_bal + checking_bal;

                // Zero out account 0
                double zero = 0.0;
                savings_record_0->SetColumn(1, &zero);
                checking_record_0->SetColumn(1, &zero);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) savings_record_0->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
                handle = (Cache::Handle*) checking_record_0->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                // Update checking for account 1
                RecordSchema* checking_index_schema_1 = transaction_manager_->GetPrimaryIndexSchema(CHECKING_TABLE_ID);
                DynamicCompoundKey checking_key_1 = SmallBankKeyGenerator::GenerateCheckingKey(amal_param->custid_1_, checking_index_schema_1);
                Record* checking_record_1         = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key_1, checking_record_1, READ_WRITE));

                double checking_bal_1 = 0.0;
                checking_record_1->GetColumn(1, &checking_bal_1);
                checking_bal_1 += total;
                checking_record_1->SetColumn(1, &checking_bal_1);

#if defined(TO)
                handle = (Cache::Handle*) checking_record_1->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class BalanceProcedure : public StoredProcedure {
        public:
            BalanceProcedure() {
                context_.txn_type_ = BALANCE;
            }

            virtual ~BalanceProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                BalanceParam* bal_param = static_cast<BalanceParam*>(param);

                // Read savings and checking
                RecordSchema* savings_index_schema = transaction_manager_->GetPrimaryIndexSchema(SAVINGS_TABLE_ID);
                DynamicCompoundKey savings_key = SmallBankKeyGenerator::GenerateSavingsKey(bal_param->custid_, savings_index_schema);
                Record* savings_record         = nullptr;
                DB_QUERY(SearchRecord(SAVINGS_TABLE_ID, savings_key, savings_record, READ_ONLY));

                RecordSchema* checking_index_schema = transaction_manager_->GetPrimaryIndexSchema(CHECKING_TABLE_ID);
                DynamicCompoundKey checking_key = SmallBankKeyGenerator::GenerateCheckingKey(bal_param->custid_, checking_index_schema);
                Record* checking_record         = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key, checking_record, READ_ONLY));

                double savings_bal = 0.0, checking_bal = 0.0;
                savings_record->GetColumn(1, &savings_bal);
                checking_record->GetColumn(1, &checking_bal);

                double total = savings_bal + checking_bal;
                ret.Memcpy(ret.size_, (char*) &total, sizeof(double));
                ret.size_ += sizeof(double);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) savings_record->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
                handle = (Cache::Handle*) checking_record->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class DepositCheckingProcedure : public StoredProcedure {
        public:
            DepositCheckingProcedure() {
                context_.txn_type_ = DEPOSIT_CHECKING;
            }

            virtual ~DepositCheckingProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                DepositCheckingParam* deposit_param = static_cast<DepositCheckingParam*>(param);

                RecordSchema* index_schema = transaction_manager_->GetPrimaryIndexSchema(CHECKING_TABLE_ID);
                DynamicCompoundKey checking_key = SmallBankKeyGenerator::GenerateCheckingKey(deposit_param->custid_, index_schema);
                Record* checking_record         = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key, checking_record, READ_WRITE));

                double checking_bal = 0.0;
                checking_record->GetColumn(1, &checking_bal);
                checking_bal += deposit_param->amount_;
                checking_record->SetColumn(1, &checking_bal);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) checking_record->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class SendPaymentProcedure : public StoredProcedure {
        public:
            SendPaymentProcedure() {
                context_.txn_type_ = SEND_PAYMENT;
            }

            virtual ~SendPaymentProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                SendPaymentParam* payment_param = static_cast<SendPaymentParam*>(param);

                // Deduct from account 0's checking
                RecordSchema* checking_index_schema = transaction_manager_->GetPrimaryIndexSchema(CHECKING_TABLE_ID);
                DynamicCompoundKey checking_key_0 =
                    SmallBankKeyGenerator::GenerateCheckingKey(payment_param->custid_0_, checking_index_schema);
                Record* checking_record_0 = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key_0, checking_record_0, READ_WRITE));

                double checking_bal_0 = 0.0;
                checking_record_0->GetColumn(1, &checking_bal_0);
                checking_bal_0 -= payment_param->amount_;
                checking_record_0->SetColumn(1, &checking_bal_0);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) checking_record_0->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                // Add to account 1's checking
                DynamicCompoundKey checking_key_1 =
                    SmallBankKeyGenerator::GenerateCheckingKey(payment_param->custid_1_, checking_index_schema);
                Record* checking_record_1 = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key_1, checking_record_1, READ_WRITE));

                double checking_bal_1 = 0.0;
                checking_record_1->GetColumn(1, &checking_bal_1);
                checking_bal_1 += payment_param->amount_;
                checking_record_1->SetColumn(1, &checking_bal_1);

#if defined(TO)
                handle = (Cache::Handle*) checking_record_1->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class TransactSavingsProcedure : public StoredProcedure {
        public:
            TransactSavingsProcedure() {
                context_.txn_type_ = TRANSACT_SAVINGS;
            }

            virtual ~TransactSavingsProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                TransactSavingsParam* transact_param = static_cast<TransactSavingsParam*>(param);

                RecordSchema* index_schema = transaction_manager_->GetPrimaryIndexSchema(SAVINGS_TABLE_ID);
                DynamicCompoundKey savings_key = SmallBankKeyGenerator::GenerateSavingsKey(transact_param->custid_, index_schema);
                Record* savings_record         = nullptr;
                DB_QUERY(SearchRecord(SAVINGS_TABLE_ID, savings_key, savings_record, READ_WRITE));

                double savings_bal = 0.0;
                savings_record->GetColumn(1, &savings_bal);
                savings_bal += transact_param->amount_;
                savings_record->SetColumn(1, &savings_bal);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) savings_record->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class WriteCheckProcedure : public StoredProcedure {
        public:
            WriteCheckProcedure() {
                context_.txn_type_ = WRITE_CHECK;
            }

            virtual ~WriteCheckProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                WriteCheckParam* check_param = static_cast<WriteCheckParam*>(param);

                RecordSchema* savings_index_schema = transaction_manager_->GetPrimaryIndexSchema(SAVINGS_TABLE_ID);
                DynamicCompoundKey savings_key = SmallBankKeyGenerator::GenerateSavingsKey(check_param->custid_, savings_index_schema);
                Record* savings_record         = nullptr;
                DB_QUERY(SearchRecord(SAVINGS_TABLE_ID, savings_key, savings_record, READ_ONLY));

                RecordSchema* checking_index_schema = transaction_manager_->GetPrimaryIndexSchema(CHECKING_TABLE_ID);
                DynamicCompoundKey checking_key = SmallBankKeyGenerator::GenerateCheckingKey(check_param->custid_, checking_index_schema);
                Record* checking_record         = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key, checking_record, READ_WRITE));

                double savings_bal = 0.0, checking_bal = 0.0;
                savings_record->GetColumn(1, &savings_bal);
                checking_record->GetColumn(1, &checking_bal);

                double total = savings_bal + checking_bal;
                if (total < check_param->amount_) {
                    checking_bal -= (check_param->amount_ + 1.0); // overdraft penalty
                } else {
                    checking_bal -= check_param->amount_;
                }
                checking_record->SetColumn(1, &checking_bal);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) savings_record->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
                handle = (Cache::Handle*) checking_record->Get_Handle();
                if (handle) {
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class AnalyticalScanProcedure : public StoredProcedure {
        public:
            AnalyticalScanProcedure() {
                context_.txn_type_ = ANALYTICAL_SCAN;
            }

            virtual ~AnalyticalScanProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                AnalyticalScanParam* scan_param = static_cast<AnalyticalScanParam*>(param);

                RecordSchema* savings_index_schema = transaction_manager_->GetPrimaryIndexSchema(SAVINGS_TABLE_ID);
                RecordSchema* checking_index_schema = transaction_manager_->GetPrimaryIndexSchema(CHECKING_TABLE_ID);

                // Scan the specified range of accounts using iterators
                DynamicCompoundKey savings_start_key = SmallBankKeyGenerator::GenerateSavingsKey(scan_param->start_account_, savings_index_schema);
                DynamicCompoundKey checking_start_key = SmallBankKeyGenerator::GenerateCheckingKey(scan_param->start_account_, checking_index_schema);
                
                auto savings_iter = transaction_manager_->CreateScanIteratorFromKey(SAVINGS_TABLE_ID, savings_start_key);
                auto checking_iter = transaction_manager_->CreateScanIteratorFromKey(CHECKING_TABLE_ID, checking_start_key);
                
                int64_t accounts_scanned = 0;
                
                // Scan savings accounts using iterator
                if (savings_iter) {
                    char savings_key_buf[64];
                    char savings_value_buf[64];
                    DynamicCompoundKey savings_key(savings_key_buf, savings_index_schema);
                    GlobalAddress gaddr;
                    
                    while (savings_iter->Valid() && accounts_scanned < scan_param->num_accounts_to_scan_) {
                        if (savings_iter->GetNext(savings_key, savings_value_buf, gaddr)) {
                            Record* savings_record = nullptr;
                            // Read savings record directly using GlobalAddress from iterator
                            DB_QUERY(ReadRecordByAddress(SAVINGS_TABLE_ID, savings_record, gaddr, SCAN_READ));
                            
                            if (savings_record) {
                                double savings_bal = 0.0;
                                savings_record->GetColumn(1, &savings_bal);
#if defined(TO)
                                Cache::Handle* handle = (Cache::Handle*) savings_record->Get_Handle();
                                if (handle) {
                                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                                }
#endif
                                // Clean up the local record after use to avoid heap explosion
                                // The global record is already deleted in SelectRecordCC
                                transaction_manager_->CleanupLastScanReadRecord();
                                savings_record = nullptr;
                            }
                            accounts_scanned++;
                        }
                        savings_iter->Next();
                    }
                }
                
                // Scan checking accounts using iterator
                accounts_scanned = 0;
                if (checking_iter) {
                    char checking_key_buf[64];
                    char checking_value_buf[64];
                    DynamicCompoundKey checking_key(checking_key_buf, checking_index_schema);
                    GlobalAddress gaddr;
                    
                    while (checking_iter->Valid() && accounts_scanned < scan_param->num_accounts_to_scan_) {
                        if (checking_iter->GetNext(checking_key, checking_value_buf, gaddr)) {
                            Record* checking_record = nullptr;
                            // Read checking record directly using GlobalAddress from iterator
                            DB_QUERY(ReadRecordByAddress(CHECKING_TABLE_ID, checking_record, gaddr, SCAN_READ));
                            
                            if (checking_record) {
                                double checking_bal = 0.0;
                                checking_record->GetColumn(1, &checking_bal);
#if defined(TO)
                                Cache::Handle* handle = (Cache::Handle*) checking_record->Get_Handle();
                                if (handle) {
                                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                                }
#endif
                                // Clean up the local record after use to avoid heap explosion
                                // The global record is already deleted in SelectRecordCC
                                transaction_manager_->CleanupLastScanReadRecord();
                                checking_record = nullptr;
                            }
                            accounts_scanned++;
                        }
                        checking_iter->Next();
                    }
                }
                // printf("Analytical Scan: Node %d, Thread %zu, Accounts scanned: %ld\n", 
                //        default_gallocator->GetID(), thread_id_, accounts_scanned);
                // fflush(stdout);

                return transaction_manager_->CommitTransaction(ret);
            }
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
