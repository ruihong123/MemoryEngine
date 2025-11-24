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
                DynamicCompoundKey savings_key_0 = SmallBankKeyGenerator::GenerateSavingsKey(amal_param->custid_0_);
                Record* savings_record_0         = nullptr;
                DB_QUERY(SearchRecord(SAVINGS_TABLE_ID, savings_key_0, savings_record_0, READ_WRITE));

                DynamicCompoundKey checking_key_0 = SmallBankKeyGenerator::GenerateCheckingKey(amal_param->custid_0_);
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
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                handle = (Cache::Handle*) checking_record_0->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
#endif

                // Update checking for account 1
                DynamicCompoundKey checking_key_1 = SmallBankKeyGenerator::GenerateCheckingKey(amal_param->custid_1_);
                Record* checking_record_1         = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key_1, checking_record_1, READ_WRITE));

                double checking_bal_1 = 0.0;
                checking_record_1->GetColumn(1, &checking_bal_1);
                checking_bal_1 += total;
                checking_record_1->SetColumn(1, &checking_bal_1);

#if defined(TO)
                handle = (Cache::Handle*) checking_record_1->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
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
                DynamicCompoundKey savings_key = SmallBankKeyGenerator::GenerateSavingsKey(bal_param->custid_);
                Record* savings_record         = nullptr;
                DB_QUERY(SearchRecord(SAVINGS_TABLE_ID, savings_key, savings_record, READ_ONLY));

                DynamicCompoundKey checking_key = SmallBankKeyGenerator::GenerateCheckingKey(bal_param->custid_);
                Record* checking_record         = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key, checking_record, READ_ONLY));

                double savings_bal = 0.0, checking_bal = 0.0;
                savings_record->GetColumn(1, &savings_bal);
                checking_record->GetColumn(1, &checking_bal);

                double total = savings_bal + checking_bal;
                ret.Memcpy(ret.size_, (char*) &total, sizeof(double));
                ret.size_ += sizeof(double);

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

                DynamicCompoundKey checking_key = SmallBankKeyGenerator::GenerateCheckingKey(deposit_param->custid_);
                Record* checking_record         = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key, checking_record, READ_WRITE));

                double checking_bal = 0.0;
                checking_record->GetColumn(1, &checking_bal);
                checking_bal += deposit_param->amount_;
                checking_record->SetColumn(1, &checking_bal);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) checking_record->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
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
                DynamicCompoundKey checking_key_0 =
                    SmallBankKeyGenerator::GenerateCheckingKey(payment_param->custid_0_);
                Record* checking_record_0 = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key_0, checking_record_0, READ_WRITE));

                double checking_bal_0 = 0.0;
                checking_record_0->GetColumn(1, &checking_bal_0);
                checking_bal_0 -= payment_param->amount_;
                checking_record_0->SetColumn(1, &checking_bal_0);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) checking_record_0->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
#endif

                // Add to account 1's checking
                DynamicCompoundKey checking_key_1 =
                    SmallBankKeyGenerator::GenerateCheckingKey(payment_param->custid_1_);
                Record* checking_record_1 = nullptr;
                DB_QUERY(SearchRecord(CHECKING_TABLE_ID, checking_key_1, checking_record_1, READ_WRITE));

                double checking_bal_1 = 0.0;
                checking_record_1->GetColumn(1, &checking_bal_1);
                checking_bal_1 += payment_param->amount_;
                checking_record_1->SetColumn(1, &checking_bal_1);

#if defined(TO)
                handle = (Cache::Handle*) checking_record_1->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
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

                DynamicCompoundKey savings_key = SmallBankKeyGenerator::GenerateSavingsKey(transact_param->custid_);
                Record* savings_record         = nullptr;
                DB_QUERY(SearchRecord(SAVINGS_TABLE_ID, savings_key, savings_record, READ_WRITE));

                double savings_bal = 0.0;
                savings_record->GetColumn(1, &savings_bal);
                savings_bal += transact_param->amount_;
                savings_record->SetColumn(1, &savings_bal);

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) savings_record->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
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

                DynamicCompoundKey savings_key = SmallBankKeyGenerator::GenerateSavingsKey(check_param->custid_);
                Record* savings_record         = nullptr;
                DB_QUERY(SearchRecord(SAVINGS_TABLE_ID, savings_key, savings_record, READ_ONLY));

                DynamicCompoundKey checking_key = SmallBankKeyGenerator::GenerateCheckingKey(check_param->custid_);
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
                Cache::Handle* handle = (Cache::Handle*) checking_record->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
