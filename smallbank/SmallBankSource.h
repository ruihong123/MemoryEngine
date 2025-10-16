#ifndef __DATABASE_SMALLBANK_SOURCE_H__
#define __DATABASE_SMALLBANK_SOURCE_H__

#include "BenchmarkSource.h"
#include "SmallBankConstants.h"
#include "SmallBankParams.h"
#include "SmallBankRandomGenerator.h"
#include "SmallBankTxnParams.h"
#include <random>

namespace DSMEngine {
    namespace SmallBankBenchmark {

        class SmallBankSource : public BenchmarkSource {
        public:
            SmallBankSource(SmallBankScaleParams* scale_params, IORedirector* redirector, size_t num_txn,
                size_t source_type, size_t thread_count, size_t dist_ratio = 0)
                : BenchmarkSource(redirector, num_txn, source_type, thread_count, dist_ratio),
                  scale_params_(scale_params) {}

            virtual ~SmallBankSource() {}

        private:
            SmallBankScaleParams* scale_params_;
            SmallBankRandomGenerator random_gen_;

            virtual void StartGeneration() {
                ParamBatch* tuples = new ParamBatch(gParamBatchSize);

                for (size_t i = 0; i < num_txn_; ++i) {
                    int rand_num    = random_gen_.GenerateInteger(1, 100);
                    TxnParam* param = nullptr;

                    if (rand_num <= AMALGAMATE_PERCENT) {
                        param = GenerateAmalgamateParam();
                    } else if (rand_num <= AMALGAMATE_PERCENT + BALANCE_PERCENT) {
                        param = GenerateBalanceParam();
                    } else if (rand_num <= AMALGAMATE_PERCENT + BALANCE_PERCENT + DEPOSIT_CHECKING_PERCENT) {
                        param = GenerateDepositCheckingParam();
                    } else if (rand_num <= AMALGAMATE_PERCENT + BALANCE_PERCENT + DEPOSIT_CHECKING_PERCENT
                                               + SEND_PAYMENT_PERCENT) {
                        param = GenerateSendPaymentParam();
                    } else if (rand_num <= AMALGAMATE_PERCENT + BALANCE_PERCENT + DEPOSIT_CHECKING_PERCENT
                                               + SEND_PAYMENT_PERCENT + TRANSACT_SAVINGS_PERCENT) {
                        param = GenerateTransactSavingsParam();
                    } else {
                        param = GenerateWriteCheckParam();
                    }

                    tuples->push_back(param);

                    if ((i + 1) % gParamBatchSize == 0) {
                        redirector_ptr_->PushParameterBatch(tuples);
                        tuples = new ParamBatch(gParamBatchSize);
                    }
                }

                if (tuples->size() != 0) {
                    redirector_ptr_->PushParameterBatch(tuples);
                } else {
                    delete tuples;
                }
            }

            int64_t GetRandomAccountId() {
                return random_gen_.GenerateInteger(0, scale_params_->num_accounts_ - 1);
            }

            AmalgamateParam* GenerateAmalgamateParam() {
                AmalgamateParam* param = new AmalgamateParam();
                param->custid_0_       = GetRandomAccountId();
                param->custid_1_       = GetRandomAccountId();
                while (param->custid_1_ == param->custid_0_) {
                    param->custid_1_ = GetRandomAccountId();
                }
                return param;
            }

            BalanceParam* GenerateBalanceParam() {
                BalanceParam* param = new BalanceParam();
                param->custid_      = GetRandomAccountId();
                return param;
            }

            DepositCheckingParam* GenerateDepositCheckingParam() {
                DepositCheckingParam* param = new DepositCheckingParam();
                param->custid_              = GetRandomAccountId();
                param->amount_              = random_gen_.GenerateDouble(TRANSFER_AMOUNT_MIN, TRANSFER_AMOUNT_MAX);
                return param;
            }

            SendPaymentParam* GenerateSendPaymentParam() {
                SendPaymentParam* param = new SendPaymentParam();
                param->custid_0_        = GetRandomAccountId();
                param->custid_1_        = GetRandomAccountId();
                while (param->custid_1_ == param->custid_0_) {
                    param->custid_1_ = GetRandomAccountId();
                }
                param->amount_ = random_gen_.GenerateDouble(TRANSFER_AMOUNT_MIN, TRANSFER_AMOUNT_MAX);
                return param;
            }

            TransactSavingsParam* GenerateTransactSavingsParam() {
                TransactSavingsParam* param = new TransactSavingsParam();
                param->custid_              = GetRandomAccountId();
                param->amount_              = random_gen_.GenerateDouble(TRANSFER_AMOUNT_MIN, TRANSFER_AMOUNT_MAX);
                return param;
            }

            WriteCheckParam* GenerateWriteCheckParam() {
                WriteCheckParam* param = new WriteCheckParam();
                param->custid_         = GetRandomAccountId();
                param->amount_         = random_gen_.GenerateDouble(TRANSFER_AMOUNT_MIN, TRANSFER_AMOUNT_MAX);
                return param;
            }
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
