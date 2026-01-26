#ifndef __DATABASE_SMALLBANK_SOURCE_H__
#define __DATABASE_SMALLBANK_SOURCE_H__

#include "BenchmarkSource.h"
#include "BenchmarkArguments.h"
#include "SmallBankConstants.h"
#include "SmallBankParams.h"
#include "SmallBankRandomGenerator.h"
#include "SmallBankTxnParams.h"
#include <random>
#include <cmath>

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

                // Calculate frequency weights similar to TPCC
                double frequency_weights[7];
                frequency_weights[0] = AMALGAMATE_PERCENT;
                frequency_weights[1] = BALANCE_PERCENT;
                frequency_weights[2] = DEPOSIT_CHECKING_PERCENT;
                frequency_weights[3] = SEND_PAYMENT_PERCENT;
                frequency_weights[4] = TRANSACT_SAVINGS_PERCENT;
                frequency_weights[5] = WRITE_CHECK_PERCENT;
                
                // Calculate analytical scan weight to achieve ~1% of total throughput
                // Standard SmallBank total = 15+15+15+25+15+15 = 100
                double standard_total = AMALGAMATE_PERCENT + BALANCE_PERCENT + DEPOSIT_CHECKING_PERCENT + 
                                       SEND_PAYMENT_PERCENT + TRANSACT_SAVINGS_PERCENT + WRITE_CHECK_PERCENT;
                if (FREQUENCY_ANALYTICAL_SCAN > 0 && standard_total > 0) {
                    // Calculate weight to achieve ~1%: A = 0.01 * (StandardTotal + A) => A = 0.0101 * StandardTotal
                    frequency_weights[6] = std::max(1.0, std::round(standard_total * 0.0101));
                } else {
                    frequency_weights[6] = 0;
                }

                // Normalize to percentages (0-100)
                double total = 0;
                for (size_t i = 0; i < 7; ++i) {
                    total += frequency_weights[i];
                }
                if (total > 0) {
                    for (size_t i = 0; i < 7; ++i) {
                        frequency_weights[i] = frequency_weights[i] * 100.0 / total;
                    }
                    // Create cumulative distribution
                    for (size_t i = 1; i < 7; ++i) {
                        frequency_weights[i] += frequency_weights[i - 1];
                    }
                } else {
                    // Default distribution if all zeros
                    frequency_weights[0] = AMALGAMATE_PERCENT;
                    frequency_weights[1] = AMALGAMATE_PERCENT + BALANCE_PERCENT;
                    frequency_weights[2] = frequency_weights[1] + DEPOSIT_CHECKING_PERCENT;
                    frequency_weights[3] = frequency_weights[2] + SEND_PAYMENT_PERCENT;
                    frequency_weights[4] = frequency_weights[3] + TRANSACT_SAVINGS_PERCENT;
                    frequency_weights[5] = frequency_weights[4] + WRITE_CHECK_PERCENT;
                    frequency_weights[6] = 100.0;
                }

                for (size_t i = 0; i < num_txn_; ++i) {
                    int rand_num    = random_gen_.GenerateInteger(1, 100);
                    TxnParam* param = nullptr;

                    if (rand_num <= frequency_weights[0]) {
                        param = GenerateAmalgamateParam();
                    } else if (rand_num <= frequency_weights[1]) {
                        param = GenerateBalanceParam();
                    } else if (rand_num <= frequency_weights[2]) {
                        param = GenerateDepositCheckingParam();
                    } else if (rand_num <= frequency_weights[3]) {
                        param = GenerateSendPaymentParam();
                    } else if (rand_num <= frequency_weights[4]) {
                        param = GenerateTransactSavingsParam();
                    } else if (rand_num <= frequency_weights[5]) {
                        param = GenerateWriteCheckParam();
                    } else {
                        param = GenerateAnalyticalScanParam();
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
                // Use skewed distribution: 90% of accesses within first 4% of accounts
                return random_gen_.GenerateAccountId(scale_params_->num_accounts_);
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

            AnalyticalScanParam* GenerateAnalyticalScanParam() {
                AnalyticalScanParam* param = new AnalyticalScanParam();
                // Calculate 1% of accounts to scan (similar to hot table scanner)
                int64_t num_accounts_to_scan = std::max(static_cast<int64_t>(1), 
                    static_cast<int64_t>(std::ceil(scale_params_->num_accounts_ * 0.01)));
                
                // Randomly select starting account (ensuring we don't exceed bounds)
                int64_t max_start = scale_params_->num_accounts_ - num_accounts_to_scan;
                if (max_start < 0) {
                    max_start = 0;
                    num_accounts_to_scan = scale_params_->num_accounts_;
                }
                
                param->start_account_ = random_gen_.GenerateInteger64(0, max_start);
                param->num_accounts_to_scan_ = num_accounts_to_scan;
                return param;
            }
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
