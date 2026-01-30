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
                
                // Calculate analytical scan weight to achieve 1/1000 of total throughput
                // Standard SmallBank total = 15000+15000+15000+25000+15000+15000 = 100000
                double standard_total = AMALGAMATE_PERCENT + BALANCE_PERCENT + DEPOSIT_CHECKING_PERCENT + 
                                       SEND_PAYMENT_PERCENT + TRANSACT_SAVINGS_PERCENT + WRITE_CHECK_PERCENT;
                if (FREQUENCY_ANALYTICAL_SCAN > 0 && standard_total > 0) {
                    // Calculate weight to achieve 1/1000: A = 0.001 * (StandardTotal + A) => A ≈ 0.001 * StandardTotal
                    // With standard_total = 100000, this gives A ≈ 100
                    frequency_weights[6] = std::max(1.0, std::round(standard_total * 0.001));
                } else {
                    frequency_weights[6] = 0;
                }

                // Calculate total and create cumulative distribution (not normalized to 100)
                // This allows proper representation of 1/1000 (0.1%) frequencies
                double total = 0;
                for (size_t i = 0; i < 7; ++i) {
                    total += frequency_weights[i];
                }
                if (total > 0) {
                    // Create cumulative distribution using actual frequency weights
                    for (size_t i = 1; i < 7; ++i) {
                        frequency_weights[i] += frequency_weights[i - 1];
                    }
                } else {
                    // Default distribution if all zeros (cumulative frequency weights)
                    // Using original percentage values scaled by 1000: 15, 15, 15, 25, 15, 15
                    frequency_weights[0] = 15000.0;
                    frequency_weights[1] = 30000.0;  // 15000 + 15000
                    frequency_weights[2] = 45000.0;  // 30000 + 15000
                    frequency_weights[3] = 70000.0;  // 45000 + 25000
                    frequency_weights[4] = 85000.0;  // 70000 + 15000
                    frequency_weights[5] = 100000.0; // 85000 + 15000
                    frequency_weights[6] = 100000.0;  // No analytical scan in default
                    total = 100000.0;
                }

                for (size_t i = 0; i < num_txn_; ++i) {
                    // Generate random number in range [1, total] to support frequencies smaller than 1%
                    int rand_num = random_gen_.GenerateInteger(1, static_cast<int>(total));
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
