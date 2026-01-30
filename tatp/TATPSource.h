#ifndef __DATABASE_TATP_SOURCE_H__
#define __DATABASE_TATP_SOURCE_H__

#include "BenchmarkSource.h"
#include "BenchmarkArguments.h"
#include "TATPConstants.h"
#include "TATPParams.h"
#include "TATPRandomGenerator.h"
#include "TATPTxnParams.h"
#include <random>
#include <cmath>

namespace DSMEngine {
    namespace TATPBenchmark {

        class TATPSource : public BenchmarkSource {
        public:
            TATPSource(TATPScaleParams* scale_params, IORedirector* redirector, size_t num_txn, size_t source_type,
                size_t thread_count, size_t dist_ratio = 0)
                : BenchmarkSource(redirector, num_txn, source_type, thread_count, dist_ratio),
                  scale_params_(scale_params) {}

            virtual ~TATPSource() {}

        private:
            TATPScaleParams* scale_params_;
            TATPRandomGenerator random_gen_;

            virtual void StartGeneration() {
                ParamBatch* tuples = new ParamBatch(gParamBatchSize);

                // Calculate frequency weights similar to TPCC
                double frequency_weights[8];
                frequency_weights[0] = GET_SUBSCRIBER_DATA_PERCENT;
                frequency_weights[1] = GET_NEW_DESTINATION_PERCENT;
                frequency_weights[2] = GET_ACCESS_DATA_PERCENT;
                frequency_weights[3] = UPDATE_SUBSCRIBER_DATA_PERCENT;
                frequency_weights[4] = UPDATE_LOCATION_PERCENT;
                frequency_weights[5] = INSERT_CALL_FORWARDING_PERCENT;
                frequency_weights[6] = DELETE_CALL_FORWARDING_PERCENT;
                
                // Calculate analytical scan weight to achieve 1/1000 of total throughput
                // Standard TATP total = 35000+10000+35000+2000+14000+2000+2000 = 100000
                double standard_total = GET_SUBSCRIBER_DATA_PERCENT + GET_NEW_DESTINATION_PERCENT + 
                                       GET_ACCESS_DATA_PERCENT + UPDATE_SUBSCRIBER_DATA_PERCENT + 
                                       UPDATE_LOCATION_PERCENT + INSERT_CALL_FORWARDING_PERCENT + 
                                       DELETE_CALL_FORWARDING_PERCENT;
                if (FREQUENCY_ANALYTICAL_SCAN > 0 && standard_total > 0) {
                    // Calculate weight to achieve 1/1000: A = 0.001 * (StandardTotal + A) => A ≈ 0.001 * StandardTotal
                    // With standard_total = 100000, this gives A ≈ 100
                    frequency_weights[7] = std::max(1.0, std::round(standard_total * 0.001));
                } else {
                    frequency_weights[7] = 0;
                }

                // Calculate total and create cumulative distribution (not normalized to 100)
                // This allows proper representation of 1/1000 (0.1%) frequencies
                double total = 0;
                for (size_t i = 0; i < 8; ++i) {
                    total += frequency_weights[i];
                }
                if (total > 0) {
                    // Create cumulative distribution using actual frequency weights
                    for (size_t i = 1; i < 8; ++i) {
                        frequency_weights[i] += frequency_weights[i - 1];
                    }
                } else {
                    // Default distribution if all zeros (cumulative frequency weights)
                    // Using original percentage values scaled by 1000: 35, 10, 35, 2, 14, 2, 2
                    frequency_weights[0] = 35000.0;
                    frequency_weights[1] = 45000.0;  // 35000 + 10000
                    frequency_weights[2] = 80000.0;   // 45000 + 35000
                    frequency_weights[3] = 82000.0;  // 80000 + 2000
                    frequency_weights[4] = 96000.0;  // 82000 + 14000
                    frequency_weights[5] = 98000.0;  // 96000 + 2000
                    frequency_weights[6] = 100000.0; // 98000 + 2000
                    frequency_weights[7] = 100000.0; // No analytical scan in default
                    total = 100000.0;
                }

                for (size_t i = 0; i < num_txn_; ++i) {
                    // Generate random number in range [1, total] to support frequencies smaller than 1%
                    int rand_num = random_gen_.GenerateInteger(1, static_cast<int>(total));
                    TxnParam* param = nullptr;

                    if (rand_num <= frequency_weights[0]) {
                        param = GenerateGetSubscriberDataParam();
                    } else if (rand_num <= frequency_weights[1]) {
                        param = GenerateGetNewDestinationParam();
                    } else if (rand_num <= frequency_weights[2]) {
                        param = GenerateGetAccessDataParam();
                    } else if (rand_num <= frequency_weights[3]) {
                        param = GenerateUpdateSubscriberDataParam();
                    } else if (rand_num <= frequency_weights[4]) {
                        param = GenerateUpdateLocationParam();
                    } else if (rand_num <= frequency_weights[5]) {
                        param = GenerateInsertCallForwardingParam();
                    } else if (rand_num <= frequency_weights[6]) {
                        param = GenerateDeleteCallForwardingParam();
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

            int64_t GetRandomSubscriberId() {
                // Use non-uniform distribution (NURand) per TATP specification
                return random_gen_.GenerateSubscriberId(scale_params_->num_subscribers_);
            }

            GetSubscriberDataParam* GenerateGetSubscriberDataParam() {
                GetSubscriberDataParam* param = new GetSubscriberDataParam();
                param->s_id_                  = GetRandomSubscriberId();
                return param;
            }

            GetNewDestinationParam* GenerateGetNewDestinationParam() {
                GetNewDestinationParam* param = new GetNewDestinationParam();
                param->s_id_                  = GetRandomSubscriberId();
                param->sf_type_               = random_gen_.GenerateUInt8(SF_TYPE_MIN, SF_TYPE_MAX);
                param->start_time_            = random_gen_.GenerateUInt8(START_TIME_MIN, START_TIME_MAX);
                param->end_time_              = random_gen_.GenerateUInt8(param->start_time_, END_TIME_MAX);
                return param;
            }

            GetAccessDataParam* GenerateGetAccessDataParam() {
                GetAccessDataParam* param = new GetAccessDataParam();
                param->s_id_              = GetRandomSubscriberId();
                param->ai_type_           = random_gen_.GenerateUInt8(AI_TYPE_MIN, AI_TYPE_MAX);
                return param;
            }

            UpdateSubscriberDataParam* GenerateUpdateSubscriberDataParam() {
                UpdateSubscriberDataParam* param = new UpdateSubscriberDataParam();
                param->s_id_                     = GetRandomSubscriberId();
                param->bit_1_                    = random_gen_.GenerateUInt8(0, 1);
                param->sf_type_                  = random_gen_.GenerateUInt8(SF_TYPE_MIN, SF_TYPE_MAX);
                param->data_a_                   = random_gen_.GenerateUInt8(0, 255);
                return param;
            }

            UpdateLocationParam* GenerateUpdateLocationParam() {
                UpdateLocationParam* param = new UpdateLocationParam();
                param->s_id_               = GetRandomSubscriberId();
                param->vlr_location_       = random_gen_.GenerateUInt32(VLR_LOCATION_MIN, VLR_LOCATION_MAX);
                return param;
            }

            InsertCallForwardingParam* GenerateInsertCallForwardingParam() {
                InsertCallForwardingParam* param = new InsertCallForwardingParam();
                param->s_id_                     = GetRandomSubscriberId();
                param->sf_type_                  = random_gen_.GenerateUInt8(SF_TYPE_MIN, SF_TYPE_MAX);
                param->start_time_ = random_gen_.GenerateUInt8(START_TIME_MIN + START_TIME_MAX + 1, END_TIME_MAX);
                param->end_time_   = random_gen_.GenerateUInt8(param->start_time_, END_TIME_MAX);
                random_gen_.GenerateNumberString(param->numberx_, CF_NUMBERX_LENGTH);
                return param;
            }

            DeleteCallForwardingParam* GenerateDeleteCallForwardingParam() {
                DeleteCallForwardingParam* param = new DeleteCallForwardingParam();
                param->s_id_                     = GetRandomSubscriberId();
                param->sf_type_                  = random_gen_.GenerateUInt8(SF_TYPE_MIN, SF_TYPE_MAX);
                param->start_time_               = random_gen_.GenerateUInt8(START_TIME_MIN, START_TIME_MAX);
                return param;
            }

            AnalyticalScanParam* GenerateAnalyticalScanParam() {
                AnalyticalScanParam* param = new AnalyticalScanParam();
                // Calculate 1% of subscribers to scan (similar to hot table scanner)
                int64_t num_subscribers_to_scan = std::max(static_cast<int64_t>(1), 
                    static_cast<int64_t>(std::ceil(scale_params_->num_subscribers_ * 0.01)));
                
                // Randomly select starting subscriber (ensuring we don't exceed bounds)
                int64_t max_start = scale_params_->num_subscribers_ - num_subscribers_to_scan;
                if (max_start < 0) {
                    max_start = 0;
                    num_subscribers_to_scan = scale_params_->num_subscribers_;
                }
                
                param->start_subscriber_ = random_gen_.GenerateInteger64(0, max_start);
                param->num_subscribers_to_scan_ = num_subscribers_to_scan;
                return param;
            }
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
