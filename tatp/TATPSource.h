#ifndef __DATABASE_TATP_SOURCE_H__
#define __DATABASE_TATP_SOURCE_H__

#include "BenchmarkSource.h"
#include "TATPConstants.h"
#include "TATPParams.h"
#include "TATPRandomGenerator.h"
#include "TATPTxnParams.h"
#include <random>

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

                for (size_t i = 0; i < num_txn_; ++i) {
                    int rand_num    = random_gen_.GenerateInteger(1, 100);
                    TxnParam* param = nullptr;

                    if (rand_num <= GET_SUBSCRIBER_DATA_PERCENT) {
                        param = GenerateGetSubscriberDataParam();
                    } else if (rand_num <= GET_SUBSCRIBER_DATA_PERCENT + GET_NEW_DESTINATION_PERCENT) {
                        param = GenerateGetNewDestinationParam();
                    } else if (rand_num
                               <= GET_SUBSCRIBER_DATA_PERCENT + GET_NEW_DESTINATION_PERCENT + GET_ACCESS_DATA_PERCENT) {
                        param = GenerateGetAccessDataParam();
                    } else if (rand_num <= GET_SUBSCRIBER_DATA_PERCENT + GET_NEW_DESTINATION_PERCENT
                                               + GET_ACCESS_DATA_PERCENT + UPDATE_SUBSCRIBER_DATA_PERCENT) {
                        param = GenerateUpdateSubscriberDataParam();
                    } else if (rand_num <= GET_SUBSCRIBER_DATA_PERCENT + GET_NEW_DESTINATION_PERCENT
                                               + GET_ACCESS_DATA_PERCENT + UPDATE_SUBSCRIBER_DATA_PERCENT
                                               + UPDATE_LOCATION_PERCENT) {
                        param = GenerateUpdateLocationParam();
                    } else if (rand_num <= GET_SUBSCRIBER_DATA_PERCENT + GET_NEW_DESTINATION_PERCENT
                                               + GET_ACCESS_DATA_PERCENT + UPDATE_SUBSCRIBER_DATA_PERCENT
                                               + UPDATE_LOCATION_PERCENT + INSERT_CALL_FORWARDING_PERCENT) {
                        param = GenerateInsertCallForwardingParam();
                    } else {
                        param = GenerateDeleteCallForwardingParam();
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
                return random_gen_.GenerateInteger(0, scale_params_->num_subscribers_ - 1);
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
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
