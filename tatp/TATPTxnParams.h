#ifndef __DATABASE_TATP_TXN_PARAMS_H__
#define __DATABASE_TATP_TXN_PARAMS_H__

#include "TATPConstants.h"
#include "TxnParam.h"
#include <cstdint>

namespace DSMEngine {
    namespace TATPBenchmark {

        class GetSubscriberDataParam : public TxnParam {
        public:
            GetSubscriberDataParam() {
                type_ = GET_SUBSCRIBER_DATA;
            }
            virtual ~GetSubscriberDataParam() {}

            int64_t s_id_;
        };

        class GetNewDestinationParam : public TxnParam {
        public:
            GetNewDestinationParam() {
                type_ = GET_NEW_DESTINATION;
            }
            virtual ~GetNewDestinationParam() {}

            int64_t s_id_;
            uint8_t sf_type_;
            uint8_t start_time_;
            uint8_t end_time_;
        };

        class GetAccessDataParam : public TxnParam {
        public:
            GetAccessDataParam() {
                type_ = GET_ACCESS_DATA;
            }
            virtual ~GetAccessDataParam() {}

            int64_t s_id_;
            uint8_t ai_type_;
        };

        class UpdateSubscriberDataParam : public TxnParam {
        public:
            UpdateSubscriberDataParam() {
                type_ = UPDATE_SUBSCRIBER_DATA;
            }
            virtual ~UpdateSubscriberDataParam() {}

            int64_t s_id_;
            uint8_t bit_1_;
            uint8_t sf_type_;
            uint8_t data_a_;
        };

        class UpdateLocationParam : public TxnParam {
        public:
            UpdateLocationParam() {
                type_ = UPDATE_LOCATION;
            }
            virtual ~UpdateLocationParam() {}

            int64_t s_id_;
            uint32_t vlr_location_;
        };

        class InsertCallForwardingParam : public TxnParam {
        public:
            InsertCallForwardingParam() {
                type_ = INSERT_CALL_FORWARDING;
            }
            virtual ~InsertCallForwardingParam() {}

            int64_t s_id_;
            uint8_t sf_type_;
            uint8_t start_time_;
            uint8_t end_time_;
            char numberx_[16];
        };

        class DeleteCallForwardingParam : public TxnParam {
        public:
            DeleteCallForwardingParam() {
                type_ = DELETE_CALL_FORWARDING;
            }
            virtual ~DeleteCallForwardingParam() {}

            int64_t s_id_;
            uint8_t sf_type_;
            uint8_t start_time_;
        };

        class AnalyticalScanParam : public TxnParam {
        public:
            AnalyticalScanParam() {
                type_ = ANALYTICAL_SCAN;
            }
            virtual ~AnalyticalScanParam() {}

            int64_t start_subscriber_;  // Starting subscriber ID for scan
            int64_t num_subscribers_to_scan_;  // Number of subscribers to scan (1% by default)
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
