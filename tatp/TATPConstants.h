#ifndef __DATABASE_TATP_CONSTANTS_H__
#define __DATABASE_TATP_CONSTANTS_H__

#include <string>

namespace DSMEngine {
    namespace TATPBenchmark {

        /********************procedure **********************/
        enum TxnType : size_t {
            GET_SUBSCRIBER_DATA,
            GET_NEW_DESTINATION,
            GET_ACCESS_DATA,
            UPDATE_SUBSCRIBER_DATA,
            UPDATE_LOCATION,
            INSERT_CALL_FORWARDING,
            DELETE_CALL_FORWARDING,
            kTxnTypeCount
        };

        /******************** table **********************/
        enum TableType : size_t {
            SUBSCRIBER_TABLE_ID,
            ACCESS_INFO_TABLE_ID,
            SPECIAL_FACILITY_TABLE_ID,
            CALL_FORWARDING_TABLE_ID,
            kTableCount
        };

        /******************** constants *************************/
        const int DEFAULT_NUM_SUBSCRIBERS = 2000000;

        // Subscriber table
        const int SUB_NBR_PADDING_SIZE    = 15;
        const int SUBSCRIBER_PADDING_SIZE = 5;

        // Access Info table
        const int ACCESS_TYPES_PER_SUBSCRIBER = 4;
        const int AI_TYPE_MIN                 = 1;
        const int AI_TYPE_MAX                 = 4;
        const int DATA1_MIN                   = 1;
        const int DATA1_MAX                   = 255;
        const int DATA2_MIN                   = 1;
        const int DATA2_MAX                   = 255;
        const int DATA3_LENGTH                = 3;
        const int DATA4_LENGTH                = 5;

        // Special Facility and Call Forwarding tables
        const int SF_TYPES_PER_SUBSCRIBER = 3;
        const int SF_TYPE_MIN             = 1;
        const int SF_TYPE_MAX             = 4;
        const int START_TIME_MIN          = 0;
        const int START_TIME_MAX          = 2;
        const int END_TIME_MIN            = 1;
        const int END_TIME_MAX            = 24;
        const int CF_NUMBERX_LENGTH       = 15;

        // Transaction mix percentages (from TATP specification)
        const int GET_SUBSCRIBER_DATA_PERCENT    = 35;
        const int GET_NEW_DESTINATION_PERCENT    = 10;
        const int GET_ACCESS_DATA_PERCENT        = 35;
        const int UPDATE_SUBSCRIBER_DATA_PERCENT = 2;
        const int UPDATE_LOCATION_PERCENT        = 14;
        const int INSERT_CALL_FORWARDING_PERCENT = 2;
        const int DELETE_CALL_FORWARDING_PERCENT = 2;

        // BIT types for subscriber
        const int VLR_LOCATION_MIN = 1;
        const int VLR_LOCATION_MAX = (1 << 31) - 1;

        // Hot table scanner configuration
        // Percentage of users to scan per transaction (10% = 0.1)
        const double HOT_SCAN_USER_PERCENTAGE = 0.001;

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
