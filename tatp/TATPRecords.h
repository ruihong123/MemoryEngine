#ifndef __DATABASE_TATP_RECORDS_H__
#define __DATABASE_TATP_RECORDS_H__

#include <cstdint>
#include <sstream>
#include <string>

namespace DSMEngine {
    namespace TATPBenchmark {

        /******************** records **********************/
        struct SubscriberRecord {
            int64_t s_id_;
            char sub_nbr_[16]; // 15 + 1 for null terminator
            uint8_t bit_[10];
            uint16_t hex_[10];
            uint16_t byte2_[10];
            uint32_t msc_location_;
            uint32_t vlr_location_;
        };

        static std::string SubscriberToString(SubscriberRecord& record) {
            std::stringstream ss;
            ss << record.s_id_ << ";" << record.sub_nbr_ << ";" << record.msc_location_ << ";" << record.vlr_location_;
            return ss.str();
        }

        struct AccessInfoRecord {
            int64_t s_id_;
            uint8_t ai_type_;
            uint8_t data1_;
            uint8_t data2_;
            char data3_[4]; // 3 + 1 for null terminator
            char data4_[6]; // 5 + 1 for null terminator
        };

        static std::string AccessInfoToString(AccessInfoRecord& record) {
            std::stringstream ss;
            ss << record.s_id_ << ";" << (int) record.ai_type_ << ";" << (int) record.data1_ << ";"
               << (int) record.data2_;
            return ss.str();
        }

        struct SpecialFacilityRecord {
            int64_t s_id_;
            uint8_t sf_type_;
            uint8_t is_active_;
            uint8_t error_cntrl_;
            uint8_t data_a_;
            char data_b_[6]; // 5 + 1 for null terminator
        };

        static std::string SpecialFacilityToString(SpecialFacilityRecord& record) {
            std::stringstream ss;
            ss << record.s_id_ << ";" << (int) record.sf_type_ << ";" << (int) record.is_active_;
            return ss.str();
        }

        struct CallForwardingRecord {
            int64_t s_id_;
            uint8_t sf_type_;
            uint8_t start_time_;
            uint8_t end_time_;
            char numberx_[16]; // 15 + 1 for null terminator
        };

        static std::string CallForwardingToString(CallForwardingRecord& record) {
            std::stringstream ss;
            ss << record.s_id_ << ";" << (int) record.sf_type_ << ";" << (int) record.start_time_ << ";"
               << (int) record.end_time_;
            return ss.str();
        }

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
