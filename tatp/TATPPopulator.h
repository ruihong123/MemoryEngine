#ifndef __DATABASE_TATP_POPULATOR_H__
#define __DATABASE_TATP_POPULATOR_H__

#include "Meta.h"
#include "RecordSchema.h"
#include "TATPConstants.h"
#include "TATPKeyGenerator.h"
#include "TATPParams.h"
#include "TATPRandomGenerator.h"
#include "TATPRecords.h"
#include "Table.h"
#include "TableDirectory.h"
#include <cassert>
#include <iostream>

namespace DSMEngine {
    namespace TATPBenchmark {

        class TATPPopulator {
        public:
            TATPPopulator(TableDirectory* table_directory) : table_directory_(table_directory) {}

            void PopulateDatabase() {
                std::cout << "Populating TATP database..." << std::endl;

                PopulateSubscriber();
                PopulateAccessInfo();
                PopulateSpecialFacility();
                PopulateCallForwarding();

                std::cout << "TATP database populated successfully." << std::endl;
            }

        private:
            void PopulateSubscriber() {
                int start = tatp_scale_params.starting_subscriber_;
                int end   = tatp_scale_params.ending_subscriber_;
                int total = end - start + 1;
                std::cout << "Populating Subscriber table: " << total << " records (S" 
                          << start << "-S" << end << ")..." << std::endl;

                Table* subscriber_table = table_directory_->tables_[SUBSCRIBER_TABLE_ID];
                RecordSchema* schema    = subscriber_table->GetSchema();

                for (int64_t s_id = start; s_id <= end; ++s_id) {
                    if ((s_id - start) % 100000 == 0 && s_id > start) {
                        std::cout << "  Subscribers: " << (s_id - start) << "/" << total << std::endl;
                    }
                    SubscriberRecord record;
                    record.s_id_ = s_id;
                    random_gen_.GenerateNumberString(record.sub_nbr_, SUB_NBR_PADDING_SIZE);

                    for (int i = 0; i < 10; ++i) {
                        record.bit_[i]   = random_gen_.GenerateUInt8(0, 1);
                        record.hex_[i]   = random_gen_.GenerateUInt16(0, 15);
                        record.byte2_[i] = random_gen_.GenerateUInt16(0, 255);
                    }

                    record.msc_location_ = random_gen_.GenerateUInt32(1, (1U << 31) - 1);
                    record.vlr_location_ = random_gen_.GenerateUInt32(VLR_LOCATION_MIN, VLR_LOCATION_MAX);

                    DynamicCompoundKey key = TATPKeyGenerator::GenerateSubscriberKey(s_id);

                    GlobalAddress tuple_addr;
                    Cache::Handle* handle;
                    char* tuple_buffer;
                    subscriber_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                    Record* tuple = new Record(schema, tuple_buffer);
                    memcpy(tuple->data_ptr_, &record, schema->GetRecordTotalSize());
                    tuple->SetVisible(true);

                    subscriber_table->InsertPriIndex(key, 1, tuple_addr);
                    delete tuple;

                    default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                }

                std::cout << "Subscriber table populated with " << (end - start + 1) << " records." << std::endl;
            }

            void PopulateAccessInfo() {
                int start = tatp_scale_params.starting_subscriber_;
                int end   = tatp_scale_params.ending_subscriber_;
                int total_subscribers = end - start + 1;
                int total_records = total_subscribers * ACCESS_TYPES_PER_SUBSCRIBER;
                std::cout << "Populating AccessInfo table: ~" << total_records << " records..." << std::endl;

                Table* access_info_table = table_directory_->tables_[ACCESS_INFO_TABLE_ID];
                RecordSchema* schema     = access_info_table->GetSchema();

                int count = 0;

                for (int64_t s_id = start; s_id <= end; ++s_id) {
                    for (int ai_type = AI_TYPE_MIN; ai_type <= ACCESS_TYPES_PER_SUBSCRIBER; ++ai_type) {
                        AccessInfoRecord record;
                        record.s_id_    = s_id;
                        record.ai_type_ = ai_type;
                        record.data1_   = random_gen_.GenerateUInt8(DATA1_MIN, DATA1_MAX);
                        record.data2_   = random_gen_.GenerateUInt8(DATA2_MIN, DATA2_MAX);
                        random_gen_.GenerateAlphaNumString(record.data3_, DATA3_LENGTH);
                        random_gen_.GenerateAlphaNumString(record.data4_, DATA4_LENGTH);

                        DynamicCompoundKey key = TATPKeyGenerator::GenerateAccessInfoKey(s_id, ai_type);

                        GlobalAddress tuple_addr;
                        Cache::Handle* handle;
                        char* tuple_buffer;
                        access_info_table->AllocateNewTuple(
                            tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                        Record* tuple = new Record(schema, tuple_buffer);
                        memcpy(tuple->data_ptr_, &record, schema->GetRecordTotalSize());
                        tuple->SetVisible(true);

                        access_info_table->InsertPriIndex(key, 1, tuple_addr);
                        delete tuple;

                        default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                        count++;
                    }
                }

                std::cout << "AccessInfo table populated with " << count << " records." << std::endl;
            }

            void PopulateSpecialFacility() {
                int start = tatp_scale_params.starting_subscriber_;
                int end   = tatp_scale_params.ending_subscriber_;
                int total_subscribers = end - start + 1;
                int total_records = total_subscribers * SF_TYPES_PER_SUBSCRIBER;
                std::cout << "Populating SpecialFacility table: ~" << total_records << " records..." << std::endl;

                Table* sf_table      = table_directory_->tables_[SPECIAL_FACILITY_TABLE_ID];
                RecordSchema* schema = sf_table->GetSchema();

                int count = 0;

                for (int64_t s_id = start; s_id <= end; ++s_id) {
                    for (int sf_type = SF_TYPE_MIN; sf_type <= SF_TYPES_PER_SUBSCRIBER; ++sf_type) {
                        SpecialFacilityRecord record;
                        record.s_id_        = s_id;
                        record.sf_type_     = sf_type;
                        record.is_active_   = random_gen_.GenerateUInt8(0, 1);
                        record.error_cntrl_ = random_gen_.GenerateUInt8(0, 255);
                        record.data_a_      = random_gen_.GenerateUInt8(0, 255);
                        random_gen_.GenerateAlphaNumString(record.data_b_, DATA4_LENGTH);

                        DynamicCompoundKey key = TATPKeyGenerator::GenerateSpecialFacilityKey(s_id, sf_type);

                        GlobalAddress tuple_addr;
                        Cache::Handle* handle;
                        char* tuple_buffer;
                        sf_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                        Record* tuple = new Record(schema, tuple_buffer);
                        memcpy(tuple->data_ptr_, &record, schema->GetRecordTotalSize());
                        tuple->SetVisible(true);

                        sf_table->InsertPriIndex(key, 1, tuple_addr);
                        delete tuple;

                        default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                        count++;
                    }
                }

                std::cout << "SpecialFacility table populated with " << count << " records." << std::endl;
            }

            void PopulateCallForwarding() {
                int start = tatp_scale_params.starting_subscriber_;
                int end   = tatp_scale_params.ending_subscriber_;
                int total_subscribers = end - start + 1;
                int total_records = total_subscribers * SF_TYPES_PER_SUBSCRIBER * (START_TIME_MAX - START_TIME_MIN + 1);
                std::cout << "Populating CallForwarding table: ~" << total_records << " records..." << std::endl;

                Table* cf_table      = table_directory_->tables_[CALL_FORWARDING_TABLE_ID];
                RecordSchema* schema = cf_table->GetSchema();

                int count = 0;

                for (int64_t s_id = start; s_id <= end; ++s_id) {
                    for (int sf_type = SF_TYPE_MIN; sf_type <= SF_TYPES_PER_SUBSCRIBER; ++sf_type) {
                        for (int start_time = START_TIME_MIN; start_time <= START_TIME_MAX; ++start_time) {
                            CallForwardingRecord record;
                            record.s_id_       = s_id;
                            record.sf_type_    = sf_type;
                            record.start_time_ = start_time;
                            record.end_time_   = random_gen_.GenerateUInt8(END_TIME_MIN, END_TIME_MAX);
                            random_gen_.GenerateNumberString(record.numberx_, CF_NUMBERX_LENGTH);

                            DynamicCompoundKey key =
                                TATPKeyGenerator::GenerateCallForwardingKey(s_id, sf_type, start_time);

                            GlobalAddress tuple_addr;
                            Cache::Handle* handle;
                            char* tuple_buffer;
                            cf_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                            Record* tuple = new Record(schema, tuple_buffer);
                            memcpy(tuple->data_ptr_, &record, schema->GetRecordTotalSize());
                            tuple->SetVisible(true);

                            cf_table->InsertPriIndex(key, 1, tuple_addr);
                            delete tuple;

                            default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                            count++;
                        }
                    }
                }

                std::cout << "CallForwarding table populated with " << count << " records." << std::endl;
            }

            TableDirectory* table_directory_;
            TATPRandomGenerator random_gen_;
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
