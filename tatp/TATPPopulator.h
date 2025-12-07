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
#include "Common.h"
#include "ColumnInfo.h"
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
                ReportDatabaseSize();
            }

            void ReportDatabaseSize() {
                uint64_t total_data_size = 0;
                uint64_t total_index_size = 0;
                uint64_t total_records = 0;
                const double INDEX_FILL_FACTOR = 0.5; // B-tree typically ~50% full

                std::cout << "\n=== TATP Database Size Estimation ===" << std::endl;
                for (size_t i = 0; i < table_directory_->GetTableCount(); ++i) {
                    Table* table = table_directory_->tables_[i];
                    if (table == nullptr) continue;

                    uint64_t record_count = table->GetRecordCount();
                    uint64_t record_size = table->GetSchema()->GetRecordTotalSize();
                    uint64_t index_entry_size = table->GetPrimaryIndexSchema()->GetRecordTotalSize();
                    
                    uint64_t data_size = record_count * record_size;
                    // Account for B-tree fill factor (~50%): actual space = theoretical space / fill_factor
                    uint64_t index_size = (uint64_t)((record_count * index_entry_size) / INDEX_FILL_FACTOR);

                    total_data_size += data_size;
                    total_index_size += index_size;
                    total_records += record_count;

                    std::cout << "Table " << i << " (" << table->GetTableName() << "): "
                              << record_count << " records, "
                              << "Data: " << data_size * 1.0 / 1024 / 1024 << " MB, "
                              << "Index: " << index_size * 1.0 / 1024 / 1024 << " MB" << std::endl;
                }

                std::cout << "Total: " << total_records << " records, "
                          << "Data: " << total_data_size * 1.0 / 1024 / 1024 << " MB, "
                          << "Index: " << total_index_size * 1.0 / 1024 / 1024 << " MB, "
                          << "Total: " << (total_data_size + total_index_size) * 1.0 / 1024 / 1024 << " MB" << std::endl;
                std::cout << "=====================================\n" << std::endl;
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
                assert(schema->GetColumnSize(32) == 4);
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

                    RecordSchema* index_schema = subscriber_table->GetPrimaryIndexSchema();
                    DynamicCompoundKey key = TATPKeyGenerator::GenerateSubscriberKey(s_id, index_schema);

                    GlobalAddress tuple_addr;
                    Cache::Handle* handle;
                    char* tuple_buffer;
                    subscriber_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                    Record record_in_cache = Record(schema, tuple_buffer);
                    record_in_cache.SetColumn(0, &record.s_id_);
                    record_in_cache.SetColumn(1, record.sub_nbr_, schema->GetColumnSize(1));
                    for (int i = 0; i < 10; ++i) {
                        record_in_cache.SetColumn(2 + i, &record.bit_[i]);
                    }
                    for (int i = 0; i < 10; ++i) {
                        record_in_cache.SetColumn(12 + i, &record.hex_[i]);
                    }
                    for (int i = 0; i < 10; ++i) {
                        record_in_cache.SetColumn(22 + i, &record.byte2_[i]);
                    }
                    record_in_cache.SetColumn(32, &record.msc_location_);
                    record_in_cache.SetColumn(33, &record.vlr_location_);
                    
                    // Initialize metadata fields
                    MetaColumn meta;
#if defined(TO)
                    meta.Rts_ = 0;
#endif
#if defined(TO) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
                    meta.Wts_ = 0;
#endif
#if defined(MVOCC)
                    meta.prev_version_ = GlobalAddress::Null();
                    meta.prev_delta_epoch_ = 0;
                    meta.prev_delta_data_size_ = 0;
#endif
                    meta.is_visible_ = true;
#if defined(TO) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
                    record_in_cache.PutMeta(meta);
#endif

                    subscriber_table->InsertPriIndex(key, 1, tuple_addr);

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

                        RecordSchema* index_schema = access_info_table->GetPrimaryIndexSchema();
                        DynamicCompoundKey key = TATPKeyGenerator::GenerateAccessInfoKey(s_id, ai_type, index_schema);

                        GlobalAddress tuple_addr;
                        Cache::Handle* handle;
                        char* tuple_buffer;
                        access_info_table->AllocateNewTuple(
                            tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                        Record record_in_cache = Record(schema, tuple_buffer);
                        record_in_cache.SetColumn(0, &record.s_id_);
                        record_in_cache.SetColumn(1, &record.ai_type_);
                        record_in_cache.SetColumn(2, &record.data1_);
                        record_in_cache.SetColumn(3, &record.data2_);
                        record_in_cache.SetColumn(4, record.data3_, 4);
                        record_in_cache.SetColumn(5, record.data4_, 6);
                        
                        // Initialize metadata fields
                        MetaColumn meta;
#if defined(TO)
                        meta.Rts_ = 0;
#endif
#if defined(TO) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
                        meta.Wts_ = 0;
#endif
#if defined(MVOCC)
                        meta.prev_version_ = GlobalAddress::Null();
                        meta.prev_delta_epoch_ = 0;
                        meta.prev_delta_data_size_ = 0;
#endif
                        meta.is_visible_ = true;
#if defined(TO) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
                        record_in_cache.PutMeta(meta);
#endif

                        access_info_table->InsertPriIndex(key, 1, tuple_addr);

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

                        RecordSchema* index_schema = sf_table->GetPrimaryIndexSchema();
                        DynamicCompoundKey key = TATPKeyGenerator::GenerateSpecialFacilityKey(s_id, sf_type, index_schema);

                        GlobalAddress tuple_addr;
                        Cache::Handle* handle;
                        char* tuple_buffer;
                        sf_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                        Record record_in_cache = Record(schema, tuple_buffer);
                        record_in_cache.SetColumn(0, &record.s_id_);
                        record_in_cache.SetColumn(1, &record.sf_type_);
                        record_in_cache.SetColumn(2, &record.is_active_);
                        record_in_cache.SetColumn(3, &record.error_cntrl_);
                        record_in_cache.SetColumn(4, &record.data_a_);
                        record_in_cache.SetColumn(5, record.data_b_, 6);
                        
                        // Initialize metadata fields
                        MetaColumn meta;
#if defined(TO)
                        meta.Rts_ = 0;
#endif
#if defined(TO) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
                        meta.Wts_ = 0;
#endif
#if defined(MVOCC)
                        meta.prev_version_ = GlobalAddress::Null();
                        meta.prev_delta_epoch_ = 0;
                        meta.prev_delta_data_size_ = 0;
#endif
                        meta.is_visible_ = true;
#if defined(TO) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
                        record_in_cache.PutMeta(meta);
#endif

                        sf_table->InsertPriIndex(key, 1, tuple_addr);

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

                            RecordSchema* index_schema = cf_table->GetPrimaryIndexSchema();
                            DynamicCompoundKey key =
                                TATPKeyGenerator::GenerateCallForwardingKey(s_id, sf_type, start_time, index_schema);

                            GlobalAddress tuple_addr;
                            Cache::Handle* handle;
                            char* tuple_buffer;
                            cf_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                            Record record_in_cache = Record(schema, tuple_buffer);
                            record_in_cache.SetColumn(0, &record.s_id_);
                            record_in_cache.SetColumn(1, &record.sf_type_);
                            record_in_cache.SetColumn(2, &record.start_time_);
                            record_in_cache.SetColumn(3, &record.end_time_);
                            record_in_cache.SetColumn(4, record.numberx_, 16);
                            
                            // Initialize metadata fields
                            MetaColumn meta;
#if defined(TO)
                            meta.Rts_ = 0;
#endif
#if defined(TO) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
                            meta.Wts_ = 0;
#endif
#if defined(MVOCC)
                            meta.prev_version_ = GlobalAddress::Null();
                            meta.prev_delta_epoch_ = 0;
                            meta.prev_delta_data_size_ = 0;
#endif
                            meta.is_visible_ = true;
#if defined(TO) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
                            record_in_cache.PutMeta(meta);
#endif

                            cf_table->InsertPriIndex(key, 1, tuple_addr);

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
