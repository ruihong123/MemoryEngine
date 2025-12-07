#ifndef __TATP_INITIATOR_H__
#define __TATP_INITIATOR_H__

#include "EngineInitiator.h"
#include "Meta.h"
#include "TATPConstants.h"

namespace DSMEngine {
    namespace TATPBenchmark {

        class TATPInitiator : public EngineInitiator {
        public:
            TATPInitiator(const size_t& thread_count, ClusterConfig* config) : EngineInitiator(thread_count, config) {
                printf("Initialize the TATPInitiator\n");
            }

            ~TATPInitiator() {}

        protected:
            virtual void RegisterTables(char* const storage_addr, const std::vector<RecordSchema*>& schemas) override {
                printf("schema table count is %lu\n", schemas.size());
                TableDirectory storage_manager;
                storage_manager.BulkRegisterTables(schemas, default_gallocator);
                storage_manager.Serialize(storage_addr);
            }

            virtual void RegisterSchemas(std::vector<RecordSchema*>& schemas) override {
                schemas.resize(kTableCount, nullptr);
                InitSubscriberSchema(schemas[SUBSCRIBER_TABLE_ID]);
                InitAccessInfoSchema(schemas[ACCESS_INFO_TABLE_ID]);
                InitSpecialFacilitySchema(schemas[SPECIAL_FACILITY_TABLE_ID]);
                InitCallForwardingSchema(schemas[CALL_FORWARDING_TABLE_ID]);
            }

        public:
            static void InitSubscriberSchema(RecordSchema*& schema) {
                std::vector<ColumnInfo*> columns;
                columns.push_back(new ColumnInfo("s_id", ValueType::INT64));
                columns.push_back(new ColumnInfo("sub_nbr", ValueType::FIXCHAR, static_cast<size_t>(16)));

                // bit fields (10 values)
                for (int i = 0; i < 10; ++i) {
                    std::string col_name = "bit_" + std::to_string(i);
                    columns.push_back(new ColumnInfo(col_name.c_str(), ValueType::INT8));
                }

                // hex fields (10 values)
                for (int i = 0; i < 10; ++i) {
                    std::string col_name = "hex_" + std::to_string(i);
                    columns.push_back(new ColumnInfo(col_name.c_str(), ValueType::INT16));
                }

                // byte2 fields (10 values)
                for (int i = 0; i < 10; ++i) {
                    std::string col_name = "byte2_" + std::to_string(i);
                    columns.push_back(new ColumnInfo(col_name.c_str(), ValueType::INT16));
                }

                columns.push_back(new ColumnInfo("msc_location", ValueType::INT32));
                columns.push_back(new ColumnInfo("vlr_location", ValueType::INT32));
                columns.push_back(new ColumnInfo("meta", ValueType::META));

                schema = new RecordSchema(SUBSCRIBER_TABLE_ID);
                schema->BulkloadColumns(columns);
                size_t col_ids[] = {0};
                schema->SetPrimaryColumns(col_ids, 1);
                schema->SetPartitionColumns(col_ids, 1);
                assert(schema->GetColumnSize(32) == 4);

            }

            static void InitAccessInfoSchema(RecordSchema*& schema) {
                std::vector<ColumnInfo*> columns;
                columns.push_back(new ColumnInfo("s_id", ValueType::INT64));
                columns.push_back(new ColumnInfo("ai_type", ValueType::INT8));
                columns.push_back(new ColumnInfo("data1", ValueType::INT8));
                columns.push_back(new ColumnInfo("data2", ValueType::INT8));
                columns.push_back(new ColumnInfo("data3", ValueType::FIXCHAR, static_cast<size_t>(4)));
                columns.push_back(new ColumnInfo("data4", ValueType::FIXCHAR, static_cast<size_t>(6)));
                columns.push_back(new ColumnInfo("meta", ValueType::META));

                schema = new RecordSchema(ACCESS_INFO_TABLE_ID);
                schema->BulkloadColumns(columns);
                size_t col_ids[] = {0, 1}; // s_id, ai_type
                schema->SetPrimaryColumns(col_ids, 2);
                size_t par_col_ids[] = {0};
                schema->SetPartitionColumns(par_col_ids, 1);
            }

            static void InitSpecialFacilitySchema(RecordSchema*& schema) {
                std::vector<ColumnInfo*> columns;
                columns.push_back(new ColumnInfo("s_id", ValueType::INT64));
                columns.push_back(new ColumnInfo("sf_type", ValueType::INT8));
                columns.push_back(new ColumnInfo("is_active", ValueType::INT8));
                columns.push_back(new ColumnInfo("error_cntrl", ValueType::INT8));
                columns.push_back(new ColumnInfo("data_a", ValueType::INT8));
                columns.push_back(new ColumnInfo("data_b", ValueType::FIXCHAR, static_cast<size_t>(6)));
                columns.push_back(new ColumnInfo("meta", ValueType::META));

                schema = new RecordSchema(SPECIAL_FACILITY_TABLE_ID);
                schema->BulkloadColumns(columns);
                size_t col_ids[] = {0, 1}; // s_id, sf_type
                schema->SetPrimaryColumns(col_ids, 2);
                size_t par_col_ids[] = {0};
                schema->SetPartitionColumns(par_col_ids, 1);
            }

            static void InitCallForwardingSchema(RecordSchema*& schema) {
                std::vector<ColumnInfo*> columns;
                columns.push_back(new ColumnInfo("s_id", ValueType::INT64));
                columns.push_back(new ColumnInfo("sf_type", ValueType::INT8));
                columns.push_back(new ColumnInfo("start_time", ValueType::INT8));
                columns.push_back(new ColumnInfo("end_time", ValueType::INT8));
                columns.push_back(new ColumnInfo("numberx", ValueType::FIXCHAR, static_cast<size_t>(16)));
                columns.push_back(new ColumnInfo("meta", ValueType::META));

                schema = new RecordSchema(CALL_FORWARDING_TABLE_ID);
                schema->BulkloadColumns(columns);
                size_t col_ids[] = {0, 1, 2}; // s_id, sf_type, start_time
                schema->SetPrimaryColumns(col_ids, 3);
                size_t par_col_ids[] = {0};
                schema->SetPartitionColumns(par_col_ids, 1);
            }
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
