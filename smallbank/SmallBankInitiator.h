#ifndef __SMALLBANK_INITIATOR_H__
#define __SMALLBANK_INITIATOR_H__

#include "EngineInitiator.h"
#include "Meta.h"
#include "SmallBankConstants.h"

namespace DSMEngine {
    namespace SmallBankBenchmark {

        class SmallBankInitiator : public EngineInitiator {
        public:
            SmallBankInitiator(const size_t& thread_count, ClusterConfig* config)
                : EngineInitiator(thread_count, config) {
                printf("Initialize the SmallBankInitiator\n");
            }

            ~SmallBankInitiator() {}

        protected:
            virtual void RegisterTables(char* const storage_addr, const std::vector<RecordSchema*>& schemas) override {
                printf("schema table count is %lu\n", schemas.size());
                TableDirectory storage_manager;
                storage_manager.BulkRegisterTables(schemas, default_gallocator);
                storage_manager.Serialize(storage_addr);
            }

            virtual void RegisterSchemas(std::vector<RecordSchema*>& schemas) override {
                schemas.resize(kTableCount, nullptr);
                InitAccountsSchema(schemas[ACCOUNTS_TABLE_ID]);
                InitSavingsSchema(schemas[SAVINGS_TABLE_ID]);
                InitCheckingSchema(schemas[CHECKING_TABLE_ID]);
            }

        public:
            static void InitAccountsSchema(RecordSchema*& schema) {
                std::vector<ColumnInfo*> columns;
                columns.push_back(new ColumnInfo("custid", ValueType::INT64));
                columns.push_back(new ColumnInfo("name", ValueType::FIXCHAR, static_cast<size_t>(64)));
                columns.push_back(new ColumnInfo("meta", ValueType::META));

                schema = new RecordSchema(ACCOUNTS_TABLE_ID);
                schema->BulkloadColumns(columns);
                size_t col_ids[] = {0};
                schema->SetPrimaryColumns(col_ids, 1);
                schema->SetPartitionColumns(col_ids, 1);
            }

            static void InitSavingsSchema(RecordSchema*& schema) {
                std::vector<ColumnInfo*> columns;
                columns.push_back(new ColumnInfo("custid", ValueType::INT64));
                columns.push_back(new ColumnInfo("bal", ValueType::DOUBLE));
                columns.push_back(new ColumnInfo("meta", ValueType::META));

                schema = new RecordSchema(SAVINGS_TABLE_ID);
                schema->BulkloadColumns(columns);
                size_t col_ids[] = {0};
                schema->SetPrimaryColumns(col_ids, 1);
                schema->SetPartitionColumns(col_ids, 1);
            }

            static void InitCheckingSchema(RecordSchema*& schema) {
                std::vector<ColumnInfo*> columns;
                columns.push_back(new ColumnInfo("custid", ValueType::INT64));
                columns.push_back(new ColumnInfo("bal", ValueType::DOUBLE));
                columns.push_back(new ColumnInfo("meta", ValueType::META));

                schema = new RecordSchema(CHECKING_TABLE_ID);
                schema->BulkloadColumns(columns);
                size_t col_ids[] = {0};
                schema->SetPrimaryColumns(col_ids, 1);
                schema->SetPartitionColumns(col_ids, 1);
            }
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
