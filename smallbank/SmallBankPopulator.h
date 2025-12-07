#ifndef __DATABASE_SMALLBANK_POPULATOR_H__
#define __DATABASE_SMALLBANK_POPULATOR_H__

#include "Meta.h"
#include "RecordSchema.h"
#include "SmallBankConstants.h"
#include "SmallBankKeyGenerator.h"
#include "SmallBankParams.h"
#include "SmallBankRandomGenerator.h"
#include "SmallBankRecords.h"
#include "Table.h"
#include "TableDirectory.h"
#include "Common.h"
#include "ColumnInfo.h"
#include <cassert>
#include <iostream>

namespace DSMEngine {
    namespace SmallBankBenchmark {

        class SmallBankPopulator {
        public:
            SmallBankPopulator(TableDirectory* table_directory) : table_directory_(table_directory) {}

            void PopulateDatabase() {
                std::cout << "Populating SmallBank database..." << std::endl;

                PopulateAccounts();
                PopulateSavings();
                PopulateChecking();

                std::cout << "SmallBank database populated successfully." << std::endl;
                ReportDatabaseSize();
            }

            void ReportDatabaseSize() {
                uint64_t total_data_size = 0;
                uint64_t total_index_size = 0;
                uint64_t total_records = 0;
                const double INDEX_FILL_FACTOR = 0.5; // B-tree typically ~50% full

                std::cout << "\n=== SmallBank Database Size Estimation ===" << std::endl;
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
                std::cout << "==========================================\n" << std::endl;
            }

        private:
            void PopulateAccounts() {
                int start = smallbank_scale_params.starting_account_;
                int end   = smallbank_scale_params.ending_account_;
                int total = end - start + 1;
                std::cout << "Populating Accounts table: " << total << " records (A" 
                          << start << "-A" << end << ")..." << std::endl;

                Table* accounts_table = table_directory_->tables_[ACCOUNTS_TABLE_ID];
                RecordSchema* schema  = accounts_table->GetSchema();

                for (int64_t custid = start; custid <= end; ++custid) {
                    if ((custid - start) % 1000000 == 0 && custid > start) {
                        std::cout << "  Accounts: " << (custid - start) << "/" << total << std::endl;
                    }
                    AccountsRecord record;
                    record.custid_ = custid;
                    random_gen_.GenerateCustomerName(record.name_, NAME_LENGTH);

                    RecordSchema* index_schema = accounts_table->GetPrimaryIndexSchema();
                    DynamicCompoundKey key = SmallBankKeyGenerator::GenerateAccountsKey(custid, index_schema);

                    GlobalAddress tuple_addr;
                    Cache::Handle* handle;
                    char* tuple_buffer;
                    accounts_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                    Record record_in_cache = Record(schema, tuple_buffer);
                    record_in_cache.SetColumn(0, &record.custid_);
                    record_in_cache.SetColumn(1, record.name_, NAME_LENGTH);
                    
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

                    accounts_table->InsertPriIndex(key, 1, tuple_addr);

                    default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                }

                std::cout << "Accounts table populated with " << (end - start + 1) << " records." << std::endl;
            }

            void PopulateSavings() {
                int start = smallbank_scale_params.starting_account_;
                int end   = smallbank_scale_params.ending_account_;
                int total = end - start + 1;
                std::cout << "Populating Savings table: " << total << " records..." << std::endl;

                Table* savings_table = table_directory_->tables_[SAVINGS_TABLE_ID];
                RecordSchema* schema = savings_table->GetSchema();

                for (int64_t custid = start; custid <= end; ++custid) {
                    if ((custid - start) % 1000000 == 0 && custid > start) {
                        std::cout << "  Savings: " << (custid - start) << "/" << total << std::endl;
                    }
                    SavingsRecord record;
                    record.custid_ = custid;
                    record.bal_    = random_gen_.GenerateDouble(BALANCE_MIN, BALANCE_MAX);

                    RecordSchema* index_schema = savings_table->GetPrimaryIndexSchema();
                    DynamicCompoundKey key = SmallBankKeyGenerator::GenerateSavingsKey(custid, index_schema);

                    GlobalAddress tuple_addr;
                    Cache::Handle* handle;
                    char* tuple_buffer;
                    savings_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                    Record record_in_cache = Record(schema, tuple_buffer);
                    record_in_cache.SetColumn(0, &record.custid_);
                    record_in_cache.SetColumn(1, &record.bal_);
                    
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

                    savings_table->InsertPriIndex(key, 1, tuple_addr);

                    default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                }

                std::cout << "Savings table populated with " << (end - start + 1) << " records." << std::endl;
            }

            void PopulateChecking() {
                int start = smallbank_scale_params.starting_account_;
                int end   = smallbank_scale_params.ending_account_;
                int total = end - start + 1;
                std::cout << "Populating Checking table: " << total << " records..." << std::endl;

                Table* checking_table = table_directory_->tables_[CHECKING_TABLE_ID];
                RecordSchema* schema  = checking_table->GetSchema();

                for (int64_t custid = start; custid <= end; ++custid) {
                    if ((custid - start) % 1000000 == 0 && custid > start) {
                        std::cout << "  Checking: " << (custid - start) << "/" << total << std::endl;
                    }
                    CheckingRecord record;
                    record.custid_ = custid;
                    record.bal_    = random_gen_.GenerateDouble(BALANCE_MIN, BALANCE_MAX);

                    RecordSchema* index_schema = checking_table->GetPrimaryIndexSchema();
                    DynamicCompoundKey key = SmallBankKeyGenerator::GenerateCheckingKey(custid, index_schema);

                    GlobalAddress tuple_addr;
                    Cache::Handle* handle;
                    char* tuple_buffer;
                    checking_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                    Record record_in_cache = Record(schema, tuple_buffer);
                    record_in_cache.SetColumn(0, &record.custid_);
                    record_in_cache.SetColumn(1, &record.bal_);
                    
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

                    checking_table->InsertPriIndex(key, 1, tuple_addr);

                    default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                }

                std::cout << "Checking table populated with " << (end - start + 1) << " records." << std::endl;
            }

            TableDirectory* table_directory_;
            SmallBankRandomGenerator random_gen_;
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
