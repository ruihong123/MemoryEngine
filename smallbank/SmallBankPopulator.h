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
            }

        private:
            void PopulateAccounts() {
                std::cout << "Populating Accounts table..." << std::endl;

                Table* accounts_table = table_directory_->tables_[ACCOUNTS_TABLE_ID];
                RecordSchema* schema  = accounts_table->GetSchema();

                int start = smallbank_scale_params.starting_account_;
                int end   = smallbank_scale_params.ending_account_;

                for (int64_t custid = start; custid <= end; ++custid) {
                    AccountsRecord record;
                    record.custid_ = custid;
                    random_gen_.GenerateCustomerName(record.name_, NAME_LENGTH);

                    DynamicCompoundKey key = SmallBankKeyGenerator::GenerateAccountsKey(custid);

                    GlobalAddress tuple_addr;
                    Cache::Handle* handle;
                    char* tuple_buffer;
                    accounts_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                    Record* tuple = new Record(schema, tuple_buffer);
                    memcpy(tuple->data_ptr_, &record, schema->GetRecordTotalSize());
                    tuple->SetVisible(true);

                    accounts_table->InsertPriIndex(key, 1, tuple_addr);
                    delete tuple;

                    default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                }

                std::cout << "Accounts table populated with " << (end - start + 1) << " records." << std::endl;
            }

            void PopulateSavings() {
                std::cout << "Populating Savings table..." << std::endl;

                Table* savings_table = table_directory_->tables_[SAVINGS_TABLE_ID];
                RecordSchema* schema = savings_table->GetSchema();

                int start = smallbank_scale_params.starting_account_;
                int end   = smallbank_scale_params.ending_account_;

                for (int64_t custid = start; custid <= end; ++custid) {
                    SavingsRecord record;
                    record.custid_ = custid;
                    record.bal_    = random_gen_.GenerateDouble(BALANCE_MIN, BALANCE_MAX);

                    DynamicCompoundKey key = SmallBankKeyGenerator::GenerateSavingsKey(custid);

                    GlobalAddress tuple_addr;
                    Cache::Handle* handle;
                    char* tuple_buffer;
                    savings_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                    Record* tuple = new Record(schema, tuple_buffer);
                    memcpy(tuple->data_ptr_, &record, schema->GetRecordTotalSize());
                    tuple->SetVisible(true);

                    savings_table->InsertPriIndex(key, 1, tuple_addr);
                    delete tuple;

                    default_gallocator->SELCC_Exclusive_UnLock(TOPAGE(tuple_addr), handle);
                }

                std::cout << "Savings table populated with " << (end - start + 1) << " records." << std::endl;
            }

            void PopulateChecking() {
                std::cout << "Populating Checking table..." << std::endl;

                Table* checking_table = table_directory_->tables_[CHECKING_TABLE_ID];
                RecordSchema* schema  = checking_table->GetSchema();

                int start = smallbank_scale_params.starting_account_;
                int end   = smallbank_scale_params.ending_account_;

                for (int64_t custid = start; custid <= end; ++custid) {
                    CheckingRecord record;
                    record.custid_ = custid;
                    record.bal_    = random_gen_.GenerateDouble(BALANCE_MIN, BALANCE_MAX);

                    DynamicCompoundKey key = SmallBankKeyGenerator::GenerateCheckingKey(custid);

                    GlobalAddress tuple_addr;
                    Cache::Handle* handle;
                    char* tuple_buffer;
                    checking_table->AllocateNewTuple(tuple_buffer, tuple_addr, handle, default_gallocator, nullptr);

                    Record* tuple = new Record(schema, tuple_buffer);
                    memcpy(tuple->data_ptr_, &record, schema->GetRecordTotalSize());
                    tuple->SetVisible(true);

                    checking_table->InsertPriIndex(key, 1, tuple_addr);
                    delete tuple;

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
