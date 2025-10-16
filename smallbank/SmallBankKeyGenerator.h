#ifndef __DATABASE_SMALLBANK_KEY_GENERATOR_H__
#define __DATABASE_SMALLBANK_KEY_GENERATOR_H__

#include "DynamicCompoundKey.h"
#include "SmallBankConstants.h"
#include <cstring>

namespace DSMEngine {
    namespace SmallBankBenchmark {

        class SmallBankKeyGenerator {
        public:
            // Generate key for Accounts table
            static DynamicCompoundKey GenerateAccountsKey(int64_t custid, RecordSchema* index_schema_ptr = nullptr) {
                static thread_local char prim_buffer[sizeof(int64_t)];
                memcpy(prim_buffer, &custid, sizeof(int64_t));
                DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
                return ret;
            }

            // Generate key for Savings table
            static DynamicCompoundKey GenerateSavingsKey(int64_t custid, RecordSchema* index_schema_ptr = nullptr) {
                static thread_local char prim_buffer[sizeof(int64_t)];
                memcpy(prim_buffer, &custid, sizeof(int64_t));
                DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
                return ret;
            }

            // Generate key for Checking table
            static DynamicCompoundKey GenerateCheckingKey(int64_t custid, RecordSchema* index_schema_ptr = nullptr) {
                static thread_local char prim_buffer[sizeof(int64_t)];
                memcpy(prim_buffer, &custid, sizeof(int64_t));
                DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
                return ret;
            }
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
