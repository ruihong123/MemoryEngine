#ifndef __DATABASE_SMALLBANK_RANDOM_GENERATOR_H__
#define __DATABASE_SMALLBANK_RANDOM_GENERATOR_H__

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>

namespace DSMEngine {
    namespace SmallBankBenchmark {

        class SmallBankRandomGenerator {
        public:
            SmallBankRandomGenerator() : gen_(std::random_device{}()) {}

            // Generate random integer in [min, max]
            int GenerateInteger(int min, int max) {
                std::uniform_int_distribution<> dis(min, max);
                return dis(gen_);
            }

            // Generate random int64_t in [min, max]
            int64_t GenerateInteger64(int64_t min, int64_t max) {
                std::uniform_int_distribution<int64_t> dis(min, max);
                return dis(gen_);
            }

            // Generate random double in [min, max]
            double GenerateDouble(double min, double max) {
                std::uniform_real_distribution<> dis(min, max);
                return dis(gen_);
            }

            // Generate random customer name
            void GenerateCustomerName(char* name, int length) {
                static const char alphanum[] = "0123456789"
                                               "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                               "abcdefghijklmnopqrstuvwxyz";

                for (int i = 0; i < length - 1; ++i) {
                    name[i] = alphanum[GenerateInteger(0, sizeof(alphanum) - 2)];
                }
                name[length - 1] = '\0';
            }

            // Generate random account ID with skewed distribution
            // 90% of accesses are within the first 4% of accounts (hot accounts)
            // 10% of accesses are within the remaining 96% of accounts (cold accounts)
            int64_t GenerateAccountId(int64_t num_accounts) {
                if (num_accounts <= 0) {
                    return 0;
                }
                
                // Calculate the hot account range (first 4% of accounts)
                int64_t hot_range_end = static_cast<int64_t>(num_accounts * 0.04);
                if (hot_range_end < 1) {
                    hot_range_end = 1;
                }
                
                // 90% probability: select from hot accounts [0, hot_range_end - 1]
                // 10% probability: select from cold accounts [hot_range_end, num_accounts - 1]
                int rand_percent = GenerateInteger(1, 100);
                
                if (rand_percent <= 90) {
                    // Hot account: within first 4%
                    return GenerateInteger64(0, hot_range_end - 1);
                } else {
                    // Cold account: remaining 96%
                    if (hot_range_end >= num_accounts) {
                        // Edge case: if hot range covers all accounts, just return hot range
                        return GenerateInteger64(0, hot_range_end - 1);
                    }
                    return GenerateInteger64(hot_range_end, num_accounts - 1);
                }
            }

        private:
            std::mt19937 gen_;
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
