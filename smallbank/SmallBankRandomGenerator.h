#ifndef __DATABASE_SMALLBANK_RANDOM_GENERATOR_H__
#define __DATABASE_SMALLBANK_RANDOM_GENERATOR_H__

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

            // Generate random account ID
            int64_t GenerateAccountId(int num_accounts) {
                return GenerateInteger(0, num_accounts - 1);
            }

        private:
            std::mt19937 gen_;
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
