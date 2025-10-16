#ifndef __DATABASE_TATP_RANDOM_GENERATOR_H__
#define __DATABASE_TATP_RANDOM_GENERATOR_H__

#include <cstdlib>
#include <cstring>
#include <random>
#include <string>

namespace DSMEngine {
    namespace TATPBenchmark {

        class TATPRandomGenerator {
        public:
            TATPRandomGenerator() : gen_(std::random_device{}()) {}

            // Generate random integer in [min, max]
            int GenerateInteger(int min, int max) {
                std::uniform_int_distribution<> dis(min, max);
                return dis(gen_);
            }

            // Generate random uint8_t in [min, max]
            uint8_t GenerateUInt8(int min, int max) {
                return static_cast<uint8_t>(GenerateInteger(min, max));
            }

            // Generate random uint16_t in [min, max]
            uint16_t GenerateUInt16(int min, int max) {
                return static_cast<uint16_t>(GenerateInteger(min, max));
            }

            // Generate random uint32_t in [min, max]
            uint32_t GenerateUInt32(uint32_t min, uint32_t max) {
                std::uniform_int_distribution<uint32_t> dis(min, max);
                return dis(gen_);
            }

            // Generate random string of digits
            void GenerateNumberString(char* str, int length) {
                for (int i = 0; i < length; ++i) {
                    str[i] = '0' + GenerateInteger(0, 9);
                }
                str[length] = '\0';
            }

            // Generate random alphanumeric string
            void GenerateAlphaNumString(char* str, int length) {
                static const char alphanum[] = "0123456789"
                                               "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                               "abcdefghijklmnopqrstuvwxyz";

                for (int i = 0; i < length; ++i) {
                    str[i] = alphanum[GenerateInteger(0, sizeof(alphanum) - 2)];
                }
                str[length] = '\0';
            }

            // Generate random subscriber ID
            int64_t GenerateSubscriberId(int num_subscribers) {
                return GenerateInteger(0, num_subscribers - 1);
            }

        private:
            std::mt19937 gen_;
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
