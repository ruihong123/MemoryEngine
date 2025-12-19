#ifndef __DATABASE_TATP_RANDOM_GENERATOR_H__
#define __DATABASE_TATP_RANDOM_GENERATOR_H__

#include <cstdint>
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

            // Generate random int64_t in [min, max]
            int64_t GenerateInteger64(int64_t min, int64_t max) {
                std::uniform_int_distribution<int64_t> dis(min, max);
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

            // Generate random subscriber ID using non-uniform distribution (NURand)
            // According to TATP specification: NURand(A, x, y) = (((get_random(0, A) | get_random(x, y)) % (y-x+1)) + x
            // Reference: https://tatpbenchmark.sourceforge.net/TATP_Description.pdf
            int64_t GenerateSubscriberId(int64_t num_subscribers) {
                if (num_subscribers <= 0) {
                    return 0;
                }
                
                // Determine constant A based on subscriber table size (per TATP spec)
                int64_t A;
                if (num_subscribers <= 1000000) {
                    A = 65535;
                } else if (num_subscribers <= 10000000) {
                    A = 1048575;
                } else {
                    A = 2097151;
                }
                
                // Subscriber IDs range from 0 to num_subscribers - 1 (0-based indexing)
                int64_t x = 0;
                int64_t y = num_subscribers - 1;
                
                // Generate two random numbers using bitwise OR as per spec
                int64_t r1 = GenerateInteger64(0, A);
                int64_t r2 = GenerateInteger64(x, y);
                
                // Apply NURand formula: ((r1 | r2) % (y-x+1)) + x
                int64_t result = ((r1 | r2) % (y - x + 1)) + x;
                return result;
            }

        private:
            std::mt19937 gen_;
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
