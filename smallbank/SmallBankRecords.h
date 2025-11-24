#ifndef __DATABASE_SMALLBANK_RECORDS_H__
#define __DATABASE_SMALLBANK_RECORDS_H__

#include <sstream>
#include <string>

namespace DSMEngine {
    namespace SmallBankBenchmark {

        /******************** records **********************/
        struct AccountsRecord {
            int64_t custid_;
            char name_[64];
        };

        static std::string AccountsToString(AccountsRecord& account) {
            std::stringstream ss;
            ss << account.custid_ << ";" << account.name_;
            return ss.str();
        }

        struct SavingsRecord {
            int64_t custid_;
            double bal_;
        };

        static std::string SavingsToString(SavingsRecord& savings) {
            std::stringstream ss;
            ss << savings.custid_ << ";" << savings.bal_;
            return ss.str();
        }

        struct CheckingRecord {
            int64_t custid_;
            double bal_;
        };

        static std::string CheckingToString(CheckingRecord& checking) {
            std::stringstream ss;
            ss << checking.custid_ << ";" << checking.bal_;
            return ss.str();
        }

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
