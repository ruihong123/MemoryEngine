#ifndef __DATABASE_SMALLBANK_CONSTANTS_H__
#define __DATABASE_SMALLBANK_CONSTANTS_H__

#include <string>

namespace DSMEngine {
    namespace SmallBankBenchmark {

        /********************procedure **********************/
        enum TxnType : size_t {
            AMALGAMATE,
            BALANCE,
            DEPOSIT_CHECKING,
            SEND_PAYMENT,
            TRANSACT_SAVINGS,
            WRITE_CHECK,
            ANALYTICAL_SCAN,
            kTxnTypeCount
        };

        /******************** table **********************/
        enum TableType : size_t { ACCOUNTS_TABLE_ID, SAVINGS_TABLE_ID, CHECKING_TABLE_ID, kTableCount };

        /******************** constants *************************/
        const int DEFAULT_NUM_ACCOUNTS = 80000000;
        const int BALANCE_MIN          = 10000;
        const int BALANCE_MAX          = 50000;
        const int NAME_LENGTH          = 64;

        // Transaction mix frequency weights (from SmallBank specification)
        // Using frequency weights instead of percentages to avoid overflow when calculating 1/1000
        // Base multiplier: 1000 (sum = 100000 for standard transactions)
        // Note: ANALYTICAL_SCAN_PERCENT is configurable via command line, default is 0
        const int AMALGAMATE_PERCENT       = 15000;
        const int BALANCE_PERCENT          = 15000;
        const int DEPOSIT_CHECKING_PERCENT = 15000;
        const int SEND_PAYMENT_PERCENT     = 25000;
        const int TRANSACT_SAVINGS_PERCENT = 15000;
        const int WRITE_CHECK_PERCENT      = 15000;
        // Default analytical scan percentage (can be overridden via command line)
        const int ANALYTICAL_SCAN_PERCENT_DEFAULT = 0;

        // Transaction amount range
        const int TRANSFER_AMOUNT_MIN = 1;
        const int TRANSFER_AMOUNT_MAX = 1000;

        // Hot table scanner configuration
        // Percentage of users to scan per transaction (10% = 0.1)
        const double HOT_SCAN_USER_PERCENTAGE = 0.001;

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
