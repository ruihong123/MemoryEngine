#ifndef __DATABASE_SMALLBANK_TXN_PARAMS_H__
#define __DATABASE_SMALLBANK_TXN_PARAMS_H__

#include "SmallBankConstants.h"
#include "TxnParam.h"

namespace DSMEngine {
    namespace SmallBankBenchmark {

        class AmalgamateParam : public TxnParam {
        public:
            AmalgamateParam() {
                type_ = AMALGAMATE;
            }
            virtual ~AmalgamateParam() {}

            int64_t custid_0_;
            int64_t custid_1_;
        };

        class BalanceParam : public TxnParam {
        public:
            BalanceParam() {
                type_ = BALANCE;
            }
            virtual ~BalanceParam() {}

            int64_t custid_;
        };

        class DepositCheckingParam : public TxnParam {
        public:
            DepositCheckingParam() {
                type_ = DEPOSIT_CHECKING;
            }
            virtual ~DepositCheckingParam() {}

            int64_t custid_;
            double amount_;
        };

        class SendPaymentParam : public TxnParam {
        public:
            SendPaymentParam() {
                type_ = SEND_PAYMENT;
            }
            virtual ~SendPaymentParam() {}

            int64_t custid_0_;
            int64_t custid_1_;
            double amount_;
        };

        class TransactSavingsParam : public TxnParam {
        public:
            TransactSavingsParam() {
                type_ = TRANSACT_SAVINGS;
            }
            virtual ~TransactSavingsParam() {}

            int64_t custid_;
            double amount_;
        };

        class WriteCheckParam : public TxnParam {
        public:
            WriteCheckParam() {
                type_ = WRITE_CHECK;
            }
            virtual ~WriteCheckParam() {}

            int64_t custid_;
            double amount_;
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
