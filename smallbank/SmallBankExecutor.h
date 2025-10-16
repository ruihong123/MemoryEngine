#ifndef __DATABASE_SMALLBANK_EXECUTOR_H__
#define __DATABASE_SMALLBANK_EXECUTOR_H__

#include "SmallBankConstants.h"
#include "SmallBankParams.h"
#include "SmallBankProcedure.h"
#include "TransactionExecutor.h"

namespace DSMEngine {
    namespace SmallBankBenchmark {

        class SmallBankExecutor : public TransactionExecutor {
        public:
            SmallBankExecutor(
                IORedirector* const redirector, TableDirectory* storage_manager, size_t thread_count, bool log_enabled)
                : TransactionExecutor(redirector, storage_manager, thread_count, log_enabled) {}

            virtual ~SmallBankExecutor() {}

            virtual void PrepareProcedures() {
                registers_[AMALGAMATE]       = []() { return new AmalProcedure(); };
                registers_[BALANCE]          = []() { return new BalanceProcedure(); };
                registers_[DEPOSIT_CHECKING] = []() { return new DepositCheckingProcedure(); };
                registers_[SEND_PAYMENT]     = []() { return new SendPaymentProcedure(); };
                registers_[TRANSACT_SAVINGS] = []() { return new TransactSavingsProcedure(); };
                registers_[WRITE_CHECK]      = []() { return new WriteCheckProcedure(); };

                deregisters_[AMALGAMATE]       = [](StoredProcedure* p) { delete p; };
                deregisters_[BALANCE]          = [](StoredProcedure* p) { delete p; };
                deregisters_[DEPOSIT_CHECKING] = [](StoredProcedure* p) { delete p; };
                deregisters_[SEND_PAYMENT]     = [](StoredProcedure* p) { delete p; };
                deregisters_[TRANSACT_SAVINGS] = [](StoredProcedure* p) { delete p; };
                deregisters_[WRITE_CHECK]      = [](StoredProcedure* p) { delete p; };
            }
        };

    } // namespace SmallBankBenchmark
} // namespace DSMEngine

#endif
