#ifndef __DATABASE_TATP_PROCEDURE_H__
#define __DATABASE_TATP_PROCEDURE_H__

#include "StoredProcedure.h"
#include "TATPConstants.h"
#include "TATPKeyGenerator.h"
#include "TATPTxnParams.h"
#include <iostream>

namespace DSMEngine {
    namespace TATPBenchmark {

        class GetSubscriberDataProcedure : public StoredProcedure {
        public:
            GetSubscriberDataProcedure() {
                context_.txn_type_ = GET_SUBSCRIBER_DATA;
            }

            virtual ~GetSubscriberDataProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                GetSubscriberDataParam* gsd_param = static_cast<GetSubscriberDataParam*>(param);

                // Read subscriber record
                DynamicCompoundKey sub_key = TATPKeyGenerator::GenerateSubscriberKey(gsd_param->s_id_);
                Record* sub_record         = nullptr;
                DB_QUERY(SearchRecord(SUBSCRIBER_TABLE_ID, sub_key, sub_record, READ_ONLY));

                // Copy data to return buffer
                if (sub_record) {
                    ret.Memcpy(ret.size_, sub_record->data_ptr_, sub_record->GetRecordSize());
                    ret.size_ += sub_record->GetRecordSize();
                }

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class GetNewDestinationProcedure : public StoredProcedure {
        public:
            GetNewDestinationProcedure() {
                context_.txn_type_ = GET_NEW_DESTINATION;
            }

            virtual ~GetNewDestinationProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                GetNewDestinationParam* gnd_param = static_cast<GetNewDestinationParam*>(param);

                // Read special facility record
                DynamicCompoundKey sf_key =
                    TATPKeyGenerator::GenerateSpecialFacilityKey(gnd_param->s_id_, gnd_param->sf_type_);
                Record* sf_record = nullptr;
                DB_QUERY(SearchRecord(SPECIAL_FACILITY_TABLE_ID, sf_key, sf_record, READ_ONLY));

                // Read call forwarding records for start_time range
                for (uint8_t start_time = gnd_param->start_time_; start_time <= gnd_param->end_time_; ++start_time) {
                    DynamicCompoundKey cf_key =
                        TATPKeyGenerator::GenerateCallForwardingKey(gnd_param->s_id_, gnd_param->sf_type_, start_time);
                    Record* cf_record = nullptr;
                    DB_QUERY(SearchRecord(CALL_FORWARDING_TABLE_ID, cf_key, cf_record, READ_ONLY));

                    if (cf_record) {
                        ret.Memcpy(ret.size_, cf_record->data_ptr_, cf_record->GetRecordSize());
                        ret.size_ += cf_record->GetRecordSize();
                    }
                }

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class GetAccessDataProcedure : public StoredProcedure {
        public:
            GetAccessDataProcedure() {
                context_.txn_type_ = GET_ACCESS_DATA;
            }

            virtual ~GetAccessDataProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                GetAccessDataParam* gad_param = static_cast<GetAccessDataParam*>(param);

                // Read access info record
                DynamicCompoundKey ai_key =
                    TATPKeyGenerator::GenerateAccessInfoKey(gad_param->s_id_, gad_param->ai_type_);
                Record* ai_record = nullptr;
                DB_QUERY(SearchRecord(ACCESS_INFO_TABLE_ID, ai_key, ai_record, READ_ONLY));

                if (ai_record) {
                    uint8_t data1, data2;
                    ai_record->GetColumn(2, &data1);
                    ai_record->GetColumn(3, &data2);

                    ret.Memcpy(ret.size_, (char*) &data1, sizeof(uint8_t));
                    ret.size_ += sizeof(uint8_t);
                    ret.Memcpy(ret.size_, (char*) &data2, sizeof(uint8_t));
                    ret.size_ += sizeof(uint8_t);
                }

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class UpdateSubscriberDataProcedure : public StoredProcedure {
        public:
            UpdateSubscriberDataProcedure() {
                context_.txn_type_ = UPDATE_SUBSCRIBER_DATA;
            }

            virtual ~UpdateSubscriberDataProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                UpdateSubscriberDataParam* usd_param = static_cast<UpdateSubscriberDataParam*>(param);

                // Update subscriber bit_1
                DynamicCompoundKey sub_key = TATPKeyGenerator::GenerateSubscriberKey(usd_param->s_id_);
                Record* sub_record         = nullptr;
                DB_QUERY(SearchRecord(SUBSCRIBER_TABLE_ID, sub_key, sub_record, READ_WRITE));

                if (sub_record) {
                    sub_record->SetColumn(2, &usd_param->bit_1_); // bit_[1]
                }

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) sub_record->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
#endif

                // Update special facility data_a
                DynamicCompoundKey sf_key =
                    TATPKeyGenerator::GenerateSpecialFacilityKey(usd_param->s_id_, usd_param->sf_type_);
                Record* sf_record = nullptr;
                DB_QUERY(SearchRecord(SPECIAL_FACILITY_TABLE_ID, sf_key, sf_record, READ_WRITE));

                if (sf_record) {
                    sf_record->SetColumn(4, &usd_param->data_a_);
                }

#if defined(TO)
                handle = (Cache::Handle*) sf_record->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class UpdateLocationProcedure : public StoredProcedure {
        public:
            UpdateLocationProcedure() {
                context_.txn_type_ = UPDATE_LOCATION;
            }

            virtual ~UpdateLocationProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                UpdateLocationParam* ul_param = static_cast<UpdateLocationParam*>(param);

                // Update subscriber vlr_location
                DynamicCompoundKey sub_key = TATPKeyGenerator::GenerateSubscriberKey(ul_param->s_id_);
                Record* sub_record         = nullptr;
                DB_QUERY(SearchRecord(SUBSCRIBER_TABLE_ID, sub_key, sub_record, READ_WRITE));

                if (sub_record) {
                    sub_record->SetColumn(33, &ul_param->vlr_location_); // vlr_location field
                }

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) sub_record->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class InsertCallForwardingProcedure : public StoredProcedure {
        public:
            InsertCallForwardingProcedure() {
                context_.txn_type_ = INSERT_CALL_FORWARDING;
            }

            virtual ~InsertCallForwardingProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                InsertCallForwardingParam* icf_param = static_cast<InsertCallForwardingParam*>(param);

                // Read special facility to check if active
                DynamicCompoundKey sf_key =
                    TATPKeyGenerator::GenerateSpecialFacilityKey(icf_param->s_id_, icf_param->sf_type_);
                Record* sf_record = nullptr;
                DB_QUERY(SearchRecord(SPECIAL_FACILITY_TABLE_ID, sf_key, sf_record, READ_WRITE));

                uint8_t is_active = 0;
                if (sf_record) {
                    sf_record->GetColumn(2, &is_active);
                    if (!is_active) {
                        is_active = 1;
                        sf_record->SetColumn(2, &is_active);
                    }
                }

#if defined(TO)
                Cache::Handle* handle = (Cache::Handle*) sf_record->Get_Handle();
                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
#endif

                // Insert new call forwarding record
                Cache::Handle* cf_handle;
                GlobalAddress cf_addr;
                Record* cf_record = nullptr;
                DB_QUERY(AllocateNewRecord(CALL_FORWARDING_TABLE_ID, cf_handle, cf_addr, cf_record));

                cf_record->SetColumn(0, &icf_param->s_id_);
                cf_record->SetColumn(1, &icf_param->sf_type_);
                cf_record->SetColumn(2, &icf_param->start_time_);
                cf_record->SetColumn(3, &icf_param->end_time_);
                cf_record->SetColumn(4, icf_param->numberx_);

                DynamicCompoundKey cf_key = TATPKeyGenerator::GenerateCallForwardingKey(
                    icf_param->s_id_, icf_param->sf_type_, icf_param->start_time_);
                DB_QUERY(InsertRecord(CALL_FORWARDING_TABLE_ID, cf_key, 1, cf_record, cf_handle, cf_addr));

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class DeleteCallForwardingProcedure : public StoredProcedure {
        public:
            DeleteCallForwardingProcedure() {
                context_.txn_type_ = DELETE_CALL_FORWARDING;
            }

            virtual ~DeleteCallForwardingProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                DeleteCallForwardingParam* dcf_param = static_cast<DeleteCallForwardingParam*>(param);

                // Delete call forwarding record
                DynamicCompoundKey cf_key = TATPKeyGenerator::GenerateCallForwardingKey(
                    dcf_param->s_id_, dcf_param->sf_type_, dcf_param->start_time_);
                Record* cf_record = nullptr;
                DB_QUERY(SearchRecord(CALL_FORWARDING_TABLE_ID, cf_key, cf_record, DELETE_ONLY));

#if defined(TO)
                if (cf_record) {
                    Cache::Handle* handle = (Cache::Handle*) cf_record->Get_Handle();
                    transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                }
#endif

                return transaction_manager_->CommitTransaction(ret);
            }
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
