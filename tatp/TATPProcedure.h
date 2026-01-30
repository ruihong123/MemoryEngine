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
                RecordSchema* index_schema = transaction_manager_->GetPrimaryIndexSchema(SUBSCRIBER_TABLE_ID);
                DynamicCompoundKey sub_key = TATPKeyGenerator::GenerateSubscriberKey(gsd_param->s_id_, index_schema);
                Record* sub_record         = nullptr;
                DB_QUERY(SearchRecord(SUBSCRIBER_TABLE_ID, sub_key, sub_record, READ_ONLY));

                // Copy data to return buffer
                if (sub_record) {
                    ret.Memcpy(ret.size_, sub_record->data_ptr_, sub_record->GetRecordSize());
                    ret.size_ += sub_record->GetRecordSize();
#if defined(TO)
                    Cache::Handle* handle = (Cache::Handle*) sub_record->Get_Handle();
                    if (handle) {
                        transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                    }
#endif
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
                RecordSchema* sf_index_schema = transaction_manager_->GetPrimaryIndexSchema(SPECIAL_FACILITY_TABLE_ID);
                DynamicCompoundKey sf_key =
                    TATPKeyGenerator::GenerateSpecialFacilityKey(gnd_param->s_id_, gnd_param->sf_type_, sf_index_schema);
                Record* sf_record = nullptr;
                DB_QUERY(SearchRecord(SPECIAL_FACILITY_TABLE_ID, sf_key, sf_record, READ_ONLY));

                if (sf_record) {
#if defined(TO)
                    Cache::Handle* handle = (Cache::Handle*) sf_record->Get_Handle();
                    if (handle) {
                        transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                    }
#endif
                }

                // Read call forwarding records for start_time range
                RecordSchema* cf_index_schema = transaction_manager_->GetPrimaryIndexSchema(CALL_FORWARDING_TABLE_ID);
                for (uint8_t start_time = gnd_param->start_time_; start_time <= gnd_param->end_time_; ++start_time) {
                    DynamicCompoundKey cf_key =
                        TATPKeyGenerator::GenerateCallForwardingKey(gnd_param->s_id_, gnd_param->sf_type_, start_time, cf_index_schema);
                    Record* cf_record = nullptr;
                    DB_QUERY(SearchRecord(CALL_FORWARDING_TABLE_ID, cf_key, cf_record, READ_ONLY));

                    if (cf_record) {
                        // the memcopy below can result in buffer overflow problem as our ret buffer is not large enough
                        // ret.Memcpy(ret.size_, cf_record->data_ptr_, cf_record->GetRecordSize());
                        // ret.size_ += cf_record->GetRecordSize();
#if defined(TO)
                        Cache::Handle* handle = (Cache::Handle*) cf_record->Get_Handle();
                        if (handle) {
                            transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                        }
#endif
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
                RecordSchema* index_schema = transaction_manager_->GetPrimaryIndexSchema(ACCESS_INFO_TABLE_ID);
                DynamicCompoundKey ai_key =
                    TATPKeyGenerator::GenerateAccessInfoKey(gad_param->s_id_, gad_param->ai_type_, index_schema);
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
#if defined(TO)
                    Cache::Handle* handle = (Cache::Handle*) ai_record->Get_Handle();
                    if (handle) {
                        transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                    }
#endif
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
                RecordSchema* sub_index_schema = transaction_manager_->GetPrimaryIndexSchema(SUBSCRIBER_TABLE_ID);
                DynamicCompoundKey sub_key = TATPKeyGenerator::GenerateSubscriberKey(usd_param->s_id_, sub_index_schema);
                Record* sub_record         = nullptr;
                DB_QUERY(SearchRecord(SUBSCRIBER_TABLE_ID, sub_key, sub_record, READ_WRITE));

                if (sub_record) {
                    sub_record->SetColumn(2, &usd_param->bit_1_); // bit_[1]
#if defined(TO)
                    Cache::Handle* handle = (Cache::Handle*) sub_record->Get_Handle();
                    if (handle) {
                        transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                    }
#endif
                }

                // Update special facility data_a
                RecordSchema* sf_index_schema = transaction_manager_->GetPrimaryIndexSchema(SPECIAL_FACILITY_TABLE_ID);
                DynamicCompoundKey sf_key =
                    TATPKeyGenerator::GenerateSpecialFacilityKey(usd_param->s_id_, usd_param->sf_type_, sf_index_schema);
                Record* sf_record = nullptr;
                DB_QUERY(SearchRecord(SPECIAL_FACILITY_TABLE_ID, sf_key, sf_record, READ_WRITE));

                if (sf_record) {
                    sf_record->SetColumn(4, &usd_param->data_a_);
#if defined(TO)
                    Cache::Handle* handle = (Cache::Handle*) sf_record->Get_Handle();
                    if (handle) {
                        transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                    }
#endif
                }

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
                RecordSchema* index_schema = transaction_manager_->GetPrimaryIndexSchema(SUBSCRIBER_TABLE_ID);
                DynamicCompoundKey sub_key = TATPKeyGenerator::GenerateSubscriberKey(ul_param->s_id_, index_schema);
                Record* sub_record         = nullptr;
                DB_QUERY(SearchRecord(SUBSCRIBER_TABLE_ID, sub_key, sub_record, READ_WRITE));

                if (sub_record) {
                    sub_record->SetColumn(33, &ul_param->vlr_location_); // vlr_location field
#if defined(TO)
                    Cache::Handle* handle = (Cache::Handle*) sub_record->Get_Handle();
                    if (handle) {
                        transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                    }
#endif
                }

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
                RecordSchema* sf_index_schema = transaction_manager_->GetPrimaryIndexSchema(SPECIAL_FACILITY_TABLE_ID);
                DynamicCompoundKey sf_key =
                    TATPKeyGenerator::GenerateSpecialFacilityKey(icf_param->s_id_, icf_param->sf_type_, sf_index_schema);
                Record* sf_record = nullptr;
                DB_QUERY(SearchRecord(SPECIAL_FACILITY_TABLE_ID, sf_key, sf_record, READ_WRITE));

                uint8_t is_active = 0;
                if (sf_record) {
                    sf_record->GetColumn(2, &is_active);
                    if (!is_active) {
                        is_active = 1;
                        sf_record->SetColumn(2, &is_active);
                    }
#if defined(TO)
                    Cache::Handle* handle = (Cache::Handle*) sf_record->Get_Handle();
                    if (handle) {
                        transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                    }
#endif
                }

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

                RecordSchema* cf_index_schema = transaction_manager_->GetPrimaryIndexSchema(CALL_FORWARDING_TABLE_ID);
                DynamicCompoundKey cf_key = TATPKeyGenerator::GenerateCallForwardingKey(
                    icf_param->s_id_, icf_param->sf_type_, icf_param->start_time_, cf_index_schema);
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
                RecordSchema* index_schema = transaction_manager_->GetPrimaryIndexSchema(CALL_FORWARDING_TABLE_ID);
                DynamicCompoundKey cf_key = TATPKeyGenerator::GenerateCallForwardingKey(
                    dcf_param->s_id_, dcf_param->sf_type_, dcf_param->start_time_, index_schema);
                Record* cf_record = nullptr;
                DB_QUERY(SearchRecord(CALL_FORWARDING_TABLE_ID, cf_key, cf_record, READ_ONLY));

                if (cf_record) {
#if defined(TO)
                    Cache::Handle* handle = (Cache::Handle*) cf_record->Get_Handle();
                    if (handle) {
                        transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                    }
#endif
                }

                return transaction_manager_->CommitTransaction(ret);
            }
        };

        class AnalyticalScanProcedure : public StoredProcedure {
        public:
            AnalyticalScanProcedure() {
                context_.txn_type_ = ANALYTICAL_SCAN;
            }

            virtual ~AnalyticalScanProcedure() {}

            virtual bool Execute(TxnParam* param, CharArray& ret) {
                AnalyticalScanParam* scan_param = static_cast<AnalyticalScanParam*>(param);

                RecordSchema* access_index_schema = transaction_manager_->GetPrimaryIndexSchema(ACCESS_INFO_TABLE_ID);
                RecordSchema* sf_index_schema = transaction_manager_->GetPrimaryIndexSchema(SPECIAL_FACILITY_TABLE_ID);
                RecordSchema* subscriber_index_schema = transaction_manager_->GetPrimaryIndexSchema(SUBSCRIBER_TABLE_ID);

                Record* record = nullptr;

                // Scan subscribers using iterator starting from start_subscriber_
                DynamicCompoundKey subscriber_start_key = TATPKeyGenerator::GenerateSubscriberKey(scan_param->start_subscriber_, subscriber_index_schema);
                auto subscriber_iter = transaction_manager_->CreateScanIteratorFromKey(SUBSCRIBER_TABLE_ID, subscriber_start_key);
                
                if (subscriber_iter) {
                    char subscriber_key_buf[64];
                    char subscriber_value_buf[64];
                    DynamicCompoundKey subscriber_key(subscriber_key_buf, subscriber_index_schema);
                    GlobalAddress gaddr;
                    int64_t subscribers_scanned = 0;
                    
                    while (subscriber_iter->Valid() && subscribers_scanned < scan_param->num_subscribers_to_scan_) {
                        if (subscriber_iter->GetNext(subscriber_key, subscriber_value_buf, gaddr)) {
                            // Read subscriber record directly using GlobalAddress from iterator
                            DB_QUERY(ReadRecordByAddress(SUBSCRIBER_TABLE_ID, record, gaddr, SCAN_READ));
#if defined(TO)
                            Cache::Handle* handle = (Cache::Handle*) record->Get_Handle();
                            if (handle) {
                                transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                            }
#endif
                            // Clean up the local record after use to avoid heap explosion
                            // The global record is already deleted in SelectRecordCC
                            transaction_manager_->CleanupLastScanReadRecord();
                            record = nullptr;
                            
                            // Extract subscriber ID from key to scan related tables
                            // For TATP, subscriber ID is the first field in the key
                            int64_t s_id = scan_param->start_subscriber_ + subscribers_scanned;
                            
                            // Scan access_info for this subscriber using iterator
                            DynamicCompoundKey access_start_key = TATPKeyGenerator::GenerateAccessInfoKey(s_id, AI_TYPE_MIN, access_index_schema);
                            auto access_iter = transaction_manager_->CreateScanIteratorFromKey(ACCESS_INFO_TABLE_ID, access_start_key);
                            if (access_iter) {
                                char access_key_buf[64];
                                char access_value_buf[64];
                                DynamicCompoundKey access_key(access_key_buf, access_index_schema);
                                GlobalAddress access_gaddr;
                                uint8_t access_types_scanned = 0;
                                
                                while (access_iter->Valid() && access_types_scanned <= (AI_TYPE_MAX - AI_TYPE_MIN)) {
                                    if (access_iter->GetNext(access_key, access_value_buf, access_gaddr)) {
                                        DB_QUERY(ReadRecordByAddress(ACCESS_INFO_TABLE_ID, record, access_gaddr, SCAN_READ));
#if defined(TO)
                                        Cache::Handle* handle = (Cache::Handle*) record->Get_Handle();
                                        if (handle) {
                                            transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                                        }
#endif
                                        // Clean up the local record after use to avoid heap explosion
                                        // The global record is already deleted in SelectRecordCC
                                        transaction_manager_->CleanupLastScanReadRecord();
                                        record = nullptr;
                                        access_types_scanned++;
                                    }
                                    access_iter->Next();
                                }
                            }
                            
                            // Scan special_facility for this subscriber using iterator
                            DynamicCompoundKey sf_start_key = TATPKeyGenerator::GenerateSpecialFacilityKey(s_id, SF_TYPE_MIN, sf_index_schema);
                            auto sf_iter = transaction_manager_->CreateScanIteratorFromKey(SPECIAL_FACILITY_TABLE_ID, sf_start_key);
                            if (sf_iter) {
                                char sf_key_buf[64];
                                char sf_value_buf[64];
                                DynamicCompoundKey sf_key(sf_key_buf, sf_index_schema);
                                GlobalAddress sf_gaddr;
                                uint8_t sf_types_scanned = 0;
                                
                                while (sf_iter->Valid() && sf_types_scanned <= (SF_TYPE_MAX - SF_TYPE_MIN)) {
                                    if (sf_iter->GetNext(sf_key, sf_value_buf, sf_gaddr)) {
                                        DB_QUERY(ReadRecordByAddress(SPECIAL_FACILITY_TABLE_ID, record, sf_gaddr, SCAN_READ));
#if defined(TO)
                                        Cache::Handle* handle = (Cache::Handle*) record->Get_Handle();
                                        if (handle) {
                                            transaction_manager_->ReleaseLatchForGCL(handle->gptr, handle);
                                        }
#endif
                                        // Clean up the local record after use to avoid heap explosion
                                        // The global record is already deleted in SelectRecordCC
                                        transaction_manager_->CleanupLastScanReadRecord();
                                        record = nullptr;
                                        sf_types_scanned++;
                                    }
                                    sf_iter->Next();
                                }
                            }
                            
                            subscribers_scanned++;
                        }
                        subscriber_iter->Next();
                    }
                }

                return transaction_manager_->CommitTransaction(ret);
            }
        };

    } // namespace TATPBenchmark
} // namespace DSMEngine

#endif
