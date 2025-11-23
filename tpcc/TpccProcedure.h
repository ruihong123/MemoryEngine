// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_PROCEDURE_H__
#define __DATABASE_TPCC_PROCEDURE_H__

#include "StoredProcedure.h"
#include "TpccKeyGenerator.h"
#include "TpccTxnParams.h"
#include <iostream>
#include <string>

namespace DSMEngine {
    namespace TpccBenchmark {
        class DeliveryProcedure : public StoredProcedure {
        public:
            DeliveryProcedure() {
                context_.txn_type_ = DELIVERY;
            }

            virtual ~DeliveryProcedure() {
            }

            virtual bool Execute(TxnParam *param, CharArray &ret) {
                DeliveryParam *delivery_param = static_cast<DeliveryParam *>(param);
                for (int no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id) {
                    /*SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = ? AND NO_W_ID = ? ORDER BY NO_O_ID ASC LIMIT 1
                     DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER WHERE NO_O_ID = ? AND NO_D_ID = ?
                     we workaround here to avoid 'order by', 'limit', 'delete', by introducing a new table DISTRICT_NEW_ORDER, get no_o_id and update it*/
                    Record *district_new_order_record = nullptr;
                    DynamicCompoundKey district_new_order_key = GetDistrictNewOrderPrimaryKey(no_d_id,
                                                                                              delivery_param->w_id_,
                                                                                              transaction_manager_->GetPrimaryIndexSchema(
                                                                                                      DISTRICT_NEW_ORDER_TABLE_ID));
                    DB_QUERY(SearchRecord(DISTRICT_NEW_ORDER_TABLE_ID, district_new_order_key,
                                          district_new_order_record, READ_WRITE));
                    int no_o_id = 0;
                    district_new_order_record->GetColumn(2, &no_o_id);
                    assert(no_o_id != 0);

                    if (no_o_id < CUSTOMERS_PER_DISTRICT) {
                        no_o_ids[no_d_id - 1] = no_o_id;
                        int next_o_id = no_o_id + 1;
                        district_new_order_record->SetColumn(2, &next_o_id);
                    } else if (no_o_id == CUSTOMERS_PER_DISTRICT) {
                        no_o_ids[no_d_id - 1] = no_o_id;
                        int next_o_id = 1;
                        district_new_order_record->SetColumn(2, &next_o_id);
                    } else {
                        assert(false);
                    }
//
//      IndexKey new_order_key = GetNewOrderPrimaryKey(no_o_id, no_d_id,
//                                                     delivery_param->w_id_);
//      // the code below can hold two latch at the same time, potentially result in deadlock.
//      // reimplement the abortion lock acquire to solve this problem.
//      Record *new_order_record = nullptr;
//        DB_QUERY(SearchRecord(&context_, NEW_ORDER_TABLE_ID,
//                              new_order_key, new_order_record,
//                              READ_ONLY))
//      if (new_order_record) {
//        no_o_ids[no_d_id - 1] = no_o_id;
//        int next_o_id = no_o_id + 1;
//        district_new_order_record->SetColumn(2, &next_o_id);
//#if defined(TO)
//          held_handle_ = ((Cache::Handle*)new_order_record->Get_Handle());
//          assert(held_handle_->gptr!=GlobalAddress::Null());
//          transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
//#endif
//
//      } else {
//        // when cannot find any no_o_id, let next_o_id wrap around again and drop this order delivery
//        // TODO: this place should be modified after implementing an efficient index.
//
//          no_o_ids[no_d_id - 1] = -1;
//        int next_o_id = 1;
//        district_new_order_record->SetColumn(2, &next_o_id);
//      }
#if defined(TO)

                    held_handle_ = ((Cache::Handle*)district_new_order_record->Get_Handle());
                    assert(held_handle_->gptr!=GlobalAddress::Null());
                    transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                }

                for (int no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id) {
                    if (no_o_ids[no_d_id - 1] == -1) {
                        continue;
                    }
                    // SELECT O_C_ID  FROM TPCCConstants.TABLENAME_OPENORDER WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
                    // UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
                    DynamicCompoundKey order_key = GetOrderPrimaryKey(no_o_ids[no_d_id - 1], no_d_id,
                                                                      delivery_param->w_id_,
                                                                      transaction_manager_->GetPrimaryIndexSchema(
                                                                              ORDER_TABLE_ID));
                    Record *order_record = nullptr;
                    DB_QUERY(
                            SearchRecord(ORDER_TABLE_ID, order_key, order_record, READ_WRITE));
                    order_record->SetColumn(5, &delivery_param->o_carrier_id_);
                    int c_id = 0;
                    int ol_cnt = 0;
                    order_record->GetColumn(1, &c_id);
                    order_record->GetColumn(6, &ol_cnt);
                    assert(c_id != 0);
                    c_ids[no_d_id - 1] = c_id;
                    no_o_ol_cnt[no_d_id - 1] = 0;
#if defined(TO)
                    held_handle_ = ((Cache::Handle*)order_record->Get_Handle());
                    assert(held_handle_->gptr!=GlobalAddress::Null());
                    transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                }

                for (int no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id) {
                    if (no_o_ids[no_d_id - 1] == -1) {
                        continue;
                    }
                    double sum = 0, tmp = 0;
                    for (int i = 1; i < no_o_ol_cnt[no_d_id - 1] + 1; ++i) {
                        DynamicCompoundKey order_line_key = GetOrderLinePrimaryKey(no_o_ids[no_d_id - 1], no_d_id,
                                                                                   delivery_param->w_id_, i,
                                                                                   transaction_manager_->GetPrimaryIndexSchema(
                                                                                           ORDER_LINE_TABLE_ID));
                        Record *order_line_record = nullptr;
                        DB_QUERY(SearchRecord(ORDER_LINE_TABLE_ID, order_line_key,
                                              order_line_record, READ_WRITE));
                        order_line_record->SetColumn(6, &delivery_param->ol_delivery_d_);
                        order_line_record->GetColumn(8, &tmp);
                        sum += tmp;
#if defined(TO)
                        held_handle_ = ((Cache::Handle*)order_line_record->Get_Handle());
                        assert(held_handle_->gptr!=GlobalAddress::Null());
                        transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                    }

                    sums[no_d_id - 1] = sum;
                }

                for (int no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id) {
                    if (no_o_ids[no_d_id - 1] == -1) {
                        continue;
                    }
                    // "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?"
                    DynamicCompoundKey customer_key = GetCustomerPrimaryKey(c_ids[no_d_id - 1], no_d_id,
                                                                            delivery_param->w_id_,
                                                                            transaction_manager_->GetPrimaryIndexSchema(
                                                                                    CUSTOMER_TABLE_ID));
                    Record *customer_record = nullptr;
                    DB_QUERY(SearchRecord(CUSTOMER_TABLE_ID, customer_key, customer_record,
                                          READ_WRITE));
                    double balance = 0.0;
                    customer_record->GetColumn(16, &balance);
                    balance += sums[no_d_id - 1];
                    customer_record->SetColumn(16, &balance);
#if defined(TO)
                    held_handle_ = ((Cache::Handle*)customer_record->Get_Handle());
                    assert(held_handle_->gptr!=GlobalAddress::Null());
                    transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                }

                for (size_t no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id) {
                    ret.Memcpy(ret.size_, (char *) (&no_o_ids[no_d_id - 1]), sizeof(int));
                    ret.size_ += sizeof(int);
                    ret.Memcpy(ret.size_, (char *) (&no_d_id), sizeof(int));
                    ret.size_ += sizeof(int);
                }
                return transaction_manager_->CommitTransaction(ret);
            }

        private:
            int no_o_ids[DISTRICTS_PER_WAREHOUSE];
            int no_o_ol_cnt[DISTRICTS_PER_WAREHOUSE];
            double sums[DISTRICTS_PER_WAREHOUSE];
            int c_ids[DISTRICTS_PER_WAREHOUSE];
        };

        class NewOrderProcedure : public StoredProcedure {
        public:
            NewOrderProcedure() {
                context_.txn_type_ = NEW_ORDER;
            }

            virtual ~NewOrderProcedure() {
            }

            virtual bool Execute(TxnParam *param, CharArray &ret) {
//    epicLog(LOG_DEBUG, "thread_id=%u,start new order", thread_id_);
                NewOrderParam *new_order_param = static_cast<NewOrderParam *>(param);
                double total = 0;
                //new_order_param->ol_cnt_ : number of item to be checked out.
                for (size_t i = 0; i < new_order_param->ol_cnt_; ++i) {
                    int item_id = new_order_param->i_ids_[i];
                    // "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?"
                    DynamicCompoundKey item_key = GetItemPrimaryKey(item_id,
                                                                    transaction_manager_->GetPrimaryIndexSchema(
                                                                            ITEM_TABLE_ID));
                    Record *item_record = nullptr;
                    DB_QUERY(SearchRecord(ITEM_TABLE_ID, item_key, item_record,
                                          READ_ONLY))
//      if (transaction_manager_->SearchRecord(
//          &context_, ITEM_TABLE_ID, item_key, item_record,
//          (AccessType) new_order_param->item_access_type_[i]) == false) {
////        assert(false);
////        transaction_manager_->AbortTransaction();
//        return false;
//      }
                    double price = 0;
                    item_record->GetColumn(3, &price);
                    ret.Memcpy(ret.size_, (char *) (&item_id), sizeof(item_id));
                    ret.size_ += sizeof(item_id);
                    ret.Memcpy(ret.size_, (char *) (&price), sizeof(price));
                    ret.size_ += sizeof(price);
                    int ol_quantity = new_order_param->i_qtys_[i];
                    double ol_amount = ol_quantity * price;
                    ol_amounts[i] = ol_amount;
                    total += ol_amount;
#if defined(TO)
                    held_handle_ = ((Cache::Handle*)item_record->Get_Handle());
                    assert(held_handle_->gptr!=GlobalAddress::Null());
                    transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                }
                // Stock saves the quantity of each item in different warehouse.
                for (size_t i = 0; i < new_order_param->ol_cnt_; ++i) {
                    int ol_i_id = new_order_param->i_ids_[i];
                    int ol_supply_w_id = new_order_param->i_w_ids_[i];
                    // "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?"
                    // "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?"
                    DynamicCompoundKey stock_key = GetStockPrimaryKey(ol_i_id, ol_supply_w_id,
                                                                      transaction_manager_->GetPrimaryIndexSchema(
                                                                              STOCK_TABLE_ID));
                    Record *stock_record = nullptr;
                    //DB_QUERY(SearchRecord(&context_, STOCK_TABLE_ID, stock_key, stock_record, READ_WRITE));
                    DB_QUERY(SearchRecord(
                            STOCK_TABLE_ID, stock_key, stock_record,
                            READ_WRITE));

                    int ol_quantity = new_order_param->i_qtys_[i];
                    int ytd = 0;
                    stock_record->GetColumn(13, &ytd);
                    ytd += ol_quantity;
                    stock_record->SetColumn(13, &ytd);
                    int quantity = 0;
                    stock_record->GetColumn(2, &quantity);
                    if (quantity >= ol_quantity + 10) {
                        quantity -= ol_quantity;
                        stock_record->SetColumn(2, &quantity);
                    } else {
                        quantity = quantity + 91 - ol_quantity;
                        stock_record->SetColumn(2, &quantity);
                    }
                    int order_cnt = 0;
                    stock_record->GetColumn(14, &order_cnt);
                    order_cnt += 1;
                    stock_record->SetColumn(14, &order_cnt);
                    if (ol_supply_w_id != new_order_param->w_id_) {
                        int remote_cnt = 0;
                        stock_record->GetColumn(15, &remote_cnt);
                        remote_cnt += 1;
                        stock_record->SetColumn(15, &remote_cnt);
                    }
                    int dist_column = new_order_param->d_id_ + 2;
                    stock_record->GetColumn(dist_column, s_dists[i]);
#if defined(TO)
                    held_handle_ = ((Cache::Handle*)stock_record->Get_Handle());
                    assert(held_handle_->gptr!=GlobalAddress::Null());
                    transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                }
                // "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?"
                DynamicCompoundKey warehouse_key = GetWarehousePrimaryKey(new_order_param->w_id_,
                                                                          transaction_manager_->GetPrimaryIndexSchema(
                                                                                  WAREHOUSE_TABLE_ID));
                Record *warehouse_record = nullptr;
                DB_QUERY(SearchRecord(WAREHOUSE_TABLE_ID, warehouse_key, warehouse_record,
                                      READ_ONLY));
                double w_tax = 0;
                warehouse_record->GetColumn(7, &w_tax);
#if defined(TO)
                held_handle_ = ((Cache::Handle*)warehouse_record->Get_Handle());
                assert(held_handle_->gptr!=GlobalAddress::Null());
                transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                // "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?"
                // "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?"
                DynamicCompoundKey district_key = GetDistrictPrimaryKey(new_order_param->d_id_,
                                                                        new_order_param->w_id_,
                                                                        transaction_manager_->GetPrimaryIndexSchema(
                                                                                DISTRICT_TABLE_ID));
                Record *district_record = nullptr;
                DB_QUERY(SearchRecord(DISTRICT_TABLE_ID, district_key, district_record,
                                      READ_WRITE));
                int d_next_o_id = 0;
                district_record->GetColumn(10, &d_next_o_id);
                assert(d_next_o_id > 0);
                ret.Memcpy(ret.size_, (char *) (&d_next_o_id), sizeof(d_next_o_id));
                ret.size_ += sizeof(d_next_o_id);
                double d_tax = 0.0;
                district_record->GetColumn(8, &d_tax);
                int o_id = d_next_o_id + 1;
                district_record->SetColumn(10, &o_id);
//      printf("District table is changed over o_id %d with snapshotnumber %llu\n", o_id, transaction_manager_->snapshot_ts);
//      fflush(stdout);
#if defined(TO)
                held_handle_ = ((Cache::Handle*)district_record->Get_Handle());
                assert(held_handle_->gptr!=GlobalAddress::Null());
                transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                // "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
                DynamicCompoundKey customer_key = GetCustomerPrimaryKey(new_order_param->c_id_,
                                                                        new_order_param->d_id_,
                                                                        new_order_param->w_id_,
                                                                        transaction_manager_->GetPrimaryIndexSchema(
                                                                                CUSTOMER_TABLE_ID));
                Record *customer_record = nullptr;
                DB_QUERY(SearchRecord(CUSTOMER_TABLE_ID, customer_key, customer_record,
                                      READ_ONLY));
                double c_discount = 0;
                customer_record->GetColumn(15, &c_discount);
                for (size_t i = 0; i < new_order_param->ol_cnt_; ++i) {
                    ol_amounts[i] *= (1 - c_discount) * (1 + w_tax + d_tax);
                }
#if defined(TO)
                held_handle_ = ((Cache::Handle*)customer_record->Get_Handle());
                assert(held_handle_->gptr!=GlobalAddress::Null());
                transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                //TODO: adjust the ol_amounts[i] by c_discount.

                // "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)"
                Cache::Handle *new_order_handle = nullptr;
                char *new_order_buffer;
                GlobalAddress new_order_gaddr = GlobalAddress::Null();
                // TODO: need to allocate a allocation in the transaction manager, and make sure that the access_handle for the same
                // cache line was not locked muliple time.
                Record *new_order_record = nullptr;
//      = new Record(
//              transaction_manager_->storage_manager_->tables_[NEW_ORDER_TABLE_ID]->GetSchema(), new_order_buffer);
                DB_QUERY(AllocateNewRecord(NEW_ORDER_TABLE_ID, new_order_handle,
                                           new_order_gaddr, new_order_record))
//      printf("Pointer of new_order_buffer: %p, all local this stack is around %p\n", new_order_buffer, &new_order_buffer);
//      fflush(stdout);
                //    ->storage_manager_->tables_[NEW_ORDER_TABLE_ID]->AllocateNewTuple(
//            new_order_buffer, new_order_gaddr, new_order_handle, gallocators[thread_id_]);

                new_order_record->SetColumn(0, (char *) (&d_next_o_id));
                new_order_record->SetColumn(1, (char *) (&new_order_param->d_id_));
                new_order_record->SetColumn(2, (char *) (&new_order_param->w_id_));
                new_order_record->SetVisible(true);
//    if (new_order_param->new_order_access_type_ != READ_ONLY) {
//      new_order_record->Serialize(new_order_addr, gallocators[thread_id_]);
//    }
                DynamicCompoundKey new_order_key = GetNewOrderPrimaryKey(d_next_o_id,
                                                                         new_order_param->d_id_,
                                                                         new_order_param->w_id_,
                                                                         transaction_manager_->GetPrimaryIndexSchema(
                                                                                 NEW_ORDER_TABLE_ID));
                DB_QUERY(InsertRecord(NEW_ORDER_TABLE_ID, new_order_key, 1,
                                      new_order_record, new_order_handle, new_order_gaddr));

                int all_local = true;
                for (auto &w_id: new_order_param->i_w_ids_) {
                    all_local = (all_local && (new_order_param->w_id_ == w_id));
                }
                // "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                Cache::Handle *order_handle = nullptr;
                char *order_buffer = nullptr;
                GlobalAddress order_gaddr = GlobalAddress::Null();
                Record *order_record;
//      = new Record(
//              transaction_manager_->storage_manager_->
//                      tables_[ORDER_TABLE_ID]->GetSchema(), order_buffer);
                DB_QUERY(AllocateNewRecord(ORDER_TABLE_ID, order_handle, order_gaddr,
                                           order_record))

//      GAddr order_addr = gallocators[thread_id_]->Malloc(
//        transaction_manager_->storage_manager_->
//        tables_[ORDER_TABLE_ID]->GetRecordTotalSize());
//      printf("Insert new order order id %d, d_id %d, w_id %d\n", d_next_o_id, new_order_param->d_id_, new_order_param->w_id_);

                order_record->SetColumn(0, (char *) (&d_next_o_id));
                order_record->SetColumn(1, (char *) (&new_order_param->c_id_));
                order_record->SetColumn(2, (char *) (&new_order_param->d_id_));
                order_record->SetColumn(3, (char *) (&new_order_param->w_id_));
                order_record->SetColumn(4, (char *) (&new_order_param->o_entry_d_));
                order_record->SetColumn(5, (char *) (&NULL_CARRIER_ID));
                order_record->SetColumn(6, (char *) (&new_order_param->ol_cnt_));
                order_record->SetColumn(7, (char *) (&all_local));
                order_record->SetVisible(true);
//    if (new_order_param->order_access_type_ != READ_ONLY) {
//      order_record->Serialize(order_addr, gallocators[thread_id_]);
//    }
                DynamicCompoundKey order_key = GetOrderPrimaryKey(d_next_o_id, new_order_param->d_id_,
                                                                  new_order_param->w_id_,
                                                                  transaction_manager_->GetPrimaryIndexSchema(
                                                                          ORDER_TABLE_ID));
                DB_QUERY(InsertRecord(ORDER_TABLE_ID, order_key, 1, order_record,
                                      order_handle, order_gaddr));

                for (size_t i = 0; i < new_order_param->ol_cnt_; ++i) {
                    int ol_number = i + 1;
                    int ol_i_id = new_order_param->i_ids_[i];
                    int ol_supply_w_id = new_order_param->i_w_ids_[i];
                    int ol_quantity = new_order_param->i_qtys_[i];
                    // "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    Cache::Handle *order_line_handle = nullptr;
                    GlobalAddress order_line_gaddr = GlobalAddress::Null();
                    char *order_line_buffer = nullptr;
                    Record *order_line_record;
//        = new Record(
//                transaction_manager_->storage_manager_->
//                        tables_[ORDER_LINE_TABLE_ID]->GetSchema(), order_line_buffer);
                    DB_QUERY(AllocateNewRecord(ORDER_LINE_TABLE_ID, order_line_handle,
                                               order_line_gaddr, order_line_record))


                    order_line_record->SetColumn(0, (char *) (&d_next_o_id));
                    order_line_record->SetColumn(1, (char *) (&new_order_param->d_id_));
                    order_line_record->SetColumn(2, (char *) (&new_order_param->w_id_));
                    order_line_record->SetColumn(3, (char *) (&ol_number));
                    order_line_record->SetColumn(4, (char *) (&ol_i_id));
                    order_line_record->SetColumn(5, (char *) (&ol_supply_w_id));
                    order_line_record->SetColumn(6, (char *) (&new_order_param->o_entry_d_));
                    order_line_record->SetColumn(7, (char *) (&ol_quantity));
                    order_line_record->SetColumn(8, (char *) (&ol_amounts[i]));
                    order_line_record->SetColumn(9, s_dists[i]);
                    order_line_record->SetVisible(true);
//      if (new_order_param->order_line_access_type_[i] != READ_ONLY) {
//        order_line_record->Serialize(order_line_addr, gallocators[thread_id_]);
//      }
                    DynamicCompoundKey order_line_key = GetOrderLinePrimaryKey(d_next_o_id,
                                                                               new_order_param->d_id_,
                                                                               new_order_param->w_id_,
                                                                               ol_number,
                                                                               transaction_manager_->GetPrimaryIndexSchema(
                                                                                       ORDER_LINE_TABLE_ID));
                    //order_line_keys[1] = GetOrderLineSecondaryKey(d_next_o_id, new_order_param->d_id_, new_order_param->w_id_);
                    DB_QUERY(InsertRecord(ORDER_LINE_TABLE_ID, order_line_key, 1,
                                          order_line_record, order_line_handle,
                                          order_line_gaddr));
                }

                ret.Memcpy(ret.size_, (char *) (&w_tax), sizeof(w_tax));
                ret.size_ += sizeof(w_tax);
                ret.Memcpy(ret.size_, (char *) (&d_tax), sizeof(d_tax));
                ret.size_ += sizeof(d_tax);
                ret.Memcpy(ret.size_, (char *) (&c_discount), sizeof(c_discount));
                ret.size_ += sizeof(c_discount);
                total *= (1 - c_discount) * (1 + w_tax + d_tax);
                ret.size_ += sizeof(total);
                return transaction_manager_->CommitTransaction(ret);
            }

        private:
            double ol_amounts[15];
            char s_dists[15][33];
        };

        class PaymentProcedure : public StoredProcedure {
        public:
            PaymentProcedure() {
                context_.txn_type_ = PAYMENT;
            }

            virtual ~PaymentProcedure() {
            }

            virtual bool Execute(TxnParam *param, CharArray &ret) {
//    epicLog(LOG_DEBUG, "thread_id=%u,start payment", thread_id_);
                PaymentParam *payment_param = static_cast<PaymentParam *>(param);
                // "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?"
                // "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?"

                DynamicCompoundKey warehouse_key = GetWarehousePrimaryKey(payment_param->w_id_,
                                                                          transaction_manager_->GetPrimaryIndexSchema(
                                                                                  WAREHOUSE_TABLE_ID));
                Record *warehouse_record = nullptr;
                DB_QUERY(SearchRecord(WAREHOUSE_TABLE_ID, warehouse_key, warehouse_record,
                                      READ_WRITE));
                double w_ytd = 0;
                warehouse_record->GetColumn(8, &w_ytd);
                ret.Memcpy(ret.size_, (char *) (&w_ytd), sizeof(w_ytd));
                ret.size_ += sizeof(w_ytd);
                double new_w_ytd = w_ytd + payment_param->h_amount_;
                warehouse_record->SetColumn(8, &new_w_ytd);
#if defined(TO)
                held_handle_ = ((Cache::Handle*)warehouse_record->Get_Handle());
                assert(held_handle_->gptr!=GlobalAddress::Null());
                transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                // "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?"
                // "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?"
                DynamicCompoundKey district_key = GetDistrictPrimaryKey(payment_param->d_id_,
                                                                        payment_param->w_id_,
                                                                        transaction_manager_->GetPrimaryIndexSchema(
                                                                                DISTRICT_TABLE_ID));
                Record *district_record = nullptr;
                DB_QUERY(SearchRecord(DISTRICT_TABLE_ID, district_key, district_record,
                                      READ_WRITE));
//      printf("District table is changed over d_ytd with snapshotnumber %llu\n", transaction_manager_->snapshot_ts);
//      fflush(stdout);
                double d_ytd = 0;
                district_record->GetColumn(9, &d_ytd);
                ret.Memcpy(ret.size_, (char *) (&d_ytd), sizeof(d_ytd));
                ret.size_ += sizeof(d_ytd);
                double new_d_ytd = d_ytd + payment_param->h_amount_;
                district_record->SetColumn(9, &new_d_ytd);
#if defined(TO)
                held_handle_ = ((Cache::Handle*)district_record->Get_Handle());
                assert(held_handle_->gptr!=GlobalAddress::Null());
                transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                Record *customer_record = nullptr;
                if (payment_param->c_id_ == -1) {
                    // "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST"

                } else {
                    // "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
                    DynamicCompoundKey customer_key = GetCustomerPrimaryKey(payment_param->c_id_,
                                                                            payment_param->c_d_id_,
                                                                            payment_param->c_w_id_,
                                                                            transaction_manager_->GetPrimaryIndexSchema(
                                                                                    CUSTOMER_TABLE_ID));
                    DB_QUERY(SearchRecord(CUSTOMER_TABLE_ID, customer_key, customer_record,
                                          READ_WRITE));
                }
                // "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
                // "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
                double balance = 0.0;
                customer_record->GetColumn(16, &balance);
                balance -= payment_param->h_amount_;
                customer_record->SetColumn(16, &balance);
                double ytd_payment = 0.0;
                customer_record->GetColumn(17, &ytd_payment);
                ytd_payment += payment_param->h_amount_;
                customer_record->SetColumn(17, &ytd_payment);
                int payment_cnt = 0;
                customer_record->GetColumn(18, &payment_cnt);
                payment_cnt += 1;
                customer_record->SetColumn(18, &payment_cnt);
#if defined(TO)
                held_handle_ = ((Cache::Handle*)customer_record->Get_Handle());
                assert(held_handle_->gptr!=GlobalAddress::Null());
                transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                // "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                Cache::Handle *history_handle = nullptr;
                char *history_buffer = nullptr;
                GlobalAddress history_gaddr;
                Record *history_record = nullptr;
//      = new Record(
//              transaction_manager_->storage_manager_->
//                      tables_[HISTORY_TABLE_ID]->GetSchema(), history_buffer);
                DB_QUERY(AllocateNewRecord(HISTORY_TABLE_ID, history_handle,
                                           history_gaddr, history_record))


                history_record->SetColumn(0, (char *) (&payment_param->c_id_));
                history_record->SetColumn(1, (char *) (&payment_param->c_d_id_));
                history_record->SetColumn(2, (char *) (&payment_param->c_w_id_));
                history_record->SetColumn(3, (char *) (&payment_param->d_id_));
                history_record->SetColumn(4, (char *) (&payment_param->w_id_));
                history_record->SetColumn(5, (char *) (&payment_param->h_date_));
                history_record->SetColumn(6, (char *) (&payment_param->h_amount_));
                history_record->SetVisible(true);
//    if (payment_param->history_access_type_ != READ_ONLY) {
//      history_record->Serialize(history_addr, gallocators[thread_id_]);
//    }
                DynamicCompoundKey history_key = GetHistoryPrimaryKey(payment_param->c_id_,
                                                                      payment_param->d_id_,
                                                                      payment_param->w_id_,
                                                                      transaction_manager_->GetPrimaryIndexSchema(
                                                                              HISTORY_TABLE_ID));
                DB_QUERY(InsertRecord(HISTORY_TABLE_ID, history_key, 1, history_record,
                                      history_handle, history_gaddr));

                return transaction_manager_->CommitTransaction(ret);
            }

        };

        class OrderStatusProcedure : public StoredProcedure {
        public:
            OrderStatusProcedure() {
                context_.txn_type_ = ORDER_STATUS;
            }

            virtual ~OrderStatusProcedure() {
            }

            virtual bool Execute(TxnParam *param, CharArray &ret) {
                OrderStatusParam *order_status_param = static_cast<OrderStatusParam *>(param);

                if (order_status_param->c_id_ == -1) {
//       "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST"
                } else {
//       "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
                }

                //    "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1"
                // Use c_id to replace o_id to avoid the secondary , c_id is guaranteed to be smaller than district next o ID.
                DynamicCompoundKey order_key = GetOrderPrimaryKey(order_status_param->c_id_, order_status_param->d_id_,
                                                                  order_status_param->w_id_,
                                                                  transaction_manager_->GetPrimaryIndexSchema(
                                                                          ORDER_TABLE_ID));
                Record *order_record = nullptr;

                DB_QUERY(SearchRecord(ORDER_TABLE_ID, order_key, order_record, READ_ONLY));

                //"getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?"
                int o_id = 0;
                int ol_cnt = 0;
                order_record->GetColumn(0, &o_id);
                order_record->GetColumn(6, &ol_cnt);
#if defined(TO)
                held_handle_ = ((Cache::Handle*)order_record->Get_Handle());
                assert(held_handle_->gptr!=GlobalAddress::Null());
                transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                assert(o_id != 0);
                assert(ol_cnt <= 16);

                for (int i = 1; i < ol_cnt + 1; ++i) {
                    Record *order_line_record = nullptr;
                    DynamicCompoundKey order_line_key = GetOrderLinePrimaryKey(o_id,
                                                                               order_status_param->d_id_,
                                                                               order_status_param->w_id_,
                                                                               i,
                                                                               transaction_manager_->GetPrimaryIndexSchema(
                                                                                       ORDER_LINE_TABLE_ID));
                    DB_QUERY(SearchRecord(ORDER_LINE_TABLE_ID, order_line_key,
                                          order_line_record, READ_ONLY));
                    int i_id = 0;
                    order_line_record->GetColumn(4, &i_id);
                    assert(i_id != 0);
                    ret.Memcpy(ret.size_, (char *) (&i_id), sizeof(i_id));
                    ret.size_ += sizeof(i_id);
#if defined(TO)
                    held_handle_ = ((Cache::Handle*)order_line_record->Get_Handle());
                    assert(held_handle_->gptr!=GlobalAddress::Null());
                    transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                }

                return transaction_manager_->CommitTransaction(ret);
            }

        };

        class StockLevelProcedure : public StoredProcedure {
        public:
            StockLevelProcedure() {
                context_.txn_type_ = STOCK_LEVEL;
            }

            virtual ~StockLevelProcedure() {
            }

            virtual bool Execute(TxnParam *param, CharArray &ret) {
                StockLevelParam *stock_level_param = static_cast<StockLevelParam *>(param);
//     "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?"
                DynamicCompoundKey district_key = GetDistrictPrimaryKey(stock_level_param->d_id_,
                                                                        stock_level_param->w_id_,
                                                                        transaction_manager_->GetPrimaryIndexSchema(
                                                                                DISTRICT_TABLE_ID));
                Record *district_record = nullptr;
                DB_QUERY(SearchRecord(DISTRICT_TABLE_ID, district_key, district_record,
                                      READ_ONLY));
                int d_next_o_id = 0;
                district_record->GetColumn(10, &d_next_o_id);
                assert(d_next_o_id != 0);

#if defined(TO)
                held_handle_ = ((Cache::Handle*)district_record->Get_Handle());
                assert(held_handle_->gptr!=GlobalAddress::Null());
                transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                size_t count = 0;
                for (int o_id = d_next_o_id - 5; o_id < d_next_o_id; ++o_id) {
                    // "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?"
                    DynamicCompoundKey order_key = GetOrderPrimaryKey(o_id, stock_level_param->d_id_,
                                                                      stock_level_param->w_id_,
                                                                      transaction_manager_->GetPrimaryIndexSchema(
                                                                              ORDER_TABLE_ID));
                    Record *order_record = nullptr;
                    DB_QUERY(SearchRecord(ORDER_TABLE_ID, order_key, order_record, READ_ONLY));
                    //It is possible that the new order first modify the district next o id, but the real new order was not inserted to the DB yet.
                    if (!order_record) {
                        transaction_manager_->AbortTransaction();
                        return false;
                    }
                    int ol_cnt = 0;
                    order_record->GetColumn(6, &ol_cnt);
                    count += ol_cnt;
#if defined(TO)
                    held_handle_ = ((Cache::Handle*)order_record->Get_Handle());
                    assert(held_handle_->gptr!=GlobalAddress::Null());
                    transaction_manager_->ReleaseLatchForGCL(held_handle_->gptr, held_handle_);
#endif
                }

                ret.Memcpy(ret.size_, (char *) (&d_next_o_id), sizeof(int));
                ret.size_ += sizeof(int);
                ret.Memcpy(ret.size_, (char *) (&count), sizeof(size_t));
                ret.size_ += sizeof(size_t);

                return transaction_manager_->CommitTransaction(ret);
            }
        };

    }
}
#endif
