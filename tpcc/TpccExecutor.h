// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_EXECUTOR_H__
#define __DATABASE_TPCC_EXECUTOR_H__

#include "TpccConstants.h"
#include "TpccParams.h"
#include "TpccKeyGenerator.h"
#include "TpccProcedure.h"
#include "TransactionExecutor.h"

namespace DSMEngine {
namespace TpccBenchmark {
class TpccExecutor : public TransactionExecutor {
public:
  TpccExecutor(IORedirector *const redirector, TableDirectory *storage_manager,
               size_t thread_count_, bool log_enabled,
               bool enable_latency_recording = false)
      : TransactionExecutor(redirector, storage_manager, thread_count_,
                            log_enabled, enable_latency_recording) {}
  ~TpccExecutor() {}

  virtual int GetPartitionStart() const override {
    return tpcc_scale_params.starting_warehouse_;
  }

  virtual int GetPartitionEnd() const override {
    return tpcc_scale_params.ending_warehouse_;
  }

  virtual int GetNumItemsPerPartition() const override {
    return num_wh_per_par;
  }

  virtual int GetPartitionKeyBits() const override { return kWarehouseBits; }

  virtual void PrepareProcedures() {
    registers_[TupleType::DELIVERY] = []() {
      DeliveryProcedure *procedure = new DeliveryProcedure();
      return procedure;
    };
    registers_[TupleType::NEW_ORDER] = []() {
      NewOrderProcedure *procedure = new NewOrderProcedure();
      return procedure;
    };
    registers_[TupleType::PAYMENT] = []() {
      PaymentProcedure *procedure = new PaymentProcedure();
      return procedure;
    };
    registers_[TupleType::ORDER_STATUS] = []() {
      OrderStatusProcedure *procedure = new OrderStatusProcedure();
      return procedure;
    };
    registers_[TupleType::STOCK_LEVEL] = []() {
      StockLevelProcedure *procedure = new StockLevelProcedure();
      return procedure;
    };

    deregisters_[TupleType::DELIVERY] = [](StoredProcedure *procedure) {
      delete procedure;
      procedure = NULL;
    };
    deregisters_[TupleType::NEW_ORDER] = [](StoredProcedure *procedure) {
      delete procedure;
      procedure = NULL;
    };
    deregisters_[TupleType::PAYMENT] = [](StoredProcedure *procedure) {
      delete procedure;
      procedure = NULL;
    };
    deregisters_[TupleType::ORDER_STATUS] = [](StoredProcedure *procedure) {
      delete procedure;
      procedure = NULL;
    };
    deregisters_[TupleType::STOCK_LEVEL] = [](StoredProcedure *procedure) {
      delete procedure;
      procedure = NULL;
    };
  }

protected:
  virtual void
  ConfigureHotTableScanner(std::vector<HotTableScanTask> &tasks) override {
    using namespace TpccBenchmark;
    if (storage_manager_ == nullptr || storage_manager_->tables_.empty()) {
      return;
    }

    const int start_wh = tpcc_scale_params.starting_warehouse_;
    const int end_wh = tpcc_scale_params.ending_warehouse_;
    const int max_items = tpcc_scale_params.num_items_;
    const int max_district = tpcc_scale_params.num_districts_per_warehouse_;
    const int max_customer = tpcc_scale_params.num_customers_per_district_;
    const int max_order = tpcc_scale_params.num_customers_per_district_;
    const int max_new_order = tpcc_scale_params.num_new_orders_per_district_;

    if (start_wh <= 0 || end_wh < start_wh || max_items <= 0 ||
        max_district <= 0 || max_customer <= 0) {
      return;
    }

    struct StockCursor {
      int warehouse;
      int item;
      int start_wh;
      int end_wh;
      int max_item;
      void Advance() {
        ++item;
        if (item > max_item) {
          item = 1;
          ++warehouse;
          if (warehouse > end_wh) {
            warehouse = start_wh;
          }
        }
      }
    };

    auto stock_table_it = storage_manager_->tables_.find(STOCK_TABLE_ID);
    if (stock_table_it != storage_manager_->tables_.end()) {
      auto stock_schema = stock_table_it->second->GetPrimaryIndexSchema();
      if (stock_schema != nullptr && max_items > 0) {
        auto stock_cursor = std::make_shared<StockCursor>(StockCursor{
            start_wh, 1, start_wh, end_wh, max_items});
        tasks.push_back(
            HotTableScanTask{"tpcc_stock_scan",
                             [stock_cursor, stock_schema](TransactionManager &mgr) {
                               Record *record = nullptr;
                               DynamicCompoundKey key =
                                   GetStockPrimaryKey(stock_cursor->item,
                                                      stock_cursor->warehouse,
                                                      stock_schema);
                               if (!mgr.SearchRecord(STOCK_TABLE_ID, key, record,
                                                     READ_ONLY)) {
                                 return;
                               }
                               CharArray ret;
                               if (mgr.CommitTransaction(ret)) {
                                 stock_cursor->Advance();
                               }
                             },
                             5});
      }
    }

    struct CustomerCursor {
      int warehouse;
      int district;
      int customer;
      int start_wh;
      int end_wh;
      int max_district;
      int max_customer;
      void Advance() {
        ++customer;
        if (customer > max_customer) {
          customer = 1;
          ++district;
          if (district > max_district) {
            district = 1;
            ++warehouse;
            if (warehouse > end_wh) {
              warehouse = start_wh;
            }
          }
        }
      }
    };

    auto customer_table_it =
        storage_manager_->tables_.find(CUSTOMER_TABLE_ID);
    if (customer_table_it != storage_manager_->tables_.end()) {
      auto customer_schema =
          customer_table_it->second->GetPrimaryIndexSchema();
      if (customer_schema != nullptr) {
        auto customer_cursor = std::make_shared<CustomerCursor>(
            CustomerCursor{start_wh, 1, 1, start_wh, end_wh, max_district,
                           max_customer});
        tasks.push_back(HotTableScanTask{
            "tpcc_customer_scan",
            [customer_cursor, customer_schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key = GetCustomerPrimaryKey(
                  customer_cursor->customer, customer_cursor->district,
                  customer_cursor->warehouse, customer_schema);
              if (!mgr.SearchRecord(CUSTOMER_TABLE_ID, key, record,
                                    READ_ONLY)) {
                return;
              }
              CharArray ret;
              if (mgr.CommitTransaction(ret)) {
                customer_cursor->Advance();
              }
            },
            5});
      }
    }

    struct OrderCursor {
      int warehouse;
      int district;
      int order;
      int start_wh;
      int end_wh;
      int max_district;
      int max_order;
      void Advance() {
        ++order;
        if (order > max_order) {
          order = 1;
          ++district;
          if (district > max_district) {
            district = 1;
            ++warehouse;
            if (warehouse > end_wh) {
              warehouse = start_wh;
            }
          }
        }
      }
    };

    auto order_table_it = storage_manager_->tables_.find(ORDER_TABLE_ID);
    if (order_table_it != storage_manager_->tables_.end()) {
      auto order_schema = order_table_it->second->GetPrimaryIndexSchema();
      if (order_schema != nullptr) {
        auto order_cursor = std::make_shared<OrderCursor>(OrderCursor{
            start_wh, 1, 1, start_wh, end_wh, max_district, max_order});
        tasks.push_back(HotTableScanTask{
            "tpcc_order_scan",
            [order_cursor, order_schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key = GetOrderPrimaryKey(
                  order_cursor->order, order_cursor->district,
                  order_cursor->warehouse, order_schema);
              if (!mgr.SearchRecord(ORDER_TABLE_ID, key, record, READ_ONLY)) {
                return;
              }
              CharArray ret;
              if (mgr.CommitTransaction(ret)) {
                order_cursor->Advance();
              }
            },
            5});
      }
    }

    auto new_order_table_it =
        storage_manager_->tables_.find(NEW_ORDER_TABLE_ID);
    if (new_order_table_it != storage_manager_->tables_.end()) {
      auto new_order_schema =
          new_order_table_it->second->GetPrimaryIndexSchema();
      if (new_order_schema != nullptr && max_new_order > 0) {
        auto new_order_cursor = std::make_shared<OrderCursor>(OrderCursor{
            start_wh, 1, 1, start_wh, end_wh, max_district, max_new_order});
        tasks.push_back(HotTableScanTask{
            "tpcc_new_order_scan",
            [new_order_cursor, new_order_schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key = GetNewOrderPrimaryKey(
                  new_order_cursor->order, new_order_cursor->district,
                  new_order_cursor->warehouse, new_order_schema);
              if (!mgr.SearchRecord(NEW_ORDER_TABLE_ID, key, record,
                                    READ_ONLY)) {
                return;
              }
              CharArray ret;
              if (mgr.CommitTransaction(ret)) {
                new_order_cursor->Advance();
              }
            },
            5});
      }
    }

    struct OrderLineCursor {
      int warehouse;
      int district;
      int order;
      int line;
      int start_wh;
      int end_wh;
      int max_district;
      int max_order;
      int max_line;
      void Advance() {
        ++line;
        if (line > max_line) {
          line = 1;
          ++order;
          if (order > max_order) {
            order = 1;
            ++district;
            if (district > max_district) {
              district = 1;
              ++warehouse;
              if (warehouse > end_wh) {
                warehouse = start_wh;
              }
            }
          }
        }
      }
    };

    auto order_line_table_it =
        storage_manager_->tables_.find(ORDER_LINE_TABLE_ID);
    if (order_line_table_it != storage_manager_->tables_.end()) {
      auto order_line_schema =
          order_line_table_it->second->GetPrimaryIndexSchema();
      if (order_line_schema != nullptr) {
        auto order_line_cursor = std::make_shared<OrderLineCursor>(
            OrderLineCursor{start_wh, 1, 1, 1, start_wh, end_wh, max_district,
                            max_order, MAX_OL_CNT});
        tasks.push_back(HotTableScanTask{
            "tpcc_order_line_scan",
            [order_line_cursor, order_line_schema](TransactionManager &mgr) {
              Record *record = nullptr;
              DynamicCompoundKey key = GetOrderLinePrimaryKey(
                  order_line_cursor->order, order_line_cursor->district,
                  order_line_cursor->warehouse, order_line_cursor->line,
                  order_line_schema);
              if (!mgr.SearchRecord(ORDER_LINE_TABLE_ID, key, record,
                                    READ_ONLY)) {
                return;
              }
              CharArray ret;
              if (mgr.CommitTransaction(ret)) {
                order_line_cursor->Advance();
              }
            },
            5});
      }
    }
  }
};
} // namespace TpccBenchmark
} // namespace DSMEngine
#endif
