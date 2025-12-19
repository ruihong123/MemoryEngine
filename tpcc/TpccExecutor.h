// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_EXECUTOR_H__
#define __DATABASE_TPCC_EXECUTOR_H__

#include "TpccConstants.h"
#include "TpccParams.h"
#include "TpccKeyGenerator.h"
#include "TpccProcedure.h"
#include "TransactionExecutor.h"
#include <cmath>
#include <atomic>
#include <memory>

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
    const int global_total_warehouses = tpcc_scale_params.num_warehouses_; // Global total across all partitions
    const int max_items = tpcc_scale_params.num_items_;
    const int max_district = tpcc_scale_params.num_districts_per_warehouse_;
    const int max_customer = tpcc_scale_params.num_customers_per_district_;

    if (start_wh <= 0 || end_wh < start_wh || global_total_warehouses <= 0 || max_items <= 0 ||
        max_district <= 0 || max_customer <= 0) {
      return;
    }

    // Calculate number of warehouses to scan per transaction (10% by default, configurable)
    // Based on global total, not just local partition
    const int warehouses_per_scan = std::max(1, static_cast<int>(std::ceil(global_total_warehouses * HOT_SCAN_WAREHOUSE_PERCENTAGE)));

    // Create scan task that scans all data for 10% of warehouses per transaction
    auto district_table_it = storage_manager_->tables_.find(DISTRICT_TABLE_ID);
    auto stock_table_it = storage_manager_->tables_.find(STOCK_TABLE_ID);
    auto warehouse_table_it = storage_manager_->tables_.find(WAREHOUSE_TABLE_ID);
    auto customer_table_it = storage_manager_->tables_.find(CUSTOMER_TABLE_ID);
    
    if (district_table_it == storage_manager_->tables_.end() ||
        stock_table_it == storage_manager_->tables_.end() ||
        warehouse_table_it == storage_manager_->tables_.end() ||
        customer_table_it == storage_manager_->tables_.end()) {
      return; // Tables not found, skip hot scan configuration
    }
    
    auto district_schema = district_table_it->second->GetPrimaryIndexSchema();
    auto stock_schema = stock_table_it->second->GetPrimaryIndexSchema();
    auto warehouse_schema = warehouse_table_it->second->GetPrimaryIndexSchema();
    auto customer_schema = customer_table_it->second->GetPrimaryIndexSchema();
    
    // State tracking for scanning multiple warehouses per transaction
    auto current_warehouse_start = std::make_shared<int>(start_wh); // Start of current warehouse batch
    auto current_wh = std::make_shared<int>(start_wh); // Current warehouse being scanned
    auto current_district = std::make_shared<int>(1); // Current district
    auto current_customer = std::make_shared<int>(1); // Current customer
    auto current_item = std::make_shared<int>(1); // Current item
    auto scan_phase = std::make_shared<int>(0); // 0=district, 1=stock, 2=warehouse, 3=customer
    auto in_scan = std::make_shared<bool>(false); // true if currently scanning
    //todo: maybe we can use btr iterator to accelerate the table scan.
    tasks.push_back(
        HotTableScanTask("tpcc_multi_warehouse_scan",
                         [current_warehouse_start, current_wh, current_district, current_customer, current_item,
                          district_schema, stock_schema, warehouse_schema, customer_schema,
                          scan_phase, in_scan, start_wh, global_total_warehouses, max_district, max_items, max_customer,
                          warehouses_per_scan](TransactionManager &mgr, const std::atomic<bool> &should_run) -> bool {
                           Record *record = nullptr;
                           
                           // Loop until a transaction completes (commits or aborts)
                           while (should_run.load(std::memory_order_acquire)) {
                             if (!(*in_scan)) {
                               // Start a new scan batch
                               *in_scan = true;
                               *current_wh = *current_warehouse_start;
                               *current_district = 1;
                               *current_customer = 1;
                               *current_item = 1;
                               *scan_phase = 0; // Start with districts
                             }
                             
                             // Scan all data for warehouses in current batch
                             switch (*scan_phase) {
                               case 0: { // Scan districts
                                 int wh = *current_wh;
                                 int d = *current_district;
                                 
                                 // Read district record
                                 DynamicCompoundKey key = GetDistrictPrimaryKey(d, wh, district_schema);
                                 if (!mgr.SearchRecord(DISTRICT_TABLE_ID, key, record, READ_ONLY)) {
                                   *in_scan = false;
                                   return false; // Transaction aborted
                                 }
#if defined(TO)
                                 Cache::Handle* held_handle = ((Cache::Handle*)record->Get_Handle());
                                 assert(held_handle->gptr != GlobalAddress::Null());
                                 mgr.ReleaseLatchForGCL(held_handle->gptr, held_handle);
#endif
                               
                                 // Advance to next district
                                 ++d;
                                 if (d > max_district) {
                                   // Finished all districts for current warehouse, move to next warehouse
                                   d = 1;
                                   ++wh;
                                   // Check if we've exceeded the batch or global total
                                   int batch_end = *current_warehouse_start + warehouses_per_scan - 1;
                                   if (wh > batch_end || wh > global_total_warehouses) {
                                     // Finished all districts for all warehouses in batch, move to stock phase
                                     *scan_phase = 1;
                                     *current_wh = *current_warehouse_start;
                                     *current_item = 1;
                                   } else {
                                     *current_wh = wh;
                                   }
                                   *current_district = d;
                                 } else {
                                   *current_district = d;
                                 }
                                 break;
                               }
                               case 1: { // Scan stock
                                 int wh = *current_wh;
                                 int item = *current_item;
                                 
                                 // Read stock record
                                 DynamicCompoundKey key = GetStockPrimaryKey(item, wh, stock_schema);
                                 if (!mgr.SearchRecord(STOCK_TABLE_ID, key, record, READ_ONLY)) {
                                   *in_scan = false;
                                   return false; // Transaction aborted
                                 }
#if defined(TO)
                                 Cache::Handle* held_handle = ((Cache::Handle*)record->Get_Handle());
                                 assert(held_handle->gptr != GlobalAddress::Null());
                                 mgr.ReleaseLatchForGCL(held_handle->gptr, held_handle);
#endif
                                 
                                 // Advance to next item
                                 ++item;
                                 if (item > max_items) {
                                   // Finished all items for current warehouse, move to next warehouse
                                   item = 1;
                                   ++wh;
                                   // Check if we've exceeded the batch or global total
                                   int batch_end = *current_warehouse_start + warehouses_per_scan - 1;
                                   if (wh > batch_end || wh > global_total_warehouses) {
                                     // Finished all stock for all warehouses in batch, move to warehouse phase
                                     *scan_phase = 2;
                                     *current_wh = *current_warehouse_start;
                                   } else {
                                     *current_wh = wh;
                                   }
                                   *current_item = item;
                                 } else {
                                   *current_item = item;
                                 }
                                 break;
                               }
                               case 2: { // Scan warehouses
                                 int wh = *current_wh;
                                 
                                 // Read warehouse record
                                 DynamicCompoundKey key = GetWarehousePrimaryKey(wh, warehouse_schema);
                                 if (!mgr.SearchRecord(WAREHOUSE_TABLE_ID, key, record, READ_ONLY)) {
                                   *in_scan = false;
                                   return false; // Transaction aborted
                                 }
#if defined(TO)
                                 Cache::Handle* held_handle = ((Cache::Handle*)record->Get_Handle());
                                 assert(held_handle->gptr != GlobalAddress::Null());
                                 mgr.ReleaseLatchForGCL(held_handle->gptr, held_handle);
#endif
                                 
                                 // Advance to next warehouse
                                 ++wh;
                                 // Check if we've exceeded the batch or global total
                                 int batch_end = *current_warehouse_start + warehouses_per_scan - 1;
                                 if (wh > batch_end || wh > global_total_warehouses) {
                                   // Finished all warehouses in batch, move to customer phase
                                   *scan_phase = 3;
                                   *current_wh = *current_warehouse_start;
                                   *current_district = 1;
                                   *current_customer = 1;
                                 } else {
                                   *current_wh = wh;
                                 }
                                 break;
                               }
                               case 3: { // Scan customers
                                 int wh = *current_wh;
                                 int d = *current_district;
                                 int c = *current_customer;
                                 
                                 // Read customer record
                                 DynamicCompoundKey key = GetCustomerPrimaryKey(c, d, wh, customer_schema);
                                 if (!mgr.SearchRecord(CUSTOMER_TABLE_ID, key, record, READ_ONLY)) {
                                   *in_scan = false;
                                   return false; // Transaction aborted
                                 }
#if defined(TO)
                                 Cache::Handle* held_handle = ((Cache::Handle*)record->Get_Handle());
                                 assert(held_handle->gptr != GlobalAddress::Null());
                                 mgr.ReleaseLatchForGCL(held_handle->gptr, held_handle);
#endif
                                 
                                 // Advance to next customer
                                 ++c;
                                 if (c > max_customer) {
                                   // Finished all customers for current district, move to next district
                                   c = 1;
                                   ++d;
                                   if (d > max_district) {
                                     // Finished all districts for current warehouse, move to next warehouse
                                     d = 1;
                                     ++wh;
                                     // Check if we've exceeded the batch or global total
                                     int batch_end = *current_warehouse_start + warehouses_per_scan - 1;
                                     if (wh > batch_end || wh > global_total_warehouses) {
                                       // Finished scanning all data for all warehouses in batch
                                       // Commit transaction
                                       CharArray ret;
                                       bool committed = mgr.CommitTransaction(ret);
                                       if (committed) {
                                         *in_scan = false;
                                         // Move to next batch of warehouses (round-robin globally)
                                         *current_warehouse_start += warehouses_per_scan;
                                         if (*current_warehouse_start > global_total_warehouses) {
                                           // Wrap around: start from 1 (first warehouse globally)
                                           *current_warehouse_start = 1;
                                         }
                                         return true; // Transaction committed
                                       } else {
                                         *in_scan = false;
                                         return false; // Transaction aborted
                                       }
                                     } else {
                                       *current_wh = wh;
                                     }
                                   } else {
                                     *current_district = d;
                                   }
                                   *current_customer = c;
                                 } else {
                                   *current_customer = c;
                                 }
                                 break;
                               }
                             }
                           }
                           return false; // Should not reach here, but return false if loop exits
                         }));
  }
};
} // namespace TpccBenchmark
} // namespace DSMEngine
#endif
