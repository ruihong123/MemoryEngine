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

    // District table is the best choice for long-running reads because it's
    // updated by both NewOrder (45%) and Payment (43%) transactions, so a
    // long-running read will block ~88% of all write transactions
    struct DistrictCursor {
      int warehouse;
      int district;
      int start_wh;
      int end_wh;
      int max_district;
      void Advance() {
          ++district;
          if (district > max_district) {
            district = 1;
            ++warehouse;
            if (warehouse > end_wh) {
              warehouse = start_wh;
          }
        }
      }
    };

    // Create alternating scan between district and stock tables
    // Each node scans only its own partition (start_wh to end_wh) to avoid starvation
    // Each transaction scans ONE warehouse before committing (long-running transaction)
    auto district_table_it = storage_manager_->tables_.find(DISTRICT_TABLE_ID);
    auto stock_table_it = storage_manager_->tables_.find(STOCK_TABLE_ID);
    auto district_schema = district_table_it->second->GetPrimaryIndexSchema();
    auto stock_schema = stock_table_it->second->GetPrimaryIndexSchema();
    
    // State tracking for warehouse-by-warehouse scans
    enum ScanTableType {
      SCAN_DISTRICT = 0,
      SCAN_STOCK = 1
    };
    auto current_warehouse = std::make_shared<int>(start_wh); // Current warehouse being scanned
    auto district_scan_state = std::make_shared<int>(1); // Current district (1 to max_district)
    auto stock_scan_state = std::make_shared<int>(1); // Current item (1 to max_items)
    auto scan_table = std::make_shared<int>(SCAN_DISTRICT); // Which table to scan (0=district, 1=stock)
    auto in_scan = std::make_shared<bool>(false); // true if currently scanning a warehouse
    
    tasks.push_back(
        HotTableScanTask{"tpcc_district_stock_alternating_scan",
                         [current_warehouse, district_scan_state, stock_scan_state, 
                          district_schema, stock_schema, scan_table, in_scan, 
                          start_wh, end_wh, max_district, max_items](TransactionManager &mgr) {
                           Record *record = nullptr;
                           
                           if (!(*in_scan)) {
                             // Start a new warehouse scan
                             *in_scan = true;
                             switch (*scan_table) {
                               case SCAN_DISTRICT:
                                 // Reset district scan to beginning of current warehouse
                                 *district_scan_state = 1;
                                 break;
                               case SCAN_STOCK:
                                 // Reset stock scan to beginning of current warehouse
                                 *stock_scan_state = 1;
                                 break;
                             }
                           }
                           
                           switch (*scan_table) {
                             case SCAN_DISTRICT: {
                               // Scan all districts in ONE warehouse in one transaction
                               int wh = *current_warehouse;
                               int &d = *district_scan_state;
                               
                               // Read current district record
                               DynamicCompoundKey key =
                                   GetDistrictPrimaryKey(d, wh, district_schema);
                               if (!mgr.SearchRecord(DISTRICT_TABLE_ID, key, record, READ_ONLY)) {
                                 // If read fails, abort and retry
                                 mgr.AbortTransaction();
                                 *in_scan = false;
                                 return;
                               }
                               
                               // Advance to next district
                               ++d;
                               if (d > max_district) {
                                 // Finished scanning all districts in current warehouse
                                 // Spin for 10us to simulate aggregation/processing work
                                //  auto spin_start = std::chrono::high_resolution_clock::now();
                                //  while (std::chrono::duration_cast<std::chrono::microseconds>(
                                //            std::chrono::high_resolution_clock::now() - spin_start)
                                //            .count() < 100) {
                                //    _mm_pause(); // CPU pause hint for spin loop
                                //  }
                                 // Now commit
                                 CharArray ret;
                                 if (mgr.CommitTransaction(ret)) {
                                   *in_scan = false;
                                   *scan_table = SCAN_STOCK; // Switch to stock next
                                   // Move to next warehouse (wrap around if needed)
                                   ++(*current_warehouse);
                                   if (*current_warehouse > end_wh) {
                                     *current_warehouse = start_wh;
                                   }
                                   // Break time after scan commit (100us)
                                   auto break_start = std::chrono::high_resolution_clock::now();
                                   while (std::chrono::duration_cast<std::chrono::microseconds>(
                                             std::chrono::high_resolution_clock::now() - break_start)
                                             .count() < 500) {
                                     _mm_pause(); // CPU pause hint for spin loop
                                   }
                                 } else {
                                   // Commit failed, abort and retry
                                   mgr.AbortTransaction();
                                   *in_scan = false;
                                 }
                                 return;
                               }
                               // Continue scanning (don't commit yet)
                               break;
                             }
                             case SCAN_STOCK: {
                               // Scan all stock items in ONE warehouse in one transaction
                               int wh = *current_warehouse;
                               int &item = *stock_scan_state;
                               
                               // Read current stock record
                               DynamicCompoundKey key =
                                   GetStockPrimaryKey(item, wh, stock_schema);
                               if (!mgr.SearchRecord(STOCK_TABLE_ID, key, record, READ_ONLY)) {
                                 // If read fails, abort and retry
                                 mgr.AbortTransaction();
                                 *in_scan = false;
                                 return;
                              }
                               
                               // Advance to next stock item
                               ++item;
                               if (item > max_items) {
                                 // Finished scanning all stock items in current warehouse
                                 // Spin for 10us to simulate aggregation/processing work
                                //  auto spin_start = std::chrono::high_resolution_clock::now();
                                //  while (std::chrono::duration_cast<std::chrono::microseconds>(
                                //            std::chrono::high_resolution_clock::now() - spin_start)
                                //            .count() < 100) {
                                //    _mm_pause(); // CPU pause hint for spin loop
                                //  }
                                 // Now commit
                                CharArray ret;
                                if (mgr.CommitTransaction(ret)) {
                                   *in_scan = false;
                                   *scan_table = SCAN_DISTRICT; // Switch to district next
                                   // Move to next warehouse (wrap around if needed)
                                   ++(*current_warehouse);
                                   if (*current_warehouse > end_wh) {
                                     *current_warehouse = start_wh;
                                   }
                                   // Break time after scan commit (100us)
                                   auto break_start = std::chrono::high_resolution_clock::now();
                                   while (std::chrono::duration_cast<std::chrono::microseconds>(
                                             std::chrono::high_resolution_clock::now() - break_start)
                                             .count() < 500) {
                                     _mm_pause(); // CPU pause hint for spin loop
                                   }
                                 } else {
                                   // Commit failed, abort and retry
                                   mgr.AbortTransaction();
                                   *in_scan = false;
                                 }
                                 return;
                               }
                               // Continue scanning (don't commit yet)
                               break;
                             }
                           }
                         }});
  }
};
} // namespace TpccBenchmark
} // namespace DSMEngine
#endif
