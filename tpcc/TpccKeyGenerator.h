#ifndef __DATABASE_TPCC_KEY_GENERATOR_H__
#define __DATABASE_TPCC_KEY_GENERATOR_H__

//#include "gallocator.h"
#include "Meta.h"
#include "TpccConstants.h"
#include "TpccRecords.h"
#include "TpccParams.h"
#include "Btr.h"
#include <string>
#include <unordered_map>
#include <cassert>
#include <functional>

namespace DSMEngine {
    namespace TpccBenchmark {
        extern TpccScaleParams tpcc_scale_params;

/******************** get primary key **********************/
        static DynamicCompoundKey GetItemPrimaryKey(int i_id, RecordSchema *index_schema_ptr) {
            assert(i_id >= 1 && i_id <= tpcc_scale_params.num_items_);
//            char *prim_buffer = new char[sizeof(int)];
            static thread_local char prim_buffer[sizeof(int)];

            //copy the value into the buffer
            memcpy(prim_buffer, &i_id, sizeof(int));
            assert(index_schema_ptr->GetPrimaryKeyLength() == sizeof(int));
            assert(index_schema_ptr->GetPrimaryColumnSize(0) == sizeof(int));
            //TODO: Think about how and when to deallocate the buffer.
            // note that the schema of this key will be installed later.
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        static DynamicCompoundKey GetWarehousePrimaryKey(int w_id, RecordSchema* index_schema_ptr) {
            assert(w_id >= 1 && w_id <= tpcc_scale_params.num_warehouses_);
            assert(index_schema_ptr->GetPrimaryKeyLength() == sizeof(int));
//            char *prim_buffer = new char[sizeof(int)];
            static thread_local char prim_buffer[sizeof(int)];

            //copy the value into the buffer
            memcpy(prim_buffer, &w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        static DynamicCompoundKey GetDistrictPrimaryKey(int d_id, int d_w_id, RecordSchema* index_schema_ptr) {
            assert(
                    d_id >= 1 && d_id <= tpcc_scale_params.num_districts_per_warehouse_
                    && d_w_id >= 1 && d_w_id <= tpcc_scale_params.num_warehouses_);
            assert(index_schema_ptr->GetPrimaryKeyLength() == 2*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int)];
            static thread_local char prim_buffer[2*sizeof(int)];
            memcpy(prim_buffer, &d_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int), &d_w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        static DynamicCompoundKey GetCustomerPrimaryKey(int c_id, int c_d_id, int c_w_id, RecordSchema* index_schema_ptr) {
            assert(c_id >= 1 && c_id <= tpcc_scale_params.num_customers_per_district_);
            assert(index_schema_ptr->GetPrimaryKeyLength() == 3*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
            static thread_local char prim_buffer[3*sizeof(int)];
            memcpy(prim_buffer, &c_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int), &c_d_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int) + sizeof(int), &c_w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        //TODO: a more feasible encoder for order_key
        static DynamicCompoundKey GetOrderPrimaryKey(int o_id, int o_d_id, int o_w_id, RecordSchema* index_schema_ptr) {
            // IndexKey k = (((IndexKey) o_w_id) << kWarehouseBits);
            assert(index_schema_ptr->GetPrimaryKeyLength() == 3*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
            static thread_local char prim_buffer[3*sizeof(int)];
            memcpy(prim_buffer, &o_id, sizeof(int));

            memcpy(prim_buffer + sizeof(int), &o_d_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int) + sizeof(int), &o_w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        static DynamicCompoundKey GetDistrictNewOrderPrimaryKey(int d_id, int w_id, RecordSchema* index_schema_ptr) {
            assert(index_schema_ptr->GetPrimaryKeyLength() == 2*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int)];
            static thread_local char prim_buffer[2*sizeof(int)];
            memcpy(prim_buffer, &d_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int), &w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        static DynamicCompoundKey GetNewOrderPrimaryKey(int o_id, int d_id, int w_id, RecordSchema* index_schema_ptr) {
            assert(index_schema_ptr->GetPrimaryKeyLength() == 3*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
            static thread_local char prim_buffer[3*sizeof(int)];
            memcpy(prim_buffer, &o_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        static DynamicCompoundKey GetOrderLinePrimaryKey(int o_id, int d_id, int w_id,
                                                         int ol_no, RecordSchema* index_schema_ptr) {
            assert(index_schema_ptr->GetPrimaryKeyLength() == 4*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int)];
            static thread_local char prim_buffer[4*sizeof(int)];
            memcpy(prim_buffer, &o_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int) + sizeof(int) + sizeof(int), &ol_no, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        static DynamicCompoundKey GetHistoryPrimaryKey(int c_id, int d_id, int w_id, RecordSchema* index_schema_ptr) {
            // as new HISTORY record inserted, history record may not be unique
            // anymore
            assert(index_schema_ptr->GetPrimaryKeyLength() == 3*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
            static thread_local char prim_buffer[3*sizeof(int)];
            memcpy(prim_buffer, &c_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

        static DynamicCompoundKey GetStockPrimaryKey(int i_id, int w_id, RecordSchema* index_schema_ptr) {
            assert(index_schema_ptr->GetPrimaryKeyLength() == 2*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int)];
            static thread_local char prim_buffer[2*sizeof(int)];
            memcpy(prim_buffer, &i_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int), &w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }

// TODO: check if this is correct
        static DynamicCompoundKey GetOrderLineSecondaryKey(int o_id, int d_id, int w_id, RecordSchema* index_schema_ptr) {
            assert(index_schema_ptr->GetPrimaryKeyLength() == 3*sizeof(int));
//            char *prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
//            make it thread local?
            static thread_local char prim_buffer[sizeof(int) + sizeof(int) + sizeof(int)];
            memcpy(prim_buffer, &o_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
            memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
            DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
            return ret;
        }


// /******************** get primary key **********************/
// static IndexKey GetItemPrimaryKey(int i_id, int w_id) {
//   assert(i_id >= 1 && i_id <= tpcc_scale_params.num_items_);
//   IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
// //      | (((IndexKey) ITEM_TABLE_ID) << kTableIdLowBits);
//   k = k | i_id;
//   return k;
// }
// static IndexKey GetWarehousePrimaryKey(int w_id) {
//   assert(w_id >= 1 && w_id <= tpcc_scale_params.num_warehouses_);
//   IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
// //      | (((IndexKey) WAREHOUSE_TABLE_ID) << kTableIdLowBits);
//   return k;
// }
// static IndexKey GetDistrictPrimaryKey(int d_id, int d_w_id) {
//   assert(
//       d_id >= 1 && d_id <= tpcc_scale_params.num_districts_per_warehouse_
//           && d_w_id >= 1 && d_w_id <= tpcc_scale_params.num_warehouses_);
//   IndexKey k = (((IndexKey) d_w_id) << kWarehouseBits);
// //      | (((IndexKey) DISTRICT_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) d_id) << kDistrictLowBits);
//   return k;
// }
// static IndexKey GetCustomerPrimaryKey(int c_id, int c_d_id, int c_w_id) {
//   assert(c_id >= 1 && c_id <= tpcc_scale_params.num_customers_per_district_);
//   IndexKey k = (((IndexKey) c_w_id) << kWarehouseBits);
// //      | (((IndexKey) CUSTOMER_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) c_d_id) << kDistrictLowBits);
//   k = k | c_id;
//   return k;
// }
// //TODO: a more feasible encoder for order_key
// static IndexKey GetOrderPrimaryKey(int o_id, int o_d_id, int o_w_id) {
//   IndexKey k = (((IndexKey) o_w_id) << kWarehouseBits);
// //      | (((IndexKey) ORDER_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) o_d_id) << kDistrictLowBits);
//   k = k | (((IndexKey) o_id) << kOrderIdLowBits);
//   return k;
// }
// static IndexKey GetDistrictNewOrderPrimaryKey(int d_id, int w_id) {
//   IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
// //      | (((IndexKey) DISTRICT_NEW_ORDER_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) d_id) << kDistrictLowBits);
//   return k;
// }
// static IndexKey GetNewOrderPrimaryKey(int o_id, int d_id, int w_id) {
//   IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
// //      | (((IndexKey) NEW_ORDER_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) d_id) << kDistrictLowBits);
//   k = k | (((IndexKey) o_id) << kOrderIdLowBits);
//   return k;
// }
// static IndexKey GetOrderLinePrimaryKey(int o_id, int d_id, int w_id,
//                                        int ol_no) {
//   IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
// //      | (((IndexKey) ORDER_LINE_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) d_id) << kDistrictLowBits);
//   k = k | (((IndexKey) o_id) << kOrderIdLowBits);
//   k = k | ol_no;
//   return k;
// }
// static IndexKey GetHistoryPrimaryKey(int c_id, int d_id, int w_id) {
//   // as new HISTORY record inserted, history record may not be unique
//   // anymore
//   IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
// //      | (((IndexKey) HISTORY_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) d_id) << kDistrictLowBits);
//   k = k | c_id;
//   return k;
// }
// static IndexKey GetStockPrimaryKey(int i_id, int w_id) {
//   IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
// //      | (((IndexKey) STOCK_TABLE_ID) << kTableIdLowBits);
//   k = k | i_id;
//   return k;
// }
// static IndexKey GetOrderLineSecondaryKey(int o_id, int d_id, int w_id) {
//   IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
// //      | (((IndexKey) ORDER_LINE_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) d_id) << kDistrictLowBits);
//   k = k | (((IndexKey) o_id) << kOrderIdLowBits);
//   return k;
// }

    }
}


#endif
