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
static DynamicCompoundKeyPtr GetItemPrimaryKey(int i_id, int w_id) {
  assert(i_id >= 1 && i_id <= tpcc_scale_params.num_items_);
  char* prim_buffer = new char[sizeof(int) + sizeof(int)];
  //copy the value into the buffer
    memcpy(prim_buffer, &i_id, sizeof(int));
    memcpy(prim_buffer + sizeof(int), &w_id, sizeof(int));
//  IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
//      | (((IndexKey) ITEM_TABLE_ID) << kTableIdLowBits);
//  k = k | i_id;
  //TODO: Think about how and when to deallocate the buffer.
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
  return ret;
}
static DynamicCompoundKeyPtr GetWarehousePrimaryKey(int w_id) {
  assert(w_id >= 1 && w_id <= tpcc_scale_params.num_warehouses_);
  char* prim_buffer = new char[sizeof(int)];
  //copy the value into the buffer
    memcpy(prim_buffer, &w_id, sizeof(int));
  // IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
//      | (((IndexKey) WAREHOUSE_TABLE_ID) << kTableIdLowBits);
DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
return ret;
}
static DynamicCompoundKeyPtr GetDistrictPrimaryKey(int d_id, int d_w_id) {
  assert(
      d_id >= 1 && d_id <= tpcc_scale_params.num_districts_per_warehouse_
          && d_w_id >= 1 && d_w_id <= tpcc_scale_params.num_warehouses_);
      char* prim_buffer = new char[sizeof(int) + sizeof(int)];
    memcpy(prim_buffer, &d_id, sizeof(int));
    memcpy(prim_buffer + sizeof(int), &d_w_id, sizeof(int));
//   IndexKey k = (((IndexKey) d_w_id) << kWarehouseBits);
// //      | (((IndexKey) DISTRICT_TABLE_ID) << kTableIdLowBits);
//   k = k | (((IndexKey) d_id) << kDistrictLowBits);
DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
return ret;
}
static DynamicCompoundKeyPtr GetCustomerPrimaryKey(int c_id, int c_d_id, int c_w_id) {
  assert(c_id >= 1 && c_id <= tpcc_scale_params.num_customers_per_district_);
  char* prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &c_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &c_d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &c_w_id, sizeof(int));
  // IndexKey k = (((IndexKey) c_w_id) << kWarehouseBits);
//      | (((IndexKey) CUSTOMER_TABLE_ID) << kTableIdLowBits);
  // k = k | (((IndexKey) c_d_id) << kDistrictLowBi/ts);
  // k = k | c_id;
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
  return ret;
}
//TODO: a more feasible encoder for order_key
static DynamicCompoundKeyPtr GetOrderPrimaryKey(int o_id, int o_d_id, int o_w_id) {
  // IndexKey k = (((IndexKey) o_w_id) << kWarehouseBits);
  char* prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &o_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &o_d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &o_w_id, sizeof(int));
//      | (((IndexKey) ORDER_TABLE_ID) << kTableIdLowBits);
  // k = k | (((IndexKey) o_d_id) << kDistrictLowBits);
  // k = k | (((IndexKey) o_id) << kOrderIdLowBits);
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
  return ret;
}
static DynamicCompoundKeyPtr GetDistrictNewOrderPrimaryKey(int d_id, int w_id) {
  char* prim_buffer = new char[sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &w_id, sizeof(int));
  // IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
//      | (((IndexKey) DISTRICT_NEW_ORDER_TABLE_ID) << kTableIdLowBits);
  // k = k | (((IndexKey) d_id) << kDistrictLowBits);
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
  return ret;
}
static DynamicCompoundKeyPtr GetNewOrderPrimaryKey(int o_id, int d_id, int w_id) {
  // IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
  char* prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &o_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
//      | (((IndexKey) NEW_ORDER_TABLE_ID) << kTableIdLowBits);
  // k = k | (((IndexKey) d_id) << kDistrictLowBits);
  // k = k | (((IndexKey) o_id) << kOrderIdLowBits);
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
  return ret;
}
static DynamicCompoundKeyPtr GetOrderLinePrimaryKey(int o_id, int d_id, int w_id,
                                       int ol_no) {
  // IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
  char* prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &o_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int) + sizeof(int), &ol_no, sizeof(int));
//      | (((IndexKey) ORDER_LINE_TABLE_ID) << kTableIdLowBits);
  // k = k | (((IndexKey) d_id) << kDistrictLowBits);
  // k = k | (((IndexKey) o_id) << kOrderIdLowBits);
  // k = k | ol_no;
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
  return ret;
}
static DynamicCompoundKeyPtr GetHistoryPrimaryKey(int c_id, int d_id, int w_id) {
  // as new HISTORY record inserted, history record may not be unique
  // anymore
  // IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
  char* prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &c_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
//      | (((IndexKey) HISTORY_TABLE_ID) << kTableIdLowBits);
  // k = k | (((IndexKey) d_id) << kDistrictLowBits);
  // k = k | c_id;
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
  return ret;
}
static DynamicCompoundKeyPtr GetStockPrimaryKey(int i_id, int w_id) {
  // IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
  char* prim_buffer = new char[sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &i_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &w_id, sizeof(int));
//      | (((IndexKey) STOCK_TABLE_ID) << kTableIdLowBits);
  // k = k | i_id;
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
  return ret;
}
// TODO: check if this is correct
static DynamicCompoundKeyPtr GetOrderLineSecondaryKey(int o_id, int d_id, int w_id) {
  // IndexKey k = (((IndexKey) w_id) << kWarehouseBits);
  char* prim_buffer = new char[sizeof(int) + sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &o_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
//      | (((IndexKey) ORDER_LINE_TABLE_ID) << kTableIdLowBits);
  // k = k | (((IndexKey) d_id) << kDistrictLowBits);
  // k = k | (((IndexKey) o_id) << kOrderIdLowBits);
  DynamicCompoundKeyPtr ret(reinterpret_cast<DynamicCompoundKey*>(prim_buffer), [](DynamicCompoundKey* ptr) { delete[] ptr; });
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
