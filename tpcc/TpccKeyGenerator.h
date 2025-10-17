#ifndef __DATABASE_TPCC_KEY_GENERATOR_H__
#define __DATABASE_TPCC_KEY_GENERATOR_H__

#include "Btr.h"
#include "Meta.h"
#include "TpccConstants.h"
#include "TpccParams.h"
#include "TpccRecords.h"
#include <cassert>
#include <functional>
#include <string>
#include <unordered_map>

// Note: COMPRESSED_TPCC_KEY is now defined in Meta.h for global configuration

namespace DSMEngine {
namespace TpccBenchmark {
extern TpccScaleParams tpcc_scale_params;

#ifdef COMPRESSED_TPCC_KEY

/******************** Compressed uint64_t Key Implementation
 * **********************/
// All keys compressed into single uint64_t using bit shifting
// Uses thread-local buffer for compatibility with multi-field API

static DynamicCompoundKey
GetItemPrimaryKey(int i_id, RecordSchema *index_schema_ptr = nullptr) {
  assert(i_id >= 1 && i_id <= tpcc_scale_params.num_items_);
  static thread_local uint64_t key_buffer;
  key_buffer = static_cast<uint64_t>(i_id);
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetWarehousePrimaryKey(int w_id, RecordSchema *index_schema_ptr = nullptr) {
  assert(w_id >= 1 && w_id <= tpcc_scale_params.num_warehouses_);
  static thread_local uint64_t key_buffer;
  key_buffer = static_cast<uint64_t>(w_id) << kWarehouseBits;
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetDistrictPrimaryKey(int d_id, int d_w_id,
                      RecordSchema *index_schema_ptr = nullptr) {
  assert(d_id >= 1 && d_id <= tpcc_scale_params.num_districts_per_warehouse_ &&
         d_w_id >= 1 && d_w_id <= tpcc_scale_params.num_warehouses_);
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(d_w_id) << kWarehouseBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(d_id) << kDistrictLowBits);
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetCustomerPrimaryKey(int c_id, int c_d_id, int c_w_id,
                      RecordSchema *index_schema_ptr = nullptr) {
  assert(c_id >= 1 && c_id <= tpcc_scale_params.num_customers_per_district_);
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(c_w_id) << kWarehouseBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(c_d_id) << kDistrictLowBits);
  key_buffer = key_buffer | c_id;
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetOrderPrimaryKey(int o_id, int o_d_id, int o_w_id,
                   RecordSchema *index_schema_ptr = nullptr) {
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(o_w_id) << kWarehouseBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(o_d_id) << kDistrictLowBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(o_id) << kOrderIdLowBits);
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetDistrictNewOrderPrimaryKey(int d_id, int w_id,
                              RecordSchema *index_schema_ptr = nullptr) {
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(w_id) << kWarehouseBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(d_id) << kDistrictLowBits);
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetNewOrderPrimaryKey(int o_id, int d_id, int w_id,
                      RecordSchema *index_schema_ptr = nullptr) {
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(w_id) << kWarehouseBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(d_id) << kDistrictLowBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(o_id) << kOrderIdLowBits);
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetOrderLinePrimaryKey(int o_id, int d_id, int w_id, int ol_no,
                       RecordSchema *index_schema_ptr = nullptr) {
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(w_id) << kWarehouseBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(d_id) << kDistrictLowBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(o_id) << kOrderIdLowBits);
  key_buffer = key_buffer | ol_no;
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetHistoryPrimaryKey(int c_id, int d_id, int w_id,
                     RecordSchema *index_schema_ptr = nullptr) {
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(w_id) << kWarehouseBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(d_id) << kDistrictLowBits);
  key_buffer = key_buffer | c_id;
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetStockPrimaryKey(int i_id, int w_id,
                   RecordSchema *index_schema_ptr = nullptr) {
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(w_id) << kWarehouseBits);
  key_buffer = key_buffer | i_id;
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

static DynamicCompoundKey
GetOrderLineSecondaryKey(int o_id, int d_id, int w_id,
                         RecordSchema *index_schema_ptr = nullptr) {
  static thread_local uint64_t key_buffer;
  key_buffer = (static_cast<uint64_t>(w_id) << kWarehouseBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(d_id) << kDistrictLowBits);
  key_buffer = key_buffer | (static_cast<uint64_t>(o_id) << kOrderIdLowBits);
  return DynamicCompoundKey(reinterpret_cast<char *>(&key_buffer),
                            index_schema_ptr);
}

#else

/******************** Multi-field DynamicCompoundKey Implementation
 * **********************/
// Uses actual record schema with multiple key fields

static DynamicCompoundKey GetItemPrimaryKey(int i_id,
                                            RecordSchema *index_schema_ptr) {
  assert(i_id >= 1 && i_id <= tpcc_scale_params.num_items_);
  static thread_local char prim_buffer[sizeof(int)];
  memcpy(prim_buffer, &i_id, sizeof(int));
  assert(index_schema_ptr->GetPrimaryKeyLength() == sizeof(int));
  assert(index_schema_ptr->GetPrimaryColumnSize(0) == sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey
GetWarehousePrimaryKey(int w_id, RecordSchema *index_schema_ptr) {
  assert(w_id >= 1 && w_id <= tpcc_scale_params.num_warehouses_);
  assert(index_schema_ptr->GetPrimaryKeyLength() == sizeof(int));
  static thread_local char prim_buffer[sizeof(int)];
  memcpy(prim_buffer, &w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey
GetDistrictPrimaryKey(int d_id, int d_w_id, RecordSchema *index_schema_ptr) {
  assert(d_id >= 1 && d_id <= tpcc_scale_params.num_districts_per_warehouse_ &&
         d_w_id >= 1 && d_w_id <= tpcc_scale_params.num_warehouses_);
  assert(index_schema_ptr->GetPrimaryKeyLength() == 2 * sizeof(int));
  static thread_local char prim_buffer[2 * sizeof(int)];
  memcpy(prim_buffer, &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey
GetCustomerPrimaryKey(int c_id, int c_d_id, int c_w_id,
                      RecordSchema *index_schema_ptr) {
  assert(c_id >= 1 && c_id <= tpcc_scale_params.num_customers_per_district_);
  assert(index_schema_ptr->GetPrimaryKeyLength() == 3 * sizeof(int));
  static thread_local char prim_buffer[3 * sizeof(int)];
  memcpy(prim_buffer, &c_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &c_d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &c_w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey GetOrderPrimaryKey(int o_id, int o_d_id, int o_w_id,
                                             RecordSchema *index_schema_ptr) {
  assert(index_schema_ptr->GetPrimaryKeyLength() == 3 * sizeof(int));
  static thread_local char prim_buffer[3 * sizeof(int)];
  memcpy(prim_buffer, &o_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &o_d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &o_w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey
GetDistrictNewOrderPrimaryKey(int d_id, int w_id,
                              RecordSchema *index_schema_ptr) {
  assert(index_schema_ptr->GetPrimaryKeyLength() == 2 * sizeof(int));
  static thread_local char prim_buffer[2 * sizeof(int)];
  memcpy(prim_buffer, &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey
GetNewOrderPrimaryKey(int o_id, int d_id, int w_id,
                      RecordSchema *index_schema_ptr) {
  assert(index_schema_ptr->GetPrimaryKeyLength() == 3 * sizeof(int));
  static thread_local char prim_buffer[3 * sizeof(int)];
  memcpy(prim_buffer, &o_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey
GetOrderLinePrimaryKey(int o_id, int d_id, int w_id, int ol_no,
                       RecordSchema *index_schema_ptr) {
  assert(index_schema_ptr->GetPrimaryKeyLength() == 4 * sizeof(int));
  static thread_local char prim_buffer[4 * sizeof(int)];
  memcpy(prim_buffer, &o_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int) + sizeof(int), &ol_no,
         sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey GetHistoryPrimaryKey(int c_id, int d_id, int w_id,
                                               RecordSchema *index_schema_ptr) {
  assert(index_schema_ptr->GetPrimaryKeyLength() == 3 * sizeof(int));
  static thread_local char prim_buffer[3 * sizeof(int)];
  memcpy(prim_buffer, &c_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey GetStockPrimaryKey(int i_id, int w_id,
                                             RecordSchema *index_schema_ptr) {
  assert(index_schema_ptr->GetPrimaryKeyLength() == 2 * sizeof(int));
  static thread_local char prim_buffer[2 * sizeof(int)];
  memcpy(prim_buffer, &i_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

static DynamicCompoundKey
GetOrderLineSecondaryKey(int o_id, int d_id, int w_id,
                         RecordSchema *index_schema_ptr) {
  assert(index_schema_ptr->GetPrimaryKeyLength() == 3 * sizeof(int));
  static thread_local char prim_buffer[sizeof(int) + sizeof(int) + sizeof(int)];
  memcpy(prim_buffer, &o_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int), &d_id, sizeof(int));
  memcpy(prim_buffer + sizeof(int) + sizeof(int), &w_id, sizeof(int));
  DynamicCompoundKey ret(prim_buffer, index_schema_ptr);
  return ret;
}

#endif // COMPRESSED_TPCC_KEY

} // namespace TpccBenchmark
} // namespace DSMEngine

#endif
