#ifndef __DATABASE_STORAGE_STORAGE_MANAGER_H__
#define __DATABASE_STORAGE_STORAGE_MANAGER_H__

#include <iostream>
#include <vector>

#include "Table.h"

namespace DSMEngine {
class TableDirectory {
public:
 TableDirectory() {
    tables_ = nullptr;
    table_count_ = 0;
  }
  ~TableDirectory() {
    if (tables_) {
      assert(table_count_ > 0);
      for (size_t i = 0; i < table_count_; ++i) {
        delete tables_[i];
        tables_[i] = nullptr;
      } 
      delete[] tables_;
      tables_ = nullptr;
    }
  }

  void BulkRegisterTables(const std::vector<RecordSchema*>& schemas,
      DDSM* gallocator) {
    table_count_ = schemas.size();
    assert(table_count_ < kMaxTableNum);
    tables_ = new Table*[table_count_];
    for (size_t i = 0; i < table_count_; ++i) {
      Table* table = new Table();
      table->Init(i, schemas[i], gallocator);
      tables_[i] = table;
    }
  }
  void RegisterTable(Table* table) {
    // do we need to consider concurrency issues?
    assert(table_count_ < kMaxTableNum);
    ++table_count_;
    tables_[table_count_] = table;
//    table_name_to_id_map_[table->GetTableName()] = table_count_;

  }
  size_t GetTableId(const std::string& table_name) {
    auto it = table_name_to_id_map_.find(table_name);
    if (it != table_name_to_id_map_.end()) {
      return it->second;
    }
    return -1;
  }

  size_t GetTableCount() const {
    return table_count_;
  }

  virtual void Serialize(char* const& addr) {
    memcpy((void *) addr, &table_count_, sizeof(size_t));
    const char* cur_addr = (addr + sizeof(size_t));
    for (size_t i = 0; i < table_count_; ++i) {
      tables_[i]->Serialize(cur_addr);
      cur_addr = cur_addr + Table::GetSerializeSize();
    }
  }
    
  virtual void Deserialize( char* const& addr) {
      memcpy(&table_count_, addr, sizeof(size_t));
    const char* cur_addr = addr+ sizeof(size_t);
    tables_ = new Table*[table_count_];
    for (size_t i = 0; i < table_count_; ++i) {
      Table* table = new Table();
      table->Deserialize(cur_addr);
      tables_[i] = table;
      cur_addr = cur_addr + Table::GetSerializeSize();
    }
  }

  static size_t GetSerializeSize() {
    return sizeof(size_t) + kMaxTableNum * Table::GetSerializeSize();
  }

public:
  Table **tables_;
  std::map<std::string, int> table_name_to_id_map_;
private:
  size_t table_count_;
};
}
#endif
