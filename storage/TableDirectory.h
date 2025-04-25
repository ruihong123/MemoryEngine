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

  void RegisterTables(const std::vector<RecordSchema*>& schemas, 
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
private:
  size_t table_count_;
};
}
#endif
