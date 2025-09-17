#ifndef __DATABASE_STORAGE_TABLE_H__
#define __DATABASE_STORAGE_TABLE_H__

#include <iostream>

#include "Record.h"
#include "RecordSchema.h"
#include "Meta.h"
#include "HashIndex.h"
#include "Btr.h"
#include "thread_local.h"
//#include "Profiler.h"

namespace DSMEngine {
//    static RecordSchema* GetPrimaryIndexSchema() {
//        static RecordSchema* index_schema_ptr = nullptr;
//        if (index_schema_ptr == nullptr){
//            index_schema_ptr = new RecordSchema(65535);
//            std::vector<DSMEngine::ColumnInfo*> columns;
//            columns.push_back(new DSMEngine::ColumnInfo("primary_id", DSMEngine::ValueType::UINT64));
//            columns.push_back(new DSMEngine::ColumnInfo("gptr", DSMEngine::ValueType::UINT64));
//            index_schema_ptr->BulkloadColumns(columns);
//            size_t column_ids[1] = {0};
//            index_schema_ptr->SetPrimaryColumns(column_ids,1);
//        }
//
//        return index_schema_ptr;
//    }
    static void delete_GAddr(void *ptr) {
//        printf("Deallocate the GLOBAL ADDRESS SUccessfully\n");
        delete (GlobalAddress *) ptr;
    }

//template <typename T = IndexKey>
    class Table {
    public:
        Table() {
            schema_ptr_ = nullptr;
            primary_index_ = nullptr;
//    secondary_indexes_ = nullptr;
// TODO: it seems that the deallocator does not work, need to understand why.
            opened_block_ = new ThreadLocalPtr(&delete_GAddr);
        }

        ~Table() {
            if (primary_index_) {
//        delete primary_index_->index_scheme_ptr;
                delete primary_index_;
                primary_index_ = nullptr;
            }
        }
        RecordSchema* GetPrimaryIndexSchema() {
            return primary_index_->index_scheme_ptr;
        }

        RecordSchema *CreatePrimaryIndexSchema() {
            // has to be call after the table schema is ready.
            RecordSchema *index_schema_ptr = nullptr;
            if (index_schema_ptr == nullptr) {
                index_schema_ptr = new RecordSchema(65535, schema_ptr_->GetTableName() + "_pri_idx");
                size_t key_num = schema_ptr_->GetPrimaryColumnCount();

                std::vector<DSMEngine::ColumnInfo *> columns;
                // create new index schema for compound key
                for (size_t i = 0; i < key_num; ++i) {
                    size_t col_id = schema_ptr_->GetPrimaryColumnId(i);
                    columns.push_back(new DSMEngine::ColumnInfo(schema_ptr_->GetColumnName(col_id),
                                                                schema_ptr_->GetColumnType(col_id)));
                }
                columns.push_back(new DSMEngine::ColumnInfo("gptr", DSMEngine::ValueType::UINT64));
                index_schema_ptr->BulkloadColumns(columns);
                size_t *column_ids = new size_t[key_num];
                for (size_t i = 0; i < key_num; ++i) {
                    column_ids[i] = i;
                }
                index_schema_ptr->SetPrimaryColumns(column_ids, key_num);
                delete[] column_ids;
            }

            return index_schema_ptr;
        }

        // Only the master node (node-0) can create the new table and new index.
        void Init(size_t table_id, RecordSchema *schema_ptr, DDSM *ddsm) {
            assert(table_id == schema_ptr->GetTableId());
            table_id_ = table_id;
            table_name = schema_ptr->GetTableName();
            schema_ptr_ = schema_ptr;
            secondary_count_ = 0;
//    secondary_indexes_ = nullptr;
            // the index init shall be deprecated, since we can init the index in the constructor. If the index does not need init
            // we can tell that by the number of constructor arguments.
            DSMEngine::RecordSchema *index_schema_ptr = CreatePrimaryIndexSchema();
            primary_index_ = new Btr(ddsm, ddsm->rdma_mg->page_cache_, index_schema_ptr, DDSM::GetNextIndexID());
//    primary_index_->Init(kHashIndexBucketHeaderNum, gallocator);
        }

        // TODO: return false if the key exists in primary index already
        bool InsertPriIndex(const DynamicCompoundKey key, size_t key_num, GlobalAddress tuple_gaddr) {
            assert(TOPAGE(tuple_gaddr).offset != tuple_gaddr.offset);
            assert(key_num == 1);
            size_t primary_key_length = schema_ptr_->GetPrimaryKeyLength();
            char *key_value_pair = new char[primary_key_length + 8];
            Slice inserted_slice(key_value_pair, primary_key_length + 8);
            memcpy(key_value_pair, key.start, primary_key_length);
            memcpy(key_value_pair + primary_key_length, &tuple_gaddr, sizeof(GlobalAddress));
            primary_index_->insert(key, inserted_slice);
            delete[] key_value_pair;
            return true;
        }


        GlobalAddress SearchPriIndex(const DynamicCompoundKey key) {
            GlobalAddress tuple_gaddr = GlobalAddress::Null();
            char key_value_pair[1024];
            memset(key_value_pair, 0 , 1024);
            Slice retrieved_slice(key_value_pair, primary_index_->index_scheme_ptr->GetRecordTotalSize());
            bool find = primary_index_->search(key, retrieved_slice);
            if (find) {
                memcpy(&tuple_gaddr, key_value_pair + primary_index_->index_scheme_ptr->GetPrimaryKeyLength(), sizeof(GlobalAddress));
                assert(DynamicCompoundKey(key_value_pair, primary_index_->index_scheme_ptr) == key);
                return tuple_gaddr;
            } else {
//              printf("Key not find\n");
//          fflush(stdout);
                tuple_gaddr = GlobalAddress::Null();
                return tuple_gaddr;
            }

        }

        void ReportTableSize() const {
            uint64_t size = primary_index_->GetRecordCount()
                            * schema_ptr_->GetRecordTotalSize();
            std::cout << "table_id=" << table_id_ << ", size="
                      << size * 1.0 / 1000 / 1000 << "MB" << std::endl;
        }

        size_t GetTableId() const {
            return table_id_;
        }

        std::string GetTableName() const {
            return table_name;
        }

        size_t GetSecondaryCount() const {
            return secondary_count_;
        }

        RecordSchema *GetSchema() {
            return schema_ptr_;
        }

        size_t GetSchemaSize() const {
            return schema_ptr_->GetRecordTotalSize();
        }

        GlobalAddress *GetOpenedBlock() const {

            return (GlobalAddress *) opened_block_->Get();
        }

        void SetOpenedBlock(const GlobalAddress *opened_block) {
            opened_block_->Reset((void *) opened_block);
        }

        void Serialize(const char *&addr) {
            size_t off = 0;
            memcpy((void *) (addr + off), &table_id_, sizeof(size_t));
            off += sizeof(size_t);
            memcpy((void *) (addr + off), &secondary_count_, sizeof(size_t));
            off += sizeof(size_t);
            const char *cur_addr = (addr + off);
            schema_ptr_->Serialize(cur_addr);
            cur_addr = cur_addr + RecordSchema::GetSerializeSize();
            primary_index_->Serialize(cur_addr);
        }

        void Deserialize(const char *&addr) {
            size_t off = 0;
            memcpy(&table_id_, (void *) (addr + off), sizeof(size_t));
            off += sizeof(size_t);
            memcpy(&secondary_count_, addr + off, sizeof(size_t));
            off += sizeof(size_t);
            const char *cur_addr = addr + off;
            schema_ptr_ = new RecordSchema(table_id_);
            schema_ptr_->Deserialize(cur_addr);
            cur_addr = cur_addr + RecordSchema::GetSerializeSize();
            RecordSchema *index_schema_ptr = CreatePrimaryIndexSchema();
            primary_index_ = new Btr(default_gallocator, default_gallocator->rdma_mg->page_cache_, index_schema_ptr);
            primary_index_->Deserialize(cur_addr);
        }

        static size_t GetSerializeSize() {
            size_t ret = sizeof(size_t) * 2;
            ret += RecordSchema::GetSerializeSize();
            ret += Btr::GetSerializeSize();
            return ret;
        }

        //Allocate the tuple from new page, if there is a cached handles list, we need to consider whether the latch has already been
        // acquired.
        bool AllocateNewTuple(char *&tuple_data_, GlobalAddress &tuple_gaddr, Cache::Handle *&handle, DDSM *gallocator,
                              std::unordered_map<uint64_t, std::pair<Cache::Handle *, AccessType>> *locked_handles_) {
            void *page_buffer;
            GlobalAddress *g_addr = GetOpenedBlock();
            DataPage *page = nullptr;
            if (locked_handles_ != nullptr && g_addr != nullptr) {
                GlobalAddress cacheline_g_addr = *g_addr;
                if (locked_handles_->find(cacheline_g_addr) == locked_handles_->end()) {
//              gallocator->SELCC_Exclusive_Lock(page_buffer, *g_addr, handle);
                    if (!gallocator->TrySELCC_Exclusive_Lock(page_buffer, *g_addr, handle)) {
                        return false;
                    }
                    assert(((DataPage *) page_buffer)->hdr.table_id == table_id_);
                    (*locked_handles_)[cacheline_g_addr] = std::pair(handle, INSERT_ONLY);
                    page = reinterpret_cast<DataPage *>(page_buffer);
                } else {
                    handle = locked_handles_->at(cacheline_g_addr).first;
                    //TODO: update the hierachical lock atomically, if the lock is shared lock
                    if (locked_handles_->at(cacheline_g_addr).second == READ_ONLY) {
//                  default_gallocator->SELCC_Lock_Upgrade(page_buffer, cacheline_g_addr, handle);
                        if (!default_gallocator->SELCC_Lock_Upgrade(page_buffer, cacheline_g_addr, handle)) {
                            return false;
                        }
                    }
                    (*locked_handles_)[cacheline_g_addr].second = INSERT_ONLY;
#if ACCESS_MODE == 1
                    page_buffer = ((ibv_mr *) handle->value)->addr;
#elif ACCESS_MODE == 0
                    page_buffer = handle->value;
#endif
                    page = reinterpret_cast<DataPage *>(page_buffer);

                }
            } else if (locked_handles_ == nullptr && g_addr != nullptr) {
                gallocator->SELCC_Exclusive_Lock(page_buffer, *g_addr, handle);
//          if (!gallocator->TrySELCC_Exclusive_Lock(page_buffer, *g_addr, handle)){
//              return false;
//          }
                assert(((DataPage *) page_buffer)->hdr.table_id == table_id_);
                page = reinterpret_cast<DataPage *>(page_buffer);
            } else if (locked_handles_ != nullptr && g_addr == nullptr) {
                g_addr = new GlobalAddress();
                *g_addr = gallocator->Allocate_Remote(Regular_Page);
                SetOpenedBlock(g_addr);
                if (!gallocator->TrySELCC_Exclusive_Lock(page_buffer, *g_addr, handle)) {
                    return false;
                }
//          uint64_t cardinality = 8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) / (8ull*schema_ptr_->GetRecordTotalSize() +1);
                uint64_t cardinality = DataPage::calculate_cardinality(kLeafPageSize, schema_ptr_->GetRecordTotalSize());
                page = new(page_buffer) DataPage(*g_addr, cardinality, table_id_);
                (*locked_handles_)[*g_addr] = std::pair(handle, INSERT_ONLY);

            } else {
                assert(locked_handles_ == nullptr && g_addr == nullptr);
                g_addr = new GlobalAddress();
                *g_addr = gallocator->Allocate_Remote(Regular_Page);
                SetOpenedBlock(g_addr);
                gallocator->SELCC_Exclusive_Lock(page_buffer, *g_addr, handle);
//          if (!gallocator->TrySELCC_Exclusive_Lock(page_buffer, *g_addr, handle)){
//              return false;
//          }
//          uint64_t cardinality = 8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) / (8ull*schema_ptr_->GetRecordTotalSize() +1);
                uint64_t cardinality = DataPage::calculate_cardinality(kLeafPageSize, schema_ptr_->GetRecordTotalSize());
                page = new(page_buffer) DataPage(*g_addr, cardinality, table_id_);
            }
            assert(page->hdr.kDataCardinality > 0);

            assert(handle != nullptr);
            assert(page_buffer != nullptr);
            // TODO: if this is a new cache line, we need to initialize the header correctly.
            int cnt = 0;
            bool ret = page->AllocateRecord(cnt, GetSchema(), tuple_gaddr, tuple_data_);
//        // initialize the tuple as zero;
//        memset(tuple_data_, 0, schema_ptr_->GetRecordTotalSize());
            assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
            assert((char *) tuple_data_ - (char *) page_buffer > STRUCT_OFFSET(DataPage, data_));
            assert(((DataPage *) page_buffer)->hdr.this_page_g_ptr != GlobalAddress::Null());
            assert(ret);
            // if this page is full, close it and  create a new cache line next time.
            if (cnt == page->hdr.kDataCardinality) {
                delete g_addr;
                SetOpenedBlock(nullptr);
            }
            return true;
        }

    private:
        size_t table_id_;
        size_t secondary_count_;
        std::string table_name;

        RecordSchema *schema_ptr_;
//  HashIndex *primary_index_;
        Btr *primary_index_;
//  HashIndex **secondary_indexes_; // Currently disabled
        // todo: make the opened block thread local in RocksDB.
//  static thread_local GlobalAddress opened_block_;
        ThreadLocalPtr *opened_block_;
    };

}
#endif
