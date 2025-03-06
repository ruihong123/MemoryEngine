// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_STORAGE_RECORD_H__
#define __DATABASE_STORAGE_RECORD_H__

//#include "GAMObject.h"
//#include "CharArray.h"
//#include "Meta.h"
#include "RecordSchema.h"
#include "DeltaRecord.h"
#include <set>
//#include "Cache.h"

namespace DSMEngine {
class Record {
public:
  Record(RecordSchema *schema_ptr, char* data) : schema_ptr_(schema_ptr), data_ptr_(data) {
    data_size_ = schema_ptr_->GetSchemaSize();
    data_ptr_ = data;
    need_delete_ = false;
  }
  Record(RecordSchema *schema_ptr) : schema_ptr_(schema_ptr) {
        data_size_ = schema_ptr_->GetSchemaSize();
        data_ptr_ = new char[data_size_];
        need_delete_ = true;
    }
  ~Record() {
      if (need_delete_){
          delete[] data_ptr_;
          data_ptr_ = NULL;
      }
  }

    size_t GetTableId() const{
        return schema_ptr_->GetTableId();
    }

    void CopyTo(const Record *dst_record){
        memcpy(dst_record->data_ptr_, data_ptr_, schema_ptr_->GetSchemaSize());
    }

    void CopyFrom(const Record *src_record){
        memcpy(data_ptr_, src_record->data_ptr_, schema_ptr_->GetSchemaSize());
    }

    void SwapData(Record *src_record){
        char *tmp_ptr = data_ptr_;
        data_ptr_ = src_record->data_ptr_;
        src_record->data_ptr_ = tmp_ptr;
    }
    void * Get_Handle(){
        return handle_;
    }
    void Set_Handle(void * handle){
        handle_ = handle;
    }
    // set column. type can be any type.
    void SetColumn(const size_t &column_id, void *data){
//        printf("Memcpy to %p from %p size %lu\n", data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data, schema_ptr_->GetColumnSize(column_id));
//        fflush(stdout);
        memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data, schema_ptr_->GetColumnSize(column_id));
        if (dirty_col_ids.count(column_id) == 0){
            dirty_col_ids.insert(column_id);
        }
    }
    // rename.
    void UpdateColumn(const size_t &column_id, void*data){
        memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data, schema_ptr_->GetColumnSize(column_id));
        if (dirty_col_ids.count(column_id) == 0){
            dirty_col_ids.insert(column_id);
        }
    }

    // set column. type must be varchar.
    void SetColumn(const size_t &column_id, void* data, size_t size){
        assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR && schema_ptr_->GetColumnSize(column_id) >= size);
        memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data, size);
        if (dirty_col_ids.count(column_id) == 0){
            dirty_col_ids.insert(column_id);
        }
  }


//    // set column. type must be varchar.
//    void SetColumn(const size_t &column_id, void *data_str, const size_t &data_size){
//        assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR && schema_ptr_->GetColumnSize(column_id) >= data_size);
//        memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data_str, data_size);
//    }

    // set column. type must be varchar.
    void SetColumn(const size_t &column_id, const std::string &data){
        assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR && schema_ptr_->GetColumnSize(column_id) >= data.size());
        memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data.c_str(), data.size());
        if (dirty_col_ids.count(column_id) == 0){
            dirty_col_ids.insert(column_id);
        }
    }
    void ReSetRecord(const char* &data, size_t size){
        assert(schema_ptr_->GetSchemaSize() >= size);
        memcpy(data_ptr_, data, size);
    }
    void ReSetRecordBuff(char* data, size_t size, bool need_delete){
        assert(schema_ptr_->GetSchemaSize() == size);
        data_ptr_ = data;
        need_delete_ = need_delete;
    }

    // reference
    char* GetColumn(const size_t &column_id) const {
        return data_ptr_ + schema_ptr_->GetColumnOffset(column_id);
    }

    // copy data, memory allocated outside
    char* GetColumn(const size_t &column_id, void *data) const {
        memcpy(data, data_ptr_ + schema_ptr_->GetColumnOffset(column_id), schema_ptr_->GetColumnSize(column_id));
        return (char*)data;
    }

    // copy data, memory allocated inside
    // make sure the copy is not out-of-buffer.
//    void GetColumn(const size_t &column_id, char* &data) const {
//        assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR);
//        size_t size = schema_ptr_->GetColumnSize(column_id);
//        memcpy(data, data_ptr_ + schema_ptr_->GetColumnOffset(column_id), size);
//
//    }

    // copy data, memory allocated inside
    void GetColumn(const size_t &column_id, std::string &data) const {
        assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR);
        data.assign(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), 0, schema_ptr_->GetColumnSize(column_id));
    }

    const size_t& GetColumnSize(const size_t &column_id) const {
        return schema_ptr_->GetColumnSize(column_id);
    }

    const size_t& GetRecordSize() const {
        return schema_ptr_->GetSchemaSize();
    }

    const size_t& GetColumnCount() const {
        return schema_ptr_->GetColumnCount();
    }

    std::string GetPrimaryKey() const {
        size_t curr_offset = 0;
        size_t key_length = schema_ptr_->GetPrimaryKeyLength();
        if (key_length == 0){
            return std::string(data_ptr_, schema_ptr_->GetSchemaSize());
        }
        char *key_str = new char[key_length];
        for (size_t i = 0; i < schema_ptr_->GetPrimaryColumnCount(); ++i){
            memcpy(key_str + curr_offset, GetColumn(schema_ptr_->GetPrimaryColumnId(i)), schema_ptr_->GetPrimaryColumnSize(i));
            curr_offset += schema_ptr_->GetPrimaryColumnSize(i);
        }
        std::string ret_key(key_str, key_length);
        delete[] key_str;
        key_str = NULL;
        return ret_key;
    }
    void GetPrimaryKey(void* data) const {
        size_t curr_offset = 0;
        size_t key_length = schema_ptr_->GetPrimaryKeyLength();
        if (key_length == 0){
            assert(false);
//            return std::string(data_ptr_, schema_ptr_->GetSchemaSize());
        }
        char *key_str = (char*)data;
        for (size_t i = 0; i < schema_ptr_->GetPrimaryColumnCount(); ++i){
            memcpy(key_str + curr_offset, GetColumn(schema_ptr_->GetPrimaryColumnId(i)), schema_ptr_->GetPrimaryColumnSize(i));
            curr_offset += schema_ptr_->GetPrimaryColumnSize(i);
        }
    }


    std::string GetSecondaryKey(const size_t &index) const {
        size_t curr_offset = 0;
        size_t key_length = schema_ptr_->GetSecondaryKeyLength(index);
        if (key_length == 0){
            return std::string(data_ptr_, schema_ptr_->GetSchemaSize());
        }
        char *key_str = new char[key_length];
        for (size_t i = 0; i < schema_ptr_->GetSecondaryColumnCount(index); ++i){
            memcpy(key_str + curr_offset, GetColumn(schema_ptr_->GetSecondaryColumnId(index, i)), schema_ptr_->GetSecondaryColumnSize(index, i));
            curr_offset += schema_ptr_->GetSecondaryColumnSize(index, i);
        }
        std::string ret_key(key_str, key_length);
        delete[] key_str;
        key_str = NULL;
        return ret_key;
    }

    HashcodeType GetPartitionHashcode() const {
        HashcodeType hashcode = 0;
        for (size_t i = 0; i < schema_ptr_->GetPartitionColumnCount(); ++i){
            hashcode += *(HashcodeType*)GetColumn(schema_ptr_->GetPartitionColumnId(i));
        }
        return hashcode;
    }

        bool GetVisible() const {
            size_t meta_col_id = schema_ptr_->GetMetaColumnId();
            MetaColumn meta_col;
            GetColumn(meta_col_id, &meta_col);
            return meta_col.is_visible_;
        }

#if defined(TO)
        [[nodiscard]] uint64_t GetRTS() const {
            size_t meta_col_id = schema_ptr_->GetMetaColumnId();
            MetaColumn meta_col;
            GetColumn(meta_col_id, &meta_col);
            return meta_col.Rts_;
        }
        void PutRTS(uint64_t rts) {
            const size_t meta_col_id = schema_ptr_->GetMetaColumnId();
            MetaColumn meta_col;
            GetColumn(meta_col_id, &meta_col);
            meta_col.Rts_ = rts;
            SetColumn(meta_col_id, &meta_col);
        }
#endif
#if defined(TO) || defined(OCC) || defined(MVOCC)
        [[nodiscard]] uint64_t GetWTS() const {
            size_t meta_col_id = schema_ptr_->GetMetaColumnId();
            MetaColumn meta_col;
            GetColumn(meta_col_id, &meta_col);
            return meta_col.Wts_;
        }
        [[nodiscard]] MetaColumn GetMeta() const {
            size_t meta_col_id = schema_ptr_->GetMetaColumnId();
            MetaColumn meta_col;
            GetColumn(meta_col_id, &meta_col);
            return meta_col;
        }
        void PutWTS(uint64_t wts) {
            const size_t meta_col_id = schema_ptr_->GetMetaColumnId();
            MetaColumn meta_col;
            GetColumn(meta_col_id, &meta_col);
            assert(wts <0x700d2c00cbe9 || wts == UINT64_MAX);
            meta_col.Wts_ = wts;
            SetColumn(meta_col_id, &meta_col);
        }
        void PutMeta(MetaColumn meta) {
            const size_t meta_col_id = schema_ptr_->GetMetaColumnId();
            SetColumn(meta_col_id, &meta);
        }
#endif
        void SetVisible(bool val) {
            size_t meta_col_id = schema_ptr_->GetMetaColumnId();
            MetaColumn meta_col;
            this->GetColumn(meta_col_id, &meta_col);
            meta_col.is_visible_ = val;
            this->SetColumn(meta_col_id, &meta_col);
        }
        void roll_back(DeltaRecord *delta_record){
            // roll back the record to the previous version.
            char* start = delta_record->data_;
            char* end = start + delta_record->current_record_size_;
            while (start < end){
                size_t column_id = *(size_t*)start;
                start += sizeof(size_t);
                size_t column_size = *(size_t*)start;
                start += sizeof(size_t);
                memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), start, column_size);
                start += column_size;
            }
            // Also roll back the metadata.
            MetaColumn meta = GetMeta();
            meta.Wts_ = delta_record->Wts_;
            meta.prev_delta_ = delta_record->prev_delta_gaddr;
            PutMeta(meta);

        }


private:
    Record(const Record&);
    Record& operator=(const Record&);


public:
  RecordSchema *schema_ptr_;
  char *data_ptr_;
  bool need_delete_;
  size_t data_size_;
    bool is_visible_;
    void * handle_ = nullptr;
    std::set<uint64_t> dirty_col_ids;
};


}

#endif
