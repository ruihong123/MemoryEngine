
//
// Created by wang4996 on 22-8-8.
//

#ifndef MEMORYENGINE_PAGE_H
#define MEMORYENGINE_PAGE_H

#include "Common.h"
#include "rdma.h"
#include "Tools/slice.h"
#include "storage/ColumnInfo.h"
#include "RecordSchema.h"
#include "Record.h"
#include "DynamicCompoundKey.h"
#include <iostream>

namespace DSMEngine {

    //TODO: merge Page type and index type.
    enum Page_Type {
        P_Plain = 0, P_Internal_P = 1, P_Internal_S = 2, P_Leaf_P = 3, P_Leaf_S = 4, P_Data = 5
    };

    struct SearchResult {
        bool is_leaf;
        uint8_t level;
        GlobalAddress slibing;
        GlobalAddress next_level;
        // for future pointer swizzling design
        ibv_mr *page_hint = nullptr;
        bool find_value = false;
#ifndef NDEBUG
        // only check the first 8 bytes of the dynamic key.
        DynamicCompoundKey this_key;
        DynamicCompoundKey later_key;
        char key_padding[KEY_PADDING];
#endif
        Slice val{};

        void Reset() {
            is_leaf = false;
            level = 0;
            slibing = GlobalAddress::Null();
            next_level = GlobalAddress::Null();
            page_hint = nullptr;
            find_value = false;
#ifndef NDEBUG
//            this_key = DynamicCompoundKey::MinValue();
//            later_key = DynamicCompoundKey::MinValue();
#endif
        }
    };


    class Header_Index {
    public:
        Page_Type p_type = P_Plain;
        uint16_t dirty_upper_bound = 0;
        uint16_t dirty_lower_bound = 0;
        uint64_t p_version;
        GlobalAddress this_page_g_ptr;
        //=============================
        GlobalAddress leftmost_ptr;
        GlobalAddress sibling_ptr;
        // the last index is initialized as -1 in leaf node and internal nodes,
        // only 0 in the root node.
        int16_t last_index;
        uint32_t key_size;
        uint32_t record_size;
        uint8_t level;
        uint16_t kCardinality;

        friend class InternalPage;

        friend class RDMA_Manager;

        friend class LeafPage;

        friend class Btr;

        Header_Index() {
            leftmost_ptr = GlobalAddress::Null();
            sibling_ptr = GlobalAddress::Null();
            dirty_upper_bound = 0;
            dirty_lower_bound = 0;
            p_version = 1;
            last_index = -1;
        }

        void merge_dirty_bounds(uint16_t dirty_lower, uint16_t dirty_upper) {
            assert(dirty_lower_bound <= dirty_upper_bound);
            if (dirty_upper_bound == 0) {
                dirty_upper_bound = dirty_upper;
                dirty_lower_bound = dirty_lower;
                return;
            }
            dirty_upper_bound = std::max(dirty_upper_bound, dirty_upper);
            dirty_lower_bound = std::min(dirty_lower_bound, dirty_lower);
        }

        void reset_dirty_bounds() {
            dirty_upper_bound = 0;
            dirty_lower_bound = 0;
        }

        void debug() const {
            std::cout << "leftmost=" << leftmost_ptr << ", "
                      << "sibling=" << sibling_ptr << ", "
                      << "level=" << (int) level << ","
                      << "cnt=" << last_index + 1 << ",";
//              << "range=[" << lowest << " - " << highest << "]";
        }

    } __attribute__ ((aligned (8)));

    constexpr int RDMA_OFFSET = 0; // sizeof(Local_Meta)
    class CatalogPage {
        alignas(8) uint64_t global_lock;
    public:
        GlobalAddress root_gptrs[(kInternalPageSize - 8) / sizeof(GlobalAddress)] = {};
    };


    class InternalPage {
    public:
        // static thread_local RecordSchema *index_scheme_ptr;
        alignas(8) uint64_t global_lock;
        Header_Index hdr = {};
        char data_[1];

        friend class Btr;

        friend class Cache;

    public:
        /* The index_scheme_ptr should be */
        InternalPage(GlobalAddress left, DynamicCompoundKey key, GlobalAddress right, GlobalAddress this_page_g_ptr,
                     uint16_t cardinality, RecordSchema *schema, uint32_t level = 0) {
            assert(level > 0);
            hdr.key_size = schema->GetPrimaryKeyLength();
            hdr.record_size = hdr.key_size + sizeof(GlobalAddress);
            // the data start after the hidden upperbound and lowerbound field.
            hdr.p_type = P_Internal_P;
            hdr.leftmost_ptr = left;
            hdr.level = level;
            SetRecordByIndex(0, key, right, schema);
            hdr.last_index = 0;
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kCardinality = cardinality;
            hdr.p_version = 1;
            SetHighest(DynamicCompoundKey::MaxValue(schema), schema);
            SetLowest(DynamicCompoundKey::MinValue(schema), schema);
        }

        explicit InternalPage(GlobalAddress this_page_g_ptr, RecordSchema *schema,
                              uint32_t level = 0) {
            assert(level > 0);
            hdr.key_size = schema->GetPrimaryKeyLength();
            hdr.record_size = hdr.key_size + sizeof(GlobalAddress);
            hdr.p_type = P_Internal_S;
            hdr.level = level;
            SetRecordByIndex(0, DynamicCompoundKey::MinValue(schema), GlobalAddress::Null(), schema);
            assert(this_page_g_ptr != GlobalAddress::Null());
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kCardinality = calculate_cardinality(kInternalPageSize, schema);
            hdr.p_version = 1;
            SetHighest(DynamicCompoundKey::MaxValue(schema), schema);
            SetLowest(DynamicCompoundKey::MinValue(schema), schema);
        }

        void SetRecordByIndex(int index, const DynamicCompoundKey &key, GlobalAddress gaddr, RecordSchema *schema) {
            uint32_t key_size = hdr.key_size;
            uint32_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            char *target_ptr = data_ptr + index * record_size;
            DynamicCompoundKey first_key(target_ptr, schema);
            first_key.deepcopy_from(key);
            auto *first_value = reinterpret_cast<GlobalAddress *>(target_ptr + key_size);
            *first_value = gaddr;
        }

        DynamicCompoundKey GetRecordKeyByIndex(int index, RecordSchema *schema) {
            uint16_t key_size = hdr.key_size;
            uint64_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            char *target_ptr = data_ptr + index * record_size;
            return {target_ptr, schema};
        }
        void GetRecordKeyByIndex_DeepCopy(int index, DynamicCompoundKey dest) {
            assert(hdr.key_size > 0 && hdr.record_size > 0);
            uint16_t key_size = hdr.key_size;
            uint64_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            char *from_ptr = data_ptr + index * record_size;
            std::memcpy(dest.start, from_ptr, key_size);
        }

        GlobalAddress GetRecordValueByIndex(int index) {
            uint16_t key_size = hdr.key_size;
            uint64_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            char *target_ptr = data_ptr + index * record_size;
            return *(GlobalAddress *) (target_ptr + key_size);
        }

        void SetHighest(DynamicCompoundKey highest, RecordSchema *scheme) {
            DynamicCompoundKey highest_key(data_, scheme);
            highest_key.deepcopy_from(highest);
        }

        void SetLowest(DynamicCompoundKey lowest, RecordSchema *scheme) {
            uint64_t key_size = scheme->GetPrimaryKeyLength();
            DynamicCompoundKey highest_key(data_ + key_size, scheme);
            highest_key.deepcopy_from(lowest);
        }

        DynamicCompoundKey GetHighest(RecordSchema *scheme) const {
            return {const_cast<char *>(data_), scheme};
        }

        DynamicCompoundKey GetLowest(RecordSchema *scheme) const {
            uint64_t key_size = scheme->GetPrimaryKeyLength();
            assert(key_size == hdr.key_size);
            return {const_cast<char *>(data_ + key_size), scheme};
        }

        static uint64_t calculate_cardinality(uint64_t page_size, RecordSchema *schema_ptr) {
            uint64_t key_size = schema_ptr->GetPrimaryKeyLength();
            uint64_t record_size = key_size + sizeof(GlobalAddress);
            // the last uint8_t is for the page forward checking. 2*key_size is for the hidden upperbound and lowerbound.
            return (page_size - STRUCT_OFFSET(InternalPage, data_[0]) - 2 * key_size - sizeof(uint8_t)) / record_size;
        }

        bool internal_page_search(const DynamicCompoundKey &k, void *result_ptr, RecordSchema *index_schema_ptr);

        bool internal_page_store(GlobalAddress page_addr, const DynamicCompoundKey &k, GlobalAddress value, int level,
                                 RecordSchema *schema_ptr, class RedoLogger* redo_logger = nullptr);
    };

    class LeafPage {
    public:
        // if busy we will not cache it in cache, switch back to the Naive
        alignas(8) uint64_t global_lock;
        Header_Index hdr;
        char data_[1];// The data segment is beyond this class.
        friend class Btr;

    public:
        LeafPage(GlobalAddress this_page_g_ptr, uint16_t leaf_cardinality, RecordSchema *schema,
                 uint32_t level = 0) {
            assert(level == 0);
            hdr.p_type = P_Leaf_P;
            hdr.level = level;
            hdr.key_size = schema->GetPrimaryKeyLength();
            hdr.record_size = schema->GetRecordTotalSize();
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kCardinality = leaf_cardinality;
            hdr.p_version = 1;
            SetHighest(DynamicCompoundKey::MaxValue(schema), schema);
            SetLowest(DynamicCompoundKey::MinValue(schema), schema);
        }

        void SetRecordByIndex(int index, Slice s, RecordSchema *schema) {
            uint16_t key_size = hdr.key_size;
            uint64_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            char *target_ptr = data_ptr + index * record_size;
            DynamicCompoundKey first_key(target_ptr, schema);
            // todo
        }

        DynamicCompoundKey GetRecordKeyByIndex(int index, RecordSchema *schema) {
            assert(hdr.key_size > 0 && hdr.record_size > 0);
            uint16_t key_size = hdr.key_size;
            uint64_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            char *target_ptr = data_ptr + index * record_size;
            return {target_ptr, schema};
        }

        void *GetRecordPtrByIndex(int index) {
            uint16_t key_size = hdr.key_size;
            uint64_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            char *target_ptr = data_ptr + index * record_size;
            return target_ptr;
        }

        void SetHighest(DynamicCompoundKey highest, RecordSchema *scheme) {
            DynamicCompoundKey highest_key(data_, scheme);
            highest_key.deepcopy_from(highest);
        }

        void SetLowest(DynamicCompoundKey lowest, RecordSchema *scheme) {
            uint64_t key_size = scheme->GetPrimaryKeyLength();
            DynamicCompoundKey lowest_key(data_ + key_size, scheme);
            lowest_key.deepcopy_from(lowest);
        }

        DynamicCompoundKey GetHighest(RecordSchema *scheme) const {
            return {const_cast<char *>(data_), scheme};
        }

        DynamicCompoundKey GetLowest(RecordSchema *scheme) const {
            uint64_t key_size = scheme->GetPrimaryKeyLength();
            return {const_cast<char *>(data_ + key_size), scheme};
        }

        static uint64_t calculate_cardinality(uint64_t page_size, RecordSchema *scheme) {
            uint64_t record_size = scheme->GetRecordTotalSize();
            uint64_t key_size = scheme->GetPrimaryKeyLength();
            // the last uint8_t is for the page forward checking. 2*key_size is for the hidden upperbound and lowerbound.
            return (page_size - STRUCT_OFFSET(LeafPage, data_[0]) - 2 * key_size - sizeof(uint8_t)) / record_size;
        }

        void leaf_page_search(const DynamicCompoundKey &k, SearchResult &result, GlobalAddress g_page_ptr,
                              RecordSchema *record_scheme);

        // search by lowerbound ( the range should include the target key). iter.Getkey <= k (target key is included)
        // note: we can not modify it to be set at the first key that is >= k, becuase it is possible that the returned index
        // can exceed the last index.
        int leaf_page_pos_lb(const DynamicCompoundKey &k, RecordSchema *record_scheme);

        int leaf_page_find_pos_ub(const DynamicCompoundKey &k, SearchResult &result, RecordSchema *record_scheme);

        void GetDeepByPosition(int pos, RecordSchema *schema_ptr, DynamicCompoundKey &key, void *buff);

        void GetShallowByPosition(int pos, RecordSchema *schema_ptr, DynamicCompoundKey &key, void *&buff);

        // if node is full return true, if not full return false.
        bool leaf_page_store(const DynamicCompoundKey &k, const Slice &v, int &cnt, RecordSchema *index_schema, 
                            class RedoLogger* redo_logger = nullptr, GlobalAddress page_addr = GlobalAddress::Null());

        // if need merge return true, if not needed return false.
        bool leaf_page_delete(const DynamicCompoundKey &k, int &cnt, SearchResult &result, RecordSchema *record_scheme,
                             class RedoLogger* redo_logger = nullptr, GlobalAddress page_addr = GlobalAddress::Null());
    };


    class Header {
    public:
        Page_Type p_type = P_Data;
        uint16_t dirty_upper_bound = 0;
        uint16_t dirty_lower_bound = 0;
        uint64_t p_version;
        GlobalAddress this_page_g_ptr;
        // =============================
        int32_t number_of_records;
        uint32_t kDataCardinality;
        uint32_t table_id;
        uint64_t lsn_;

        friend class RDMA_Manager;
        friend class DataPage;
        Header() {
            dirty_upper_bound = 0;
            dirty_lower_bound = 0;
            number_of_records = 0;
            p_version = 1;
            table_id = 0;
        }

        void merge_dirty_bounds(uint16_t dirty_lower, uint16_t dirty_upper) {
            assert(dirty_lower_bound <= dirty_upper_bound);
            if (dirty_upper_bound == 0) {
                dirty_upper_bound = dirty_upper;
                dirty_lower_bound = dirty_lower;
                return;
            }
            dirty_upper_bound = std::max(dirty_upper_bound, dirty_upper);
            dirty_lower_bound = std::min(dirty_lower_bound, dirty_lower);
        }

        void reset_dirty_bounds() {
            dirty_upper_bound = 0;
            dirty_lower_bound = 0;
        }
    } __attribute__ ((aligned (8)));


    class DataPage {
    public:
        alignas(8) uint64_t global_lock;
        Header hdr;
#ifdef DYNAMIC_ANALYSE_PAGE
        char data_[1];// The data segment is beyond this class.
#else
        LeafEntry<TKey, Value> records[kLeafCardinality] = {};
#endif
//  uint8_t padding[LeafPagePadding];
//        uint8_t rear_version;

        friend class Btr;

    public:
        DataPage(GlobalAddress this_page_g_ptr, uint32_t data_cardinality, uint32_t id) {
            hdr.p_type = P_Data;
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kDataCardinality = data_cardinality;
            hdr.table_id = id;
            hdr.number_of_records = 0;
            hdr.lsn_ = 0;
            hdr.p_version = 1;
            hdr.reset_dirty_bounds();

            uint32_t bitmap_words = (data_cardinality + 63) / 64;
            uint32_t bitmap_bytes = bitmap_words * sizeof(uint64_t);
            std::memset(data_, 0, bitmap_bytes);
        }

        static uint64_t calculate_cardinality(uint64_t page_size, uint64_t record_size) {
//            8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) / (8ull*schema_ptr_->GetRecordTotalSize() +1);
            return 8ull * (page_size - STRUCT_OFFSET(DataPage, data_[0]) - sizeof(uint64_t) - sizeof(uint8_t)) /
                   (8ull * record_size + 1);
        }

        bool InsertRecord(const Slice &tuple, int &cnt, RecordSchema *record_scheme, GlobalAddress &g_addr);

        bool AllocateRecord(int &cnt, RecordSchema *record_scheme, GlobalAddress &g_addr, char *&data_buffer);

        bool DeleteRecord(GlobalAddress g_addr, RecordSchema *record_scheme);

        int find_empty_spot_from_bitmap(uint64_t *bitmap, uint32_t number_of_bits);

        void set_bitmap(uint64_t *bitmap, size_t index);

        void reset_bitmap(uint64_t *bitmap, size_t index);
    };
}
#endif //SELCC_PAGE_H
