
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
#include <iostream>

namespace DSMEngine{
    //TODO: merge Page type and index type.
    enum Page_Type { P_Plain = 0, P_Internal_P = 1, P_Internal_S = 2, P_Leaf_P = 3, P_Leaf_S = 4, P_Data = 5};

    struct SearchResult {
        bool is_leaf;
        uint8_t level;
        GlobalAddress slibing;
        GlobalAddress next_level;
        // for future pointer swizzling design
        ibv_mr* page_hint = nullptr;
        bool find_value = false;
#ifndef NDEBUG
        // only check the first 8 bytes of the dynamic key.
        uint64_t this_key;
        uint64_t later_key;
        char key_padding[KEY_PADDING];
#endif
        Slice val{};
        void Reset(){
            is_leaf = false;
            level = 0;
            slibing = GlobalAddress::Null();
            next_level = GlobalAddress::Null();
            page_hint = nullptr;
            find_value = false;
#ifndef NDEBUG
            this_key = 0;
            later_key = 0;
#endif
        }
    };


    class Header_Index {
    public:
        Page_Type p_type = P_Plain;
        uint16_t dirty_upper_bound = 0;
        uint16_t dirty_lower_bound = 0;
//        uint64_t p_version = 0;
        GlobalAddress this_page_g_ptr;
        //=============================
        GlobalAddress leftmost_ptr;
        GlobalAddress sibling_ptr;
        // the last index is initialized as -1 in leaf node and internal nodes,
        // only 0 in the root node.
        int16_t last_index;
        uint8_t level;
        uint16_t kCardinality;

        template<class K> friend class InternalPage;
        friend class RDMA_Manager;

        friend class LeafPage;

        friend class Btr;
        Header_Index() {
            leftmost_ptr = GlobalAddress::Null();
            sibling_ptr = GlobalAddress::Null();
            dirty_upper_bound = 0;
            dirty_lower_bound = 0;
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
                      << "level=" << (int)level << ","
                      << "cnt=" << last_index + 1 << ",";
//              << "range=[" << lowest << " - " << highest << "]";
        }
        DynamicCompoundKey get_highest() const {

        }
        DynamicCompoundKey get_lowest() const {

        }

    } __attribute__ ((aligned (8)));

    template<class Key>
    class InternalEntry {
    public:
        Key key = {};
//        char key_padding[KEY_PADDING] = "";
        GlobalAddress ptr = GlobalAddress::Null();
        InternalEntry() {
//            ptr = GlobalAddress::Null();
//    key = 0;
//            key = {};
        }
    } __attribute__((packed));
    //TODO (potential bug): recalcuclate the kInternalCardinality, if we take alignment into consideration
    // the caculation below may not correct.
    struct Local_Meta {
        uint8_t issued_ticket;
        uint8_t current_ticket;
        uint8_t local_lock_byte;
        uint8_t hand_time;
        uint32_t hand_over;//can be only 1 byte.
    };
    constexpr int RDMA_OFFSET  = 0; // sizeof(Local_Meta)
    class CatalogPage {
        alignas(8) uint64_t global_lock;
    public:
        GlobalAddress root_gptrs[(kInternalPageSize -8) / sizeof(GlobalAddress)] = {};
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
        InternalPage(GlobalAddress left, const Key &key, GlobalAddress right, GlobalAddress this_page_g_ptr, int cardinality, 
            RecordSchema *scheme, bool secondary = false,  uint32_t level = 0) {
            assert(level> 0);
            if (secondary){
                hdr.p_type = P_Internal_P;

            }else{
                hdr.p_type = P_Internal_S;
            }
            hdr.leftmost_ptr = left;
            hdr.level = level;
            
            records[0].key = key;
            records[0].ptr = right;
            records[1].ptr = GlobalAddress::Null();
            hdr.last_index = 0;
            assert(this_page_g_ptr!= GlobalAddress::Null());
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kCardinality = cardinality;
        }
        void SetHigest(DynamicCompoundKey& highest, RecordSchema *scheme) {
            DynamicCompoundKey highest_key(data_, scheme);
            highest_key.copy_from(highest);
        }
        void SetLowest(DynamicCompoundKey& lowest, RecordSchema *scheme) {
            uint64_t key_size = scheme->GetPrimaryKeyLength();
            DynamicCompoundKey highest_key(data_+key_size, scheme);
            highest_key.copy_from(highest);
        }
        DynamicCompoundKey GetHighest(RecordSchema *scheme) const {
            return DynamicCompoundKey(data_, scheme);
        }
        DynamicCompoundKey GetLowest(RecordSchema *scheme) const {
            uint64_t key_size = scheme->GetPrimaryKeyLength();
            return DynamicCompoundKey(data_ + key_size, scheme);
        }

        explicit InternalPage(GlobalAddress this_page_g_ptr, bool secondary = false, uint32_t level = 0) {
            assert(level > 0);
            if (secondary){
                hdr.p_type = P_Internal_S;
            }else{
                hdr.p_type = P_Internal_P;
            }
            hdr.level = level;
            records[0].ptr = GlobalAddress::Null();
            assert(this_page_g_ptr!= GlobalAddress::Null());
            hdr.this_page_g_ptr = this_page_g_ptr;
        }
        bool internal_page_search(const Key &k, void *result_ptr);
        bool internal_page_store(GlobalAddress page_addr, const Key &k, GlobalAddress value, int level);
    };

    class LeafPage {
    public:
        // if busy we will not cache it in cache, switch back to the Naive
        alignas(8) uint64_t global_lock;
        Header_Index hdr;
#ifdef DYNAMIC_ANALYSE_PAGE
        char data_[1];// The data segment is beyond this class.
#else
                LeafEntry<TKey, Value> records[kLeafCardinality] = {};
#endif

        friend class Btr;
    public:
        LeafPage(GlobalAddress this_page_g_ptr, uint16_t leaf_cardinality, uint16_t leaf_recordsize, bool secondary = false,
                 uint32_t level = 0) {
            assert(level == 0);
            if(!secondary){
                hdr.p_type = P_Leaf_P;

            }else{
                hdr.p_type = P_Leaf_S;
            }
            hdr.level = level;
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kCardinality = leaf_cardinality;
        }
        static uint64_t calculate_cardinality(uint64_t page_size, uint64_t record_size) {
            return (page_size - STRUCT_OFFSET(LeafPage<TKey>, data_[0]) - sizeof(uint8_t)) / record_size;
        }
        void leaf_page_search(const TKey &k, SearchResult<TKey> &result, GlobalAddress g_page_ptr,
                              RecordSchema *record_scheme);
        //search by lowerbound (include the target key).
        int leaf_page_pos_lb(const TKey &k, GlobalAddress g_page_ptr, RecordSchema *record_scheme);
        int leaf_page_find_pos_ub(const TKey &k, SearchResult<TKey> &result, RecordSchema *record_scheme);
        void GetByPosition(int pos, RecordSchema *schema_ptr, TKey &key, void* buff);
        // if node is full return true, if not full return false.
        bool leaf_page_store(const TKey &k, const Slice &v, int &cnt, RecordSchema *record_scheme);
        // if need merge return true, if not needed return false.
        bool leaf_page_delete(const TKey &k, int &cnt, SearchResult<TKey> &result, RecordSchema *record_scheme);
    };

            
    class Header {
    public:
        Page_Type p_type = P_Data;
        uint16_t dirty_upper_bound = 0;
        uint16_t dirty_lower_bound = 0;
//        uint64_t p_version = 0;
        GlobalAddress this_page_g_ptr;
        // =============================
        int32_t number_of_records;
        friend class RDMA_Manager;
        friend class DataPage;
        uint32_t kDataCardinality;
        uint32_t table_id;
        Header() {
            dirty_upper_bound = 0;
            dirty_lower_bound = 0;
            number_of_records = 0;
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

        template<class K> friend class Btr;

    public:
        DataPage(GlobalAddress this_page_g_ptr, uint32_t data_cardinality, uint32_t id) {
            hdr.p_type = P_Data;
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kDataCardinality = data_cardinality;
            hdr.table_id = id;
        }
        static uint64_t calculate_cardinality(uint64_t page_size, uint64_t record_size) {
//            8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) / (8ull*schema_ptr_->GetSchemaSize() +1);
            return 8ull*(page_size - STRUCT_OFFSET(DataPage, data_[0]) - sizeof(uint64_t) - sizeof(uint8_t)) / (8ull*record_size +1);
        }
        bool InsertRecord(const Slice &tuple, int &cnt, RecordSchema *record_scheme, GlobalAddress& g_addr);
        bool AllocateRecord(int &cnt, RecordSchema *record_scheme, GlobalAddress& g_addr, char*& data_buffer);
        bool DeleteRecord(GlobalAddress g_addr, RecordSchema *record_scheme);
        int find_empty_spot_from_bitmap(uint64_t* bitmap, uint32_t number_of_bits);
        void set_bitmap(uint64_t* bitmap, size_t index);
        void reset_bitmap(uint64_t* bitmap, size_t index);
    };
}
#endif //SELCC_PAGE_H
