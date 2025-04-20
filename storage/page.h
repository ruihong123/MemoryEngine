
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
//    enum Index_Type { Primary_Idx = 0, Secondary_Idx = 1};
    template<class Key>
    struct SearchResult {
        bool is_leaf;
        uint8_t level;
        GlobalAddress slibing;
        GlobalAddress next_level;
        // for future pointer swizzling design
        ibv_mr* page_hint = nullptr;
        bool find_value = false;
#ifndef NDEBUG
        Key this_key;
        Key later_key;

        char key_padding[KEY_PADDING];
#endif
//        Value val;
        Slice val{};
//        char* val[kMaxAttributeLength];
//        size_t val_size;
//        char value_padding[VALUE_PADDING];
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


    template<typename T>
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
        //TODO: reserve struture or pointer for the invalidation bit (if not valid, a RDMA read is required),
        // globale address (for checking whether this page is what we want) cache handle pointer.
//        bool valid_page;
        int16_t last_index;
        uint8_t level;

        template<class K> friend class InternalPage;
        friend class RDMA_Manager;

        template<class K> friend class LeafPage;

        template<class K> friend class Btr;
//        friend class IndexCache;
//        uint16_t LeafRecordSize;
        uint16_t kLeafCardinality;
        //        T* try_var{};
        T lowest{};
        T highest{};
        Header_Index() {
            leftmost_ptr = GlobalAddress::Null();
            sibling_ptr = GlobalAddress::Null();
            dirty_upper_bound = 0;
            dirty_lower_bound = 0;
            last_index = -1;
//            valid_page = true;
            lowest = kKeyMin<T>;
            highest = kKeyMax<T>;
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
#ifdef CACHECOHERENCEPROTOCOL

//    template<class Key, class Value>
//    class LeafEntry {
//    public:
////        uint8_t f_version : 4;
//        Key key{};
////        char key_padding[KEY_PADDING] = "";
//        Value value{};
////        char value_padding[VALUE_PADDING] = "";
////        uint8_t r_version : 4;
//
//        LeafEntry() {
////            f_version = 0;
////            r_version = 0;
//            value = kValueNull<Key>;
//            key = 0;
////      key = {};
//        }
//    } __attribute__((packed));
#else
    class LeafEntry {
    public:
        uint8_t f_version : 4;
        Key key = {};
        char key_padding[KEY_PADDING] = "";
        Value value = {};
        char value_padding[VALUE_PADDING] = "";
        uint8_t r_version : 4;

        LeafEntry() {
            f_version = 0;
            r_version = 0;
            value = kValueNull;
            key = 0;
//      key = {};
        }
    } __attribute__((packed));
#endif
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
//    constexpr int kInternalCardinality =
//            (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) -RDMA_OFFSET) /
//            sizeof(InternalEntry);
//
//    constexpr int kLeafCardinality =
//            (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) / sizeof(LeafEntry);
    class CatalogPage {
        alignas(8) uint64_t global_lock;
    public:
        GlobalAddress root_gptrs[(kInternalPageSize -8) / sizeof(GlobalAddress)] = {};
    };

    template<class Key>
    class InternalPage {
        // private:
        //TODO: we can make the local lock metaddata outside the page.
    public:
        constexpr static int kInternalCardinality = (kInternalPageSize - sizeof(Header_Index<Key>) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) /
                                                    sizeof(InternalEntry<Key>);
//        Local_Meta local_lock_meta;
//        std::atomic<uint8_t> front_version;
//        uint8_t front_version;
        alignas(8) uint64_t global_lock;
//        uint8_t busy;
//        uint8_t front_version = 0;
        Header_Index<Key> hdr = {};
        InternalEntry<Key> records[kInternalCardinality] = {};
//        char data[1] = {};
//  uint8_t padding[InternalPagePadding];
//        alignas(8) uint8_t rear_version;

        template<class K> friend class Btr;
        friend class Cache;

    public:
        // this is called when tree grows, The page initialization will not reset the global lock byte.
        InternalPage(GlobalAddress left, const Key &key, GlobalAddress right, GlobalAddress this_page_g_ptr, bool secondary = false,
                     uint32_t level = 0) {
            assert(level> 0);
//            assert(STRUCT_OFFSET(InternalPage<Key>, local_lock_meta) == 0);
            if (secondary){
                hdr.p_type = P_Internal_P;

            }else{
                hdr.p_type = P_Internal_S;
            }
            hdr.leftmost_ptr = left;
            hdr.level = level;
//            hdr.p_version = 0;
//            hdr.valid_page = true;
//            global_lock = 0;
            records[0].key = key;
            records[0].ptr = right;
            records[1].ptr = GlobalAddress::Null();

            hdr.last_index = 0;
            assert(this_page_g_ptr!= GlobalAddress::Null());
            hdr.this_page_g_ptr = this_page_g_ptr;
        }

        explicit InternalPage(GlobalAddress this_page_g_ptr, bool secondary = false, uint32_t level = 0) {
            assert(level > 0);
            if (secondary){
                hdr.p_type = P_Internal_S;
            }else{
                hdr.p_type = P_Internal_P;
            }
            hdr.level = level;
//            global_lock = 0;
            records[0].ptr = GlobalAddress::Null();
//            local_metadata_init();
            assert(this_page_g_ptr!= GlobalAddress::Null());
            hdr.this_page_g_ptr = this_page_g_ptr;
        }

        void debug() const {
            std::cout << "InternalPage@ ";
            hdr.debug();
//            std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
//                      << "]" << std::endl;
        }
        bool internal_page_search(const Key &k, void *result_ptr);
        bool internal_page_store(GlobalAddress page_addr, const Key &k, GlobalAddress value, int level);
    };
#ifdef CACHECOHERENCEPROTOCOL
    template<typename TKey>
    class LeafPage {
    public:
//        constexpr static int kLeafCardinality = (kLeafPageSize - sizeof(Header<TKey>) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) / sizeof(LeafEntry<TKey, Value>);
//        Local_Meta local_lock_meta;
        // if busy we will not cache it in cache, switch back to the Naive
        alignas(8) uint64_t global_lock;
//        uint8_t busy;
//        uint8_t front_version;
        Header_Index<TKey> hdr;
#ifdef DYNAMIC_ANALYSE_PAGE
        char data_[1];// The data segment is beyond this class.
#else
                LeafEntry<TKey, Value> records[kLeafCardinality] = {};
#endif
//  uint8_t padding[LeafPagePadding];
//        uint8_t rear_version;

        template<class K> friend class Btr;
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
            hdr.kLeafCardinality = leaf_cardinality;
//            hdr.p_version = 0;
//            global_lock = 0;
//            records[0].value = {0};

//            front_version = 0;
//            rear_version = 0;
//            local_lock_meta.local_lock_byte = 0;
//            local_lock_meta.current_ticket = 0;
//            local_lock_meta.issued_ticket = 0;
//            local_lock_meta.hand_over = 0;
//            local_lock_meta.hand_time = 0;

//            embedding_lock = 1;
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
//        constexpr static int kLeafCardinality = (kLeafPageSize - sizeof(Header<TKey>) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) / sizeof(LeafEntry<TKey, Value>);
//        Local_Meta local_lock_meta;
        // if busy we will not cache it in cache, switch back to the Naive
        alignas(8) uint64_t global_lock;
//        uint8_t busy;
//        uint8_t front_version;
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
//            global_lock = 0;
//            records[0].value = {0};

//            front_version = 0;
//            rear_version = 0;
//            local_lock_meta.local_lock_byte = 0;
//            local_lock_meta.current_ticket = 0;
//            local_lock_meta.issued_ticket = 0;
//            local_lock_meta.hand_over = 0;
//            local_lock_meta.hand_time = 0;

//            embedding_lock = 1;
        }

//        void local_metadata_init(){
//            __atomic_store_n(&local_lock_meta.local_lock_byte, 0, mem_cst_seq);
//            local_lock_meta.current_ticket = 0;
//            local_lock_meta.issued_ticket = 0;
//
//            local_lock_meta.hand_over = 0;
//            local_lock_meta.hand_time = 0;
//        }

//        void Data_page_search(const TKey &k, SearchResult <TKey, Value> &result, ibv_mr local_mr_copied,
//                              GlobalAddress g_page_ptr, RecordSchema *record_scheme);
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

#else
    class LeafPage {
//    private:
        Local_Meta local_lock_meta;
        alignas(8) uint64_t global_lock;
        alignas(8) uint64_t reference_bitmap;
        uint8_t front_version;
        Header hdr;
        LeafEntry records[kLeafCardinality] = {};

//  uint8_t padding[LeafPagePadding];
        uint8_t rear_version;

        friend class Btr;

    public:
        LeafPage(GlobalAddress this_page_g_ptr, uint32_t level = 0) {
            hdr.level = level;
            hdr.this_page_g_ptr = this_page_g_ptr;
            records[0].value = kValueNull;

            front_version = 0;
            rear_version = 0;
            local_lock_meta.local_lock_byte = 0;
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;
            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;

//            embedding_lock = 1;
        }

//        void set_consistent() {
//            front_version++;
//            rear_version = front_version;
//#ifdef CONFIG_ENABLE_CRC
//            this->crc =
//        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//#endif
//        }

        bool check_consistent() const {

            bool succ = true;
#ifdef CONFIG_ENABLE_CRC
            auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
#endif

            succ = succ && (rear_version == front_version);
            if (!succ) {
                // this->debug();
            }

            return succ;
        }

        void debug() const {
            std::cout << "LeafPage@ ";
            hdr.debug();
            std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
                      << "]" << std::endl;
        }
        void leaf_page_search(const Key &k, SearchResult &result, ibv_mr local_mr_copied, GlobalAddress g_page_ptr);
    } __attribute__((packed));
#endif
}
#endif //SELCC_PAGE_H
