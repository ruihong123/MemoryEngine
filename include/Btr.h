#if !defined(_BTR_H_)
#define _BTR_H_


#include "storage/rdma.h"
#include "cache.h"
#include "DDSM.h"
#include "storage/page.h"
#include <atomic>
#include <city.h>
#include <functional>
#include "Timer.h"
#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
//#include <linux/membarrier.h>
#include "port/likely.h"
#include "utils/hash.h"
#include "port/port_posix.h"
#include "utils/Local_opt_locks.h"
//#include "storage/Table.h"
class IndexCache;

template<class Key, class Value>
struct Request {
    bool is_search;
    Key k;
    Value v;
};
template<class Key, class Value>
class RequstGen {
public:
    RequstGen() = default;
    virtual Request<Key,Value> next() { return Request<Key, Value>{}; }
};


//using CoroFunc = std::function<RequstGen<class Key, class Value> *(int, DSMEngine::RDMA_Manager *, int)>;
extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];
extern uint64_t invalid_counter[MAX_APP_THREAD][8];
extern uint64_t lock_fail[MAX_APP_THREAD][8];
extern uint64_t pattern[MAX_APP_THREAD][8];
extern uint64_t hierarchy_lock[MAX_APP_THREAD][8];
extern uint64_t handover_count[MAX_APP_THREAD][8];
extern uint64_t hot_filter_count[MAX_APP_THREAD][8];
extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
extern bool Show_Me_The_Print;
extern int TimePrintCounter[MAX_APP_THREAD];

namespace DSMEngine {
    struct DynamicCompoundKey {
        // This is a dynamic compound key that can be used in the B-tree.
        // this class is a helper class which enables the directly comparison between the dynamic compound keys.
        char* start = nullptr; // the compind key should always smaller than 1 KB.

        RecordSchema* schema_ptr = nullptr;

        DynamicCompoundKey(char* buff, RecordSchema* schema)
        : start(buff), schema_ptr(schema){}
        //copy constructor
        // 
        void copy_from(const DynamicCompoundKey& other) {
            assert(schema_ptr == other.schema_ptr);
            size_t key_length = schema_ptr->GetPrimaryKeyLength();
            std::memcpy(start, other.start, key_length);
        }
        // Move constructor 
        DynamicCompoundKey(DynamicCompoundKey&& other) noexcept {
            schema_ptr = other.schema_ptr;
            start = other.start;
            other.start = nullptr; 
        }
        // DynamicCompoundKey& operator=(const DynamicCompoundKey& other) {
        //     if (start)
        //     {
        //         assert(schema_ptr == other.schema_ptr);
        //         size_t key_length = schema_ptr->GetPrimaryKeyLength();
        //         std::memcpy(start, other.start, key_length);
        //     }else{
        //         schema_ptr = other.schema_ptr;
        //         start = other.start;
        //     }
        //     return *this;
        // }
        // Equality ==
        bool operator==(const DynamicCompoundKey& other) const {
            return compare(other) == 0;
        }

        // Inequality !=
        bool operator!=(const DynamicCompoundKey& other) const {
            return compare(other) != 0;
        }

        // Less than <
        bool operator<(const DynamicCompoundKey& other) const {
            return compare(other) < 0;
        }

        // Greater than >
        bool operator>(const DynamicCompoundKey& other) const {
            return compare(other) > 0;
        }

        // Less than or equal <=
        bool operator<=(const DynamicCompoundKey& other) const {
            return compare(other) <= 0;
        }

        // Greater than or equal >=
        bool operator>=(const DynamicCompoundKey& other) const {
            return compare(other) >= 0;
        }
        static DynamicCompoundKey& MinValue() {
            static char value_buff[1024] = {0};
            DynamicCompoundKey& k = *reinterpret_cast<DynamicCompoundKey*>(value_buff);
            if (schema_ptr == nullptr) {
                // Handle the case where schema_ptr is not set
                // This could be a static_assert or throw an exception
                // For now, we just return an empty key
                return k;
            }
            size_t key_length = schema_ptr->GetPrimaryKeyLength();
            // std::memset(k.start, 0, key_length);     
            return k;
        }
        static DynamicCompoundKey& MaxValue() {
            static char value_buff[1024] = {255};
            DynamicCompoundKey& k = *reinterpret_cast<DynamicCompoundKey*>(value_buff);
            if (schema_ptr == nullptr) {
                // Handle the case where schema_ptr is not set
                // This could be a static_assert or throw an exception
                // For now, we just return an empty key
                return k;
            }
            size_t key_length = schema_ptr->GetPrimaryKeyLength();
            // std::memset(k.start, 0, key_length);     
            return k;
        }
    

    private:
        int compare(const DynamicCompoundKey& other) const {
            using namespace DSMEngine;
            assert(schema_ptr != nullptr);

            const RecordSchema* schema = schema_ptr;
            size_t num_fields = schema->GetPrimaryColumnCount();
            size_t offset = 0;

            for (size_t i = 0; i < num_fields; ++i) {
                size_t col_id = schema->GetPrimaryColumnId(i);
                size_t size = schema->GetPrimaryColumnSize(i);
                const auto& type = schema->GetColumnType(col_id);

                const char* a = start + offset;
                const char* b = other.start + offset;

                switch (type) {
                    case ValueType::INT: {
                        int32_t va = *reinterpret_cast<const int32_t*>(a);
                        int32_t vb = *reinterpret_cast<const int32_t*>(b);
                        if (va != vb) return va < vb ? -1 : 1;
                        break;
                    }
                    case ValueType::INT64: {
                        int64_t va = *reinterpret_cast<const int64_t*>(a);
                        int64_t vb = *reinterpret_cast<const int64_t*>(b);
                        if (va != vb) return va < vb ? -1 : 1;
                        break;
                    }
                    case ValueType::FIXCHAR: {
                        int cmp = std::memcmp(a, b, size);
                        if (cmp != 0) return cmp < 0 ? -1 : 1;
                        break;
                    }
                    default:
                        fprintf(stderr, "[DynamicCompoundKey] Unsupported type in compare!\n");
                        std::abort();
                }

                offset += size;
            }

            return 0; // all parts equal
        }
    };
    typedef std::shared_ptr<DynamicCompoundKey> DynamicCompoundKeyPtr;

    template<class Key>
    class InternalPage;

    template<class Key>
    class LeafPage;


//TODO: There are two ways to define types in the btree, one is to define the types in the class, the other is to define the types in the template.
// the other is to attach a index_scheme_ptr. Currently we mixed these two ways, which is not good. We need to guarantee that
// the index_scheme_ptr is coherent with the template in the btree.
    template<typename Key>
    class Btr {
//friend class DSMEngine::InternalPage;
    public:
        struct iterator {
        public:
            iterator(LeafPage<Key> *node, Cache_Handle* handle, uint32_t position, RecordSchema *scheme_ptr, DDSM *dsm)
                    : node(node), handle(handle), position_idx(position), scheme_ptr(scheme_ptr), dsm(dsm) {
                valid = true;

            }
            iterator()= default;
            iterator(iterator&& iter) noexcept {
                node = iter.node;
                handle = iter.handle;
                position_idx = iter.position_idx;
                scheme_ptr = iter.scheme_ptr;
                dsm = iter.dsm;
                valid = iter.valid;
            }
            void initialize(LeafPage<Key> *node_t, Cache_Handle* handle_t, uint32_t position_t, RecordSchema *scheme_ptr_t, DDSM *dsm_t){
                node = node_t;
                handle = handle_t;
                position_idx = position_t;
                scheme_ptr = scheme_ptr_t;
                dsm = dsm_t;
                valid = true;
            }
            ~iterator(){
                if (handle != nullptr){
                    assert(node != nullptr);
                    dsm->SELCC_Shared_UnLock(handle->gptr, handle);
                }else{
                    assert(node == nullptr);
                }
            }
            void Get(Key& key, void* buff){
                node->GetByPosition(position_idx, scheme_ptr, key, buff);
            }
            void Next(){
                if (position_idx < node->hdr.last_index){
                    position_idx++;
                } else {
                    // todo: move to the next leaf node.
                    GlobalAddress next_leaf = node->hdr.sibling_ptr;
                    if (next_leaf == GlobalAddress::Null()){
                        valid = false;
                        return;
                    }
                    dsm->SELCC_Shared_UnLock(handle->gptr, handle);
                    void* page_buffer;
                    dsm->SELCC_Shared_Lock(page_buffer, next_leaf, handle);
                    node = (LeafPage<Key> *)page_buffer;
                    position_idx = 0;
                }
            }
//        void Prev();
            bool Valid(){
                return valid;
            }
            void SetValid(bool flag){
                valid = flag;
            }

        private:
            // The node in the tree the iterator is pointing at.
            LeafPage<Key> *node = nullptr;
            // The position_idx within the node of the tree the iterator is pointing at.
            Cache_Handle* handle = nullptr; // use the SELLC latch inside the handle to protect the access of iterator.
            uint32_t position_idx = 0; // offset within the leaf node
            bool valid = false;
            RecordSchema *scheme_ptr = nullptr;
            DDSM *dsm = nullptr;
        };


        // Assign a unique id to the tree, and allocate the root node by itself
        Btr(DDSM *dsm, Cache *cache_ptr, RecordSchema *record_scheme_ptr, uint16_t Btr_id, bool secondary = false);
        //Btree waiting for serialization. get the root node from memcached
        Btr(DDSM *dsm, Cache *cache_ptr, RecordSchema *record_scheme_ptr);

        void insert(const Key &k, const Slice &v);

        bool remove(const Key &k);

        bool search(const Key &k, const Slice &v);
        bool remove(const Key &k, const Slice &v);
        //Remember to destroy the iterator after use.
        iterator begin();
        // Finds the first element whose key is not less than key. the iterator always move forward.
        // TODO: make the return not a point but a moved object.
        iterator lower_bound(const Key &key);
        void clear_statistics();
        void Serialize(const char*& addr) {
            size_t off = 0;
            memcpy((char*)addr + off, &tree_id, sizeof(uint64_t));
            off += sizeof(uint64_t);
        }
        void Deserialize(const char*& addr) {
            size_t off = 0;
            memcpy(&tree_id, (char*)addr + off, sizeof(uint64_t));
            off += sizeof(uint64_t);
            //TODO: refactor the code below. need to move the attribute initialization to a separate function.
            // Some initialization of attributes which does not need to be serilizaed.
            Cache::Handle* dummy_mr;
            get_root_ptr_protected(dummy_mr);

        }
        static size_t GetSerializeSize() {
            return sizeof(uint64_t);
        }
        uint64_t GetRecordCount(){
            return num_of_record;
        }
        uint64_t GetRootRecordCount(){
            // This function is just for debugging purpose.
            Cache::Handle* handle = cached_root_page_handle.load();
            void* page_buffer;
            Header_Index<Key> * header = nullptr;
//            InternalPage<Key>* page = nullptr;
            ibv_mr* mr = (ibv_mr*)handle->value;
//            assert(mr == (ibv_mr*)handle->value);
            page_buffer = mr->addr;
            header = (Header_Index<Key> *) ((char *) page_buffer + (STRUCT_OFFSET(InternalPage<Key>, hdr)));
            // if is root, then we should always bypass the cache.
//            page = (InternalPage<Key> *)page_buffer;
            return header->last_index + 1;
        }
//    static RDMA_Manager * rdma_mg;
        // TODO: potential bug, if mulitple btrees shared the same retry counter, will it be a problem?
        //  used for the retry counter for nested function call such as sibling pointer access.
        static thread_local int nested_retry_counter;
        RecordSchema *index_scheme_ptr;
        uint64_t num_of_record = 0;
        uint16_t leaf_cardinality_ = 0;
        bool secondary_ = false;
    private:
        RWSpinMutex root_mtx;// in case of contention in the root cache
        uint64_t tree_id;
        GlobalAddress left_most_leaf = GlobalAddress::Null();
        // The cached_root_handle_ref is to keep track of the number of reference to the root page handle.
        // this is necessary because the access of cached_root_page_handle will bypass the cache,
        // so the refs in the handle will not be increased, when accessing the root node, which can result in
        // the access on an evicted/destroyed cache handle if the root node is not accessed frequetly.
        // To solve this problem we introduce another reference mechanism in the tree for the cached root node.
        // The btree fetch the root handle from the cache (with handle refs ++ implicitly), when the cached_root_handle is invalid,
        // the cache root handle will not immediately be replaced with handle's refs --. Instead, the invaliding thread
        // will wait for the unfinished access on the old root handle, by checking the cached_root_handle_ref.
        std::atomic<Cache::Handle*> cached_root_page_handle = nullptr;
        std::atomic<GlobalAddress> g_root_ptr = GlobalAddress::Null();
        std::atomic<uint8_t> tree_height = 0;
        static thread_local size_t round_robin_cur;
        static thread_local std::shared_mutex *lock_coupling_memo[define::kMaxLevelOfTree];
        static thread_local SearchResult<Key> *search_result_memo;
        std::vector<LocalLockNode *> local_locks;
//        std::vector<LocalLockNode *> local_locks;
        Cache *page_cache;
        DDSM* ddms_ = nullptr;
        RDMA_Manager *rdma_mg = nullptr;
#ifndef NDEBUG
#endif
        void print_verbose();

        void before_operation();

        GlobalAddress get_root_ptr_ptr();

        GlobalAddress get_root_ptr_protected(Cache::Handle *&root_hint_handle);

        GlobalAddress get_root_ptr(Cache::Handle *&root_hint_handle);

        void refetch_rootnode();

        bool update_new_root(GlobalAddress left, const Key &k, GlobalAddress right, int level, GlobalAddress old_root);
        void invalidate_root(GlobalAddress gptr){
            std::unique_lock<RWSpinMutex> l(root_mtx);
            if (gptr == g_root_ptr.load()) {
                g_root_ptr.store(GlobalAddress::Null());
            }
        };
        // Insert a key and a point at a particular level (level != 0), the node is unknown
        bool insert_internal(Key &k, GlobalAddress &v, int target_level);



        // Node ID in GLobalAddress for a tree pointer should be the id in the Memory pool
        // THis funciton will get the page by the page addr and search the pointer for the
        // next level if it is not leaf page. If it is a leaf page, just put the value in the
        // result. this funciton = fetch the page + internal page serach + leafpage search + re-read
        bool internal_page_search(GlobalAddress page_addr, const Key &k, SearchResult<Key> &result, int &level,
                                  bool isroot, Cache::Handle *handle);


        bool leaf_page_search(GlobalAddress page_addr, const Key &k, SearchResult<Key> &result, int level);
        bool leaf_page_delete(GlobalAddress page_addr, const Key &k, SearchResult<Key> &result, int level);
        // create a iterator for the range query.
        bool leaf_page_find(GlobalAddress page_addr, const Key &k, SearchResult<Key> &result, iterator &iter, int level);
//        void internal_page_search(const Key &k, SearchResult &result);

//    void leaf_page_search(LeafPage *page, const Key &k, SearchResult &result);
        // store a key and a pointer to an known internal node.
        // Note: node range [barrer1, barrer2)
        bool internal_page_store(GlobalAddress page_addr, Key &k, GlobalAddress &v, int level);

        //store a key and value to a leaf page [lowest, highest). If it is secondary index, then range could be [lowest, highest], where lowest == highest.
        // Our code logic make it impossible to have duplicated key like this [a,b,c,d,d,d,d] [d,e,e,e,e,f], where dupilated keys covers last few records in one node and
        // spill to the next node for a few records. We make sure this never happen by our code logics in split.
        bool leaf_page_store(GlobalAddress page_addr, const Key &k, const Slice &v, Key &split_key,
                             GlobalAddress &sibling_addr, int level);

//        bool leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
//                           CoroContext *cxt, int coro_id);

    };



}
#endif //BTR_H