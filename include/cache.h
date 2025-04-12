// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the table_cache
// capacity.  For example, a table_cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin table_cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable table_cache sizing, etc.)

#ifndef STORAGE_DSMEngine_INCLUDE_CACHE_H_
#define STORAGE_DSMEngine_INCLUDE_CACHE_H_

//#define LOCAL_LOCK_DEBUG
#include <cstdint>
#include <set>
#include "Tools/export.h"
#include "Tools/slice.h"
#include <boost/thread.hpp>
#include <stack>
#include "Config.h"
#include "storage/rdma.h"
#ifdef TIMEPRINT
void Reset_cache_counters();
uint64_t Calculate_cache_counters();
#endif
namespace DSMEngine {
//class RDMA_Manager;
//class ibv_mr;


    // LRU table_cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the table_cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the table_cache.
//
// The table_cache keeps two linked lists of items in the table_cache.  All items in the
// table_cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the table_cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the table_cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.

class DSMEngine_EXPORT Cache;

// Create a new table_cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
DSMEngine_EXPORT Cache* NewLRUCache(size_t capacity);
#define BUFFER_HANDOVER
//#define EARLY_LOCK_RELEASE
//TODO: Early lock release still buggy, the latch sometime will be released twice.
constexpr uint8_t Invalid_Node_ID = 255;

    struct Cache_Handle {
        struct PendingPageForward{
            std::atomic<uint8_t > next_holder_id = Invalid_Node_ID;
            std::atomic<void*> next_receive_page_buf = nullptr;
            std::atomic<uint32_t> next_receive_rkey = 0;
            std::atomic<uint8_t > starvation_priority = 0;
            std::atomic<RDMA_Command_Type > next_inv_message_type = invalid_command_;
            void SetStates(uint8_t next_holder_id, void* next_receive_buf, uint32_t next_receive_rkey, uint8_t starvation_priority, RDMA_Command_Type next_inv_message_type){
                this->next_holder_id.store(next_holder_id);
                this->next_receive_page_buf.store(next_receive_buf);
                this->next_receive_rkey.store(next_receive_rkey);
                this->starvation_priority.store(starvation_priority);
                this->next_inv_message_type.store(next_inv_message_type);
            }
            void ClearStates(){
                this->next_holder_id.store(Invalid_Node_ID);
                this->next_receive_page_buf.store(nullptr);
                this->next_receive_rkey.store(0);
                this->starvation_priority.store(0);
                this->next_inv_message_type.store(invalid_command_);
            }
            void AssertStatesCleared(){
#ifndef NDEBUG
                assert(this->next_holder_id == Invalid_Node_ID);
                assert(this->next_receive_page_buf == nullptr);
                assert(this->next_receive_rkey == 0);
                assert(this->starvation_priority == 0);
                assert(this->next_inv_message_type == invalid_command_);
#endif
            }
            void AssertStatesExist(){
#ifndef NDEBUG
                assert(this->next_holder_id != Invalid_Node_ID);
                assert(this->next_receive_page_buf != nullptr);
                assert(this->next_receive_rkey != 0);
//                assert(this->starvation_priority != 0);
                assert(this->next_inv_message_type != invalid_command_);
#endif
            }
        };
    public:
        void* value = nullptr; // NOTE: the value is the pointer to ibv_mr not the buffer!!!! be careful.
        std::atomic<uint32_t> refs;     // References, if zero this cache is in the free list.
        //TODO: the internal node may not need the rw_mtx below, maybe we can delete them.
        std::atomic<int> remote_lock_status = 0; // 0 unlocked, 1 read locked, 2 write lock
        GlobalAddress gptr = GlobalAddress::Null();
        // TODO: the variable below can be removed.
        std::atomic<int> lock_pending_num = 0;
#ifdef WRITER_STARV_SPIN_BASE
        std::atomic<int> reader_spin_time = 0;
        uint8_t last_writer_starvation_priority = 0;
#endif
//        std::chrono::time_point<std::chrono::high_resolution_clock> timer_begin;
        RWSpinMutex rw_mtx; // low overhead rw spin lock and write have higher priority than read.
        SpinMutex buffered_inv_mtx; // clear state mutex
        std::atomic<uint16_t> read_lock_counter = 0;
        std::atomic<uint16_t> write_lock_counter = 0;
//        std::atomic<int> read_lock_holder_num = 0;
//        std::atomic<int> lock_handover_count = 0;
//        std::atomic<bool> timer_on = false;
//        std::atomic<bool> timer_alarmed = false;

        //1 reader invalidation urge. 2 writer invalidation urge
        //TODO: make urging type inside buffer_inv_message
        std::atomic<uint8_t > remote_urging_type = 0;
        //TODO: make the pending page forward remember mulitple read invalidation request, and process accordingly
        PendingPageForward buffer_inv_message;
//#ifdef DIRTY_ONLY_FLUSH
// store the dirty boundary in the page content.
//        uint16_t dirty_upper_bound = 0;
//        uint16_t dirty_lower_bound = 0;
//#endif
//        std::atomic<uint8_t > remote_xlock_next = 0;
//        std::atomic<uint8_t> strategy = 1; // strategy 1 normal read write locking without releasing, strategy 2. Write lock with release, optimistic latch free read.
#ifndef PAGE_FREE_LIST
        bool keep_the_mr = false;
#endif
#ifndef NDEBUG
//        uint16_t last_modifier_thread_id = 0;
#endif
//#ifdef EARLY_LOCK_RELEASE
//        bool mr_in_use = false;
//#endif
//        std::shared_mutex rw_mtx;
        static RDMA_Manager* rdma_mg;
#ifdef LOCAL_LOCK_DEBUG
        std::set<uint16_t> holder_ids;
        std::mutex holder_id_mtx;
#endif
        void (*deleter)(Cache_Handle* handle);
        ~Cache_Handle(){}
        void clear_pending_inv_states(){
//            buffered_inv_mtx.lock();
//            lock_pending_num.store(0);
//            read_lock_holder_num.store(0);
//            lock_handover_count.store(0);
//            timer_on.store(false);
//            timer_alarmed.store(false);
            read_lock_counter.store(0);
            write_lock_counter.store(0);
            remote_urging_type.store(0);
#ifdef WRITER_STARV_SPIN_BASE
            last_writer_starvation_priority = 0;
//            reader_spin_time.store(0);
#endif
            buffer_inv_message.ClearStates();

//#ifdef EARLY_LOCK_RELEASE
//            mr_in_use = false;
//#endif
//            remote_xlock_next.store(0);
//            strategy.store(1);
//            buffered_inv_mtx.unlock();
        }
        void assert_no_handover_states(){
#ifndef NDEBUG
//            assert(lock_pending_num == 0);
//            assert(read_lock_counter == 0);
//            assert(write_lock_counter == 0);
            assert(remote_urging_type == 0);
            buffer_inv_message.AssertStatesCleared();
#endif

        }
        void assert_with_handover_states(){
#ifndef NDEBUG
//            assert(lock_pending_num != 0);
//            assert(read_lock_counter != 0);
//            assert(write_lock_counter != 0);
            assert(remote_urging_type != 0);
            buffer_inv_message.AssertStatesExist();
#endif

        }
        void reader_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);
        bool try_reader_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);

        void reader_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *mr);
        void updater_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);
        bool try_upgrade_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);

        bool try_updater_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);

        void upgrade_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);

        void updater_writer_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);
//        void invalidate_current_entry(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *mr, ibv_mr* cas_mr);
        // Blind write, carefully used.
        void writer_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);
        void writer_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);
        bool global_Rlock_update(ibv_mr *local_mr, GlobalAddress lock_addr, ibv_mr *cas_buffer, CoroContext *cxt = nullptr,
                                 int coro_id = 0);
        void process_buffered_inv_message(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr,
                                          ibv_mr *mr, bool need_spin);
        void drop_buffered_inv_message(ibv_mr *local_mr, RDMA_Manager *rdma_mg);
    };

class DSMEngine_EXPORT Cache {
 public:
  Cache() = default;
    typedef Cache_Handle Handle;

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the table_cache.

  virtual size_t GetCapacity() = 0;

  //TODO: change the slice key to GlobalAddress& key.

  // Insert a mapping from key->value into the table_cache and assign it
  // the specified charge against the total table_cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(Cache::Handle* handle)) = 0;

  // If the table_cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;
  //Atomic cache look up and Insert a new handle atomically for the key if not found.
  virtual Handle* LookupInsert(const Slice& key, void* value,
                                size_t charge,
                                void (*deleter)(Cache::Handle* handle)) = 0;
  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;
    virtual void Release_Inv(Handle* handle) = 0;
  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the table_cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;
//TODO: a new function which pop out a least recent used entry. which will be reused later.

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same table_cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its table_cache keys.
  virtual uint64_t NewId() = 0;

  // Remove all table_cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Internal_and_Leaf implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // DSMEngine may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // table_cache.
  virtual size_t TotalCharge() const = 0;

 private:
  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;
};

    struct LRUHandle : Cache::Handle {


        LRUHandle* next_hash;// Next LRUhandle in the hash
        std::atomic<LRUHandle*> next;
        std::atomic<LRUHandle*> prev;
        uint64_t charge;
        size_t key_length;
        std::atomic<bool> in_cache;     // Whether entry is in the table_cache.
        uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
//        char key_data[1];  // Beginning of key
        void init(){
            assert(remote_lock_status == 0);
            assert(remote_urging_type == 0);
            gptr = GlobalAddress::Null();

            deleter = nullptr;
            charge = 0;
            key_length = 0;
            hash = 0;
            in_cache = false;
            refs = 0;  // for the returned handle.
        }
        Slice key() const {
            // next_ is only equal to this if the LRU handle is the list head of an
            // empty list. List heads never have meaningful keys.
            assert(next != this);
            assert(key_length == 8);
            return Slice((char*)&gptr, key_length);
        }
    };
class LocalBuffer {

    public:
    LocalBuffer(const CacheConfig &cache_config);

        uint64_t data;
        uint64_t size;

    private:
    };

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
public:
    HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
    ~HandleTable() { delete[] list_; }

    LRUHandle* Lookup(const Slice& key, uint32_t hash) {
        assert(elems_ == page_cache_shadow.size());
        return *FindPointer(key, hash);
    }
    //
    LRUHandle* Insert(LRUHandle* h) {
        assert(elems_ == page_cache_shadow.size());
        LRUHandle** ptr = FindPointer(h->key(), h->hash);
        LRUHandle* old = *ptr;
        // if we find a LRUhandle whose key is same as h, we replace that LRUhandle
        // with h, if not find, we just put the h at the end of the list.
        h->next_hash = (old == nullptr ? nullptr : old->next_hash);
        *ptr = h;
        if (old == nullptr) {
            ++elems_;
            if (elems_ > length_) { // length_ is the size limit for current cache.
                // Since each table_cache entry is fairly large, we aim for a small
                // average linked list length (<= 1).
                Resize();
            }
        }
#ifndef NDEBUG
        GlobalAddress gprt = h->key().ToGlobalAddress();
//        fprintf(stdout, "page of %lu is inserted into the cache table\n", gprt.offset);
        page_cache_shadow.insert({gprt, h});
#endif
        return old;
    }

    LRUHandle* Remove(Slice key, uint32_t hash) {
        assert(elems_ == page_cache_shadow.size());
#ifndef NDEBUG
        GlobalAddress gprt = key.ToGlobalAddress();
//          printf("page of %lu is removed from the cache table", gprt.offset);
        auto erased_num  = page_cache_shadow.erase(key.ToGlobalAddress());
#endif
        LRUHandle** ptr = FindPointer(key, hash);
        LRUHandle* result = *ptr;
        //TODO: only erase those lru handles which has been accessed. THis can prevent
        // a index block being invalidated multiple times.
        if (result != nullptr) {
            //*ptr belongs to the Handle previous to the result.
            *ptr = result->next_hash;// ptr is the "next_hash" in the handle previous to the result
            --elems_;
            assert(erased_num == 1);
        }else{
            assert(false);
        }
        return result;
    }


private:
    // The table consists of an array of buckets where each bucket is
    // a linked list of table_cache entries that hash into the bucket.
    uint32_t length_;
    uint32_t elems_;
    LRUHandle** list_;
#ifndef NDEBUG
    std::map<uint64_t , void*> page_cache_shadow;
#endif
    // Return a pointer to slot that points to a table_cache entry that
    // matches key/hash.  If there is no such table_cache entry, return a
    // pointer to the trailing slot in the corresponding linked list.
    LRUHandle** FindPointer(Slice key, uint32_t hash) {
        LRUHandle** ptr = &list_[hash & (length_ - 1)];
        while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
            ptr = &(*ptr)->next_hash;
        }
//#ifndef NDEBUG
//      if (*ptr == nullptr){
////          void* returned_ptr = page_cache_shadow[key.ToGlobalAddress()];
//          assert(page_cache_shadow.find(key.ToGlobalAddress()) == page_cache_shadow.end());
//      }else{
//          assert(page_cache_shadow.find(key.ToGlobalAddress()) != page_cache_shadow.end());
//        }
//#endif
        // This iterator will stop at the LRUHandle whose next_hash is nullptr or its nexthash's
        // key and hash value is the target.
        return ptr;
    }

    void Resize() {
        uint32_t new_length = 4;// it originally is 4
        while (new_length < elems_) {
            new_length *= 2;
        }
        LRUHandle** new_list = new LRUHandle*[new_length];
        memset(new_list, 0, sizeof(new_list[0]) * new_length);
        uint32_t count = 0;
        //TOTHINK: will each element in list_ be supposed to have only one element?
        // Probably yes.
        for (uint32_t i = 0; i < length_; i++) {
            LRUHandle* h = list_[i];
            while (h != nullptr) {
                LRUHandle* next = h->next_hash;
                uint32_t hash = h->hash;
                LRUHandle** ptr = &new_list[hash & (new_length - 1)];
                h->next_hash = *ptr;
                *ptr = h;
                h = next;
                count++;
            }
        }
        assert(elems_ == count);// the elems_ is larger than the actual count now. It is not correct.
        delete[] list_;
        list_ = new_list;
        length_ = new_length;
    }
};
// A single shard of sharded table_cache.
class LRUCache {
public:
    LRUCache();
    ~LRUCache();
#ifdef PAGE_FREE_LIST
    void init();
#endif
    // Separate from constructor so caller can easily make an array of LRUCache
    void SetCapacity(size_t capacity) { capacity_ = capacity; }

    // Like Cache methods, but with an extra "hash" parameter.
    Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                          size_t charge,
                          void (*deleter)(Cache::Handle* handle));
    Cache::Handle* Lookup(const Slice& key, uint32_t hash);
    Cache::Handle* LookupInsert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(Cache::Handle* handle));
    //TODO: make the release not acquire the cache lock.
    void Release(Cache::Handle* handle);
    void Release_Inv(Cache::Handle* handle);
    void Erase(const Slice& key, uint32_t hash);
    void Prune();
    size_t TotalCharge() const {
//    MutexLock l(&table_mutex_);
//    ReadLock l(&table_mutex_);
        std::shared_lock<RWSpinMutex> l(table_mutex_);
        return usage_;
    }
    //support concurrent access.
    void push_free_list(LRUHandle* e);
    LRUHandle* pop_free_list();
    bool need_eviction();
    void prepare_free_list();
    std::pair<LRUHandle*, LRUHandle*> bulk_remove_LRU_list(size_t size);
    void bulk_insert_free_list(std::pair<LRUHandle *, LRUHandle *> start_end, size_t size);

private:
    void List_Remove(LRUHandle* e);
    void List_Append(LRUHandle* list, LRUHandle* e);
    void Ref(LRUHandle* e);
//    void Ref_in_LookUp(LRUHandle* e);
        void Unref(LRUHandle *e);
    void Unref_Inv(LRUHandle *e);
//    void Unref_WithoutLock(LRUHandle* e);
    bool FinishErase(LRUHandle *e) EXCLUSIVE_LOCKS_REQUIRED(table_mutex_);
    // todo: make the table_mutex_ a shared mutex. and hence reduce the bottle neck in the cache table.
//    mutable SpinMutex table_mutex_;
    // Initialized before use.
    size_t capacity_;

    // table_mutex_ protects the following state.
//  mutable port::RWMutex table_mutex_;
    size_t usage_;

    mutable RWSpinMutex table_mutex_;
    HandleTable table_;


    // Dummy head of LRU list.
    // lru.prev is newest entry, lru.next is oldest entry.
    // Entries have refs==1 and in_cache==true.
    LRUHandle lru_;
    size_t lru_size_ = 0;

    // Dummy head of in-use list.
    // Entries are in use by clients, and have refs >= 2 and in_cache==true.
    LRUHandle in_use_;
    mutable SpinMutex list_mutex_;


#ifdef PAGE_FREE_LIST
    mutable SpinMutex free_list_mtx_;
    LRUHandle free_list_;
    size_t free_list_size_;
    size_t free_list_trigger_limit_;
#endif
//    static std::atomic<uint64_t> counter;
};

}  // namespace DSMEngine

#endif  // STORAGE_DSMEngine_INCLUDE_CACHE_H_
