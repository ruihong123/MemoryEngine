// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "include/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
//#include <infiniband/verbs.h>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "utils/hash.h"
#include "utils/mutexlock.h"
#include "HugePageAlloc.h"
#include "rdma.h"
#include "storage/page.h"

// DO not enable the two at the same time otherwise there will be a bug.


uint64_t cache_miss[MAX_APP_THREAD][8]; // cache miss include invalidation.
uint64_t cache_hit_valid[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
extern bool Show_Me_The_Print;
#ifdef TIMEPRINT
uint64_t cache_lookup_total[MAX_APP_THREAD] = {0};
uint64_t cache_lookup_times[MAX_APP_THREAD] = {0};
void Reset_cache_counters(){
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        cache_lookup_total[i] = 0;
        cache_lookup_times[i] = 0;
    }

}

uint64_t Calculate_cache_counters(){
    uint64_t sum = 0;
    uint64_t times = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        sum += cache_lookup_total[i];
        times += cache_lookup_times[i];
    }
    return sum/times;

}
#endif

int TimePrintCounter[MAX_APP_THREAD];
namespace DSMEngine {
//std::atomic<uint64_t> LRUCache::counter = 0;
RDMA_Manager* Cache::Handle::rdma_mg = nullptr;
Cache::~Cache() {}







LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
  free_list_.next = &free_list_;
  free_list_.prev = &free_list_;
  free_list_size_ = 0;

}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
      Unref(e);
    e = next;
  }
}
#ifdef PAGE_FREE_LIST
void LRUCache::init(){
    size_t cache_line_limit = capacity_/kLeafPageSize+1;
    free_list_trigger_limit_ = cache_line_limit * FREELIST_RATIO;
    for (size_t i = 0; i < capacity_/kLeafPageSize+1; ++i) {
        auto e = new LRUHandle();
        e->init();
        auto mr = new ibv_mr{};
        auto rdma_mg = RDMA_Manager::Get_Instance();
        rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
        e->value = mr;
        push_free_list(e);
    }
    assert(free_list_size_<= cache_line_limit +1000*1000);

}
#endif
//Can we use the lock within the handle to reduce the conflict here so that the critical seciton
// of the cache shard lock will be minimized.
    void LRUCache::Ref(LRUHandle* e) {
        unsigned int ticket = e->refs.fetch_add(1);
        if (ticket == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
            SpinLock l(&list_mutex_);
            List_Remove(e);
            lru_size_--;
            List_Append(&in_use_, e);
        }
//        e->refs++;
        assert(e->refs <=100);
    }

#ifndef PAGE_FREE_LIST
void LRUCache::Unref(LRUHandle *e, SpinLock *spin_l) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
      //Finish erase will only goes here, or directly return. it will never goes to next if clause
#ifdef EARLY_LOCK_RELEASE
      if (spin_l!= nullptr){
          //Early releasing the lock to avoid the RDMA lock releasing in the critical section.
          assert(spin_l->check_own() == true);
          spin_l->Unlock();
      }
#endif
      assert(!e->in_cache);
    (*e->deleter)(e);
//    free(e);
    delete e;

  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRU_Remove(e);// remove from in_use list move to LRU list.
    LRU_Append(&lru_, e);
  }
}
#else
//NOte that the back ground threads never use this function. because this does not apply batch eviction.
    void LRUCache::Unref(LRUHandle *e) {
        assert(e->refs > 0);
        unsigned int ticket = e->refs.fetch_sub(1);
        // todo: make refs atomic. and remove the need of exclusive latch for reference and unreference.or create another
        //  spin mutex for the LRU list or for the e reference and dereference.

        if (ticket == 1) {  // Deallocate.
            //Finish erase will only goes here, or directly return. it will never goes to next if clause
            assert(!e->in_cache);
            (*e->deleter)(e);
            push_free_list(e);
        } else if (e->in_cache && ticket == 2) {
            SpinLock l(&list_mutex_);
            // No longer in use; move to lru_ list.
            List_Remove(e);// remove from in_use list move to LRU list.
            List_Append(&lru_, e);
            lru_size_++;

        }
    }
#endif

    void LRUCache::Unref_Inv(LRUHandle *e) {
        assert(e->refs > 0);
        unsigned int ticket = e->refs.fetch_sub(1);
        if (ticket == 1) {  // Deallocate.

            assert(false);
            assert(!e->in_cache);
            (*e->deleter)(e);
            delete e;

        } else if (e->in_cache && ticket == 2) {
            SpinLock l(&list_mutex_);
            // No longer in use; move to lru_ list.
            List_Remove(e);// remove from in_use list move to LRU list.
            List_Append(lru_.next, e);
            lru_size_++;
        }
    }


void LRUCache::List_Remove(LRUHandle* e) {
#ifndef NDEBUG
//    if (e->gptr.offset < 10480863232){
//        printf("page %lu is being remove from a list", e->gptr.offset);
//    }
#endif
  e->next.load()->prev = e->prev.load();
  e->prev.load()->next = e->next.load();
  e->next = nullptr;
  e->prev = nullptr;
}

void LRUCache::List_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev.store(list->prev);
  e->prev.load()->next = e;
  e->next.load()->prev = e;
}

//Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
//    //TODO: WHEN there is a miss, directly call the RDMA refetch and put it into the
//    // cache.
////  MutexLock l(&table_mutex_);
//    LRUHandle *e;
//    {
//        table_mutex_.ReadLock();
//        assert(usage_ <= capacity_);
//        //TOTHINK(ruihong): should we update the lru list after look up a key?
//        //  Answer: Ref will refer this key and later, the outer function has to call
//        // Unref or release which will update the lRU list.
//        e = table_.Lookup(key, hash);
//        if (e != nullptr) {
//            Ref_in_LookUp(e);
//        }else{
//            table_mutex_.ReadUnlock();
//        }
//    }
//
//  return reinterpret_cast<Cache::Handle*>(e);
//}
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
//  MutexLock l(&table_mutex_);
    std::shared_lock<RWSpinMutex> l(table_mutex_);
    //TOTHINK(ruihong): shoul we update the lru list after look up a key?
    //  Answer: Ref will refer this key and later, the outer function has to call
    // Unref or release which will update the lRU list.
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != nullptr) {
        assert(e->refs >= 1);
        Ref(e);
    }
    return reinterpret_cast<Cache::Handle*>(e);
}


void LRUCache::Release(Cache::Handle* handle) {
//  MutexLock l(&table_mutex_);
//  WriteLock l(&table_mutex_);
    // TODO: the spin mutex below can be removed.
    std::shared_lock<RWSpinMutex> l(table_mutex_);
    Unref(reinterpret_cast<LRUHandle *>(handle));
//    assert(reinterpret_cast<LRUHandle*>(handle)->refs != 0);
}
void LRUCache::Release_Inv(Cache::Handle* handle) {

    std::shared_lock<RWSpinMutex> l(table_mutex_);
    Unref_Inv(reinterpret_cast<LRUHandle *>(handle));
}

#ifndef PAGE_FREE_LIST
    Cache::Handle *DSMEngine::LRUCache::LookupInsert(const Slice &key, uint32_t hash, void *value, size_t charge,
                                                 void (*deleter)(Cache::Handle* handle)) {
    assert(!SpinLock::check_own());
    SpinLock l(&table_mutex_);
    //TOTHINK(ruihong): shoul we update the lru list after look up a key?
    //  Answer: Ref will refer this key and later, the outer function has to call
    // Unref or release which will update the lRU list.
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != nullptr) {
        assert(e->refs >= 1);
        Ref(e);
//        assert(e->refs <=2);
//        DEBUG_PRINT("cache hit when searching the leaf node");
        return reinterpret_cast<Cache::Handle*>(e);
    }else{
//        fprintf(stdout, "Did not find cache entry for %lu\n", (*(GlobalAddress*)key.data()).offset);
        // This LRU handle is not initialized.
        // TODO: get the LRU handle from the free list.
        e = new LRUHandle();
//                reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

        e->value = value;
        e->remote_lock_status = 0;
        e->remote_urging_type = 0;
        e->gptr = *(GlobalAddress*)key.data();

        e->deleter = deleter;
        e->charge = charge;
        e->key_length = key.size();
        e->hash = hash;
        e->in_cache = false;
        e->refs = 1;  // for the returned handle.
//        std::memcpy(e->key_data, key.data(), key.size());
        if (capacity_ > 0) {
            e->refs++;  // for the table_cache's reference. refer here and unrefer outside
            e->in_cache = true;
            LRU_Append(&in_use_, e);// Finally it will be pushed into LRU list
            usage_ += charge;
            FinishErase(table_.Insert(e), &l);//table_.Insert(e) will return LRUhandle with duplicate key as e, and then delete it by FinishErase
//#ifndef NDEBUG
//            if (e->gptr.offset < 9480863232){
//                printf("page of %lu is inserted into the cache", e->gptr.offset);
//            }
//#endif
        } else {  // don't do caching. (capacity_==0 is supported and turns off caching.)
            // next is read by key() in an assert, so it must be initialized
            e->next = nullptr;
        }
#ifdef EARLY_LOCK_RELEASE
        //In case that insert a handle which has already exisit, the old handle need to be deallocated
        if (!l.check_own()){
            assert(false);
            l.Lock();
        }
#endif
        assert(usage_ <= capacity_ + kLeafPageSize + kInternalPageSize);
        // This will remove some entry from LRU if the table_cache over size.
#ifdef BUFFER_HANDOVER
        bool already_foward_the_mr = false;
#endif
//        if (counter.fetch_add(1) == 100000){
//            printf("capacity is %zu, usage is %zu\n", capacity_, usage_);
//            counter = 0;
//        }
        int counter = 0;
        while (usage_ > capacity_ && lru_.next != &lru_) {
            assert(counter == 0);
            LRUHandle* old = lru_.next;
            assert(old->refs == 1);


            //TODO; check free list first for the free page.
            // Directly reuse the mr if the evicted cache entry is the same size as the new inserted on.
#ifdef BUFFER_HANDOVER
//            bool rw_locked = false;

            if (value == nullptr && !already_foward_the_mr && ((ibv_mr*)old->value)->length == charge){
                old->keep_the_mr = true;
                e->value = old->value;
                already_foward_the_mr = true;
//                e->rw_mtx.lock();
//                rw_locked = true;
            }
            std::unique_lock<RWSpinMutex> lck(e->rw_mtx);

            //If there is early lock release, then the handle may be accessed by other threads,
            // before the mr has been dirty flushed. We need to make sure the following accessor,
            // can not access the mr before we finish the old mr deallocating (flush the dirty page)
            //TODO: ACQUIRE THE READ-write LATCH IN the cache handle
#endif
            assert(l.check_own());
            bool erased = FinishErase(table_.Remove(old->key(), old->hash), &l);
#ifdef EARLY_LOCK_RELEASE
//            if (rw_locked){
//                e->rw_mtx.unlock();
//            }
            if (!l.check_own()){
                l.Lock();
            }
#endif
            if (!erased) {  // to avoid unused variable when compiled NDEBUG
                assert(erased);
            }
            counter++;
        }
//        assert(usage_ <= capacity_);

        return reinterpret_cast<Cache::Handle*>(e);
    }
}

//If the inserted key has already existed, then the old LRU handle will be removed from
// the cache, but it may not garbage-collected right away.
Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(Cache::Handle* handle)) {
//  MutexLock l(&table_mutex_);

  //TODO: set the LRUHandle within the page, so that we can check the reference, during the direct access, or we reserver
  // a place hodler for the address pointer to the LRU handle of the page.
  LRUHandle* e = new LRUHandle();
//      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

    e->remote_lock_status = 0;
    e->remote_urging_type = 0;
    e->gptr = *(GlobalAddress*)key.data();

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
//  std::memcpy(e->key_data, key.data(), key.size());
//  WriteLock l(&table_mutex_);
  SpinLock l(&table_mutex_);
  if (capacity_ > 0) {
    e->refs++;  // for the table_cache's reference. refer here and unrefer outside
    e->in_cache = true;
    LRU_Append(&in_use_, e);// Finally it will be pushed into LRU list
    usage_ += charge;
      FinishErase(table_.Insert(e), &l);//table_.Insert(e) will return LRUhandle with duplicate key as e, and then delete it by FinishErase
  } else {  // don't do caching. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
#ifdef EARLY_LOCK_RELEASE

    if (!l.check_own()){
        l.Lock();
    }
#endif
        assert(usage_ <= capacity_ + kLeafPageSize + kInternalPageSize);
  // This will remove some entry from LRU if the table_cache over size.
#ifdef BUFFER_HANDOVER
            bool already_foward_the_mr = false;
#endif

    while (usage_ > capacity_ && lru_.next != &lru_) {

    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
#ifdef BUFFER_HANDOVER
//    bool rw_locked = false;
      if (value == nullptr && !already_foward_the_mr && ((ibv_mr*)old->value)->length == charge){
          old->keep_the_mr = true;
          e->value = old->value;
          assert(((ibv_mr*)e->value)->addr != nullptr);
          already_foward_the_mr = true;
//          e->rw_mtx.lock();
      }
      std::unique_lock<RWSpinMutex> lck(e->rw_mtx);
#endif
        assert(l.check_own());
    bool erased = FinishErase(table_.Remove(old->key(), old->hash), &l);
    //some times the finsih Erase will release the spinlock to let other threads working during the RDMA lock releasing.
    //We need to regain the lock here in case that there is another cache entry eviction.
#ifdef EARLY_LOCK_RELEASE
//        if (rw_locked){
//            e->rw_mtx.unlock();
//        }
      if (!l.check_own()){
          l.Lock();
      }
#endif
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}
#else
    Cache::Handle *DSMEngine::LRUCache::LookupInsert(const Slice &key, uint32_t hash, void *value, size_t charge,
                                                     void (*deleter)(Cache::Handle* handle)) {
        assert(!SpinLock::check_own());
        std::shared_lock<RWSpinMutex> l(table_mutex_);
        //TOTHINK(ruihong): shoul we update the lru list after look up a key?
        //  Answer: Ref will refer this key and later, the outer function has to call
        // Unref or release which will update the lRU list.
        LRUHandle* e = table_.Lookup(key, hash);
        if (e != nullptr) {
            assert(e->refs >= 1);
            Ref(e);
//        assert(e->refs <=2);
//        DEBUG_PRINT("cache hit when searching the leaf node");
            return reinterpret_cast<Cache::Handle*>(e);
        }else{

            l.unlock();
            // use double check locking to reduce the contention on the exclusive lock
            std::unique_lock<RWSpinMutex> l2(table_mutex_);
            LRUHandle* e = table_.Lookup(key, hash);
            if (e != nullptr) {
                assert(e->refs >= 1);
                Ref(e);
                return reinterpret_cast<Cache::Handle*>(e);
            }else{
                // Get from LRU free list.
                e = pop_free_list();
                if (e==&free_list_){
                    // Fail to get a free page from free page list, then get a LRU handle from the end of LRU list.
                    LRUHandle* old = lru_.next; // next is the oldest element in the free list
                    table_.Remove(old->key(), old->hash);
                    e = old;
                    assert(old->refs == 1);
                    // The function below can result in latch release and dirty page flush back in the critical path
                    List_Remove(e);
                    lru_size_--;
                    e->in_cache = true;
                    usage_ -= e->charge;
                    e->refs.fetch_sub(1);
                    assert(e->refs == 0);
                    //Finish erase will only goes here, or directly return. it will never goes to next if clause
                    (*e->deleter)(e); // must be synchronized with the RDMA write back.
                }
                if (value){
                    // This is for backward compatibility.
                    RDMA_Manager::Get_Instance()->Deallocate_Local_RDMA_Slot(((ibv_mr*)e->value)->addr, Regular_Page);
                    e->value = value;
                }
                assert(e);
                e->assert_no_handover_states();
                e->gptr = *(GlobalAddress*)key.data();
                e->deleter = deleter;
                e->charge = charge;
                e->key_length = key.size();
                e->hash = hash;
                e->next_hash = nullptr;
                e->in_cache = true;
                assert(e->refs == 0);
                e->refs.store(1);  // for the returned handle.
                assert(!e->next.load());
                assert(!e->prev.load());

                if (capacity_ > 0) {
                    e->refs.fetch_add(1);  // for the table_cache's reference. refer here and unrefer outside
                    e->in_cache = true;
                    List_Append(&in_use_, e);// Finally it will be pushed into LRU list
                    usage_ += charge;
                    FinishErase(table_.Insert(e));//table_.Insert(e) will return LRUhandle with duplicate key as e, and then delete it by FinishErase
                } else {  // don't do caching. (capacity_==0 is supported and turns off caching.)
                    // next is read by key() in an assert, so it must be initialized
                    e->next = nullptr;
                }
                assert(usage_ <= capacity_ + 2*kLeafPageSize + kInternalPageSize); // make sure the usage is bounded.
                return reinterpret_cast<Cache::Handle*>(e);
            }

        }
    }
    Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                    size_t charge,
                                    void (*deleter)(Cache::Handle* handle)) {
//  MutexLock l(&table_mutex_);
        std::unique_lock<RWSpinMutex> l(table_mutex_);
        //TODO: set the LRUHandle within the page, so that we can check the reference, during the direct access, or we reserver
        // a place hodler for the address pointer to the LRU handle of the page.
        LRUHandle* e = pop_free_list();
        if (e==&free_list_){
            // Fail to get a free page from free page list, then get a LRU handle from the end of LRU list.
            LRUHandle* old = lru_.next; // next is the oldest element in the free list
            table_.Remove(old->key(), old->hash);
            e = old;
            assert(old->refs == 1);

            // The function below can result in latch release and dirty page flush back in the critical path
            List_Remove(e);
            lru_size_--;
            usage_ -= e->charge;
            e->refs.fetch_sub(1);
            assert(e->refs == 0);
            //Finish erase will only goes here, or directly return. it will never goes to next if clause
            (*e->deleter)(e);
        }
        if (value) {
            // This is for backward compatibility.
            RDMA_Manager::Get_Instance()->Deallocate_Local_RDMA_Slot(((ibv_mr *) e->value)->addr, Regular_Page);
            e->value = value;
        }
        e->assert_no_handover_states();
//        e->remote_lock_status = 0;
//        e->remote_urging_type = 0;
        e->gptr = *(GlobalAddress*)key.data();
//#ifdef DIRTY_ONLY_FLUSH
//        e->dirty_upper_bound = 0;
//        e->dirty_lower_bound = 0;
//#endif
        e->value = value;
        e->deleter = deleter;
        e->charge = charge;
        e->key_length = key.size();
        e->hash = hash;
        e->next_hash = nullptr;
        e->in_cache = true;
        e->refs.store(1);  // for the returned handle.
        assert(!e->next.load());
        assert(!e->prev.load());
        if (capacity_ > 0) {
            e->refs.fetch_add(1);  // for the table_cache's reference. refer here and unrefer outside
            e->in_cache = true;
            List_Append(&in_use_, e);// Finally it will be pushed into LRU list
            usage_ += charge;
            FinishErase(table_.Insert(e));//table_.Insert(e) will return LRUhandle with duplicate key as e, and then delete it by FinishErase
        } else {  // don't do caching. (capacity_==0 is supported and turns off caching.)
            // next is read by key() in an assert, so it must be initialized
            e->next = nullptr;
        }
        assert(usage_ <= capacity_ + 2*kLeafPageSize + kInternalPageSize); // make sure the usage is bounded.
        return reinterpret_cast<Cache::Handle*>(e);
    }
#endif
// If e != nullptr, finish removing *e from the table_cache;
// it must have already been removed from the hash table.  Return whether e != nullptr.
// Remove the handle from LRU and change the usage.
    bool LRUCache::FinishErase(LRUHandle *e) {

  if (e != nullptr) {
    assert(e->in_cache);
      List_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
  // decrease the reference of cache, making it not pinned by cache, but it
  // can still be pinned outside the cache.
      Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
//  MutexLock l(&table_mutex_);
//  WriteLock l(&table_mutex_);
    std::unique_lock<RWSpinMutex> l(table_mutex_);
    FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
//  MutexLock l(&table_mutex_);
//  WriteLock l(&table_mutex_);
    std::unique_lock<RWSpinMutex> l(table_mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}
void LRUCache::push_free_list(DSMEngine::LRUHandle *e) {
    free_list_mtx_.lock();
    List_Append(&free_list_, e);
    free_list_size_++;
    free_list_mtx_.unlock();
}
LRUHandle* LRUCache::pop_free_list() {
    free_list_mtx_.lock();
    LRUHandle* e = free_list_.next;
    if (e != &free_list_){
        List_Remove(e);
        free_list_size_--;
        assert(free_list_size_ <capacity_/kLeafPageSize+1000*1000);
    }

    free_list_mtx_.unlock();
    return e;
}
bool LRUCache::need_eviction() {
    return free_list_size_ < free_list_trigger_limit_;
}

void LRUCache::prepare_free_list() {
    std::unique_lock<RWSpinMutex> lck1(table_mutex_);
    if (need_eviction()){
        long recycle_num = (free_list_trigger_limit_ - free_list_size_)/FREELIST_THREAD_NUM;
        if(recycle_num <= 0){
            return;
        }
        auto start_end_pair = bulk_remove_LRU_list(recycle_num);
        auto e = start_end_pair.first;
        // todo: current type of aysnchronous work request mechanism is not the optimial one, we can still optmize it.
        while (e != nullptr){
            e->in_cache = false;
            usage_ -= e->charge;
            table_.Remove(e->key(), e->hash);
            e = e->next;
        }
        lck1.unlock();
        e = start_end_pair.first;
        while (e != nullptr){
            e->refs--;
            (*e->deleter)(e);
            e = e->next;

        }
        SpinLock lck2(&free_list_mtx_);
        bulk_insert_free_list(start_end_pair, recycle_num);
    }



}


// should be protected by table_mutex_ outside the function.
std::pair<LRUHandle*, LRUHandle*> LRUCache::bulk_remove_LRU_list(size_t size) {
//    SpinLock l(&table_mutex_);
    LRUHandle* start_handle = lru_.next; // oldest
    LRUHandle* end_handle = lru_.next;
    for (size_t i = 1; i < size; ++i) {
        end_handle = end_handle->next;
    }
    end_handle->next.load()->prev = start_handle->prev.load();
    start_handle->prev.load()->next = end_handle->next.load();
    end_handle->next = nullptr;
    start_handle->prev = nullptr;

#ifndef NDEBUG
        if (size >1){
            assert(start_handle->next.load()->next != start_handle);

        }
#endif
    return std::make_pair(start_handle, end_handle);
}
void LRUCache::bulk_insert_free_list(std::pair<LRUHandle *, LRUHandle *> start_end, size_t size) {
    start_end.second->next.store(&free_list_);
    start_end.first->prev.store(free_list_.prev);
    free_list_.prev.store(start_end.second);
    start_end.first->prev.load()->next.store(start_end.first);
    free_list_size_ += size;
//    e->next = list;
//    e->prev.store(list->prev);
//    e->prev.load()->next = e;
//    e->next.load()->prev = e;

}


static const int kNumShardBits = 7;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
#ifdef PAGE_FREE_LIST
    std::thread free_list_thread_s[FREELIST_THREAD_NUM];
    bool bg_exit_signal_ = false;
#endif
    port::Mutex id_mutex_;
  uint64_t last_id_;
  size_t capacity_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    capacity_ = capacity;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
#ifdef PAGE_FREE_LIST
      shard_[s].init();
#endif
    }
      for (unsigned int i = 0; i < FREELIST_THREAD_NUM; ++i) {
          free_list_thread_s[i] = std::thread([this](unsigned int seed){
                auto rdma_mg = RDMA_Manager::Get_Instance();
//              srand(seed);
              // stage 1:
              // 1. check the free list size, if it is less than the threshold, then start the eviction.
              while (!bg_exit_signal_){
                  size_t j = rand_r(&seed) % kNumShards;
                  //TODO: try to think of a way making the most pending work request and also guaratee the correctness of the cache.
                  // Maximize the aysnchronous work request. there is a limit for 1 queue pair (16 pending RDMA read/atomic). However the overall pending request
                  // for RDMA atomic is much more. If one queue is satuated, we can quickly switch to another to continue filling in the request.
                  // beside, for the dirty page flush back, we need a buffer to avoid the page content being overwritten.
                  if (shard_[j].need_eviction()){
                      shard_[j].prepare_free_list();
                      // we can just enable the async flushing for the eviction by enable unsignaled work request for 15 work request and
                      // we make a signaled work request at the 16th work request. In this case, every work request only wait for 1/16 RDMA roundtrip time.
                  }
              }


          }, i);
          free_list_thread_s[i].detach();
      }
  }
  ~ShardedLRUCache() override {
      bg_exit_signal_ = true;
  }
  size_t GetCapacity() override{
      return capacity_;
  }
    // if there has already been a cache entry with the same key, the old one will be
    // removed from the cache, but it may not be garbage collected right away
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(Cache::Handle* handle)) override {
#ifndef NDEBUG
        assert(capacity_ >= 1000);
        if (TotalCharge() > 0.9 * capacity_ ){
            for (int i = 0; i < kNumShards - 1; ++i) {
                if (shard_[i+1].TotalCharge() >0 && shard_[i].TotalCharge()/shard_[i+1].TotalCharge() >= 2){
//                    printf("Uneven cache distribution\n");
                    assert(false);
                    break;
                }
            }
        }

#endif
    const uint32_t hash = HashSlice(key);

//        auto handle = shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
//        printf("Insert: refer to handle %p\n", handle);

        return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  //TODO: Change the search key to GlobalAddress.
  Handle* Lookup(const Slice& key) override {
      assert(capacity_ >= 1000);
    const uint32_t hash = HashSlice(key);

//    auto handle = shard_[Shard(hash)].Lookup(key, hash);
//      printf("Look up: refer to handle %p\n", handle);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
    Handle* LookupInsert(const Slice& key,  void* value,
                         size_t charge,
                         void (*deleter)(Cache::Handle* handle)) override{

                assert(capacity_ >= 1000);
                const uint32_t hash = HashSlice(key);
                return shard_[Shard(hash)].LookupInsert(key, hash, value, charge, deleter);
  };
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
//      printf("release handle %p\n", handle);
    shard_[Shard(h->hash)].Release(handle);
  }
    void Release_Inv(Handle* handle) override {
        LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
//      printf("release handle %p\n", handle);
        shard_[Shard(h->hash)].Release_Inv(handle);
    }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};


Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }


LocalBuffer::LocalBuffer(const CacheConfig &cache_config) {
        size = cache_config.cacheSize;
        data = (uint64_t)hugePageAlloc(size * define::GB);
    if (data == 0){
        data = (int64_t) malloc(size * define::GB);
    }
}

//    void Cache_Handle::invalidate_current_entry(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr,
//                                                 ibv_mr *mr, ibv_mr* cas_mr) {
//
//        if (this->remote_lock_status == 1){
//            rdma_mg->global_RUnlock(lock_addr, cas_mr, false, nullptr, nullptr, 0);
//            remote_lock_status.store(0);
//            //todo: spin wait to avoid write lock starvation.
//            if (remote_urging_type.load() == 1){
//                spin_wait_us(10);
//            }
//        }else if (this->remote_lock_status == 2){
//            rdma_mg->global_write_page_and_Wunlock(mr, page_addr, page_size, lock_addr);
//            remote_lock_status.store(0);
//        }else{
//            assert(false);
//        }
//    }


    void Cache_Handle::reader_pre_access(GlobalAddress page_addr, size_t page_size,
                                          GlobalAddress lock_addr, ibv_mr*& mr) {
        assert(page_addr == gptr);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }

        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_urging_type.load() > 0){
            lock_pending_num.fetch_add(1);
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            || timer_alarmed.load()
            while (handover_degree > STARVATION_THRESHOLD ){
                //wait here by no ops
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
                asm volatile("pause\n": : :"memory");
            }

            rw_mtx.lock_shared();
            lock_pending_num.fetch_sub(1);
//            read_lock_holder_num.fetch_add(1);
            read_lock_counter.fetch_add(1);
        }else{
            rw_mtx.lock_shared();
        }
//TODO: (OPTIMIZATION) we can first check what state is the cache in if it is invalid, we can
// directly acquire the write local lock.

#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif

            //TODO: make the leaf_node_search the same as leaf_node_store?
            // No, because the read here has a optimiaziton for the double check locking

            if (remote_lock_status.load() == 0){
                // upgrade the lock the write lock.
                //Can we use the std::call_once here?
#ifdef LOCAL_LOCK_DEBUG
                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.erase(RDMA_Manager::thread_id);

                }
#endif
                rw_mtx.unlock_shared();
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
//                std::unique_lock<std::shared_mutex> w_l(rw_mtx);
                rw_mtx.lock(RDMA_Manager::thread_id+384);
#ifdef LOCAL_LOCK_DEBUG

                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.insert(RDMA_Manager::thread_id);

                }
#endif
                if (remote_lock_status.load() == 0){
#ifdef WRITER_STARV_SPIN_BASE
                    if (reader_spin_time.load() >0){
                        spin_wait_us(reader_spin_time.load());
                        reader_spin_time.store(0);
                    }
#endif
                    cache_miss[RDMA_Manager::thread_id][0]++;
                    if(value) {
                        mr = (ibv_mr*)value;
                    }else{
                        mr = new ibv_mr{};
                        rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
//                        printf("value mr is null for globale page nodeid %lu, offset %lu\n", page_addr.nodeID, page_addr.offset);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);

                        //TODO: this is not guarantted to be atomic, mulitple reader can cause memory leak
                        value = mr;

                    }
                    rdma_mg->global_Rlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(1);
                }else{
                    cache_hit_valid[RDMA_Manager::thread_id][0]++;
                }
#ifdef LOCAL_LOCK_DEBUG

                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.erase(RDMA_Manager::thread_id);

                }
#endif
//                w_l.unlock();
                rw_mtx.unlock();
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
                rw_mtx.lock_shared();
#ifdef LOCAL_LOCK_DEBUG
                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.insert(RDMA_Manager::thread_id);
                }
#endif

            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;
            }
            mr = (ibv_mr*)value;



    }
    bool Cache_Handle::try_reader_pre_access(GlobalAddress page_addr, size_t page_size,
                                          GlobalAddress lock_addr, ibv_mr*& mr) {
        assert(page_addr == gptr);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }

        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_urging_type.load() > 0){
//            lock_pending_num.fetch_add(1);
            //TODO: pontential bug below, if there is try lock then the read write counter may not be updated but neve cleared.
//            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
//                //wait here by no ops
//                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//                asm volatile("pause\n": : :"memory");
//            }
            if (!rw_mtx.try_shared_lock()) {
//                lock_pending_num.fetch_sub(1);
                return false;
            }
            read_lock_counter.fetch_add(1);
//            lock_pending_num.fetch_sub(1);

//            read_lock_holder_num.fetch_add(1);
        }else{
            if (!rw_mtx.try_shared_lock()){
                return false;
            }
        }


#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif

            //TODO: make the leaf_node_search the same as leaf_node_store?
            // No, because the read here has a optimiaziton for the double check locking

            if (remote_lock_status.load() == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                // upgrade the lock the write lock.
                //Can we use the std::call_once here?
#ifdef LOCAL_LOCK_DEBUG
                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.erase(RDMA_Manager::thread_id);

                }
#endif
                rw_mtx.unlock_shared();
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
//                std::unique_lock<std::shared_mutex> w_l(rw_mtx);
                if (!rw_mtx.try_lock()){

                    return false;
                }
//                rw_mtx.lock(RDMA_Manager::thread_id+384);
#ifdef LOCAL_LOCK_DEBUG

                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.insert(RDMA_Manager::thread_id);

                }
#endif
                if ( remote_lock_status.load() == 0){
                    if(value) {
                        mr = (ibv_mr*)value;
                    }else{
                        mr = new ibv_mr{};
                        rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
//                        printf("value mr is null for globale page nodeid %lu, offset %lu\n", page_addr.nodeID, page_addr.offset);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);

                        //TODO: this is not guarantted to be atomic, mulitple reader can cause memory leak
                        value = mr;

                    }
                    if (!rdma_mg->global_Rlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr,5)){
                        rw_mtx.unlock();
                        return false;
                    }
                    remote_lock_status.store(1);
                }
#ifdef LOCAL_LOCK_DEBUG

                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.erase(RDMA_Manager::thread_id);

                }
#endif
//                w_l.unlock();
                rw_mtx.unlock();
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
                if (!rw_mtx.try_shared_lock()){

                    return false;
                }
//                rw_mtx.lock_shared();
#ifdef LOCAL_LOCK_DEBUG
                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.insert(RDMA_Manager::thread_id);
                }
#endif

            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;
            }
            mr = (ibv_mr*)value;
//        if (remote_urging_type.load() > 0){
//            read_lock_counter.fetch_add(1);
//        }

        return true;
    }

    void Cache_Handle::reader_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *mr) {
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();


//        assert(handle->refs.load() == 2);
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.erase(RDMA_Manager::thread_id);

        }
#endif
        if (remote_urging_type.load() > 0) {
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            printf("Lock starvation prevention code was executed stage 1\n");
//            if(lock_pending_num.load() > 0 && !timer_on){
//                timer_begin = std::chrono::high_resolution_clock::now();
//            }
//            || timer_alarmed.load()
            // If the lock handover time exceeds the threshold or no pending waiter for this cache line locally,
            // then we process the cached inv message.
            //|| lock_pending_num.load()==0
            // for reader access, lock pending num = 0 does not mean that the access on this data cool down.
            // we need some other mechanism to avoid the sudden cool down problem on previously read skewed data.
            // the sudden cool down read skewed data will cause buffered invalidation message not being processed, making
            // other compute node wait for endless time.
            if ( handover_degree > STARVATION_THRESHOLD ){
//                printf("Lock starvation prevention code was executed stage 2, page_adr is %p\n", page_addr);
//                fflush(stdout);
                // make sure only one thread release the global latch successfully by double check lock.
                rw_mtx.unlock_shared();
                rw_mtx.lock(RDMA_Manager::thread_id+256);
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;

                if (remote_urging_type.load() > 0 && (handover_degree > STARVATION_THRESHOLD)){
                    //double check locking, it is possible that another thread comes in and also try to process the buffer inv message.
                    assert_with_handover_states();
                    buffered_inv_mtx.lock();
//                    printf("Node %u handover over data %p times out, process the buffered inv message, the processed inv message prirority is %u\n", RDMA_Manager::node_id, page_addr, buffer_inv_message.starvation_priority.load());
//                    fflush(stdout);
                    process_buffered_inv_message(page_addr, page_size, lock_addr, mr, true);
                    buffered_inv_mtx.unlock();
                }
                rw_mtx.unlock();

                return;
            }
        }
        // In case that there is no following reader and writer, process the buffered invalidation message.
        // we need to check whether this is the only reader or writer on this cache line.
        // TODO: try to make lock_pending num inside the userspace read write lock?
//        auto pending_num = lock_pending_num.load();
//        if (rw_mtx.unlock_shared() == 1 && pending_num == 0){
//            rw_mtx.lock(RDMA_Manager::thread_id+256);
//            rw_mtx.unlock();
//        }
        rw_mtx.unlock_shared();


    }

    void Cache_Handle::updater_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_urging_type.load() > 0){
            lock_pending_num.fetch_add(1);
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            || timer_alarmed.load()
            while (handover_degree > STARVATION_THRESHOLD ){
                //wait here by no ops
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
                asm volatile("pause\n": : :"memory");
            }
            rw_mtx.lock(RDMA_Manager::thread_id+512);
            lock_pending_num.fetch_sub(1);
            write_lock_counter.fetch_add(1);
        }else{
            rw_mtx.lock(RDMA_Manager::thread_id+512);
        }
//        lock_pending_num.fetch_add(1);
//        rw_mtx.lock();
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                mr = (ibv_mr*)value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifdef PAGE_FREE_LIST
                // With page_free list the page value shall always be non-zero.
                assert(false);
#endif
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                value = mr;

            }
            // If the remote read lock is not on, lock it
            if (remote_lock_status == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                uint8_t starv_priority = 0;
                rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr, -1, &starv_priority);
#ifdef WRITER_STARV_SPIN_BASE
                last_writer_starvation_priority = starv_priority;
#endif
                remote_lock_status.store(2);



            }else if (remote_lock_status == 1){
                cache_miss[RDMA_Manager::thread_id][0]++;
//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
                if (!global_Rlock_update(mr, lock_addr, cas_mr)){
                    // can not update atomically, need to unlock the write lock and then lock the write lock. Unlock has been done already.
                    assert(remote_lock_status.load() == 0);
                    buffered_inv_mtx.lock();
                    //clear the outdated buffered inv message. as the latch state has been changed.
                    if (buffer_inv_message.next_holder_id != Invalid_Node_ID){
//                        printf("Node %u Upgrade lock from shared to modified, clear the buffered inv message on cache line %p\n", RDMA_Manager::node_id, page_addr);
                        assert(buffer_inv_message.next_inv_message_type == writer_invalidate_shared);
                        clear_pending_inv_states();
                    }
                    buffered_inv_mtx.unlock();
                    uint8_t starv_priority = 0;
                    //the Read lock has been released, we can directly acquire the write lock
                    rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr, -1, &starv_priority);
#ifdef WRITER_STARV_SPIN_BASE
                    last_writer_starvation_priority = starv_priority;
#endif
                    remote_lock_status.store(2);
                }else{
                    assert( remote_lock_status.load() == 2);
                    //TODO:
                }
            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;

            }

    }
    bool Cache_Handle::try_upgrade_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        assert(rw_mtx.issharelocked());
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_urging_type.load() > 0){
//            lock_pending_num.fetch_add(1);
//            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
//                //wait here by no ops
//                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//                asm volatile("pause\n": : :"memory");
//            }
            if (!rw_mtx.try_upgrade()){
//                lock_pending_num.fetch_sub(1);
                return false;
            }
//            rw_mtx.lock(RDMA_Manager::thread_id+512);
            write_lock_counter.fetch_add(1);
//            lock_pending_num.fetch_sub(1);
            //The continous access counter can not be added here, because the lock may be abandoned later.
        }else{
            if (!rw_mtx.try_upgrade()){
                return false;
            }
//            rw_mtx.lock(RDMA_Manager::thread_id+512);
        }
//        lock_pending_num.fetch_add(1);
//        rw_mtx.lock();
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
        bool hint_of_existence = false;
#endif
        if(value)
        {
#ifndef NDEBUG
            hint_of_existence = true;
#endif
            // This means the page has already be in the cache.
            mr = (ibv_mr*)value;
            //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
        }else{
#ifndef NDEBUG
            hint_of_existence = false;
#endif
            // This means the page was not in the cache before
            mr = new ibv_mr{};
            rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
            assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
            value = mr;

        }
        // If the remote read lock is not on, lock it
        if (remote_lock_status == 0){
            cache_miss[RDMA_Manager::thread_id][0]++;
            if (!rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr, 5)){
                rw_mtx.unlock();
                return false;
            }
            remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

        }else if (remote_lock_status == 1){
            cache_miss[RDMA_Manager::thread_id][0]++;
//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
            if (!global_Rlock_update(mr, lock_addr, cas_mr)){
                assert(remote_lock_status.load() == 0);
                buffered_inv_mtx.lock();
                //TODO: try to clear the outdated buffered inv message. as the latch state has been changed.
                if (buffer_inv_message.next_holder_id != Invalid_Node_ID){
//                        printf("Node %u Upgrade lock from shared to modified, clear the buffered inv message on cache line %p\n", RDMA_Manager::node_id, page_addr);
                    assert(buffer_inv_message.next_inv_message_type == writer_invalidate_shared);
                    clear_pending_inv_states();
                }
                buffered_inv_mtx.unlock();
                rw_mtx.unlock();
//                remote_lock_status.store(0);
                return false;
                //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                // is problematice if we want to upgrade the lock during a transaction.
                // May be we can take advantage of the lock starvation bit to solve this problem.
                //the Read lock has been released, we can directly acquire the write lock
                rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                remote_lock_status.store(2);
            }else{

                assert( remote_lock_status.load() == 2);

            }
        }else{
            cache_hit_valid[RDMA_Manager::thread_id][0]++;

        }
//        if(remote_urging_type.load() > 0){
//            write_lock_counter.fetch_add(1);
//        }

        return true;
    }
    bool Cache_Handle::try_updater_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_urging_type.load() > 0){
//            lock_pending_num.fetch_add(1);
            // TODO: delete the lcok pending and the waiting mechanims below. if there is a blocking.
//            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
//                //wait here by no ops
//                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//                asm volatile("pause\n": : :"memory");
//            }
            if (!rw_mtx.try_lock()){
//                lock_pending_num.fetch_sub(1);
                return false;
            }
//            rw_mtx.lock(RDMA_Manager::thread_id+512);
            write_lock_counter.fetch_add(1);
//            lock_pending_num.fetch_sub(1);
        }else{
            if (!rw_mtx.try_lock()){
                return false;
            }
//            rw_mtx.lock(RDMA_Manager::thread_id+512);
        }
//        lock_pending_num.fetch_add(1);
//        rw_mtx.lock();
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                mr = (ibv_mr*)value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                value = mr;

            }
            // If the remote read lock is not on, lock it
            if (remote_lock_status == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                if (!rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr, 5)){
                    rw_mtx.unlock();
                    return false;
                }
                remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

            }else if (remote_lock_status == 1){
                cache_miss[RDMA_Manager::thread_id][0]++;
//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
                if (!global_Rlock_update(mr, lock_addr, cas_mr)){
//                    remote_lock_status.store(0);
                    assert(remote_lock_status.load() == 0);
                    buffered_inv_mtx.lock();
                    //TODO: try to clear the outdated buffered inv message. as the latch state has been changed.
                    if (buffer_inv_message.next_holder_id != Invalid_Node_ID){
//                        printf("Node %u Upgrade lock from shared to modified, clear the buffered inv message on cache line %p\n", RDMA_Manager::node_id, page_addr);
                        assert(buffer_inv_message.next_inv_message_type == writer_invalidate_shared);
                        clear_pending_inv_states();
                    }
                    buffered_inv_mtx.unlock();
                    rw_mtx.unlock();
                    return false;
                    //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                    // is problematice if we want to upgrade the lock during a transaction.
                    // May be we can take advantage of the lock starvation bit to solve this problem.
                    //the Read lock has been released, we can directly acquire the write lock
                    rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(2);
                }else{

                    assert( remote_lock_status.load() == 2);

                }
            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;

            }
            //TODO: If the code below not work, delete the waiting code before try the local latch.
//        if (remote_urging_type.load() > 0){
//            write_lock_counter.fetch_add(1);
//        }
        return true;
    }
    void Cache_Handle::upgrade_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {
        assert(rw_mtx.issharelocked());
        assert(remote_lock_status == 1);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        rw_mtx.unlock_shared();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_urging_type.load() > 0){
            lock_pending_num.fetch_add(1);
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            || timer_alarmed.load()
            while (handover_degree > STARVATION_THRESHOLD ){
                //wait here by no ops
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
                asm volatile("pause\n": : :"memory");
            }
            rw_mtx.lock(RDMA_Manager::thread_id+640);
            lock_pending_num.fetch_sub(1);
            write_lock_counter.fetch_add(1);
        }else{
            rw_mtx.lock(RDMA_Manager::thread_id+640);
        }
//        lock_pending_num.fetch_add(1);
//        rw_mtx.lock();
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                mr = (ibv_mr*)value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                value = mr;

            }
            // If the remote read lock is not on, lock it
            if (remote_lock_status == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

            }else if (remote_lock_status == 1){
                cache_miss[RDMA_Manager::thread_id][0]++;
//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
                if (!global_Rlock_update(mr, lock_addr, cas_mr)){
//                    remote_lock_status.store(0);
                    //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                    // is problematice if we want to upgrade the lock during a transaction.
                    // May be we can take advantage of the lock starvation bit to solve this problem.
                    //the Read lock has been released, we can directly acquire the write lock
                    rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(2);
                }else{
                    assert( remote_lock_status.load() == 2);
                    //TODO:
                }
            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;

            }

    }

    void Cache_Handle::updater_writer_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

//        if (strategy == 1){
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.erase(RDMA_Manager::thread_id);
        }
#endif
        if (remote_urging_type.load() > 0) {
//            printf("Lock starvation prevention code was executed stage 1\n");
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            if(lock_pending_num.load() > 0 && !timer_on){
//                timer_begin = std::chrono::high_resolution_clock::now();
//                timer_on.store(true);
//            }
            assert(remote_lock_status == 2);
//            || timer_alarmed.load()
            if ( handover_degree > STARVATION_THRESHOLD ){ //|| lock_pending_num.load()==0
//                if (handover_degree > STARVATION_THRESHOLD){
//                    printf("Node %u Process the cached invalidation message over %p from node %u due to hitting the thredhold.\n", RDMA_Manager::node_id, page_addr, buffer_inv_message.next_holder_id.load());
//                    fflush(stdout);
//                } else{
//                    printf("Node %u Process the cached invalidation message over %p from node %u due to no pending waiter.\n", RDMA_Manager::node_id, page_addr, buffer_inv_message.next_holder_id.load());
//                    fflush(stdout);
//                }
                buffered_inv_mtx.lock();
                process_buffered_inv_message(page_addr, page_size, lock_addr, mr, true);
                buffered_inv_mtx.unlock();
            }
        }
        rw_mtx.unlock();
    }
    //This function should be used with caution. only used when there on the new allocated cache line. and used only once per page.
    void Cache::Handle::writer_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

        if (rdma_mg == nullptr){
                rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_urging_type.load() > 0){
            lock_pending_num.fetch_add(1);
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            || timer_alarmed.load()
            while (handover_degree > STARVATION_THRESHOLD ){
                //wait here by no ops
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
                asm volatile("pause\n": : :"memory");
            }
            rw_mtx.lock(RDMA_Manager::thread_id+768);
            lock_pending_num.fetch_sub(1);
            write_lock_counter.fetch_add(1);
        }else{
            rw_mtx.lock(RDMA_Manager::thread_id+768);
        }
#ifdef LOCAL_LOCK_DEBUG

        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                mr = (ibv_mr*)value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                value = mr;

            }
            // If the remote read lock is not on, lock it
            if (remote_lock_status == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                rdma_mg->global_Wlock_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

            }else if (remote_lock_status == 1){
                cache_miss[RDMA_Manager::thread_id][0]++;

//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
                if (!global_Rlock_update(mr, lock_addr, cas_mr)){
//
                    //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                    // is problematice if we want to upgrade the lock during a transaction.
                    // May be we can take advantage of the lock starvation bit to solve this problem.
                    //the Read lock has been released, we can directly acquire the write lock
                    rdma_mg->global_Wlock_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(2);
                }else{
//                    cache_hit_valid[RDMA_Manager::thread_id][0]++;
                    assert( remote_lock_status.load() == 2);
                    //TODO:
                }
            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;
            }
    }

    bool
    Cache_Handle::global_Rlock_update(ibv_mr * local_mr, GlobalAddress lock_addr, ibv_mr *cas_buffer, CoroContext *cxt,
                                       int coro_id) {
        assert(remote_lock_status.load() == 1);
        bool succfully_updated = rdma_mg->global_Rlock_update(local_mr, lock_addr, cas_buffer);
        if (succfully_updated){
            // TODO: we need to make the lock status update out side this function.
            buffered_inv_mtx.lock();
            if (buffer_inv_message.next_holder_id != Invalid_Node_ID){
//                printf("Node %u Upgrade lock from shared to modified, clear the buffered inv message on cache line %p\n", RDMA_Manager::node_id, lock_addr);
                assert(buffer_inv_message.next_inv_message_type == writer_invalidate_shared);
                clear_pending_inv_states();
            }
            remote_lock_status.store(2);
            buffered_inv_mtx.unlock();

//            assert(gptr == (((LeafPage<uint64_t ,uint64_t>*)(((ibv_mr*)value)->addr))->hdr.this_page_g_ptr));
            return true;
        }else{
            assert(remote_lock_status.load() == 1);
            remote_lock_status.store(0);

            return false;

        }
    }
    //Deprecated temporarilly.
    void
    Cache_Handle::writer_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {
        assert(false);



#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
        //the mechanism to avoid global lock starvation be local latch
        if (remote_urging_type.load() > 0) {
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            if(lock_pending_num.load() > 0 && !timer_on){
//                timer_begin = std::chrono::high_resolution_clock::now();
//            }
            assert(remote_lock_status == 2);
//            || timer_alarmed.load()
            if ( handover_degree > STARVATION_THRESHOLD ){
                buffered_inv_mtx.lock();
                process_buffered_inv_message(page_addr, page_size, lock_addr, mr, true);
                buffered_inv_mtx.unlock();
            }
        }
        rw_mtx.unlock();
    }
    void Cache_Handle::process_buffered_inv_message(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr,
                                                    ibv_mr *mr, bool need_spin) {
//        buffered_inv_mtx.lock();
        assert(remote_lock_status != 0);
        assert(remote_urging_type != 0);
        if (this->remote_lock_status == 1){
            assert(remote_urging_type == 2);
            assert(buffer_inv_message.next_inv_message_type == writer_invalidate_shared);
//            printf("High pririty invalidation message receive, target gcl is %p\n", page_addr);
//            fflush(stdout);
            assert(lock_addr == gptr);
            rdma_mg->global_RUnlock(lock_addr, rdma_mg->Get_local_CAS_mr());
            remote_lock_status.store(0);
            //spin wait to delay the global latch acquire for other thread and then to prevent write lock starvation.
            // However, it is possible the other thread has already enter the critical section and see the global latch is 0.
            // In this case, that thread can immediately issue  a global read lacth request. Then the remote writer is still starved.
            // Since this scenario will happen rarely, then we think this method can relieve the latch starvation to some extense.
//                    if (remote_urging_type.load() > 1){
//            auto local_mr = rdma_mg->Get_local_send_message_mr();
//            *((Page_Forward_Reply_Type* )local_mr->addr) = processed;
//            RDMA_Write_xcompute(local_mr, receive_msg_buf->buffer, receive_msg_buf->rkey,
//                                sizeof(Page_Forward_Reply_Type),
//                                target_node_id, qp_id, true, true);
#ifdef WRITER_STARV_SPIN_BASE
            // todo: there is no need to spin inside this function, and the spin time should be zero if there is no starvation detected.
            if(buffer_inv_message.starvation_priority.load()> 0){
                if (need_spin){
                    // TOCONTROL:
                    spin_wait_us(WRITER_STARV_SPIN_BASE* ( buffer_inv_message.starvation_priority.load()));
                }else{
                    reader_spin_time.store(WRITER_STARV_SPIN_BASE* (buffer_inv_message.starvation_priority.load()));
                }
            }

#endif
//                    }
        }else if (this->remote_lock_status == 2){
            if (remote_urging_type == 1 && buffer_inv_message.next_inv_message_type == reader_invalidate_modified){
                ibv_mr* local_mr = mr;
                assert(local_mr->length == kLeafPageSize);
                int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
                *(Page_Forward_Reply_Type* ) ((char*)local_mr->addr + kLeafPageSize - sizeof(Page_Forward_Reply_Type)) = processed;
                rdma_mg->RDMA_Write_xcompute(local_mr, buffer_inv_message.next_receive_page_buf,
                                             buffer_inv_message.next_receive_rkey, kLeafPageSize,
                                             buffer_inv_message.next_holder_id, qp_id, true);
//                auto time_begin = std::chrono::high_resolution_clock::now();
                //cache downgrade from Modified to Shared rather than release the lock.
                rdma_mg->global_write_page_and_WdowntoR(mr, page_addr, page_size, lock_addr, buffer_inv_message.next_holder_id.load(),
                                                        true);
//                auto time_end = std::chrono::high_resolution_clock::now();

//                printf("Time elapse for cache downgrade over cl %p is %lu\n", page_addr, std::chrono::duration_cast<std::chrono::microseconds>(time_end - time_begin).count());
//                printf("Node %u receive reader invalidate modified invalidation message from node %u over data %p get processed\n", RDMA_Manager::node_id, buffer_inv_message.next_holder_id.load(), gptr);
//                fflush(stdout);

                remote_lock_status.store(1);
            }else if (remote_urging_type == 2 && buffer_inv_message.next_inv_message_type == writer_invalidate_modified){
//                        printf("Lock starvation prevention code was executed stage 3\n");
                assert(buffer_inv_message.next_holder_id != Invalid_Node_ID);
                    assert(buffer_inv_message.next_holder_id != RDMA_Manager::node_id);
                ibv_mr* local_mr = mr;
//                //TODO: Since the page forward is asynchronous, we need to use a local buffer to support the asynchronous RDMA page forward.
//                memccpy(local_mr->addr, mr->addr, kLeafPageSize);
//                ibv_mr* local_mr = mr;
                assert(local_mr->length == kLeafPageSize);
                int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
                *(Page_Forward_Reply_Type* ) ((char*)local_mr->addr + kLeafPageSize - sizeof(Page_Forward_Reply_Type)) = processed;
                // TODO: need to use a local buffer to support the asynchronous RDMA page forward.
                rdma_mg->global_WHandover(mr, page_addr, page_size, buffer_inv_message.next_holder_id.load(), lock_addr,
                                          true);
                rdma_mg->RDMA_Write_xcompute(local_mr, buffer_inv_message.next_receive_page_buf,
                                             buffer_inv_message.next_receive_rkey, kLeafPageSize,
                                             buffer_inv_message.next_holder_id, qp_id, true);

                //TODO: The dirty page flush back here is not necessary.
//                rdma_mg->global_write_page_and_WHandover(mr, page_addr, page_size, buffer_inv_message.next_holder_id.load(), lock_addr,
//                                                         false, nullptr);
                assert(buffer_inv_message.next_holder_id.load() <= 14);

                // The sequence of this two RDMA message could be problematic, because we do not know,
                // whether the global latch release will arrive sooner than the page forward. if not the next cache holder
                // can find the old cach copy holder still there when releasing the latch.
                assert(*(Page_Forward_Reply_Type* ) ((char*)local_mr->addr + kLeafPageSize - sizeof(Page_Forward_Reply_Type)) == processed);


                remote_lock_status.store(0);
//                printf("Node %u receive writer invalidate modified invalidation message from node %u over data %p "
//                       "get processed, target buffer addr is %p\n", RDMA_Manager::node_id, buffer_inv_message.next_holder_id.load(),
//                       gptr, buffer_inv_message.next_receive_page_buf.load());
//                fflush(stdout);
//#else
//                    rdma_mg->global_write_page_and_Wunlock(mr, page_addr, page_size, lock_addr);
//                            remote_lock_status.store(0);
//                            spin_wait_us(WRITER_STARV_SPIN_BASE* (1 + starvation_priority.load()));
//#endif
//                            spin_wait_us(WRITER_STARV_SPIN_BASE* (1 + starvation_priority.load()));
//                }

            }else{
                printf("Buffered invalidation message type is %u, but the lock status now is %u\n", buffer_inv_message.next_inv_message_type.load(), remote_lock_status.load());
                fflush(stdout);
                assert(false);
            }

        }else{
            assert(false);
        }
        clear_pending_inv_states();
//        buffered_inv_mtx.unlock();
    }

    void Cache_Handle::drop_buffered_inv_message(ibv_mr *local_mr, RDMA_Manager *rdma_mg) {
        assert(buffer_inv_message.next_inv_message_type != writer_invalidate_shared);

        *((Page_Forward_Reply_Type*)local_mr->addr) = dropped;

        int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;
//        printf("Node %u Drop the buffered invalidation message over data %p, target %u, buffer addr %p, rkey %u\n",
//               RDMA_Manager::node_id, gptr, buffer_inv_message.next_holder_id.load(),
//               buffer_inv_message.next_receive_page_buf.load(), buffer_inv_message.next_receive_rkey.load());
//        fflush(stdout);
        //todo: assert the message is writer invalid modified.
        void* buffer = (char*)buffer_inv_message.next_receive_page_buf.load() + kLeafPageSize - sizeof(Page_Forward_Reply_Type);
        rdma_mg->RDMA_Write_xcompute(local_mr, buffer, buffer_inv_message.next_receive_rkey,
                                     sizeof(Page_Forward_Reply_Type),
                                     buffer_inv_message.next_holder_id, qp_id, true);
//        clear_pending_inv_states();
        buffer_inv_message.ClearStates();
    }

}  // namespace DSMEngine
