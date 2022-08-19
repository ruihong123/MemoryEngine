#include "Btr.h"

//#include "IndexCache.h"

//#include "RdmaBuffer.h"
#include "Timer.h"
#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
namespace DSMEngine {
bool enter_debug = false;

//HotBuffer hot_buf;
uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];
uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
volatile bool need_stop = false;
thread_local int Btr::round_robin_cur = 0;
thread_local CoroCall Btr::worker[define::kMaxCoro];
thread_local CoroCall Btr::master;
thread_local GlobalAddress path_stack[define::kMaxCoro]
                                     [define::kMaxLevelOfTree];
RDMA_Manager * Btr::rdma_mg = nullptr;
// for coroutine schedule
struct CoroDeadline {
  uint64_t deadline;
  uint16_t coro_id;

  bool operator<(const CoroDeadline &o) const {
    return this->deadline < o.deadline;
  }
};

//thread_local Timer timer;
thread_local std::queue<uint16_t> hot_wait_queue;
thread_local std::priority_queue<CoroDeadline> deadline_queue;

Btr::Btr(RDMA_Manager *mg, uint16_t Btr_id) : tree_id(Btr_id){
    if (rdma_mg == nullptr){
        rdma_mg = mg;
    }
  for (int i = 0; i < rdma_mg->GetMemoryNodeNum(); ++i) {
    local_locks.push_back(new LocalLockNode[define::kNumOfLock]);
    for (size_t k = 0; k < define::kNumOfLock; ++k) {
      auto &n = local_locks[i][k];
      n.ticket_lock.store(0);
      n.hand_over = false;
      n.hand_time = 0;
    }
  }

  print_verbose();

    page_cache = NewLRUCache(define::kIndexCacheSize);

//  root_ptr_ptr = get_root_ptr_ptr();

  // try to init tree and install root pointer
    rdma_mg->Allocate_Local_RDMA_Slot(cached_root_page_mr, Default);// local allocate
    if (rdma_mg->node_id == 1){
        // only the first compute node create the root node for index
        g_root_ptr = rdma_mg->Allocate_Remote_RDMA_Slot(Default, (round_robin_cur++)%rdma_mg->memory_nodes.size()); // remote allocation.
        auto root_page = new (cached_root_page_mr.addr) LeafPage;

        root_page->set_consistent();
        rdma_mg->RDMA_Write(g_root_ptr, &cached_root_page_mr, kLeafPageSize, 1, 1, FlushBuffer, std::string());
        // TODO: create a special region to store the root_ptr for every tree id.
        auto local_mr = rdma_mg->Get_local_CAS_mr(); // remote allocation.
        ibv_mr remote_mr{};
        remote_mr = *rdma_mg->global_index_table;
        // find the table enty according to the id
        remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
        rdma_mg->RDMA_CAS(&remote_mr, local_mr, 0, g_root_ptr, 1, 1, 0);
    }else{
        get_root_ptr();
    }

//  auto cas_buffer = (rdma_mg->get_rbuf(0)).get_cas_buffer();
//  bool res = rdma_mg->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
//  if (res) {
//    std::cout << "Tree root pointer value " << root_addr << std::endl;
//  } else {
//     std::cout << "fail\n";
//  }
}

void Btr::print_verbose() {

  int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
  int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
  assert(kLeafHdrOffset == kInternalHdrOffset);

  if (rdma_mg->node_id == 0) {
    std::cout << "Header size: " << sizeof(Header) << std::endl;
    std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
    std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << kLeafPageSize
              << "]" << std::endl;
    std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    std::cout << "LeafEntry size: " << sizeof(LeafEntry) << std::endl;
    std::cout << "InternalEntry size: " << sizeof(InternalEntry) << std::endl;
  }
}

inline void Btr::before_operation(CoroContext *cxt, int coro_id) {
  for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
    path_stack[coro_id][i] = GlobalAddress::Null();
  }
}

GlobalAddress Btr::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.nodeID = 0;
  addr.offset =
      define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;

  return addr;
}

extern GlobalAddress g_root_ptr;
extern int g_root_level;
extern bool enable_cache;
GlobalAddress Btr::get_root_ptr() {
// TODO: Use an RPC to get the root pointer from the first node.
  if (g_root_ptr == GlobalAddress::Null()) {
      std::unique_lock<std::mutex> l(mtx);
      if (g_root_ptr == GlobalAddress::Null()) {
          ibv_mr* local_mr = rdma_mg->Get_local_CAS_mr();

          ibv_mr remote_mr{};
          remote_mr = *rdma_mg->global_index_table;
          // find the table enty according to the id
          remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
          *(GlobalAddress*)(local_mr->addr) = GlobalAddress::Null();
          // The first compute node may not have written the root ptr to root_ptr_ptr, we need to keep polling.
          while (*(GlobalAddress*)(local_mr->addr) == GlobalAddress::Null()) {
              rdma_mg->RDMA_Read(&remote_mr, local_mr, sizeof(GlobalAddress), 1, 1, 0);
          }
          g_root_ptr = *(GlobalAddress*)local_mr->addr;
          std::cout << "Get new root" << g_root_ptr <<std::endl;

      }

    return g_root_ptr;
  } else {
    return g_root_ptr;
  }

  // std::cout << "root ptr " << root_ptr << std::endl;
}

void Btr::broadcast_new_root(GlobalAddress new_root_addr, int root_level) {
    RDMA_Request* send_pointer;
    ibv_mr send_mr = {};
//    ibv_mr receive_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    send_pointer = (RDMA_Request*)send_mr.addr;
    send_pointer->command = broadcast_root;
    send_pointer->content.root_broadcast.new_ptr = new_root_addr;
    send_pointer->content.root_broadcast.level = root_level;

//  if (root_level >= 5) {
//        enable_cache = true;
//  }
  //TODO: When we seperate the compute from the memory, how can we broad cast the new root
  // or can we wait until the compute node detect an inconsistent.

    rdma_mg->post_send<RDMA_Request>(&send_mr, 0, std::string("main"));
    ibv_wc wc[2] = {};
    //  while(wc.opcode != IBV_WC_RECV){
    //    poll_completion(&wc);
    //    if (wc.status != 0){
    //      fprintf(stderr, "Work completion status is %d \n", wc.status);
    //    }
    //
    //  }
    //  assert(wc.opcode == IBV_WC_RECV);
    if (rdma_mg->poll_completion(wc, 1, std::string("main"),
                        true, 0)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
        fprintf(stderr, "failed to poll send for remote memory register\n");
    }
}

bool Btr::update_new_root(GlobalAddress left, const Key &k,
                           GlobalAddress right, int level,
                           GlobalAddress old_root, CoroContext *cxt,
                           int coro_id) {

  auto page_buffer = rdma_mg->Get_local_read_mr();
  auto cas_buffer = rdma_mg->Get_local_CAS_mr();
    assert(left != GlobalAddress::Null());
    assert(right != GlobalAddress::Null());
  auto new_root = new (page_buffer->addr) InternalPage(left, k, right, level);

  auto new_root_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Default, (round_robin_cur++)%rdma_mg->memory_nodes.size());
  // The code below is just for debugging
//    new_root_addr.mark = 3;
  new_root->set_consistent();
  // set local cache for root address
  g_root_ptr = new_root_addr;
    tree_height = level;
  rdma_mg->RDMA_Write(new_root_addr, page_buffer, kInternalPageSize,1,1, Default);
  if (rdma_mg->RDMA_CAS(&cached_root_page_mr, cas_buffer, old_root, new_root_addr, 1,1,Default)) {
    broadcast_new_root(new_root_addr, level);
    std::cout << "new root level " << level << " " << new_root_addr
              << std::endl;
    return true;
  } else {
    std::cout << "cas root fail " << std::endl;
  }

  return false;
}

void Btr::print_and_check_tree(CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());

  auto root = get_root_ptr();
  // SearchResult result;

  GlobalAddress p = root;
  GlobalAddress levels[define::kMaxLevelOfTree];
  int level_cnt = 0;
  auto page_buffer = rdma_mg->Get_local_read_mr();
  GlobalAddress leaf_head;

next_level:

  rdma_mg->RDMA_Read(p, page_buffer, kInternalPageSize, 1, 1, Default);
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  levels[level_cnt++] = p;
  if (header->level != 0) {
    p = header->leftmost_ptr;
    goto next_level;
  } else {
    leaf_head = p;
  }

next:
    rdma_mg->RDMA_Read(leaf_head, page_buffer, kLeafPageSize, 1, 1, Default);
//  rdma_mg->read_sync(page_buffer, , kLeafPageSize);
  auto page = (LeafPage *)page_buffer;
  for (int i = 0; i < kLeafCardinality; ++i) {
    if (page->records[i].value != kValueNull) {
    }
  }
  while (page->hdr.sibling_ptr != GlobalAddress::Null()) {
    leaf_head = page->hdr.sibling_ptr;
    goto next;
  }

  // for (int i = 0; i < level_cnt; ++i) {
  //   rdma_mg->read_sync(page_buffer, levels[i], kLeafPageSize);
  //   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //   // std::cout << "addr: " << levels[i] << " ";
  //   // header->debug();
  //   // std::cout << " | ";
  //   while (header->sibling_ptr != GlobalAddress::Null()) {
  //     rdma_mg->read_sync(page_buffer, header->sibling_ptr, kLeafPageSize);
  //     header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //     // std::cout << "addr: " << header->sibling_ptr << " ";
  //     // header->debug();
  //     // std::cout << " | ";
  //   }
  //   // std::cout << "\n------------------------------------" << std::endl;
  //   // std::cout << "------------------------------------" << std::endl;
  // }
}

GlobalAddress Btr::query_cache(const Key &k) { return GlobalAddress::Null(); }

inline bool Btr::try_lock_addr(GlobalAddress lock_addr, uint64_t tag,
                               ibv_mr *buf, CoroContext *cxt, int coro_id) {
//  auto &pattern_cnt = pattern[rdma_mg->getMyThreadID()][lock_addr.nodeID];

  bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  if (hand_over) {
    return true;
  }

  {

    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
  retry:
    retry_cnt++;
    if (retry_cnt > 3000) {
      std::cout << "Deadlock " << lock_addr << std::endl;

      std::cout << rdma_mg->GetMemoryNodeNum() << ", "
                << " locked by node  " << (conflict_tag) << std::endl;
      assert(false);
      exit(0);
    }
      *(uint64_t *)buf->addr = 0;
    rdma_mg->RDMA_CAS(lock_addr, buf, 0, tag, 1,1, LockTable);
    if ((*(uint64_t*) buf->addr) == 0){
        conflict_tag = *(uint64_t*)buf->addr;
        if (conflict_tag != pre_tag) {
            retry_cnt = 0;
            pre_tag = conflict_tag;
        }
//      lock_fail[rdma_mg->getMyThreadID()][0]++;
        goto retry;
    }

  }

  return true;
}

inline void Btr::unlock_addr(GlobalAddress lock_addr, CoroContext *cxt, int coro_id, bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    releases_local_lock(lock_addr);
    return;
  }

  auto cas_buf = rdma_mg->Get_local_CAS_mr();
//    std::cout << "unlock " << lock_addr << std::endl;
  *(uint64_t*)cas_buf->addr = 0;
  if (async) {
    rdma_mg->RDMA_Write(lock_addr, cas_buf,  sizeof(uint64_t), 0,0,LockTable);
  } else {
      rdma_mg->RDMA_Write(lock_addr, cas_buf,  sizeof(uint64_t), 1,1,LockTable);
  }

  releases_local_lock(lock_addr);
}

void Btr::write_page_and_unlock(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, uint64_t *cas_buffer,
                                GlobalAddress lock_addr, CoroContext *cxt, int coro_id, bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    rdma_mg->RDMA_Write(page_addr, page_buffer, page_size, 0,0,Default);
    releases_local_lock(lock_addr);
    return;
  }
    struct ibv_send_wr sr[2];
    struct ibv_sge sge[2];
    if (async){

        rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 0, Default);
        ibv_mr* local_mr = rdma_mg->Get_local_CAS_mr();

        rdma_mg->Prepare_WR_Write(sr[1], sge[1], lock_addr, local_mr, sizeof(uint64_t), 0, Default);
        sr[0].next = &sr[1];


        *(uint64_t *)local_mr->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        rdma_mg->Batch_Submit_WRs(sr, 0, page_addr.nodeID);
    }else{
        rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 1, Default);
        ibv_mr* local_mr = rdma_mg->Get_local_CAS_mr();

        rdma_mg->Prepare_WR_Write(sr[1], sge[1], lock_addr, local_mr, sizeof(uint64_t), 1, Default);
        sr[0].next = &sr[1];


        *(uint64_t *)local_mr->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        rdma_mg->Batch_Submit_WRs(sr, 2, page_addr.nodeID);
    }

//  if (async) {
//    rdma_mg->write_batch(rs, 2, false);
//  } else {
//    rdma_mg->write_batch_sync(rs, 2, cxt);
//  }

  releases_local_lock(lock_addr);
}

void Btr::lock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr,
                             int page_size, ibv_mr *cas_buffer,
                             GlobalAddress lock_addr, uint64_t tag,
                             CoroContext *cxt, int coro_id) {
    // Can put lock and page read in a door bell batch.
    bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
    if (hand_over) {
        rdma_mg->RDMA_Read(page_addr, page_buffer, page_size, 1,1, Default);
    }

    {

        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
    retry:
        retry_cnt++;
        if (retry_cnt > 3000) {
            std::cout << "Deadlock " << lock_addr << std::endl;

            std::cout << rdma_mg->GetMemoryNodeNum() << ", "
                      << " locked by node  " << (conflict_tag) << std::endl;
            assert(false);
            exit(0);
        }
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];

        rdma_mg->Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, 0, tag, 1, Default);
        rdma_mg->Prepare_WR_Write(sr[1], sge[1], page_addr, page_buffer, page_size, 1, Default);
        *(uint64_t *)cas_buffer->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        rdma_mg->Batch_Submit_WRs(sr, 2, page_addr.nodeID);
        if ((*(uint64_t*) cas_buffer->addr) == 0){
            conflict_tag = *(uint64_t*)cas_buffer->addr;
            if (conflict_tag != pre_tag) {
                retry_cnt = 0;
                pre_tag = conflict_tag;
            }
//      lock_fail[rdma_mg->getMyThreadID()][0]++;
            goto retry;
        }

    }

//  rdma_mg->read_sync(page_buffer, page_addr, page_size, cxt);
//  pattern[rdma_mg->getMyThreadID()][page_addr.nodeID]++;
}

//void Btr::lock_bench(const Key &k, CoroContext *cxt, int coro_id) {
//  uint64_t lock_index = CityHash64((char *)&k, sizeof(k)) % define::kNumOfLock;
//
//  GlobalAddress lock_addr;
//  lock_addr.nodeID = 0;
//  lock_addr.offset = lock_index * sizeof(uint64_t);
//  auto cas_buffer = rdma_mg->get_rbuf(coro_id).get_cas_buffer();
//
//  // bool res = rdma_mg->cas_sync(lock_addr, 0, 1, cas_buffer, cxt);
//  try_lock_addr(lock_addr, 1, cas_buffer, cxt, coro_id);
//  unlock_addr(lock_addr, 1, cas_buffer, cxt, coro_id, true);
//}
// You need to make sure it is not the root level, this function will make sure the
// the insertion to this target level succcessful
// the target level can not be the root level.
void Btr::insert_internal(Key &k, GlobalAddress &v, CoroContext *cxt,
                          int coro_id, int target_level) {

    //TODO: You need to acquire a lock when you write a page
    auto root = get_root_ptr();
    SearchResult result;

    GlobalAddress p = root;
    //TODO: ADD support for root invalidate and update.


    bool isroot = true;
    // this is root is to help the tree to refresh the root node because the
    // new root broadcast is not usable if physical disaggregated.
    int level = -1;
    //TODO: What if we ustilize the cache tree height for the root level?

next: // Internal page search
    //TODO: What if the target_level is equal to the root level.
    if (!internal_page_search(p, k, result, level, isroot, cxt, coro_id)) {
        if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
            p = get_root_ptr();
            level = -1;
        }else{
            // fall back to upper level
            assert(level == result.level);
            p = path_stack[coro_id][result.level +1];
            level = result.level +1;
        }
        goto next;
    }else{
        assert(level == result.level);
        isroot = false;
        // if the root and sibling are the same, it is also okay because the
        // p will not be changed
        if (result.slibing != GlobalAddress::Null()) { // turn right
            p = result.slibing;

        }else if (result.next_level != GlobalAddress::Null()){
            assert(result.next_level != GlobalAddress::Null());
            p = result.next_level;
            level = result.level - 1;
        }else{

        }

        if (level != target_level){

            goto next;
        }

    }
    //Insert to leaf level
    Key split_key;
    GlobalAddress sibling_prt;
    if (!internal_page_store(p, k, v, level, cxt, coro_id)){
        if (path_stack[coro_id][level + 1] != GlobalAddress::Null()){
            p = path_stack[coro_id][level + 1];
            level = level + 1;
        }
        else{
            // re-search the tree from the scratch. (only happen when root and leaf are the same.)
            p = get_root_ptr();
            level = -1;
        }
        goto next;
    }

//    internal_page_store(p, k, v, level, cxt, coro_id);
}

void Btr::insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());

  before_operation(cxt, coro_id);



  auto root = get_root_ptr();
//  std::cout << "The root now is " << root << std::endl;
  SearchResult result;
  GlobalAddress p = root;
    bool isroot = true;
  // this is root is to help the tree to refresh the root node because the
  // new root broadcast is not usable if physical disaggregated.
    int level = -1;
//TODO: What if we ustilize the cache tree height for the root level?
    int target_level = 0;
next: // Internal page search
    if (!internal_page_search(p, k, result, level, isroot, cxt, coro_id)) {
        if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
            p = get_root_ptr();
            level = -1;
        }else{
            // fall back to upper level
            assert(level == result.level);
            p = path_stack[coro_id][result.level +1];
            level = result.level +1;
        }
        goto next;
    }
    else{
        assert(level == result.level);
        isroot = false;
        // if the root and sibling are the same, it is also okay because the
        // p will not be changed
        if (result.slibing != GlobalAddress::Null()) { // turn right
            p = result.slibing;

        }else if (result.next_level != GlobalAddress::Null()){
            assert(result.next_level != GlobalAddress::Null());
            p = result.next_level;
            level = result.level - 1;
        }else{}

        if (level != target_level){

            goto next;
        }

    }
    //Insert to leaf level
    Key split_key;
    GlobalAddress sibling_prt;
//    if (target_level == 0){
//
//    }
    if (!leaf_page_store(p, k, v, split_key, sibling_prt, root, 0, cxt, coro_id)){
        if (path_stack[coro_id][1] != GlobalAddress::Null()){
            p = path_stack[coro_id][1];
            level = 1;
        }
        else{
            // re-search the tree from the scratch. (only happen when root and leaf are the same.)
            p = get_root_ptr();
            level = -1;
        }
        goto next;
    }

    assert(level == 0);
    level = level +1;
    p = path_stack[coro_id][level];
//internal_store:
    // Insert to internal level if needed.
    // the level here is equals to the current insertion level
    while(sibling_prt != GlobalAddress::Null()){
        if (!internal_page_store(p, split_key, sibling_prt, level, cxt, coro_id)){
            // insertion failed, retry
            if ( path_stack[coro_id][level + 1] == GlobalAddress::Null()){
                // THis path should only happen at roof level happen.
                // TODO: What if the node is not the root node.
//                level = level + 1;
//                 insert_internal(split_key,sibling_prt, cxt, coro_id, level);

                p = get_root_ptr();
                level = -1;
            }else{
                // fall back to upper level to search for the right node at this level
                p = path_stack[coro_id][level +1];
                //TODO: Change it into while style.
re_search:
                if(!internal_page_search(p, k, result, level + 1, isroot, cxt, coro_id)){
                    // retranverse the tree by insert_internal.
                    insert_internal(split_key,sibling_prt, cxt, coro_id, level);
                    level = level + 1; // move to upper level
                    p = path_stack[coro_id][level];// move the pointer to upper level
                }else{
                    if (result.slibing != GlobalAddress::Null()) { // turn right
                        // continue searching the sibling
                        p = result.slibing;
                        goto re_search;
                    }else{
                        // do not need to chanve level.
                        p = result.next_level;
//                    level = result.level - 1;
                    }
                }

            }

        }else{
            level = level + 1;
            p = path_stack[coro_id][level];
        }

    }


//  if (res == HotResult::SUCC) {
//    hot_buf.clear(k);
//  }
}

bool Btr::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());

  auto root = get_root_ptr();
  SearchResult result;

  GlobalAddress p = root;
    bool isroot = true;
  bool from_cache = false;
//  const CacheEntry *entry = nullptr;
//  if (enable_cache) {
//    GlobalAddress cache_addr;
//    entry = page_cache->search_from_cache(k, &cache_addr);
//    if (entry) { // cache hit
////      cache_hit[rdma_mg->getMyThreadID()][0]++;
//      from_cache = true;
//      p = cache_addr;
//      isroot = false;
//    } else {
////      cache_miss[rdma_mg->getMyThreadID()][0]++;
//    }
//  }
int level = -1;
//TODO: What if we ustilize the cache tree height for the root level?
//TODO: Change it into while style code.

next: // Internal page search
    if (!internal_page_search(p, k, result, level, isroot, cxt, coro_id)) {
        if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
            p = get_root_ptr();
            level = -1;
        }else{
            // fall back to upper level
            assert(level == result.level);
            p = path_stack[coro_id][result.level +1];
            level = result.level + 1;
        }
        goto next;
    }
    else{
        assert(level == result.level);
        isroot = false;
        // Do not need to
        if (result.slibing != GlobalAddress::Null()) { // turn right
            p = result.slibing;

        }else if (result.next_level != GlobalAddress::Null()){
            assert(result.next_level != GlobalAddress::Null());
            p = result.next_level;
            level = result.level - 1;
        }else{}

        if (level > 0){

            goto next;
        }

    }

leaf_next:// Leaf page search
    if (!leaf_page_search(p, k, result, level, cxt, coro_id)){
        if (path_stack[coro_id][1] != GlobalAddress::Null()){
            p = path_stack[coro_id][1];
            level = 1;

        }
        else{
            p = get_root_ptr();
            level = -1;
        }
        goto next;
    }else{
        if (result.val != kValueNull) { // find
            v = result.val;

            return true;
        }
        if (result.slibing != GlobalAddress::Null()) { // turn right
            p = result.slibing;
            goto leaf_next;
        }
        return false; // not found
    }



//    if (result.is_leaf) {
//        if (result.val != kValueNull) { // find
//            v = result.val;
//
//            return true;
//        }
//        if (result.slibing != GlobalAddress::Null()) { // turn right
//            p = result.slibing;
//            goto next;
//        }
//        return false; // not found
//    } else {        // internal
//        p = result.slibing != GlobalAddress::Null() ? result.slibing
//                                                    : result.level;
//        goto next;
//    }
}

// TODO: Need Fix range query
//uint64_t Btr::range_query(const Key &from, const Key &to, Value *value_buffer,
//                           CoroContext *cxt, int coro_id) {
//
//  const int kParaFetch = 32;
//  thread_local std::vector<InternalPage *> result;
//  thread_local std::vector<GlobalAddress> leaves;
//
//  result.clear();
//  leaves.clear();
//  page_cache->search_range_from_cache(from, to, result);
//
//  if (result.empty()) {
//    return 0;
//  }
//
//  uint64_t counter = 0;
//  for (auto page : result) {
//    auto cnt = page->hdr.last_index + 1;
//    auto addr = page->hdr.leftmost_ptr;
//
//    // [from, to]
//    // [lowest, page->records[0].key);
//    bool no_fetch = from > page->records[0].key || to < page->hdr.lowest;
//    if (!no_fetch) {
//      leaves.push_back(addr);
//    }
//    for (int i = 1; i < cnt; ++i) {
//      no_fetch = from > page->records[i].key || to < page->records[i - 1].key;
//      if (!no_fetch) {
//        leaves.push_back(page->records[i - 1].ptr);
//      }
//    }
//
//    no_fetch = from > page->hdr.highest || to < page->records[cnt - 1].key;
//    if (!no_fetch) {
//      leaves.push_back(page->records[cnt - 1].ptr);
//    }
//  }
//
//  // printf("---- %d ----\n", leaves.size());
//  // sleep(1);
//
//  int cq_cnt = 0;
//  char *range_buffer = (rdma_mg->get_rbuf(coro_id)).get_range_buffer();
//  for (size_t i = 0; i < leaves.size(); ++i) {
//    if (i > 0 && i % kParaFetch == 0) {
//      rdma_mg->poll_rdma_cq(kParaFetch);
//      cq_cnt -= kParaFetch;
//      for (int k = 0; k < kParaFetch; ++k) {
//        auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
//        for (int i = 0; i < kLeafCardinality; ++i) {
//          auto &r = page->records[i];
//          if (r.value != kValueNull && r.f_version == r.r_version) {
//            if (r.key >= from && r.key <= to) {
//              value_buffer[counter++] = r.value;
//            }
//          }
//        }
//      }
//    }
//    rdma_mg->read(range_buffer + kLeafPageSize * (i % kParaFetch), leaves[i],
//                  kLeafPageSize, true);
//    cq_cnt++;
//  }
//
//  if (cq_cnt != 0) {
//    rdma_mg->poll_rdma_cq(cq_cnt);
//    for (int k = 0; k < cq_cnt; ++k) {
//      auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
//      for (int i = 0; i < kLeafCardinality; ++i) {
//        auto &r = page->records[i];
//        if (r.value != kValueNull && r.f_version == r.r_version) {
//          if (r.key >= from && r.key <= to) {
//            value_buffer[counter++] = r.value;
//          }
//        }
//      }
//    }
//  }
//
//  return counter;
//}

// Del needs to be rewritten
void Btr::del(const Key &k, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());
    before_operation(cxt, coro_id);



    auto root = get_root_ptr();
//  std::cout << "The root now is " << root << std::endl;
    SearchResult result;
    GlobalAddress p = root;
    bool isroot = true;
    // this is root is to help the tree to refresh the root node because the
    // new root broadcast is not usable if physical disaggregated.
    int level = -1;
//TODO: What if we ustilize the cache tree height for the root level?
    int target_level = 0;
    next: // Internal page search
    if (!internal_page_search(p, k, result, level, isroot, cxt, coro_id)) {
        if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
            p = get_root_ptr();
            level = -1;
        }else{
            // fall back to upper level
            assert(level == result.level);
            p = path_stack[coro_id][result.level +1];
            level = result.level +1;
        }
        goto next;
    }
    else{
        assert(level == result.level);
        isroot = false;
        // if the root and sibling are the same, it is also okay because the
        // p will not be changed
        if (result.slibing != GlobalAddress::Null()) { // turn right
            p = result.slibing;

        }else if (result.next_level != GlobalAddress::Null()){
            assert(result.next_level != GlobalAddress::Null());
            p = result.next_level;
            level = result.level - 1;
        }else{}

        if (level != target_level){

            goto next;
        }

    }
    //Insert to leaf level
    Key split_key;
    GlobalAddress sibling_prt;
//    if (target_level == 0){
//
//    }
    //The node merge may triggered by background thread on the memory node only.
    if (!leaf_page_del(p, k,   0, cxt, coro_id)){
        if (path_stack[coro_id][1] != GlobalAddress::Null()){
            p = path_stack[coro_id][1];
            level = 1;
        }
        else{
            // re-search the tree from the scratch. (only happen when root and leaf are the same.)
            p = get_root_ptr();
            level = -1;
        }
        goto next;
    }

  leaf_page_del(p, k, 0, cxt, coro_id);
}
/**
 * Node ID in GLobalAddress for a tree pointer should be the id in the Memory pool
 THis funciton will get the page by the page addr and search the pointer for the
 next level if it is not leaf page. If it is a leaf page, just put the value in the
 result. If this function return false then the result return nothing and we need to
 start from upper level again without cache.
 * @param page_addr
 * @param k
 * @param result
 * @param cxt
 * @param coro_id
 * @param isroot
 * @return
 */
    bool Btr::internal_page_search(GlobalAddress page_addr, const Key &k, SearchResult &result, int level, bool isroot,
                                   CoroContext *cxt, int coro_id) {

// tothink: How could I know whether this level before I actually access this page.



//  auto &pattern_cnt = pattern[rdma_mg->getMyThreadID()][page_addr.nodeID];

    int counter = 0;

    if (++counter > 100) {
    printf("re read too many times\n");
    sleep(1);
    }
    // TODO: We need to implement the lock coupling. how to avoid unnecessary RDMA for lock coupling?
    //
    Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
    Cache::Handle* handle = nullptr;
    void* page_buffer;
    Header * header;
    InternalPage* page;
    handle = page_cache->Lookup(page_id);

    //TODO: Shall we implement a shared-exclusive lock here for local contention. or we still
    // follow the optimistic latch free design?
    if (handle != nullptr){
        auto mr = (ibv_mr*)page_cache->Value(handle);
        page_buffer = mr->addr;
        header = (Header *)((char*)page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
        page = (InternalPage *)page_buffer;
        memset(&result, 0, sizeof(result));
        result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
        result.level = header->level;
//  if(!result.is_leaf)
//      assert(result.level !=0);
        assert(result.is_leaf == (level == 0));
        path_stack[coro_id][result.level] = page_addr;
    }else {

        //  pattern_cnt++;
        ibv_mr* new_mr = new ibv_mr{};
        rdma_mg->Allocate_Local_RDMA_Slot(*new_mr, Default);
        page_buffer = new_mr->addr;
        header = (Header *) ((char*)page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
        page = (InternalPage *)page_buffer;
    rdma_refetch:
        rdma_mg->RDMA_Read(page_addr,new_mr, kLeafPageSize, 1,1,Default);

        memset(&result, 0, sizeof(result));
        result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
        result.level = header->level;
        path_stack[coro_id][result.level] = page_addr;
        // This consistent check should be in the path of RDMA read only.
        if (!page->check_consistent()) {
            //TODO: What is the other thread is modifying this page but you overwrite the buffer by a reread.
            // How to tell whether the inconsistent content is from local read-write conflict or remote
            // RDMA read and write conflict

            //If this page is fetch from the remote memory, discard the page before insert to the cache,
            // then refetch the page by RDMA.
            //If this page read from the
            goto rdma_refetch;
        }
        if (result.level == 0){
            // if the root node is the leaf node this path will happen.
            assert(level = -1);
            rdma_mg->Deallocate_Local_RDMA_Slot(page_buffer, Default);
            // TODO: return true is okay.
            return leaf_page_search(page_addr, k, result, 0, cxt, coro_id);
        }

        // if there has already been a cache entry with the same key, the old one will be
        // removed from the cache, but it may not be garbage collected right away
        handle = page_cache->Insert(page_id, new_mr, 1, Deallocate_MR);

    }

    assert(result.level != 0);
    //          assert(!from_cache);

    //      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());





    if (k >= page->hdr.highest) { // should turn right
    //        printf("should turn right ");
      // TODO: if this is the root node then we need to refresh the new root.
      if (isroot){
          // invalidate the root.
          g_root_ptr = GlobalAddress::Null();
      }else{
          assert(path_stack[coro_id][result.level+1] != GlobalAddress::Null());
          page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
      }
      //TODO: What if the Erased key is still in use by other threads? THis is very likely
      // for the upper level nodes.
    //          if (path_stack[coro_id][result.level+1] != GlobalAddress::Null()){
    //              page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
    //
    //          }
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      if (isroot){
          // invalidate the root.
          g_root_ptr = GlobalAddress::Null();
      }else{
          assert(path_stack[coro_id][result.level+1] != GlobalAddress::Null());
          page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
      }
    //              printf("key %ld error in level %d\n", k, page->hdr.level);
    //              sleep(10);
    //              print_and_check_tree();
    //              assert(false);
    //TODO: Maybe we can implement a invalidation instead of the erase. which will not
    // deallocate the memolry region of this cache entry.

    //          if (path_stack[coro_id][result.level+1] != GlobalAddress::Null()){
    //              page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
    //
    //          }

      return false;
    }
    // this function will add the children pointer to the result.
    page->internal_page_search(k, result);


  page_cache->Release(handle);



  return true;
}

bool Btr::leaf_page_search(GlobalAddress page_addr, const Key &k, SearchResult &result, int level, CoroContext *cxt,
                           int coro_id) {
    int counter = 0;
re_read:
    if (++counter > 100) {
        printf("re read too many times\n");
        sleep(1);
    }
    // TODO: We need to implement the lock coupling. how to avoid unnecessary RDMA for lock coupling?
    //
    Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
    Cache::Handle* handle = nullptr;
    void* page_buffer;
    Header * header;
    assert(level == 0);
    ibv_mr* local_mr = rdma_mg->Get_local_read_mr();
    page_buffer = local_mr->addr;
    header = (Header *) ((char*)page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
    rdma_mg->RDMA_Read(page_addr,local_mr, kLeafPageSize, 1,1,Default);
    memset(&result, 0, sizeof(result));
    result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
    result.level = header->level;
//  if(!result.is_leaf)
//      assert(result.level !=0);
//        assert(result.is_leaf == (level == 0));
    path_stack[coro_id][result.level] = page_addr;
    auto page = (LeafPage *)page_buffer;
    if (!page->check_consistent()) {
        goto re_read;
    }

    if ((k < page->hdr.lowest )) { // cache is stale
        // erase the upper node from the cache and refetch the upper node to continue.
        int last_level = 1;
        if (path_stack[coro_id][last_level] != GlobalAddress::Null()){
            page_cache->Erase(Slice((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress)));

        }
        return false;// false means need to fall back
    }

    assert(result.level == 0);
    if (k >= page->hdr.highest) { // should turn right, the highest is not included
//        printf("should turn right ");
//              if (page->hdr.sibling_ptr != GlobalAddress::Null()){
        // erase the upper level from the cache
        int last_level = 1;
        if (path_stack[coro_id][last_level] != GlobalAddress::Null()){
            page_cache->Erase(Slice((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress)));

        }
        //              }
        result.slibing = page->hdr.sibling_ptr;
        return true;
    }
    page->leaf_page_search(k, result);
    return true;
}

bool
Btr::internal_page_store(GlobalAddress page_addr, Key &k, GlobalAddress &v, int level, CoroContext *cxt, int coro_id) {
    uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;
    bool need_split;
    bool insert_success;
    GlobalAddress lock_addr;
    lock_addr.nodeID = page_addr.nodeID;
    lock_addr.offset = lock_index * sizeof(uint64_t);
// Shall the page modification happen over the cached data buffer?

//Yes
    Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
    Cache::Handle* handle = nullptr;
    handle = page_cache->Lookup(page_id);
    ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
    ibv_mr* local_buffer;
    void * page_buffer;
    if (handle!= nullptr){
        local_buffer = (ibv_mr*)page_cache->Value(handle);
        page_buffer = local_buffer->addr;
        // you have to reread to data from the remote side to not missing update from other
        // nodes! Do not read the page from the cache!

        lock_and_read_page(local_buffer, page_addr, kInternalPageSize, cas_mr,
                           lock_addr, 1, cxt, coro_id);
    } else{

        local_buffer = new ibv_mr{};
        rdma_mg->Allocate_Local_RDMA_Slot(*local_buffer, Default);
        page_buffer = local_buffer->addr;
        // you have to reread to data from the remote side to not missing update from other
        // nodes! Do not read the page from the cache!

        lock_and_read_page(local_buffer, page_addr, kInternalPageSize, cas_mr,
                           lock_addr, 1, cxt, coro_id);

        handle = page_cache->Insert(page_id, local_buffer, 1, Deallocate_MR);
        // No need for consistence check here.
    }


    auto page = (InternalPage *)page_buffer;

    assert(page->hdr.level == level);
    assert(page->check_consistent());

    // This is the result that we do not lock the btree when search for the key.
    // Not sure whether this will still work if we have node merge
    // Why this node can not be the right most node
    if (k >= page->hdr.highest ) {
      // TODO: No need for node invalidation when inserting things because the tree tranversing is enough for invalidation (Erase)

        if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
          page_cache->Erase(Slice((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress)));
        }
        this->unlock_addr(lock_addr, cxt, coro_id, true);

        assert(page->hdr.sibling_ptr != GlobalAddress::Null());


        insert_success = this->internal_page_store(page->hdr.sibling_ptr, k, v, level, cxt,
                                                   coro_id);
        return insert_success;
    }

    if (k < page->hdr.lowest ) {
        // if key is smaller than the lower bound, the insert has to be restart from the
        // upper level. because the sibling pointer only points to larger one.

        if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
            page_cache->Erase(Slice((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress)));
        }
        this->unlock_addr(lock_addr, cxt, coro_id, true);

        insert_success = false;
        return insert_success;// result in fall back search on the higher level.
    }
//  assert(k >= page->hdr.lowest);

  auto cnt = page->hdr.last_index + 1;
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  bool is_update = false;
  uint16_t insert_index = 0;
  //TODO: Make it a binary search.
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
      page->records[i].ptr = v;
      // assert(false);
      is_update = true;
      break;
    }
    if (page->records[i].key < k) {
      insert_index = i + 1;
      break;
    }
  }
  assert(cnt != kInternalCardinality);

  if (!is_update) { // insert and shift
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].ptr = page->records[i - 1].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    page->hdr.last_index++;
  }
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  assert(page->records[page->hdr.last_index].key != 0);

  cnt = page->hdr.last_index + 1;
  need_split = cnt == kInternalCardinality;
  Key split_key;
  GlobalAddress sibling_addr;
  // THe internal node is different from leaf nodes because it has the
  // leftmost_ptr. THe internal nodes has n key but n+1 global pointers.
    // the internal node split pick the middle key as split key and the middle key
    // will not existed in either of the splited node
    // THe data under this internal node [lowest, highest)

    //Both internal node and leaf nodes are [lowest, highest) except for the left most
  if (need_split) { // need split
    sibling_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Default, (round_robin_cur++)%rdma_mg->memory_nodes.size());
    ibv_mr* sibling_buf = new ibv_mr{};
    rdma_mg->Allocate_Local_RDMA_Slot(*sibling_buf, Default);

    auto sibling = new (sibling_buf->addr) InternalPage(page->hdr.level);

    //    std::cout << "addr " <<  sibling_addr << " | level " <<
    //    (int)(page->hdr.level) << std::endl;
      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);
      for (int i = m + 1; i < cnt; ++i) { // move
          sibling->records[i - m - 1].key = page->records[i].key;
          sibling->records[i - m - 1].ptr = page->records[i].ptr;
      }
      page->hdr.last_index -= (cnt - m); // this is correct.
      assert(page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m - 1);
      assert(sibling->hdr.last_index == cnt - m - 1 - 1);
      sibling->hdr.leftmost_ptr = page->records[m].ptr;
      sibling->hdr.lowest = page->records[m].key;
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = page->records[m].key;

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
      page->hdr.sibling_ptr = sibling_addr;
    sibling->set_consistent();
    //the code below is just for debugging.
//    sibling_addr.mark = 2;

    rdma_mg->RDMA_Write(sibling_addr, sibling_buf, kInternalPageSize,1,1,Default);
      assert(sibling->records[sibling->hdr.last_index].ptr != GlobalAddress::Null());
      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
      k = split_key;
      v = sibling_addr;
  } else{
//      k = Key ;
    // Only set the value as null is enough
      v = GlobalAddress::Null();
  }
//  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());


    page->set_consistent();
    write_page_and_unlock(local_buffer, page_addr, kInternalPageSize, (uint64_t*)cas_mr->addr,
                          lock_addr, cxt, coro_id, need_split);

    return true;
//  if (!need_split)
//    return;
//// TODO: migrate the function outside.
//  if (root == page_addr) { // update root
//
//    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
//                        cxt, coro_id)) {
//      return;
//    }else{
//        //CAS the root_ptr_ptr failed. need to re-insert.
//    }
//  }
//
//  auto up_level = path_stack[coro_id][level + 1];
//    //TOTHINGK: What if the upper level has been splitted by other nodes.
//  if (up_level != GlobalAddress::Null()) {
//    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
//                        coro_id);
//  } else {
//      // internal page store means that you are running insert_internal, and there
//      // must be upper levels' node in the cache. so this path must be unreachable.
//    assert(false);
//  }
}

bool Btr::leaf_page_store(GlobalAddress page_addr, const Key &k, const Value &v, Key &split_key, GlobalAddress &sibling_addr,
                     GlobalAddress root, int level, CoroContext *cxt, int coro_id) {

  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;


//    char padding[VALUE_PADDING];
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);
#endif

  auto rbuf = rdma_mg->Get_local_read_mr();
  ibv_mr* cas_mr = rdma_mg->Get_local_CAS_mr();
  auto page_buffer = rbuf->addr;
  bool insert_success;
//  auto tag = rdma_mg->getThreadTag();
//  assert(tag != 0);

  lock_and_read_page(rbuf, page_addr, kLeafPageSize, cas_mr,

                     lock_addr, 1, cxt, coro_id);

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());

  if (k >= page->hdr.highest) {
      if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
          page_cache->Erase(Slice((char*)&path_stack[coro_id][1], sizeof(GlobalAddress)));
      }
      this->unlock_addr(lock_addr, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    return this->leaf_page_store(page->hdr.sibling_ptr, k, v, split_key, sibling_addr, root, level, cxt,
                                 coro_id);
  }
    if (k < page->hdr.lowest ) {
        // if key is smaller than the lower bound, the insert has to be restart from the
        // upper level. because the sibling pointer only points to larger one.

        if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
            page_cache->Erase(Slice((char*)&path_stack[coro_id][1], sizeof(GlobalAddress)));
        }
        this->unlock_addr(lock_addr, cxt, coro_id, true);

        insert_success = false;
        return insert_success;// result in fall back search on the higher level.
    }
  assert(k >= page->hdr.lowest);
// TODO: Check whether the key is larger than thelargest key of this node.
//  if yes, update the header.
  int cnt = 0;
  int empty_index = -1;
  char *update_addr = nullptr;
    // It is problematic to just check whether the value is empty, because it is possible
    // that the buffer is not initialized as 0
  for (int i = 0; i < kLeafCardinality; ++i) {

    auto &r = page->records[i];
    if (r.value != kValueNull) {
      cnt++;
      if (r.key == k) {
        r.value = v;
        // ADD MORE weight for write.
//        memcpy(r.value_padding, padding, VALUE_PADDING);

        r.f_version++;
        r.r_version = r.f_version;
        update_addr = (char *)&r;
        break;
      }
    } else if (empty_index == -1) {
      empty_index = i;
    }
  }

  assert(cnt != kLeafCardinality);

  if (update_addr == nullptr) { // insert new item
    if (empty_index == -1) {
      printf("%d cnt\n", cnt);
      assert(false);
    }

    auto &r = page->records[empty_index];
    r.key = k;
    r.value = v;
//    memcpy(r.value_padding, padding, VALUE_PADDING);
    r.f_version++;
    r.r_version = r.f_version;

    update_addr = (char *)&r;

    cnt++;
  }

  bool need_split = cnt == kLeafCardinality;
  if (!need_split) {
    assert(update_addr);
      write_page_and_unlock(
              rbuf, GADD(page_addr, (update_addr - (char *) page)),
              sizeof(LeafEntry), (uint64_t*)cas_mr->addr, lock_addr, cxt, coro_id, false);

    return true;
  } else {
    std::sort(
        page->records, page->records + kLeafCardinality,
        [](const LeafEntry &a, const LeafEntry &b) { return a.key < b.key; });
  }


//  Key split_key;
//  GlobalAddress sibling_addr;
  if (need_split) { // need split
    sibling_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Default,(round_robin_cur++)%rdma_mg->memory_nodes.size());
    //TODO: use a thread local sibling memory region to reduce the allocator contention.
    ibv_mr* sibling_mr = new ibv_mr{};
      rdma_mg->Allocate_Local_RDMA_Slot(*sibling_mr, Default);
    auto sibling = new (sibling_mr->addr) LeafPage(page->hdr.level);

    // std::cout << "addr " <<  sibling_addr << " | level " <<
    // (int)(page->hdr.level) << std::endl;

      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);

      for (int i = m; i < cnt; ++i) { // move
          sibling->records[i - m].key = page->records[i].key;
          sibling->records[i - m].value = page->records[i].value;
          page->records[i].key = 0;
          page->records[i].value = kValueNull;
      }
      //We don't care about the last index in the leaf nodes actually,
      // because we iterate all the slots to find an entry.
      page->hdr.last_index -= (cnt - m);
//      assert(page_addr == root || page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m);
//      assert(sibling->hdr.last_index == cnt -m -1);
      sibling->hdr.lowest = split_key;
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = split_key;

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
      page->hdr.sibling_ptr = sibling_addr;
    sibling->set_consistent();
    rdma_mg->RDMA_Write(sibling_addr, sibling_mr,kLeafPageSize, 1,1,Default);
  }else{
      sibling_addr = GlobalAddress::Null();
  }

  page->set_consistent();

    write_page_and_unlock(rbuf, page_addr, kLeafPageSize, (uint64_t*)cas_mr->addr,
                          lock_addr, cxt, coro_id, need_split);

  return true;
}

// Need BIG FIX
    bool Btr::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                         CoroContext *cxt, int coro_id) {
    uint64_t lock_index =
    CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

    GlobalAddress lock_addr;
    lock_addr.nodeID = page_addr.nodeID;
    lock_addr.offset = lock_index * sizeof(uint64_t);

    auto rbuf = rdma_mg->Get_local_read_mr();
    ibv_mr* cas_mr = rdma_mg->Get_local_CAS_mr();
    auto page_buffer = rbuf->addr;
    //        auto cas_buffer
    bool insert_success;

    auto tag = 1;
    try_lock_addr(lock_addr, tag, cas_mr, cxt, coro_id);

    //  auto page_buffer = rdma_mg->get_rbuf(coro_id).get_page_buffer();
    rdma_mg->RDMA_Read(page_addr, rbuf, kLeafPageSize, 1, 1, Default);
    auto page = (LeafPage *)page_buffer;

    assert(page->hdr.level == level);
    assert(page->check_consistent());
    if (k >= page->hdr.highest) {
        this->unlock_addr(lock_addr, cxt, coro_id, true);

        assert(page->hdr.sibling_ptr != GlobalAddress::Null());

        return this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
    }
    if (k < page->hdr.lowest) {
        this->unlock_addr(lock_addr, cxt, coro_id, true);

        assert(page->hdr.sibling_ptr != GlobalAddress::Null());

        return false;
    }

    auto cnt = page->hdr.last_index + 1;

    int del_index = -1;
    for (int i = 0; i < cnt; ++i) {
        if (page->records[i].key == k) { // find and update
          del_index = i;
          break;
        }
    }

    if (del_index != -1) { // remove and shift
        for (int i = del_index + 1; i < cnt; ++i) {
          page->records[i - 1].key = page->records[i].key;
          page->records[i - 1].value = page->records[i].value;
        }

        page->hdr.last_index--;

        page->set_consistent();
        rdma_mg->RDMA_Write(page_addr, rbuf, kLeafPageSize, 1,1, Default);
    }
    this->unlock_addr(lock_addr, cxt, coro_id, false);
    return true;
  // TODO: Merge page after the node is too small.
}

void Btr::run_coroutine(CoroFunc func, int id, int coro_cnt) {

  using namespace std::placeholders;

  assert(coro_cnt <= define::kMaxCoro);
  for (int i = 0; i < coro_cnt; ++i) {
    auto gen = func(i, rdma_mg, id);
    worker[i] = CoroCall(std::bind(&Btr::coro_worker, this, _1, gen, i));
  }

//  master = CoroCall(std::bind(&Btr::coro_master, this, _1, coro_cnt));

  master();
}

void Btr::coro_worker(CoroYield &yield, RequstGen *gen, int coro_id) {
  CoroContext ctx;
  ctx.coro_id = coro_id;
  ctx.master = &master;
  ctx.yield = &yield;

  Timer coro_timer;
//  auto thread_id = rdma_mg->getMyThreadID();

  while (true) {

    auto r = gen->next();

    coro_timer.begin();
    if (r.is_search) {
      Value v;
      this->search(r.k, v, &ctx, coro_id);
    } else {
      this->insert(r.k, r.v, &ctx, coro_id);
    }
    auto us_10 = coro_timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
//    latency[thread_id][us_10]++;
  }
}

//void Btr::coro_master(CoroYield &yield, int coro_cnt) {
//
//  for (int i = 0; i < coro_cnt; ++i) {
//    yield(worker[i]);
//  }
//
//  while (true) {
//
//    uint64_t next_coro_id;
//
//    if (rdma_mg->poll_rdma_cq_once(next_coro_id)) {
//      yield(worker[next_coro_id]);
//    }
//
//    if (!hot_wait_queue.empty()) {
//      next_coro_id = hot_wait_queue.front();
//      hot_wait_queue.pop();
//      yield(worker[next_coro_id]);
//    }
//
//    if (!deadline_queue.empty()) {
//      auto now = timer.get_time_ns();
//      auto task = deadline_queue.top();
//      if (now > task.deadline) {
//        deadline_queue.pop();
//        yield(worker[task.coro_id]);
//      }
//    }
//  }
//}

// Local Locks
/**
 * THere is a potenttial bug if the lock is overflow and one thread will wrongly acquire the lock. need to develop a new
 * way for local lock
 * @param lock_addr
 * @param cxt
 * @param coro_id
 * @return
 */
inline bool Btr::acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                                     int coro_id) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  bool is_local_locked = false;

  uint64_t lock_val = node.ticket_lock.fetch_add(1);
  //TOTHINK: what if the ticket out of buffer.

  uint32_t ticket = lock_val << 32 >> 32;//clear the former 32 bit
  uint32_t current = lock_val >> 32;// current is the former 32 bit in ticket lock

  // printf("%ud %ud\n", ticket, current);
  while (ticket != current) { // lock failed
    is_local_locked = true;

//    if (cxt != nullptr) {
//      hot_wait_queue.push(coro_id);
//      (*cxt->yield)(*cxt->master);
//    }

    current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
  }

  if (is_local_locked) {
//    hierarchy_lock[rdma_mg->getMyThreadID()][0]++;
  }

  node.hand_time++;

  return node.hand_over;
}

inline bool Btr::can_hand_over(GlobalAddress lock_addr) {

  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);
// only when unlocking, it need to check whether it can handover to the next, so that it do not need to UNLOCK the global lock.
// It is possible that the handover is set as false but this server is still holding the lock.
  uint32_t ticket = lock_val << 32 >> 32;//
  uint32_t current = lock_val >> 32;
// if the handover in node is true, then the other thread can get the lock without any RDMAcas
// if the handover in node is false, then the other thread will acquire the lock from by RDMA cas AGAIN
  if (ticket <= current + 1) { // no pending locks
    node.hand_over = false;// if no pending thread, then it will release the remote lock and next aquir need RDMA CAS again
  } else {
    node.hand_over = node.hand_time < define::kMaxHandOverTime; // check the limit
  }
  if (!node.hand_over) {
    node.hand_time = 0;// clear the handtime.
  } else {
//    handover_count[rdma_mg->getMyThreadID()][0]++;
  }

  return node.hand_over;
}

inline void Btr::releases_local_lock(GlobalAddress lock_addr) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

  node.ticket_lock.fetch_add((1ull << 32));
}

//void Btr::index_cache_statistics() {
//  page_cache->statistics();
//  page_cache->bench();
//}

void Btr::clear_statistics() {
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    cache_hit[i][0] = 0;
    cache_miss[i][0] = 0;
  }
}


}