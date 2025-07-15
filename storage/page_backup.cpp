//
// Created by wang4996 on 22-8-8.
//

//#include <infiniband/verbs.h>
#include "page.h"
#include "Btr.h"
namespace DSMEngine {
    template class LeafPage<uint64_t>;
    template class LeafPage<Secondary_Key<uint64_t, uint64_t>>;
//    template class LeafPage<uint64_t>;
    template class InternalPage<uint64_t>;
    template class InternalPage<Secondary_Key<uint64_t, uint64_t>>;
    template<class Key>
    bool InternalPage<Key>::internal_page_search(const Key &k, void *result_ptr) {
        SearchResult<Key>& result = *(SearchResult<Key>*)result_ptr;
        assert(k >= hdr.lowest);
//        assert(k < hdr.highest);
//        uint64_t local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
//        if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
//            return false;
//        }

//        Key highest_buffer = 0;
//        highest_buffer = hdr.highest;
        // optimistically latch free.
        //TODO (potential bug) what will happen if the record version is not consistent?

        // It is necessary to have reread in this function because the interanl page cache can be
        // updated by a concurrent writer. THe writer will pull the updates from the remote memory.
        //
        //TODO(Potential bug): the optimistic latch free is not fully correct because the orginal
        // algorithm first check the lock state then check the verison again when the read operation end.
        // the front and rear verison can guarantee the consistency between remote writer and reader but can not guarantee the
        // consistency between local writer and reader.
        // THe front and rear versions are necessary.
        // choice1: Maybe the lock check is necessary (either in the page or outside)
        // choice2: or we check whether the front verison equals the rear version to check wehther there is a
        // concurrent writer (check lock).
//    re_read:
        GlobalAddress target_global_ptr_buff;

        //TOTHINK: how to make sure that concurrent write will not result in segfault,
        // such as out of buffer for cnt.
        auto cnt = hdr.last_index + 1;
        // page->debug();
        if (k < records[0].key) {
//      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
            target_global_ptr_buff = hdr.leftmost_ptr;

//      result.upper_key = page->records[0].key;
            // check front verison here because a writer will change the front version at the beggining of a write op
            // if this has not changed, we can guarntee that there is not writer interfere.

            // TODO: maybe we need memory fence here either.
            // TOTHINK: There is no need for local reread because the data will be modified in a copy on write manner.


            result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
            result.later_key = records[0].key;
#endif



            assert(k < result.later_key);
            assert(result.next_level != GlobalAddress::Null());
            return true;
        }
        //binary search the btree node.
        uint16_t left = 0;
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
        // THe binary search algorithm below will
        // (1): for primary index: stop at the largest key that smaller or equal than the target key.
        // (2): for secondary index: stop at the largest key that smaller or equal than the target key.
        //      If it is equal, it point to the frist one. If it is smaller, pointed to the last one.
        while (left < right) {
            mid = (left + right + 1) / 2;
            //TODO: return the first entry which equals to the target key.
            if (k > records[mid].key) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if(k < records[mid].key) {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1; // why mid -1 rather than mid
            }else{
                if(hdr.p_type == P_Internal_P){
                    target_global_ptr_buff = records[mid].ptr;
                    result.next_level = target_global_ptr_buff;
                    assert(result.this_key <= k);
                    assert(result.next_level != GlobalAddress::Null());
                    return true;
                }
                int mid_t = mid;
                int prev = mid_t -1;
                while(mid_t >0 && records[mid_t].key == records[prev].key){
                    mid_t--;
                    prev = mid_t -1;
                }
                mid = mid_t;
                left = mid;
                right = mid;
            }
        }

        assert(left == right);
        target_global_ptr_buff = records[right].ptr;
        result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
        if (right < hdr.last_index){
            result.this_key = records[right].key;
            result.later_key = records[right + 1].key;
        }else{
            result.this_key = 0;
            result.later_key = hdr.highest;
        }

#endif

#ifndef NDEBUG
        if (hdr.p_type == P_Internal_P){
            assert(k < result.later_key);
        }
#endif

        assert(result.this_key <= k);

        assert(result.next_level != GlobalAddress::Null());
        return true;

    }


    template<class Key>
    bool InternalPage<Key>::internal_page_store(GlobalAddress page_addr, const Key &k, GlobalAddress value, int level) {
        auto cnt = hdr.last_index + 1;
        assert(records[hdr.last_index].ptr != GlobalAddress::Null());
        assert(records[hdr.last_index].key != 0);
        assert(cnt != kInternalCardinality);
        bool is_update = false;
        uint16_t insert_index = 0;
//--------------------------------------------------
        //binary search the btree node.
        uint16_t left = 0;
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
        if (k < records[0].key) {
            insert_index = 0;
        }else{
            while (left < right) {
                mid = (left + right + 1) / 2;

                if (k > records[mid].key) {
                    // Key at "mid" is smaller than "target".  Therefore all
                    // blocks before "mid" are uninteresting.
                    left = mid;
                } else if (k < records[mid].key) {
                    // Key at "mid" is >= "target".  Therefore all blocks at or
                    // after "mid" are uninteresting.
                    right = mid - 1;
                }else{
                    // internal node entry shall never get updated
                    if(hdr.p_type == P_Internal_P){
                        assert(false);
                        throw std::runtime_error("Internal node entry shall never get updated");
                    }else{
                        // TODO: if it is a seconday index , then the internal node could have duplicated entries, then how can I know where
                        //  my new entry should be inserted? One observation is that if the duplicated key cross multiple nodes, then
                        //  the new created key shall always be inserted in the second position.
                        int mid_n = mid;
                        int prev = mid_n -1;
                        while(prev >0 && records[prev].key == k){
                            mid_n--;
                            prev = mid_n - 1;
                        }
                        mid = mid_n;
                        left = mid;
                        right = mid;
//                        records[mid].ptr = value;
                        is_update = false;
                    }




                }
            }
            assert(left == right);
            // the binary search will stop at the largest key who is smaller or equal to the target key. then the position of insertion
            // should be the next position of the key.
            insert_index = left +1;
        }

        //--------------------------------------------
        assert(records[hdr.last_index].ptr != GlobalAddress::Null());
//        Key split_key;
//        GlobalAddress sibling_addr = GlobalAddress::Null();
        assert(!is_update);
        hdr.reset_dirty_bounds();
        //no dirty boundary needs to be updated.
        for (int i = cnt; i > insert_index; --i) {
            records[i].key = records[i - 1].key;
            records[i].ptr = records[i - 1].ptr;
        }
        records[insert_index].key = k;
        records[insert_index].ptr = value;
#ifndef NDEBUG
        uint16_t last_index_prev = hdr.last_index;
#endif
        hdr.last_index++;
        cnt++;
        assert(hdr.last_index == last_index_prev + 1);
        assert(records[hdr.last_index].ptr != GlobalAddress::Null());
        assert(records[hdr.last_index].key != 0);
        return cnt == kInternalCardinality;
    }
    template<class Key>
    int LeafPage<Key>::leaf_page_pos_lb(const Key &k, GlobalAddress g_page_ptr, RecordSchema *record_scheme) {
        size_t tuple_length = record_scheme->GetSchemaSize();
        char* tuple_start = data_;
        uint16_t left = 0;
        assert(hdr.last_index >= 0);
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
#ifndef NDEBUG
        std::vector<std::pair<uint16_t, uint16_t>> binary_history;
        char* tuple_last = data_ + hdr.last_index*tuple_length;
        auto r_last = Record(record_scheme,tuple_last);
        Key last_key;
        r_last.GetPrimaryKey(&last_key);
        if (hdr.p_type == P_Internal_P){
            assert(k < hdr.highest );
            assert(last_key < hdr.highest);
        }else{
            assert(k <= hdr.highest );
            assert(last_key <= hdr.highest);
        }

#endif
        Key temp_key;
        while (left < right) {
            mid = (left + right + 1) / 2;
            tuple_start = data_ + mid*tuple_length;
            auto r = Record(record_scheme,tuple_start);

            r.GetPrimaryKey(&temp_key);
#ifndef NDEBUG
            binary_history.push_back(std::make_pair(left, right));
#endif
            if (k > temp_key) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if (k < temp_key) {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
                mid = right;
            } else{
                //todo: secondary index need to move to the left most value that meets the condition.
                //Find the value.
                if (hdr.p_type == P_Leaf_P){
                    return mid;
                } else{
                    assert(false);
                    assert(hdr.p_type == P_Leaf_S);
                    // seek backward to find all the entries that meet the condition.
                    while (mid > 0){
                        tuple_start = data_ + (mid-1)*tuple_length;
                        auto r = Record(record_scheme,tuple_start);
//                        Key temp_key;
                        r.GetPrimaryKey(&temp_key);
                        if (temp_key == k){
                            mid--;
                        }else{
                            break;
                        }
                    }
                    return mid;
                }
            }
        }
        // Not find
        assert(right == left);
        assert(mid == right);
        tuple_start = data_ + right*tuple_length;
        auto r = Record(record_scheme,tuple_start);
//        Key temp_key;
        r.GetPrimaryKey(&temp_key);
        assert(k > temp_key || right == 0);
        if (k == temp_key){
            // only when the target key is the first key will the code goes here.
            // We treat primary and secondary the same. Even for secondary index, it is not possible that the leaf previous to
            // the current leaf has the same key, because the upper level search need to guarantee that not happening
            assert(right == 0);
            return mid;
        }else{
            // return the smallest key that is larger than the target key. if the target key is larger than all
            // entry keys in this node, then the target position should be in next node. return -1.
            int ret = right+1;
            return ret>hdr.last_index ? -1 : ret;
        }


    }
    template<class TKey>
    void LeafPage<TKey>::GetByPosition(int pos, RecordSchema *schema_ptr, TKey &key, void* buff) {
        assert(pos >= 0);
        assert(pos <= hdr.last_index);
        size_t tuple_length = schema_ptr->GetSchemaSize();
        char* tuple_start = data_ + pos*tuple_length;
        auto r = Record(schema_ptr,tuple_start);
        //Return the value of the key-value pair for index.
//        return r.GetColumn(1);
        //todo: what if the key is compound and containing multiple columns?
        assert(sizeof(TKey) == r.GetPrimaryKey().size());
//        assert(sizeof(Value) == r.GetColumnSize(1));
        memcpy(&key,  r.GetPrimaryKey().c_str(), r.GetPrimaryKey().size());
//        memcpy(&value, r.GetColumn(1), r.GetColumnSize(1));
        memcpy(buff, r.data_ptr_, r.data_size_);
    }
    template<class Key>
    void LeafPage<Key>::leaf_page_search(const Key &k, SearchResult<Key> &result, GlobalAddress g_page_ptr,
                                               RecordSchema *record_scheme) {
//        int kLeafCardinality = record_scheme->GetLeafCardi();
        size_t tuple_length = record_scheme->GetSchemaSize();
        char* tuple_start = data_;
        uint16_t left = 0;
        // TODO: the code below will be false if we execute the leaf page delete multiple times.
        assert(hdr.last_index >= 0);
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
#ifndef NDEBUG
        std::vector<std::pair<uint16_t, uint16_t>> binary_history;
        char* tuple_last = data_ + hdr.last_index*tuple_length;
        auto r_last = Record(record_scheme,tuple_last);
        Key last_key;
        r_last.GetPrimaryKey(&last_key);
        assert(k < hdr.highest );
        assert(last_key < hdr.highest);
#endif
        while (left < right) {
            mid = (left + right + 1) / 2;
            tuple_start = data_ + mid*tuple_length;
            auto r = Record(record_scheme,tuple_start);
            Key temp_key;
            r.GetPrimaryKey(&temp_key);
//            binary_history.push_back(std::make_pair(left, right));
            if (k > temp_key) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if (k < temp_key) {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            } else{
                //Find the value.
                memcpy((void*)result.val.data(),r.data_ptr_, r.GetRecordSize());
                result.find_value = true;
                return;
            }
        }
        // Not find or find on the first entry.
        assert(right == left);
        tuple_start = data_ + right*tuple_length;
        auto r = Record(record_scheme,tuple_start);
        Key temp_key;
        r.GetPrimaryKey(&temp_key);
        if (k == temp_key){
//            assert(false);
            assert(result.val.size() == r.GetRecordSize());
            memcpy((void*)result.val.data(),r.data_ptr_, r.GetRecordSize());
            result.find_value = true;
        }else{
            assert(k >temp_key || right == 0);
//            assert(false);
        }

    }
    // [lowest, highest)
    template<class TKey>
    bool LeafPage<TKey>::leaf_page_store(const TKey &k, const Slice &v, int &cnt,
                                          RecordSchema *record_scheme) {

        // It is problematic to just check whether the value is empty, because it is possible
        // that the buffer is not initialized as 0
#ifdef DYNAMIC_ANALYSE_PAGE
        // TODO: make the key-value stored with order, do not use this unordered page structure.
        //  Or use the key to check whether this holder is empty.
        cnt = hdr.last_index + 1;
        bool is_update = false;
        uint16_t insert_index = 0;
        assert(hdr.kLeafCardinality > 0);
        int tuple_length = record_scheme->GetSchemaSize();


        char* tuple_start;
        tuple_start = data_ + 0*tuple_length;

        auto r_temp = Record(record_scheme,tuple_start);
        TKey temp_key1;
        r_temp.GetPrimaryKey((char*)&temp_key1);
        assert(temp_key1 <= hdr.highest);
        if (k < temp_key1 || hdr.last_index == -1) {
            // this branc can only happen when the page is empty or the leafpage is the left most leaf page
//            assert(hdr.last_index == -1);
            insert_index = 0;
        }else{
            assert(hdr.last_index >= 0);
            uint16_t left = 0;
            uint16_t right = hdr.last_index;
            uint16_t mid = 0;
            while (left < right) {
                mid = (left + right + 1) / 2;
                tuple_start = data_ + mid*tuple_length;
                auto r = Record(record_scheme,tuple_start);
                TKey temp_key;
                r.GetPrimaryKey(&temp_key);
                if (k > temp_key) {
                    // Key at "mid" is smaller than "target".  Therefore all
                    // blocks before "mid" are uninteresting.
                    left = mid;
                } else if (k < temp_key) {
                    // Key at "mid" is >= "target".  Therefore all blocks at or
                    // after "mid" are uninteresting.
                    right = mid - 1; // why mid -1 rather than mid
                } else{
                    //Find the value.
                    assert(v.size() == r.GetRecordSize());
                    memcpy(r.data_ptr_, v.data(), r.GetRecordSize());
//                    if(hdr.p_type == P_Leaf_P){
//                        is_update = true;
//#ifdef DIRTY_ONLY_FLUSH
//                        hdr.merge_dirty_bounds(tuple_start - (char*)this, tuple_start - (char*)this + r.GetRecordSize());
//#endif
//                        assert(cnt < hdr.kLeafCardinality);
//                        return cnt == hdr.kLeafCardinality;
//                    }
                    // TODO: for search secondary index with duplicated key, the new inserted enty will be inserted into the first node contain that duplicated key,
                    //  but it may not inserted in the first position with in the node.
                    left = mid;
                    right = mid;
                }
            }
            assert(left == right);
            mid = left;
            tuple_start = data_ + left*tuple_length;
            auto r = Record(record_scheme,tuple_start);
            TKey temp_key;
            r.GetPrimaryKey(&temp_key);
            if ((k != temp_key )){
                insert_index = left +1;
            }else{
//                assert(false);
                if(hdr.p_type == P_Leaf_P){
                    assert(v.size() == r.GetRecordSize());
                    memcpy(r.data_ptr_, v.data(), r.GetRecordSize());
                    is_update = true;
#ifdef DIRTY_ONLY_FLUSH
                    hdr.merge_dirty_bounds(tuple_start - (char*)this, tuple_start - (char*)this + r.GetRecordSize());
#endif
                    assert(cnt < hdr.kLeafCardinality);
                    //if it is an update in the primary index, return here.
                    return cnt == hdr.kLeafCardinality;
                }else{
                    insert_index = left +1;
                }

            }




        }

        assert(cnt != hdr.kLeafCardinality);
        assert(!is_update);
//        if (!is_update) { // insert new item

        tuple_start = data_ + insert_index*tuple_length;
        if (insert_index <= hdr.last_index){
            // Move all the tuples at and after the insert_index,use memmove to avoid undefined behavior for overlapped address.
            memmove(tuple_start + tuple_length, tuple_start, (hdr.last_index - insert_index+1)*tuple_length);
            auto r = Record(record_scheme,tuple_start);
            assert(v.size() == r.GetRecordSize());
            r.ReSetRecord(v.data_reference(), v.size());
        }else{
//            printf("New record inserted at the end of the page\n");
            assert(insert_index < hdr.kLeafCardinality );
            auto r = Record(record_scheme,tuple_start);
            assert(v.size() == r.GetRecordSize());
            r.ReSetRecord(v.data_reference(), v.size());
        }
        cnt++;
        hdr.last_index++;
        assert(hdr.last_index < hdr.kLeafCardinality);
#ifdef DIRTY_ONLY_FLUSH
        // If the page get inserted, then the dirty range is the whole page, or flush to the end of tuple_start + (last_Index+1)*r.GetRecordSize()
        hdr.merge_dirty_bounds(sizeof(uint64_t), kLeafPageSize);
#endif
        assert(temp_key1 <= hdr.highest);
        return cnt == hdr.kLeafCardinality;
#else
        for (int i = 0; i < kLeafCardinality; ++i) {

            auto &r = records[i];
            if (r.value != kValueNull<Key>) {
                cnt++;
                if (r.key == k) {
                    r.value = v;
                    // ADD MORE weight for write.
//        memcpy(r.value_padding, padding, VALUE_PADDING);

//                    r.f_version++;
//                    r.r_version = r.f_version;
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

            auto &r = records[empty_index];
            r.key = k;
            r.value = v;
//    memcpy(r.value_padding, padding, VALUE_PADDING);
//            r.f_version++;
//            r.r_version = r.f_version;

            update_addr = (char *)&r;

            cnt++;
        }

        return cnt == kLeafCardinality;
#endif
    }
    // [lowest, highest)
    template<class TKey>
    bool LeafPage<TKey>::leaf_page_delete(const TKey &k, int &cnt, SearchResult<TKey> &result,
                                         RecordSchema *record_scheme) {

        // It is problematic to just check whether the value is empty, because it is possible
        // that the buffer is not initialized as 0
        // TODO: make the key-value stored with order, do not use this unordered page structure.
        //  Or use the key to check whether this holder is empty.
        cnt = hdr.last_index + 1;
        uint16_t insert_index = 0;
        assert(hdr.kLeafCardinality > 0);
        int tuple_length = record_scheme->GetSchemaSize();


        char* tuple_start;
        tuple_start = data_ + 0*tuple_length;

        auto r_temp = Record(record_scheme,tuple_start);
        TKey temp_key1;
        r_temp.GetPrimaryKey((char*)&temp_key1);
        assert(temp_key1 <= hdr.highest);
        if (k < temp_key1 || hdr.last_index == -1) {
            // this branc can only happen when the page is empty or the leafpage is the left most leaf page
//            assert(hdr.last_index == -1);
            insert_index = 0;
        }else{
            assert(hdr.last_index >= 0);
            uint16_t left = 0;
            uint16_t right = hdr.last_index;
            uint16_t mid = 0;
            while (left < right) {
                mid = (left + right + 1) / 2;
                tuple_start = data_ + mid*tuple_length;
                auto r = Record(record_scheme,tuple_start);
                TKey temp_key;
                r.GetPrimaryKey(&temp_key);
                if (k > temp_key) {
                    // Key at "mid" is smaller than "target".  Therefore all
                    // blocks before "mid" are uninteresting.
                    left = mid;
                } else if (k < temp_key) {
                    // Key at "mid" is >= "target".  Therefore all blocks at or
                    // after "mid" are uninteresting.
                    right = mid - 1; // why mid -1 rather than mid
                } else{
                    //Find the value.
                    // TODO: for search secondary index with duplicated key, the new inserted enty will be inserted into the first node contain that duplicated key,
                    //  but it may not inserted in the first position with in the node.
                    left = mid;
                    right = mid;
                }
            }
            assert(left == right);
            mid = left;
            tuple_start = data_ + left*tuple_length;
            auto r = Record(record_scheme,tuple_start);
            TKey temp_key;
            r.GetPrimaryKey(&temp_key);
            if ((k != temp_key )){
                result.find_value = false;
                return false;
            }else{
                insert_index = left;
            }




        }

        assert(cnt != hdr.kLeafCardinality);
//        if (!is_update) { // insert new item

        tuple_start = data_ + insert_index*tuple_length;
        if (insert_index <= hdr.last_index){
            // Move all the tuples at and after the insert_index,use memmove to avoid undefined behavior for overlapped address.
            memmove(tuple_start, tuple_start + tuple_length, (hdr.last_index - insert_index)*tuple_length);
        }else{
            assert(false);
        }
        cnt--;
        hdr.last_index--;
        assert(hdr.last_index <= hdr.kLeafCardinality-1);
#ifdef DIRTY_ONLY_FLUSH
        // If the page get inserted, then the dirty range is the whole page, or flush to the end of tuple_start + (last_Index+1)*r.GetRecordSize()
        hdr.merge_dirty_bounds(sizeof(uint64_t), kLeafPageSize);
#endif
        assert(temp_key1 <= hdr.highest);
        // Todo: decide when triger the leaf page merge.
        result.find_value = true;
        return false;

    }

    bool DataPage::InsertRecord(const Slice &tuple, int &cnt, RecordSchema *record_scheme, GlobalAddress& g_addr) {
        int tuple_length = record_scheme->GetSchemaSize();
        uint32_t bitmap_size = (hdr.kDataCardinality + 63) / 64;
        auto* bitmap = (uint64_t*)data_;
        char* data_start = data_ + bitmap_size*8;
        int empty_slot = find_empty_spot_from_bitmap(bitmap, hdr.kDataCardinality);
        if (empty_slot == -1){
            //Need to allcoate a new page
            return false;
        }
        size_t offset = empty_slot*tuple_length;
        memcpy(data_start + empty_slot*tuple_length, tuple.data(), tuple.size());
        set_bitmap(bitmap, empty_slot);
        g_addr = GADD(hdr.this_page_g_ptr, STRUCT_OFFSET(DataPage, data_) + bitmap_size*8 + offset);
        hdr.number_of_records++;
        cnt = hdr.number_of_records;
        return true;
    }

    bool DataPage::AllocateRecord(int &cnt, RecordSchema *record_scheme, GlobalAddress &g_addr, char *&data_buffer) {
        int tuple_length = record_scheme->GetSchemaSize();
        uint32_t bitmap_size = (hdr.kDataCardinality + 63) / 64;
        bitmap_size*=8;
        auto* bitmap = (uint64_t*)data_;
        int empty_slot = find_empty_spot_from_bitmap(bitmap, hdr.kDataCardinality);
        if (empty_slot == -1){
            assert(hdr.number_of_records == hdr.kDataCardinality);
            //Need to allcoate a new page
            return false;
        }
        size_t offset = empty_slot*tuple_length;
        set_bitmap(bitmap, empty_slot);
        g_addr = GADD(hdr.this_page_g_ptr, STRUCT_OFFSET(DataPage, data_) + bitmap_size + offset);
        data_buffer = data_ + bitmap_size + offset;
        hdr.number_of_records++;
        // TODO enable the dirty flush.
//#ifdef DIRTY_ONLY_FLUSH
//        hdr.merge_dirty_bounds(STRUCT_OFFSET(DataPage, data_) + bitmap_size + offset, STRUCT_OFFSET(DataPage, data_) + bitmap_size + offset + tuple_length);
////        hdr.dirty_upper_bound = 8;
////        hdr.dirty_lower_bound = STRUCT_OFFSET(DataPage, data_) + bitmap_size + offset + tuple_length;
//#endif
        cnt = hdr.number_of_records;
        return true;
    }
    //todo: maybe we need to reimplement it when we change the server from little edian to big edian.
    int DataPage::find_empty_spot_from_bitmap(uint64_t* bitmap, uint32_t number_of_bits){
        uint32_t number_of_64 = (number_of_bits + 63) / 64;
        uint32_t number_left = number_of_bits;
        uint64_t last_result = 0;
        uint64_t last_j = 0;
        for (uint32_t i = 0; i < number_of_64; ++i) {
            if (bitmap[i] != 0xFFFFFFFFFFFFFFFF){

                for (uint32_t j = 0; j < (number_left>64?64:number_left); ++j) {
                    last_result = bitmap[i] & (1ull<<j);
                    last_j = j;
                    if (last_result == 0){
                        assert(i*64 + j < number_of_bits);
                        return i*64 + j;
                    }
                }
            }
            number_left -= 64;
        }
        assert(hdr.number_of_records == hdr.kDataCardinality);
        return -1;
    }
    void DataPage::set_bitmap(uint64_t *bitmap, size_t index) {
        bitmap[index / 64] |= (1ull << (index % 64));
    }
    void DataPage::reset_bitmap(uint64_t *bitmap, size_t index) {
        bitmap[index / 64] &= ~(1ull << (index % 64));
    }

    bool DataPage::DeleteRecord(GlobalAddress g_addr, RecordSchema *record_scheme) {
        assert(g_addr.nodeID == hdr.this_page_g_ptr.nodeID);
        int tuple_length = record_scheme->GetSchemaSize();
        uint32_t bitmap_size = (hdr.kDataCardinality + 63) / 64;
        bitmap_size*=8;
        size_t page_offset = g_addr.offset - hdr.this_page_g_ptr.offset - bitmap_size - STRUCT_OFFSET(DataPage, data_);

        size_t index = page_offset / tuple_length;
        assert(page_offset% tuple_length == 0);
        uint64_t* bitmap = (uint64_t*)data_;
        reset_bitmap(bitmap, index);
        return true;

    }




}
//#else
//void LeafPage::leaf_page_search(const Key &k, SearchResult &result, ibv_mr local_mr_copied, GlobalAddress g_page_ptr) {
////    re_read:
//        Value target_value_buff{};
////        uint8_t front_v = front_version;
//        asm volatile ("sfence\n" : : );
//        asm volatile ("lfence\n" : : );
//        asm volatile ("mfence\n" : : );
//        //TODO: If record verisons are not consistent, we need to reread the page.
//        // or refetch the record. or we just remove the byteaddressable write and then do not
//        // use record level version.
//        for (int i = 0; i < kLeafCardinality; ++i) {
//            auto &r = records[i];
//            while (r.f_version != r.r_version){
////                ibv_mr target_mr = *local_mr_copied;
////                exit(0);
//                int offset = ((char*)&r - (char *) this);
//                LADD(local_mr_copied.addr, offset);
//                Btr::rdma_mg->RDMA_Read(GADD(g_page_ptr, offset), &local_mr_copied, sizeof(LeafEntry),IBV_SEND_SIGNALED,1, Internal_and_Leaf);
//
//            }
//            if (r.key == k && r.value != kValueNull ) {
//                assert(r.f_version == r.r_version);
//                target_value_buff = r.value;
//                asm volatile ("sfence\n" : : );
//                asm volatile ("lfence\n" : : );
//                asm volatile ("mfence\n" : : );
////                uint8_t rear_v = rear_version;
////                if (front_v!= rear_v)// version checking
////                    //TODO: reread from the remote side.
////                    goto re _read;
//
////                memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
////      result.value_padding = r.value_padding;
//                break;
//            }
//        }
//        result.val = target_value_buff;
//    }
//}
//#endif
