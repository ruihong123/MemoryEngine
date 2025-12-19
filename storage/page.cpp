//
// Created by wang4996 on 22-8-8.
//

//#include <infiniband/verbs.h>
#include "page.h"
#include "Btr.h"
#include "txn/RedoLogger.h"
#include "txn/LogCodec.h"
namespace DSMEngine {
    bool InternalPage::internal_page_search(const DynamicCompoundKey &k, void *result_ptr, RecordSchema* index_schema_ptr) {
        SearchResult& result = *(SearchResult*)result_ptr;
        assert(k >= GetLowest(index_schema_ptr));


        GlobalAddress target_global_ptr_buff;

        //TOTHINK: how to make sure that concurrent write will not result in segfault,
        // such as out of buffer for cnt.
        auto cnt = hdr.last_index + 1;
        if (k < GetRecordKeyByIndex(0, index_schema_ptr)) {
            target_global_ptr_buff = hdr.leftmost_ptr;
            result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
            DynamicCompoundKey first_key = GetRecordKeyByIndex(0, index_schema_ptr);
            result.later_key = first_key;
            assert(k < first_key);
#endif

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
        
        // Cache pointer calculations to avoid repeated computation in loop
        uint16_t key_size = hdr.key_size;
        uint64_t record_size = hdr.record_size;
        char *data_ptr = data_ + 2 * key_size;
        
        // Reuse a single DynamicCompoundKey object instead of creating new ones each iteration
        DynamicCompoundKey mid_key_wrapper(nullptr, index_schema_ptr);
        
        while (left < right) {
            mid = (left + right + 1) >> 1;  // Bit shift is faster than division
            
            // Direct pointer calculation - avoid function call overhead
            char *mid_ptr = data_ptr + mid * record_size;
            mid_key_wrapper.start = mid_ptr;  // Reuse existing object, just update pointer
            
            //TODO: return the first entry which equals to the target key.
            if (k > mid_key_wrapper) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if(k < mid_key_wrapper) {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1; // why mid -1 rather than mid
            }else{
                target_global_ptr_buff = *(GlobalAddress *)(mid_ptr + key_size);
                result.next_level = target_global_ptr_buff;
//                assert(result.this_key <= k);
                assert(result.next_level != GlobalAddress::Null());
                return true;
            }
        }

        assert(left == right);

        target_global_ptr_buff = GetRecordValueByIndex(right);
        result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
        if (right < hdr.last_index){
            result.this_key = GetRecordKeyByIndex(right, index_schema_ptr);
            result.later_key = GetRecordKeyByIndex(right + 1, index_schema_ptr);
        }else{
            assert(right == hdr.last_index);
            result.this_key = GetRecordKeyByIndex(right, index_schema_ptr);
            result.later_key = GetHighest(index_schema_ptr);
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


    bool InternalPage::internal_page_store(GlobalAddress page_addr, const DynamicCompoundKey &k, GlobalAddress value, int level, RecordSchema* schema_ptr, RedoLogger* redo_logger) {
        auto cnt = hdr.last_index + 1;
        assert(GetRecordValueByIndex(hdr.last_index) != GlobalAddress::Null());
        assert(GetRecordKeyByIndex(hdr.last_index, schema_ptr) != DynamicCompoundKey::MinValue(schema_ptr));
        assert(cnt != hdr.kCardinality);
        bool is_update = false;
        uint16_t insert_index = 0;
        //--------------------------------------------------
        //binary search the btree node.
        uint16_t left = 0;
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
        
        // Cache pointer calculations to avoid repeated computation in loop
        uint16_t search_key_size = hdr.key_size;
        uint64_t search_record_size = hdr.record_size;
        char *data_ptr = data_ + 2 * search_key_size;
        
        // Reuse a single DynamicCompoundKey object instead of creating new ones each iteration
        DynamicCompoundKey mid_key_wrapper(nullptr, schema_ptr);
        
        if (k < GetRecordKeyByIndex(0, schema_ptr)) {
            insert_index = 0;
        }else{
            while (left < right) {
                mid = (left + right + 1) >> 1;  // Bit shift is faster than division
                
                // Direct pointer calculation - avoid function call overhead
                char *mid_ptr = data_ptr + mid * search_record_size;
                mid_key_wrapper.start = mid_ptr;  // Reuse existing object, just update pointer
                
                if (k > mid_key_wrapper) {
                    // Key at "mid" is smaller than "target".  Therefore all
                    // blocks before "mid" are uninteresting.
                    left = mid;
                } else if (k < mid_key_wrapper) {
                    // Key at "mid" is >= "target".  Therefore all blocks at or
                    // after "mid" are uninteresting.
                    right = mid - 1;
                }else{
                    // internal node entry shall never get updated
                        assert(false);
                        throw std::runtime_error("Internal node entry shall never get updated");
                }
            }
            assert(left == right);
            // the binary search will stop at the largest key who is smaller or equal to the target key. then the position of insertion
            // should be the next position of the key.
            insert_index = left +1;
        }

        //--------------------------------------------
        assert(GetRecordValueByIndex(hdr.last_index) != GlobalAddress::Null());
//        Key split_key;
//        GlobalAddress sibling_addr = GlobalAddress::Null();
        assert(!is_update);
        hdr.reset_dirty_bounds();
        
        // Prepare logging before making changes
        LogCodec::Encoder encoder;
        bool need_logging = (redo_logger != nullptr);
        uint64_t old_page_version = hdr.p_version;
        uint64_t new_page_version = old_page_version + 1;
        
        assert(new_page_version > 1);
        uint16_t logical_region_id = page_addr.nodeID;
        
        // Calculate offsets for logging
        size_t header_offset = STRUCT_OFFSET(InternalPage, hdr);
        size_t data_offset = STRUCT_OFFSET(InternalPage, data_);
        uint32_t key_size = hdr.key_size;
        uint32_t record_size = hdr.record_size;
        size_t record_data_offset = data_offset + 2 * key_size;  // Skip lowest/highest keys
        
        //no dirty boundary needs to be updated.
        // Log the record shifts (moving records to make space)
        if (need_logging && insert_index < cnt) {
            // Calculate source and destination offsets for the shift
            // Records from insert_index to cnt-1 need to move one position right
            size_t num_records_to_move = cnt - insert_index;
            size_t src_start_offset = record_data_offset + insert_index * record_size;
            size_t dst_start_offset = src_start_offset + record_size;
            size_t move_size = num_records_to_move * record_size;
            
            encoder.AddMemmove(dst_start_offset, src_start_offset, move_size);
        }
        
        // Execute the shift
        for (int i = cnt; i > insert_index; --i) {
            SetRecordByIndex(i, GetRecordKeyByIndex(i - 1, schema_ptr), GetRecordValueByIndex(i - 1), schema_ptr);
        }
        
        // Log the new record insertion
        if (need_logging) {
            size_t new_record_offset = record_data_offset + insert_index * record_size;
            // Log key
            encoder.AddUpdateBytes(new_record_offset, k.start, key_size);
            // Log value (GlobalAddress)
            encoder.AddUpdateBytes(new_record_offset + key_size, &value, sizeof(GlobalAddress));
        }
        
        SetRecordByIndex(insert_index, k, value, schema_ptr);
#ifndef NDEBUG
        uint16_t last_index_prev = hdr.last_index;
#endif
        hdr.last_index++;
        cnt++;
        assert(hdr.last_index == last_index_prev + 1);
        assert(GetRecordValueByIndex(hdr.last_index) != GlobalAddress::Null());
        assert(GetRecordKeyByIndex(hdr.last_index, schema_ptr)  != DynamicCompoundKey::MinValue(schema_ptr));
        
        // Update page version in memory (for next operation) - always update regardless of logging
        hdr.p_version = new_page_version;
        
        // Log header update (last_index changed)
        if (need_logging) {
            size_t last_index_offset = header_offset + offsetof(Header_Index, last_index);
            encoder.AddUpdateBytes(last_index_offset, &hdr.last_index, sizeof(hdr.last_index));
            
            // // Append to redo log (page_version is already in RecordHeader, no need to log it in payload)
            // printf("[REDO_LOG_APPEND] InternalPage::internal_page_store - "
            //        "logical_region_id=%u, page_addr=(nodeID=%u, offset=%lu), "
            //        "old_page_version=%lu, new_page_version=%lu, "
            //        "last_index=%d, log_size=%zu bytes, "
            //        "key_size=%u, record_size=%u, insert_index=%d\n",
            //        logical_region_id, page_addr.nodeID, page_addr.offset,
            //        old_page_version, new_page_version,
            //        hdr.last_index, encoder.Buffer().size(),
            //        key_size, record_size, insert_index);
            redo_logger->Append(logical_region_id, page_addr, new_page_version,
                               encoder.Buffer().data(), encoder.Buffer().size()
#ifndef NDEBUG
                               , RedoLogger::LOG_INTERNAL_PAGE_STORE
#endif
                               );
        }
        
        return cnt == hdr.kCardinality;
    }

    int LeafPage::leaf_page_pos_lb(const DynamicCompoundKey &k, RecordSchema *record_scheme) {

        const uint16_t n = static_cast<uint16_t>(hdr.last_index) + 1;

        // Empty leaf: by convention, no strictly greater element here.
        if (n == 0) return -1;

        uint16_t left = 0;
        assert(hdr.last_index >= 0);
        uint32_t right = hdr.last_index;
        uint32_t mid = 0;
#ifndef NDEBUG
        std::vector<std::pair<uint16_t, uint16_t>> binary_history;
        DynamicCompoundKey last_key = GetRecordKeyByIndex(hdr.last_index, record_scheme);
        assert(k < GetHighest(record_scheme) );
        assert(last_key < GetHighest(record_scheme));
        assert(hdr.record_size == record_scheme->GetRecordTotalSize());
#endif
        
        // Cache pointer calculations to avoid repeated computation in loop
        uint16_t key_size = hdr.key_size;
        uint64_t record_size = hdr.record_size;
        char *data_ptr = data_ + 2 * key_size;
        
        // Reuse a single DynamicCompoundKey object instead of creating new ones each iteration
        DynamicCompoundKey key_mid(nullptr, record_scheme);
        
        while (left < right) {
            // Bias-free midpoint; safe for uint16_t range.
            mid = (left + right + 1) >> 1;  // Bit shift is faster than division
            
            // Direct pointer calculation - avoid function call overhead
            char *mid_ptr = data_ptr + mid * record_size;
            key_mid.start = mid_ptr;  // Reuse existing object, just update pointer

            if (k > key_mid) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if (k < key_mid) {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
                mid = right;
            } else{
                return mid;
            }
        }
        assert(left == right);
        assert(mid == right);
        key_mid.start = data_ptr + mid * record_size;

        if (k < key_mid && mid == 0) {

           return -1;
        } else{
            assert(k >= key_mid);
            return mid;
        }
    }

    void LeafPage::GetDeepByPosition(int pos, RecordSchema *schema_ptr, DynamicCompoundKey &key, void* buff) {
        assert(pos >= 0);
        assert(pos <= hdr.last_index);
        void* tuple_ptr = GetRecordPtrByIndex(pos);
        // The primary key of the record is always start from beginning and continuous.
        memcpy(key.start,  tuple_ptr, hdr.key_size);
        memcpy(buff, tuple_ptr, hdr.record_size);
    }
    void LeafPage::GetShallowByPosition(int pos, RecordSchema *schema_ptr, DynamicCompoundKey &key, void*& buff) {
        assert(pos >= 0);
        assert(pos <= hdr.last_index);
        buff = GetRecordPtrByIndex(pos);
        key = GetRecordKeyByIndex(pos, schema_ptr);
    }
    void LeafPage::leaf_page_search(const DynamicCompoundKey &k, SearchResult &result, GlobalAddress g_page_ptr,
                                               RecordSchema *record_scheme) {
        size_t tuple_length = record_scheme->GetRecordTotalSize();
        char* tuple_start;
        uint16_t left = 0;
        // TODO: the code below will be false if we execute the leaf page delete multiple times.
        assert(hdr.last_index >= 0);
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
#ifndef NDEBUG
        std::vector<std::pair<uint16_t, uint16_t>> binary_history;
        DynamicCompoundKey last_key = GetRecordKeyByIndex(hdr.last_index, record_scheme);
        assert(k < GetHighest(record_scheme) );
        assert(last_key < GetHighest(record_scheme));
#endif
        
        // Cache pointer calculations to avoid repeated computation in loop
        uint16_t key_size = hdr.key_size;
        uint64_t record_size = hdr.record_size;
        char *data_ptr = data_ + 2 * key_size;
        
        // Reuse a single DynamicCompoundKey object instead of creating new ones each iteration
        // This avoids constructor overhead and potential memory allocations
        DynamicCompoundKey mid_key_wrapper(nullptr, record_scheme);
        
        while (left < right) {
            // Calculate mid: (left + right + 1) / 2
            // Compiler should optimize division by 2, but we can use bit shift: (left + right + 1) >> 1
            mid = (left + right + 1) >> 1;  // Bit shift is faster than division
            
            // Direct pointer calculation - avoid function call overhead
            char *mid_ptr = data_ptr + mid * record_size;
            mid_key_wrapper.start = mid_ptr;  // Reuse existing object, just update pointer
            
            // Use single comparison with early exit for equality case
            if (k > mid_key_wrapper) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if (k < mid_key_wrapper) {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            } else {
                // Found the key - early exit
                assert(hdr.record_size > 0);
                memcpy((void*)result.val.data(), mid_ptr, hdr.record_size);
                result.find_value = true;
                return;
            }
        }
        
        // Not find or find on the first entry.
        assert(right == left);
        tuple_start = static_cast<char *>(GetRecordPtrByIndex(right)); //data_ + right * tuple_length;
        DynamicCompoundKey temp_key = GetRecordKeyByIndex(right, record_scheme);
        if (k == temp_key){
            assert(right == 0);
            assert(result.val.size() == record_scheme->GetRecordTotalSize());
            memcpy((void*)result.val.data(),tuple_start, record_scheme->GetRecordTotalSize());
            result.find_value = true;
        }else{
            assert(k >temp_key);
            result.find_value = false;
        }
    }
    // [lowest, highest)
    bool LeafPage::leaf_page_store(const DynamicCompoundKey &k, const Slice &v, int &cnt,
                                          RecordSchema *index_schema, RedoLogger* redo_logger, GlobalAddress page_addr) {
        cnt = hdr.last_index + 1;
        bool is_update = false;
        uint16_t insert_index = 0;
        assert(hdr.kCardinality > 0);
        DynamicCompoundKey temp_key1 = GetRecordKeyByIndex(0, index_schema);
        assert(temp_key1 <= GetHighest(index_schema));
        assert(temp_key1 == GetLowest(index_schema) || GetLowest(index_schema) == DynamicCompoundKey::MinValue(index_schema));
        char* tuple_start;
        assert(k >= temp_key1 || GetLowest(index_schema) == DynamicCompoundKey::MinValue(index_schema));
        if (hdr.last_index == -1) {

            // this branc can only happen when the page is empty or the leafpage is the left most leaf page
            insert_index = 0;
        }else{
            assert(hdr.last_index >= 0);
            uint16_t left = 0;
            uint16_t right = hdr.last_index;
            uint16_t mid = 0;
            
            // Cache pointer calculations to avoid repeated computation in loop
            uint16_t key_size = hdr.key_size;
            uint64_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            
            // Reuse a single DynamicCompoundKey object instead of creating new ones each iteration
            DynamicCompoundKey temp_key(nullptr, index_schema);
            
            while (left < right) {
                mid = (left + right + 1) >> 1;  // Bit shift is faster than division
                
                // Direct pointer calculation - avoid function call overhead
                char *mid_ptr = data_ptr + mid * record_size;
                temp_key.start = mid_ptr;  // Reuse existing object, just update pointer
                
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
                    assert(v.size() == hdr.record_size);
                    memcpy(mid_ptr, v.data(), hdr.record_size);
                    // TODO: for search secondary index with duplicated key, the new inserted enty will be inserted into the first node contain that duplicated key,
                    //  but it may not inserted in the first position with in the node.
                    left = mid;
                    right = mid;
                }
            }
            assert(left == right);
            mid = left;

            char *left_ptr = data_ptr + left * record_size;
            temp_key.start = left_ptr;
            tuple_start = left_ptr;
            if ((k > temp_key )){
                insert_index = left +1;
            } else if (k < temp_key){
                // this node has to be the leftmost one.
                assert(GetLowest(index_schema) == DynamicCompoundKey::MinValue(index_schema));
                assert(left == 0);
                insert_index = 0;
            }else{
                    assert(v.size() == hdr.record_size);
                    memcpy(temp_key.start, v.data(), hdr.record_size);
                    is_update = true;
#ifdef DIRTY_ONLY_FLUSH
                    hdr.merge_dirty_bounds((char*)tuple_start - (char*)this, (char*)tuple_start - (char*)this + hdr.record_size);
#endif
                    assert(cnt < hdr.kCardinality);
                    //if it is an update in the primary index, return here.
                    return cnt == hdr.kCardinality;
            }
        }

        assert(cnt != hdr.kCardinality);
        assert(!is_update);

        tuple_start = static_cast<char*>(GetRecordPtrByIndex(insert_index));
        
        // Prepare logging before making changes
        LogCodec::Encoder encoder;
        bool need_logging = (redo_logger != nullptr && page_addr != GlobalAddress::Null());
        uint64_t old_page_version = hdr.p_version;
        // assert(old_page_version > 0);
        uint64_t new_page_version = old_page_version + 1;
        uint16_t logical_region_id = page_addr.nodeID;
        
        if (insert_index <= hdr.last_index){
            // Move all the tuples at and after the insert_index,use memmove to avoid undefined behavior for overlapped address.
            size_t move_size = (hdr.last_index - insert_index + 1) * hdr.record_size;
            size_t src_offset = (char*)tuple_start - (char*)this;
            size_t dst_offset = src_offset + hdr.record_size;
            
            // Log the memmove operation before executing it
            if (need_logging) {
                encoder.AddMemmove(dst_offset, src_offset, move_size);
            }
            
            memmove(tuple_start + hdr.record_size, tuple_start, move_size);
            auto r = Record(index_schema, tuple_start);
            assert(v.size() == r.GetRecordSize());
            
            // Log the new record insertion
            if (need_logging) {
                encoder.AddUpdateBytes(src_offset, v.data(), hdr.record_size);
            }
            
            r.FillRecord(v.data_reference(), v.size());
        }else{
            assert(insert_index < hdr.kCardinality );
            size_t insert_offset = (char*)tuple_start - (char*)this;
            
            // Log the new record insertion (no memmove needed)
            if (need_logging) {
                encoder.AddUpdateBytes(insert_offset, v.data(), hdr.record_size);
            }
            
            auto r = Record(index_schema, tuple_start);
            assert(v.size() == r.GetRecordSize());
            r.FillRecord(v.data_reference(), v.size());
        }
        cnt++;
        hdr.last_index++;
        assert(hdr.last_index < hdr.kCardinality);
        
        // Update page version in memory (for next operation) - always update regardless of logging
        hdr.p_version = new_page_version;
        
        // Log header update (last_index changed)
        if (need_logging) {
            size_t header_offset = STRUCT_OFFSET(LeafPage, hdr);
            size_t last_index_offset = header_offset + offsetof(Header_Index, last_index);
            encoder.AddUpdateBytes(last_index_offset, &hdr.last_index, sizeof(hdr.last_index));
            
            // Append to redo log (page_version is already in RecordHeader, no need to log it in payload)
            redo_logger->Append(logical_region_id, page_addr, new_page_version, 
                               encoder.Buffer().data(), encoder.Buffer().size()
#ifndef NDEBUG
                               , RedoLogger::LOG_LEAF_PAGE_STORE
#endif
                               );
        }
#ifdef DIRTY_ONLY_FLUSH
        // If the page get inserted, then the dirty range is the whole page, or flush to the end of tuple_start + (last_Index+1)*r.GetRecordSize()
        hdr.merge_dirty_bounds(sizeof(uint64_t), kLeafPageSize);
#endif
        assert(temp_key1 <= GetHighest(index_schema));
        return cnt == hdr.kCardinality;
    }
    // [lowest, highest)
    bool LeafPage::leaf_page_delete(const DynamicCompoundKey &k, int &cnt, SearchResult &result,
                                          RecordSchema *record_scheme, RedoLogger* redo_logger, GlobalAddress page_addr) {

        // It is problematic to just check whether the value is empty, because it is possible
        // that the buffer is not initialized as 0
        // TODO: make the key-value stored with order, do not use this unordered page structure.
        //  Or use the key to check whether this holder is empty.
        cnt = hdr.last_index + 1;
        uint16_t insert_index = 0;
        assert(hdr.kCardinality > 0);
        uint32_t tuple_length = record_scheme->GetRecordTotalSize();
        assert(tuple_length == hdr.record_size);


        char* tuple_start;
        DynamicCompoundKey temp_key1;
        assert(temp_key1 <= GetHighest(record_scheme));
        if (k < temp_key1 || hdr.last_index == -1) {
            // this branc can only happen when the page is empty or the leafpage is the left most leaf page
//            assert(hdr.last_index == -1);
            insert_index = 0;
        }else{
            assert(hdr.last_index >= 0);
            uint16_t left = 0;
            uint16_t right = hdr.last_index;
            uint16_t mid = 0;
            
            // Cache pointer calculations to avoid repeated computation in loop
            uint16_t key_size = hdr.key_size;
            uint64_t record_size = hdr.record_size;
            char *data_ptr = data_ + 2 * key_size;
            
            // Reuse a single DynamicCompoundKey object instead of creating new ones each iteration
            DynamicCompoundKey temp_key(nullptr, record_scheme);
            
            while (left < right) {
                mid = (left + right + 1) >> 1;  // Bit shift is faster than division
                
                // Direct pointer calculation - avoid function call overhead
                char *mid_ptr = data_ptr + mid * record_size;
                temp_key.start = mid_ptr;  // Reuse existing object, just update pointer
                
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
            char *left_ptr = data_ptr + left * record_size;
            tuple_start = left_ptr;
            temp_key.start = left_ptr;
            if ((k != temp_key )){
                result.find_value = false;
                return false;
            }else{
                insert_index = left;
            }
        }

        assert(cnt != hdr.kCardinality);
//        if (!is_update) { // insert new item

        tuple_start = static_cast<char *>(GetRecordPtrByIndex(insert_index)); //data_ + insert_index * tuple_length;
        
        // Prepare logging before making changes
        LogCodec::Encoder encoder;
        bool need_logging = (redo_logger != nullptr && page_addr != GlobalAddress::Null() && result.find_value);
        uint64_t old_page_version = hdr.p_version;
        uint64_t new_page_version = old_page_version + 1;
        uint16_t logical_region_id = page_addr.nodeID;
        
        if (insert_index <= hdr.last_index){
            // Move all the tuples at and after the insert_index,use memmove to avoid undefined behavior for overlapped address.
            size_t move_size = static_cast<size_t>(hdr.last_index - insert_index) * static_cast<size_t>(tuple_length);
            size_t src_offset = (char*)tuple_start - (char*)this + tuple_length;
            size_t dst_offset = (char*)tuple_start - (char*)this;
            
            // Log the memmove operation before executing it
            if (need_logging && move_size > 0) {
                encoder.AddMemmove(dst_offset, src_offset, move_size);
            }
            
            memmove(tuple_start, tuple_start + tuple_length, move_size);
        }else{
            assert(false);
        }
        cnt--;
        hdr.last_index--;
        assert(hdr.last_index <= hdr.kCardinality-1);
        
        // Update page version in memory (for next operation) - always update regardless of logging
        hdr.p_version = new_page_version;
        
        // Log header update (last_index changed)
        if (need_logging) {
            size_t header_offset = STRUCT_OFFSET(LeafPage, hdr);
            size_t last_index_offset = header_offset + offsetof(Header_Index, last_index);
            encoder.AddUpdateBytes(last_index_offset, &hdr.last_index, sizeof(hdr.last_index));
            
            // Append to redo log (page_version is already in RecordHeader, no need to log it in payload)
            redo_logger->Append(logical_region_id, page_addr, new_page_version, 
                               encoder.Buffer().data(), encoder.Buffer().size()
#ifndef NDEBUG
                               , RedoLogger::LOG_LEAF_PAGE_DELETE
#endif
                               );
        }
        
#ifdef DIRTY_ONLY_FLUSH
        // If the page get inserted, then the dirty range is the whole page, or flush to the end of tuple_start + (last_Index+1)*r.GetRecordSize()
        hdr.merge_dirty_bounds(sizeof(uint64_t), kLeafPageSize);
#endif
        assert(temp_key1 <= GetHighest(record_scheme));
        // Todo: decide when triger the leaf page merge.
        result.find_value = true;
        return false;

    }

    bool DataPage::InsertRecord(const Slice &tuple, int &cnt, RecordSchema *record_scheme, GlobalAddress& g_addr) {
        int tuple_length = record_scheme->GetRecordTotalSize();
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
        int tuple_length = record_scheme->GetRecordTotalSize();
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
        int tuple_length = record_scheme->GetRecordTotalSize();
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
