//
// Created by wang4996 on 22-8-8.
//

#include <infiniband/verbs.h>
#include "page.h"
#include "Btr.h"
namespace DSMEngine{
    void InternalPage::internal_page_search(const Key &k, SearchResult &result) {

        assert(k >= hdr.lowest);
        assert(k < hdr.highest);
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
    re_read:
        GlobalAddress target_global_ptr_buff;
        uint8_t front_v = front_version;
        uint8_t rear_v = rear_version;
        if(front_v != rear_v){
            goto re_read;
        }
          asm volatile ("sfence\n" : : );
          asm volatile ("lfence\n" : : );
          asm volatile ("mfence\n" : : );

          //TOTHINK: how to make sure that concurrent write will not result in segfault,
          // such as out of buffer for cnt.
        auto cnt = hdr.last_index + 1;
        // page->debug();
        if (k < records[0].key) {
//      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
            target_global_ptr_buff = hdr.leftmost_ptr;
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
//      result.upper_key = page->records[0].key;
            // check front verison here because a writer will change the front version at the beggining of a write op
            // if this has not changed, we can guarntee that there is not writer interfere.
            front_v = front_version;
            // TODO: maybe we need memory fence here either.
            // TOTHINK: There is no need for local reread because the data will be modified in a copy on write manner.


            result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
            result.upper_key = records[0].key;
#endif
            if (front_v!= rear_v){
                goto re_read;
            }
            assert(result.next_level != GlobalAddress::Null());
            return;
        }

        for (int i = 1; i < cnt; ++i) {
            if (k < records[i].key) {
//        printf("next level key is %lu \n", page->records[i - 1].key);

                target_global_ptr_buff = records[i - 1].ptr;

                assert(records[i - 1].key <= k);
                result.upper_key = records[i - 1].key;
                front_v = front_version;


                result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
                result.upper_key = records[i].key;
#endif
                if (front_v!= rear_v){
                    goto re_read;
                }
                assert(result.next_level != GlobalAddress::Null());
                return;
            }
        }
//    printf("next level pointer is  the last value %p \n", page->records[cnt - 1].ptr);

        target_global_ptr_buff = records[cnt - 1].ptr;

        assert(records[cnt - 1].key <= k);
        front_v = front_version;


        result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
        result.upper_key = hdr.highest;
#endif
        if (front_v!= rear_v)// version checking
            goto re_read;
        assert(result.next_level != GlobalAddress::Null());
    }

    void LeafPage::leaf_page_search(const Key &k, SearchResult &result, ibv_mr local_mr_copied, GlobalAddress g_page_ptr) {
//    re_read:
        Value target_value_buff{};
//        uint8_t front_v = front_version;
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
        //TODO: If record verisons are not consistent, we need to reread the page.
        // or refetch the record. or we just remove the byteaddressable write and then do not
        // use record level version.
        for (int i = 0; i < kLeafCardinality; ++i) {
            auto &r = records[i];
            while (r.f_version != r.r_version){
//                ibv_mr target_mr = *local_mr_copied;
                int offset = ((char*)&r - (char *) this);
                LADD(local_mr_copied.addr, offset);
                Btr::rdma_mg->RDMA_Read(GADD(g_page_ptr, offset), &local_mr_copied, sizeof(LeafEntry),IBV_SEND_SIGNALED,1, Internal_and_Leaf);

            }
            if (r.key == k && r.value != kValueNull ) {
                assert(r.f_version == r.r_version);
                target_value_buff = r.value;
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
//                uint8_t rear_v = rear_version;
//                if (front_v!= rear_v)// version checking
//                    //TODO: reread from the remote side.
//                    goto re _read;

//                memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
//      result.value_padding = r.value_padding;
                break;
            }
        }
        result.val = target_value_buff;
    }
}