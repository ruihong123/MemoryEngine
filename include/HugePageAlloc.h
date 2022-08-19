#ifndef HUGEPAGEALLOC_H
#define HUGEPAGEALLOC_H


#include <cstdint>
#include "iostream"
#include <sys/mman.h>
#include <memory.h>
namespace DSMEngine{

    static bool is_mmap_work = true;

    inline void *hugePageAlloc(size_t size) {
        /**
         * mmap will actually go ahead and reserve the pages from the kernel's internal hugetlbfs mount, whose status can be
         * seen under /sys/kernel/mm/hugepages. The pages in question need to be available by the time mmap is invoked
         * (see HugePages_Free in /proc/meminfo), or mmap will fail. (https://stackoverflow.com/questions/30470972/using-mmap-and-madvise-for-huge-pages)
         */
        void *res = nullptr;
        if (is_mmap_work){
            res = mmap(NULL, size, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);

        }

        if (res == MAP_FAILED) {
            assert(is_mmap_work == true);
            printf("%s mmap failed!\n");
            is_mmap_work = false;
            res = malloc(size);
            return res;
        }else{
            return res;
        }

    }

    inline void hugePageDealloc(void* ptr, size_t size) {
        /**
         * mmap will actually go ahead and reserve the pages from the kernel's internal hugetlbfs mount, whose status can be
         * seen under /sys/kernel/mm/hugepages. The pages in question need to be available by the time mmap is invoked
         * (see HugePages_Free in /proc/meminfo), or mmap will fail. (https://stackoverflow.com/questions/30470972/using-mmap-and-madvise-for-huge-pages)
         */
        if (is_mmap_work){
            int ret = munmap(ptr, size);
        }else{

            printf("mmap is not enabled from the beginning\n");
        }
        free(ptr);

    }
}


#endif /* __HUGEPAGEALLOC_H__ */