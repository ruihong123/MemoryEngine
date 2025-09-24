//
// Created by ruihong on 9/27/23.
//
// Copyright (c) 2018 The GAM Authors


#include <thread>
#include <atomic>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <cstring>
#include <mutex>
#include <set>
#include <random>

#include "Common.h"
#include "storage/rdma.h"
#include "storage/page.h"
#include "storage/rdma.h"
#include "DDSM.h"
#include "Common.h"
#include "zipf.h"
#include "random.h"

//#define PERF_GET
//#define PERF_MALLOC

//#define BENCHMARK_DEBUG
//#define STATS_COLLECTION
//#define LOCAL_MEMORY

//TODO: shall be adjusted according to the no_thread and
//#define NUMOFBLOCKS (2516582ull) //around 48GB totally, local cache is 8GB per node. (25165824ull)
//#define SYNC_KEY NUMOFBLOCKS
#define FULLY_SHARED_WITHIN_NODE
//2516582ull =  48*1024*1024*1024/(2*1024)
#define MEMSET_GRANULARITY (64*1024)
//#define CMU_ZIPF
//#define EXCLUSIVE_HOTSPOT // exclusive hotspot per compute. workload == 1.

uint64_t NUMOFBLOCKS = 0;
uint64_t SYNC_KEY = 0;
uint64_t cache_size = 0;
int Memcache_offset = 1024;

uint64_t STEPS = 0;


using namespace DSMEngine;

uint16_t node_id;

bool is_master = false;
uint16_t tcp_port=19843;
//string ip_master = get_local_ip("eth0");
//string ip_worker = get_local_ip("eth0");
//int port_master = 12345;
//int port_worker = 12346;

const char* result_file = "result.csv";

//exp parameters
// Cache can hold 4Million cache entries. Considering the random filling mechanism,
// if we want to gurantee that the cache has been filled, we need to run 8Million iterations (2 times). space locality use 16384000
//long ITERATION_TOTAL = 2*4*8192000ull;
long ITERATION_TOTAL = 2*2*8192000ull;
//long ITERATION_TOTAL = 16384000;

long ITERATION = 0;

//long FENCE_PERIOD = 1000;
int no_thread = 2;
//int remote_ratio = 0;  //0..100
int shared_ratio = 10;  //0..100
int space_locality = 10;  //0..100
int time_locality = 10;  //0..100 (how probable it is to re-visit the current position_idx)
int read_ratio = 10;  //0..100
int op_type = 1;  //0: read/write; 1: rlock/wlock; 2: rlock+read/wlock+write
int workload = 0;  //0: random; 1: zipfian 2: multi-hotspot -1: single cache line
double zipfian_param = 1;
//int total_spot_num = 0; // used when workload == 2

int compute_num = 0;
int memory_num = 100;

float cache_th = 0.15;  //0.15
//uint64_t cache_size = 0;
uint64_t allocated_mem_size = 0;

//runtime statistics
std::atomic<long> remote_access(0);
std::atomic<long> shared_access(0);
std::atomic<long> space_local_access(0);
std::atomic<long> time_local_access(0);
std::atomic<long> read_access(0);

std::atomic<long> total_throughput(0);
std::atomic<uint64_t > avg_latency(0);

bool reset = false;

std::set<GlobalAddress> gen_accesses;
std::set<GlobalAddress> real_accesses;
std::mutex stat_lock;

int addr_size = sizeof(GlobalAddress);
int item_size = addr_size;
int items_per_block =  kLeafPageSize / item_size;
std::atomic<int> thread_sync_counter(0);

extern uint64_t cache_invalidation[MAX_APP_THREAD];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];
#ifdef GETANALYSIS
extern std::atomic<uint64_t> PrereadTotal;
extern std::atomic<uint64_t> Prereadcounter;
extern std::atomic<uint64_t> PostreadTotal;
extern std::atomic<uint64_t> Postreadcounter;
extern std::atomic<uint64_t> MemcopyTotal;
extern std::atomic<uint64_t> Memcopycounter;
extern std::atomic<uint64_t> NextStepTotal;
extern std::atomic<uint64_t> NextStepcounter;
extern std::atomic<uint64_t> WholeopTotal;
extern std::atomic<uint64_t> Wholeopcounter;
#endif
class WorkloadGenerator {
public:
    WorkloadGenerator() = default;
    virtual int getValue() = 0;
};

class ZipfianDistributionGenerator: public WorkloadGenerator {
private:
    uint64_t array_size;
    double skewness;
    std::vector<double> probabilities;
//    std::vector<int> zipfian_values;
    std::default_random_engine generator;
    std::discrete_distribution<int>* distribution;

public:
    ZipfianDistributionGenerator(uint64_t size, double s, unsigned int seed, uint8_t rank_of_spot = 0,
                                 uint8_t total_num_of_spot = 1)
            : array_size(size), skewness(s), probabilities(size), generator(seed) {
        probabilities.resize(array_size);
        uint64_t spot_interval = array_size/total_num_of_spot;
        uint64_t spot_offset = rank_of_spot*spot_interval;
        uint64_t overflow_offset = 0;
        for(int i = 0; i < array_size; ++i) {
            overflow_offset = (i+spot_offset)%array_size;
            probabilities[overflow_offset] = 1.0 / (pow(i+1, skewness));
//            zipfian_values[i] = i;
        }
        double smallest_probability = 1.0 / (pow(array_size, skewness));
// Convert smallest_probability to a string
        char buffer[50];
        snprintf(buffer, sizeof(buffer), "%.15f", smallest_probability);

        // Print the smallest_probability
        printf("Smallest Probability: %s\n", buffer);
        distribution = new std::discrete_distribution<int>(probabilities.begin(), probabilities.end());
//        std::shuffle(zipfian_values.begin(), zipfian_values.end(), generator);
    }

    int getValue() override {
//        return zipfian_values[distribution(generator)];
        return (*distribution)(generator);
    }
};

class MultiHotSpotGenerator: public WorkloadGenerator {
private:
    uint64_t array_size;
    int spot_num_;
    double skewness;
    std::vector<double> probabilities;
//    std::vector<int> zipfian_values;
    std::default_random_engine generator;
    std::discrete_distribution<int>* distribution;

public:
    MultiHotSpotGenerator(uint64_t size, double s, unsigned int seed, int spot_num) : array_size(size), spot_num_(spot_num),
    skewness(s), probabilities(size), generator(seed) {
        probabilities.resize(array_size, 0);
        uint64_t spot_interval = array_size/spot_num_;
        uint64_t spot_offset = 0;
        uint64_t overflow_offset = 0;
        for (int j = 0; j < spot_num; ++j) {
            spot_offset = spot_interval*j;
            for(int i = 0; i < array_size; ++i) {
                overflow_offset = (i+spot_offset)%array_size;
                probabilities[overflow_offset] = probabilities[overflow_offset] + 1.0 / (pow(i+1, skewness));
//            zipfian_values[i] = i;
            }
        }

        double smallest_probability = 1.0 / (pow(array_size, skewness));
// Convert smallest_probability to a string
        char buffer[50];
        snprintf(buffer, sizeof(buffer), "%.15f", smallest_probability);

        // Print the smallest_probability
        printf("Smallest Probability: %s\n", buffer);
        distribution = new std::discrete_distribution<int>(probabilities.begin(), probabilities.end());
//        std::shuffle(zipfian_values.begin(), zipfian_values.end(), generator);
    }

    int getValue() override {
//        return zipfian_values[distribution(generator)];
        return (*distribution)(generator);
    }
};

inline int GetRandom(int min, int max, unsigned int* seedp) {
    int ret = (rand_r(seedp) % (max - min)) + min;
    return ret;
}
struct timespec init_time;
void init() __attribute__ ((constructor));
void fini() __attribute__ ((destructor));
void init() {
    clock_gettime(CLOCK_REALTIME, &init_time);
}
long get_time() {
//	struct timeval start;
//	gettimeofday(&start, NULL);
//	return start.tv_sec*1000l*1000+start.tv_usec;
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    return (start.tv_sec - init_time.tv_sec) * 1000l * 1000 * 1000
           + (start.tv_nsec - init_time.tv_nsec);;
}
bool TrueOrFalse(double probability, unsigned int* seedp) {
    return (rand_r(seedp) % 100) < probability;
}



//int GetRandom(int min, int max, unsigned int* seedp) {
//	int ret = (rand_r(seedp) % (max-min)) + min;
//	return ret;
//}

int CyclingIncr(int a, int cycle_size) {
    return ++a == cycle_size ? 0 : a;
}

double Revise(double orig, int remaining, bool positive) {
    if (positive) {  //false positive
        return (remaining * orig - 1) / remaining;
    } else {  //false negative
        return (remaining * orig + 1) / remaining;
    }
}

volatile bool data_array_is_ready = false;

void Init(DDSM* ddsm, GlobalAddress data[], GlobalAddress access[], bool shared[], int id,
          unsigned int* seedp) {
    printf( "start init\n");

//  int l_remote_ratio = remote_ratio;
    int l_space_locality = space_locality;
    int l_shared_ratio = shared_ratio;
    GlobalAddress memset_buffer[MEMSET_GRANULARITY];
    GlobalAddress* memget_buffer = nullptr;
//    int memset_buffer_offset = 0;
    int current_get_block = -1;
    //the main thread (id == 0) in the master node (is_master == true)
    // is responsible for reference data access pattern
    if (is_master && id == 0) {
        for (int i = 0; i < STEPS; i++) {
            //There is no meanings to do the if clause below.
            if (TrueOrFalse(l_shared_ratio, seedp)) {
                shared[i] = true;
            } else {
                shared[i] = false;
            }
#ifdef LOCAL_MEMORY
            int ret = posix_memalign((void **)&data[i], BLOCK_SIZE, BLOCK_SIZE);
      epicAssert(!ret);
      epicAssert(data[i] % BLOCK_SIZE == 0);
#else
//      if (TrueOrFalse(remote_ratio, seedp)) {
            data[i] = ddsm->Allocate_Remote(Regular_Page);
            assert(data[i].offset <= 64ull*1024ull*1024*1024);
//      } else {
//        data[i] = alloc->AlignedMalloc(BLOCK_SIZE);
//      }
#endif
            //Register the allocation for master into a key value store.
            if (i%MEMSET_GRANULARITY == MEMSET_GRANULARITY - 1) {
                memset_buffer[i%MEMSET_GRANULARITY] = data[i];
                assert(data[i].offset <= 64ull*1024ull*1024*1024);
                printf("Memset a key %d\n", i);
                fflush(stdout);
                ddsm->memSet((const char*)&i, sizeof(i), (const char*)memset_buffer, sizeof(GlobalAddress) * MEMSET_GRANULARITY);
//                    assert(i%MEMSET_GRANULARITY == MEMSET_GRANULARITY-1);
            }else{
                memset_buffer[i%MEMSET_GRANULARITY] = data[i];
                assert(data[i].offset <= 64ull*1024ull*1024*1024);

            }
            if (i == STEPS - 1) {
                printf("Memset a key %d\n", i);
                fflush(stdout);
                ddsm->memSet((const char*)&i, sizeof(i), (const char*)memset_buffer, sizeof(GlobalAddress) * MEMSET_GRANULARITY);
            }
#ifdef BENCHMARK_DEBUG
            if (shared_ratio != 0) {
        GAddr readback;
        int back_ret = alloc->Get(i, &readback);
        epicAssert(back_ret == addr_size);
        epicAssert(readback == data[i]);
      }
#endif
        }
        data_array_is_ready = true;
    } else {
#ifdef FULLY_SHARED_WITHIN_NODE
        if (!is_master && id == 0){
            for (int i = 0; i < STEPS; i++) {
                if(shared_ratio > 0 && i == (STEPS/MEMSET_GRANULARITY)*MEMSET_GRANULARITY){
                    if (memget_buffer){
                        free(memget_buffer);
                    }
                    size_t v_size;
                    int key =  STEPS - 1;
                    memget_buffer = (GlobalAddress*)ddsm->memGet((const char*)&key, sizeof(key),  &v_size);
                    assert(v_size == sizeof(GlobalAddress) * MEMSET_GRANULARITY);
                }else if (UNLIKELY(shared_ratio > 0 && i % MEMSET_GRANULARITY == 0 )) {
                    if (memget_buffer){
                        free(memget_buffer);
                    }
                    size_t v_size;
                    int key =  i + MEMSET_GRANULARITY - 1;
                    memget_buffer = (GlobalAddress*)ddsm->memGet((const char*)&key, sizeof(key),  &v_size);
                    assert(v_size == sizeof(GlobalAddress) * MEMSET_GRANULARITY);
                }

                //we prioritize the shared ratio over other parameters
                if (i < STEPS*l_shared_ratio/100) {
                    GlobalAddress addr;
                    size_t v_size;

                    data[i] = memget_buffer[i%MEMSET_GRANULARITY];
                    assert(data[i].offset <= 64ull*1024ull*1024*1024);
                    //revise the l_remote_ratio accordingly if we get the shared addr violate the remote probability
                    shared[i] = true;
                } else {

                    data[i] = ddsm->Allocate_Remote(Regular_Page);
                    assert(data[i].offset <= 64ull*1024ull*1024*1024);



                    shared[i] = false;
                }
            }
            data_array_is_ready = true;
        }else{
            while (!data_array_is_ready){};
        }
#else
        for (int i = 0; i < STEPS; i++) {
            if(UNLIKELY(shared_ratio > 0 && i == (STEPS/MEMSET_GRANULARITY)*MEMSET_GRANULARITY)){
                if (memget_buffer){
                    delete memget_buffer;
                }
                size_t v_size;
                int key =  STEPS - 1;
                memget_buffer = (GlobalAddress*)ddsm_->memGet((const char*)&key, sizeof(key),  &v_size);
                assert(v_size == sizeof(GlobalAddress) * MEMSET_GRANULARITY);
            }else if (UNLIKELY(shared_ratio > 0 && i%MEMSET_GRANULARITY == 0 )) {
                if (memget_buffer){
                    delete memget_buffer;
                }
                size_t v_size;
                int key =  i + MEMSET_GRANULARITY - 1;
                memget_buffer = (GlobalAddress*)ddsm_->memGet((const char*)&key, sizeof(key),  &v_size);
                assert(v_size == sizeof(GlobalAddress) * MEMSET_GRANULARITY);
            }

            //we prioritize the shared ratio over other parameters
            if (TrueOrFalse(l_shared_ratio, seedp)) {
                GlobalAddress addr;
                size_t v_size;

                data[i] = memget_buffer[i%MEMSET_GRANULARITY];
                assert(data[i].offset <= 64ull*1024ull*1024*1024);
                //revise the l_remote_ratio accordingly if we get the shared addr violate the remote probability
                shared[i] = true;
            } else {

                data[i] = ddsm_->Allocate_Remote(Regular_Page);
                assert(data[i].offset <= 64ull*1024ull*1024*1024);



                shared[i] = false;
            }
        }
#endif

    }
    //access[0] = data[0];
    DSMEngine::Random64 rand(*seedp);
    access[0] = data[rand.Uniform(STEPS)];
#ifdef STATS_COLLECTION
    stat_lock.lock();
  gen_accesses.insert(TOBLOCK(access[0]));
  stat_lock.unlock();
#endif

    struct zipf_gen_state state;

    WorkloadGenerator* workload_gen = nullptr;

    if (workload == 1){
#ifdef EXCLUSIVE_HOTSPOT
        workload_gen = new ZipfianDistributionGenerator(STEPS, zipfian_param, *seedp, ddsm_->GetID()/2, compute_num);
#else

#ifdef CMU_ZIPF
        mehcached_zipf_init(&state, STEPS, zipfian_param,
                            (rdtsc() & (0x0000ffffffffffffull)) ^ id);
#else
        workload_gen = new ZipfianDistributionGenerator(STEPS, zipfian_param, *seedp);
#endif
#endif
    } else if (workload > 1){
        workload_gen = new MultiHotSpotGenerator(STEPS, zipfian_param, *seedp, workload);
    }


    // Access is the address of future acesses.
    for (int i = 1; i < 2*ITERATION; i++) {
        //PopulateOneBlock(alloc, data, ldata, i, l_remote_ratio, l_space_locality, seedp);
        GlobalAddress next;
        if (TrueOrFalse(space_locality, seedp)) {
            next = access[i - 1];
            next.offset += item_size;
            if (TOPAGE(next) != TOPAGE(access[i - 1])) {
                next = TOPAGE(access[i - 1]);
            }
        } else {
            if (workload == 0){
                GlobalAddress n = data[rand.Uniform(STEPS)];
//                while (TOPAGE(n) == TOPAGE(access[i - 1])) {
//                    n = data[GetRandom(0, STEPS, seedp)];
//                }
                next = GADD(n, rand.Uniform(items_per_block) * item_size);
            } else if (workload > 0){
#ifdef CMU_ZIPF
                uint64_t pos = mehcached_zipf_next(&state);

#else
                uint64_t pos = workload_gen->getValue();

#endif
                GlobalAddress n = data[pos];
//                while (TOPAGE(n) == TOPAGE(access[i - 1])) {
//                    pos = workload_gen->getValue();
//                    n = data[pos];
//                }
                next = GADD(n, rand.Uniform(items_per_block) * item_size);
            }else{
                assert(workload = -1);
                GlobalAddress n = data[0];
                next = GADD(n, rand.Uniform(items_per_block) * item_size);
                assert(TOPAGE(next) == data[0]);
            }


//            GlobalAddress n = data[GetRandom(0, STEPS, seedp)];
//            while (n == access[i - 1]) {
//                n = data[GetRandom(0, STEPS, seedp)];
//            }
//            next = n;
        }
        access[i] = next;
#ifdef STATS_COLLECTION
        stat_lock.lock();
    gen_accesses.insert(TOBLOCK(next));
    stat_lock.unlock();
#endif
    }
    printf("end init\n");
    if (workload_gen){
        delete workload_gen;
    }
}

bool Equal(char buf1[], char buf2[], int size) {
    int i;
    for (i = 0; i < size; i++) {
        if (buf1[i] != buf2[i]) {
            break;
        }
    }
    return i == size ? true : false;
}

void Run(DDSM* alloc, GlobalAddress data[], GlobalAddress access[],
         std::unordered_map<uint64_t , int>& addr_to_pos, bool shared[], int id,
         unsigned int* seedp, bool warmup) {

    GlobalAddress to_access = access[0];  //access starting point
    char buf[item_size];
    int ret;
    int j = 0;
//	int writes = 0;
//	GAddr fence_addr = alloc->Malloc(1);
//	epicAssert(fence_addr);
    long start = get_time();
    for (int i = 0; i < ITERATION; i++) {
//		if(writes == FENCE_PERIOD) {
//			alloc->MFence();
//			char c;
//			ret = alloc->Read(fence_addr, &c, 1);
//			epicAssert(ret == 1);
//			writes = 0;
//		}

#ifdef STATS_COLLECTION
        int pos;
    try {
      pos = addr_to_pos.at(TOBLOCK(to_access));
    } catch (const exception& e) {
      epicLog(LOG_WARNING, "cannot find pos for addr %lx\n", TOBLOCK(to_access), e.what());
      epicAssert(false);
    }
    if(WID(to_access) != alloc->GetID()) {
      remote_access++;
    }
    if(shared[pos]) {
      shared_access++;
    }
    stat_lock.lock();
    real_accesses.insert(TOBLOCK(to_access));
    //printf("%lx,%lx\n", TOBLOCK(to_access), to_access);
    stat_lock.unlock();
#endif
        switch (op_type) {
            case 0:  //blind write no need to read before write.
                if (TrueOrFalse(read_ratio, seedp)) {
                    void* page_buffer;
                    Cache::Handle* handle;
                    GlobalAddress target_cache_line = TOPAGE(to_access);
//                    uint64_t cache_line_offset = to_access.offset - target_cache_line.offset;
//                    if (UNLIKELY(cache_line_offset == STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock))){
//                        cache_line_offset += sizeof(uint64_t);
//                    }
                    alloc->SELCC_Shared_Lock(page_buffer, target_cache_line, handle);
                    memcpy(buf, (char*)page_buffer + (to_access.offset - target_cache_line.offset), item_size);
                    alloc->SELCC_Shared_UnLock(target_cache_line, handle);

                } else {
                    assert(false);
                    void* page_buffer;
                    Cache::Handle* handle;
                    memset(buf, i, item_size);
                    GlobalAddress target_cache_line = TOPAGE(to_access);
                    uint64_t cache_line_offset = to_access.offset - target_cache_line.offset;
                    if (UNLIKELY(cache_line_offset <= STRUCT_OFFSET(LeafPage, data_[0]))){
                        cache_line_offset += STRUCT_OFFSET(LeafPage, data_[0]);
                    }
                    alloc->SELCC_Exclusive_Lock_noread(page_buffer, target_cache_line, handle);
                    // Can not write to random place because we can not hurt the metadata in the page.
                    memcpy((char*)page_buffer + (cache_line_offset), buf, item_size);
                    alloc->SELCC_Exclusive_UnLock_noread(target_cache_line, handle);
                }
                break;
            case 1:  //rlock/wlock
            {
                if (TrueOrFalse(read_ratio, seedp)) {
#ifdef GETANALYSIS
                    auto wholeop_start = std::chrono::high_resolution_clock::now();
#endif
                    void* page_buffer;
                    Cache::Handle* handle;
                    GlobalAddress target_cache_line = TOPAGE(to_access);
//#ifdef GETANALYSIS
//                    auto statistic_start = std::chrono::high_resolution_clock::now();
//#endif
                    alloc->SELCC_Shared_Lock(page_buffer, target_cache_line, handle);
#ifdef GETANALYSIS
//                    auto stop = std::chrono::high_resolution_clock::now();
//                    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - statistic_start);
//                    PrereadTotal.fetch_add(duration.count());
//                    Prereadcounter.fetch_add(1);
                    auto statistic_start = std::chrono::high_resolution_clock::now();
#endif
                    memcpy(buf, (char*)page_buffer + (to_access.offset - target_cache_line.offset), item_size);
#ifdef GETANALYSIS
                    auto stop = std::chrono::high_resolution_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - statistic_start);
                    MemcopyTotal.fetch_add(duration.count());
                    Memcopycounter.fetch_add(1);
//                    statistic_start = std::chrono::high_resolution_clock::now();
#endif
                    alloc->SELCC_Shared_UnLock(target_cache_line, handle);
//#ifdef GETANALYSIS
//                    stop = std::chrono::high_resolution_clock::now();
//                    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - statistic_start);
//                    PostreadTotal.fetch_add(duration.count());
//                    Postreadcounter.fetch_add(1);
//#endif
#ifdef GETANALYSIS
                    auto wholeop_stop = std::chrono::high_resolution_clock::now();
                    auto wholeop_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(wholeop_stop - wholeop_start);
                    WholeopTotal.fetch_add(wholeop_duration.count());
                    Wholeopcounter.fetch_add(1);
#endif
                } else {
                    void* page_buffer;
                    Cache::Handle* handle;
                    memset(buf, i, item_size);
                    GlobalAddress target_cache_line = TOPAGE(to_access);

                    uint64_t cache_line_offset = to_access.offset - target_cache_line.offset;
                    if (UNLIKELY(cache_line_offset <= STRUCT_OFFSET(LeafPage, data_[0]))){
                        cache_line_offset += STRUCT_OFFSET(LeafPage, data_[0]);
                    }
                    alloc->SELCC_Exclusive_Lock(page_buffer, target_cache_line, handle);
                    auto page = (LeafPage*)page_buffer;
#ifdef DIRTY_ONLY_FLUSH
                    page->hdr.merge_dirty_bounds(cache_line_offset, cache_line_offset+item_size);
#endif

#ifndef NDEBUG
                    if (((DataPage*)page_buffer)->hdr.this_page_g_ptr == GlobalAddress::Null()){
                        ((DataPage*)page_buffer)->hdr.this_page_g_ptr = target_cache_line;
                        page->hdr.merge_dirty_bounds(STRUCT_OFFSET(LeafPage, hdr.this_page_g_ptr), STRUCT_OFFSET(LeafPage, hdr.this_page_g_ptr)+
                                                                                                                            sizeof(GlobalAddress));

                    } else{
                        assert(((DataPage*)page_buffer)->hdr.this_page_g_ptr == target_cache_line);
#ifdef DIRTY_ONLY_FLUSH
                        page->hdr.merge_dirty_bounds(cache_line_offset, cache_line_offset+item_size);
#endif
                    }
#endif
                    // Can not write to random place because we can not hurt the metadata in the page.
                    memcpy((char*)page_buffer + (cache_line_offset), buf, item_size);
                    alloc->SELCC_Exclusive_UnLock(target_cache_line, handle);
                }
                break;
            }
            case 2:  // with 10 microsecond local spinning
            {
                if (TrueOrFalse(read_ratio, seedp)) {
                    void* page_buffer;
                    Cache::Handle* handle;
                    GlobalAddress target_cache_line = TOPAGE(to_access);
                    alloc->SELCC_Shared_Lock(page_buffer, target_cache_line, handle);
                    spin_wait_us(1);
                    memcpy(buf, (char*)page_buffer + (to_access.offset - target_cache_line.offset), item_size);
                    alloc->SELCC_Shared_UnLock(target_cache_line, handle);
                } else {
                    void* page_buffer;
                    Cache::Handle* handle;
                    memset(buf, i, item_size);
                    GlobalAddress target_cache_line = TOPAGE(to_access);
                    uint64_t cache_line_offset = to_access.offset - target_cache_line.offset;
                    if (UNLIKELY(cache_line_offset <= STRUCT_OFFSET(LeafPage, data_[0]))){
                        cache_line_offset += STRUCT_OFFSET(LeafPage, data_[0]);
                    }
                    alloc->SELCC_Exclusive_Lock(page_buffer, target_cache_line, handle);
                    spin_wait_us(1);
                    auto page = (LeafPage*)page_buffer;
                    page->hdr.merge_dirty_bounds(cache_line_offset, cache_line_offset+item_size);
                    // Can not write to random place because we can not hurt the metadata in the page.
                    memcpy((char*)page_buffer + (cache_line_offset), buf, item_size);
                    alloc->SELCC_Exclusive_UnLock(target_cache_line, handle);
                }
                break;
            }
            case 3:  //try_rlock/try_wlock
            {
                if (TrueOrFalse(read_ratio, seedp)) {
                    void* page_buffer;
                    Cache::Handle* handle;
                    GlobalAddress target_cache_line = TOPAGE(to_access);
                    alloc->SELCC_Shared_Lock(page_buffer, target_cache_line, handle);
                    memcpy(buf, (char*)page_buffer + (to_access.offset - target_cache_line.offset), item_size);
                    alloc->SELCC_Shared_UnLock(target_cache_line, handle);
                } else {
                    void* page_buffer;
                    Cache::Handle* handle;
                    memset(buf, i, item_size);
                    GlobalAddress target_cache_line = TOPAGE(to_access);
                    uint64_t cache_line_offset = to_access.offset - target_cache_line.offset;
                    if (UNLIKELY(cache_line_offset <= STRUCT_OFFSET(LeafPage, data_[0]))){
                        cache_line_offset += STRUCT_OFFSET(LeafPage, data_[0]);
                    }
                    alloc->SELCC_Exclusive_Lock(page_buffer, target_cache_line, handle);
                    // Can not write to random place because we can not hurt the metadata in the page.
                    memcpy((char*)page_buffer + (cache_line_offset), buf, item_size);
                    alloc->SELCC_Exclusive_UnLock(target_cache_line, handle);
                }
                break;
            }
            default:
                printf( "unknown op type\n");
                break;
        }
#ifdef GETANALYSIS
        auto statistic_start = std::chrono::high_resolution_clock::now();
#endif
        //time locality
        if (TrueOrFalse(time_locality, seedp)) {
            //we keep to access the same addr
            //epicLog(LOG_DEBUG, "keep to access the current location");
#ifdef STATS_COLLECTION
            time_local_access++;
#endif
        } else {
            j++;
            if (j == ITERATION) {
                j = 0;
                assert(i == ITERATION - 1);
            }
#ifdef STATS_COLLECTION
            if (TOBLOCK(to_access) == TOBLOCK(access[j])) {
        space_local_access++;
      }
#endif
            to_access = access[j];
            //epicAssert(buf == to_access || addr_to_pos.count(buf) == 0);
        }
        if (i%100000 == 0 && id == 0){
            auto current = get_time();
            long elapse =  (current - start)/1000/1000;
            printf("Node %d finish %d ops, time elapse is %ld ms \n", node_id, i, elapse);
            fflush(stdout);
        }
#ifdef GETANALYSIS
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - statistic_start);
        NextStepTotal.fetch_add(duration.count());
        NextStepcounter.fetch_add(1);
#endif
    }

//    if (op_type == 0) {
//        // issue a fence and a read request to the last address to ensure all previous
//        // op have been done
//        alloc->MFence();
//        ret = alloc->Read(to_access, buf, item_size);
//    }


    long end = get_time();
    long throughput = ITERATION / ((double) (end - start) / 1000 / 1000 / 1000);
    long latency = (end - start) / ITERATION;
    printf(
            "node_id %d, thread %d, average throughput = %ld per-second, latency = %ld ns %s\n",
            node_id, id, throughput, latency, warmup ? "(warmup)" : "");
    fflush(stdout);
    if (!warmup) {
        total_throughput.fetch_add(throughput);
        avg_latency.fetch_add(latency);
    }
}

void Benchmark(int id, DDSM* alloc, GlobalAddress *data) {

    unsigned int seedp = no_thread * alloc->GetID() + id;
    printf("seedp = %d\n", seedp);
    bindCore(id);

#ifdef PERF_MALLOC
    long it = 1000000;
  long start = get_time();
  int len = sizeof(int);
  GAddr addrs[it];
  for (int i = 0; i < it; i++) {
    len = GetRandom(1, 10485, &seedp);
    addrs[i] = alloc->Malloc(len);
    alloc->Free(addrs[i]);
    epicAssert(addrs[i]);
  }
  long end = get_time();
  long duration = end - start;
  epicLog(LOG_WARNING, "Malloc (local): throughput = %lf op/s, latency = %ld ns",
      (double )it / ((double )duration / 1000 / 1000 / 1000),
      duration / it);

//	start = get_time();
//	for (int i = 0; i < it; i++) {
//		alloc->Free(addrs[i]);
//	}
//	end = get_time();
//	duration = end - start;
//	epicLog(LOG_WARNING, "Free (local): throughput = %lf op/s, latency = %ld ns",
//			(double )it / ((double )duration / 1000 / 1000 / 1000),
//			duration / it);

  start = get_time();
  for (int i = 0; i < it; i++) {
    len = GetRandom(1, 1048576, &seedp);
    addrs[i] = alloc->Malloc(len, REMOTE);
    alloc->Free(addrs[i]);
  }
  end = get_time();
  duration = end - start;
  epicLog(LOG_WARNING, "Malloc (remote): throughput = %lf op/s, latency = %ld ns",
      (double )it / ((double )duration / 1000 / 1000 / 1000),
      duration / it);

//	start = get_time();
//	for (int i = 0; i < it; i++) {
//		alloc->Free(addrs[i]);
//	}
//	end = get_time();
//	duration = end - start;
//	epicLog(LOG_WARNING, "Free (remote): throughput = %lf op/s, latency = %ld ns",
//			(double )it / ((double )duration / 1000 / 1000 / 1000),
//			duration / it);
#endif

//    GlobalAddress *data = (GlobalAddress*) malloc(sizeof(GlobalAddress) * STEPS);
    std::unordered_map<uint64_t , int> addr_to_pos;
//	GAddr** ldata =(GAddr**)malloc(sizeof(GAddr*)*STEPS);
//	//init local data arrays
//	for(int i = 0; i < STEPS; i++) {
//		ldata[i] = (GAddr*)malloc(BLOCK_SIZE);
//	}
    // gernerate 2*Iteration access target, half for warm up half for the real test
    GlobalAddress* access = (GlobalAddress*) malloc(sizeof(GlobalAddress) * 2*ITERATION);

    //bool shared[STEPS];
    bool* shared = (bool*) malloc(sizeof(bool) * STEPS);

    Init(alloc, data, access, shared, id, &seedp);

    //init addr_to_pos map
//    for (int i = 0; i < STEPS; i++) {
//        addr_to_pos[data[i]] = i;
//    }

//	//localize the shared data access part
//	for(int i = 0; i < STEPS; i++) {
//		for(int j = 0; j < items_per_block; j++) {
//			if(addr_to_pos.count(TOBLOCK(ldata[i][j])) == 0) {
//				//FIXME:remove
//				epicAssert(!(id == 0 && is_master)); //cannot be the master process
//				epicAssert(ldata[i][j] == TOBLOCK(ldata[i][j])); //should be the starting addr of the next block in the master access trace
//				//ldata[i][j] = data[GetRandom(0, STEPS, &seedp)]; //data[CyclingIncr(i, STEPS)];  //update to the starting addr of the next block in its private access trace
//				GAddr n = GADD(data[GetRandom(0, STEPS, &seedp)], GetRandom(0, items_per_block, &seedp)*item_size);
//				while(TOBLOCK(n) == TOBLOCK(data[i])) { //data[CyclingIncr(i, STEPS)];
//					n = GADD(data[GetRandom(0, STEPS, &seedp)], GetRandom(0, items_per_block, &seedp)*item_size);
//				}
//				ldata[i][j] = n;
//			}
//		}
//	}

    printf("start warmup the benchmark on thread %d", id);

    bool warmup = true;
    Run(alloc, data, access, addr_to_pos, shared, id, &seedp, warmup);
    fflush(stdout);
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        cache_invalidation[i] = 0;
        cache_hit_valid[i][0] = 0;
    }
#ifdef GETANALYSIS
    PrereadTotal = 0;
    Prereadcounter = 0;
    PostreadTotal = 0;
    Postreadcounter = 0;
    MemcopyTotal = 0;
    Memcopycounter = 0;
    NextStepTotal = 0;
    NextStepcounter = 0;
    WholeopTotal = 0;
    Wholeopcounter = 0;
#endif
#ifdef TIMEPRINT
    Reset_cache_counters();
#endif
    uint64_t SYNC_RUN_BASE = SYNC_KEY + compute_num * 2;
    int sync_id = SYNC_RUN_BASE + compute_num * node_id + id;
    if (id!= 0){
        thread_sync_counter.fetch_add(1);
    }
    while (thread_sync_counter.load() != no_thread){
        if (id == 0 && thread_sync_counter.load() == no_thread-1){
            alloc->rdma_mg->sync_with_computes_Cside();
            thread_sync_counter.fetch_add(1);
        }
        usleep(5);
    }
//    alloc->Put(sync_id, &sync_id, sizeof(int));
//
//    for (int i = 1; i <= compute_num; i++) {
//        for (int j = 0; j < no_thread; j++) {
//            epicLog(LOG_WARNING, "waiting for node %d, thread %d", i, j);
//            alloc->Get(SYNC_RUN_BASE + compute_num * i + j, &sync_id);
//            epicAssert(sync_id == SYNC_RUN_BASE + compute_num * i + j);
//            epicLog(LOG_WARNING, "get sync_id %d from node %d, thread %d", sync_id, i,
//                    j);
//        }
//    }

    warmup = false;
    // reset cache statistics
//    stat_lock.lock();
//    if (!reset) {
//        alloc->ResetCacheStatistics();
//        reset = true;
//    }
//    stat_lock.unlock();

    printf( "start run the benchmark on thread %d\n", id);
    Run(alloc, data, &access[ITERATION], addr_to_pos, shared, id, &seedp, warmup);
//#ifndef LOCAL_MEMORY
//    //make sure all the requests are complete
//    alloc->MFence();
//    alloc->WLock(data[0], BLOCK_SIZE);
//    alloc->UnLock(data[0], BLOCK_SIZE);
//#endif
}

int main(int argc, char* argv[]) {
    //the first argument should be the program name
    for (int i = 1; i < argc; i++) {

        if (strcmp(argv[i], "--is_master") == 0) {
            is_master = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--this_node_id") == 0) {
            node_id = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--tcp_port") == 0) {
            tcp_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--no_thread") == 0) {
            no_thread = atoi(argv[++i]);
//    } else if (strcmp(argv[i], "--remote_ratio") == 0) {
//      remote_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--shared_ratio") == 0) {
            shared_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--read_ratio") == 0) {
            read_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--space_locality") == 0) {
            space_locality = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--time_locality") == 0) {
            time_locality = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--op_type") == 0) {
            op_type = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--workload") == 0) {
            workload = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--zipfian_alpha") == 0) {
            zipfian_param = atof(argv[++i]);
        } else if (strcmp(argv[i], "--result_file") == 0) {
            result_file = argv[++i];  //0..100
        } else if (strcmp(argv[i], "--item_size") == 0) {
            item_size = atoi(argv[++i]);
            items_per_block = kLeafPageSize / item_size;

        } else if (strcmp(argv[i], "--cache_size") == 0) {
            cache_size = atoi(argv[++i]);
            cache_size = cache_size * 1024ull * 1024 * 1024;
        } else if (strcmp(argv[i], "--allocated_mem_size") == 0) {
            allocated_mem_size = atoi(argv[++i]);
            allocated_mem_size = allocated_mem_size * 1024ull * 1024 * 1024;
        } else if (strcmp(argv[i], "--compute_num") == 0) {
            compute_num = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--memory_num") == 0) {
            memory_num = atoi(argv[++i]);
        } else {
            fprintf(stderr, "Unrecognized option %s for benchmark\n", argv[i]);
        }
    }

#ifdef LOCAL_MEMORY
    int memory_type = 0;  //"local memory";
#else
    int memory_type = 1;  //"global memory";
#endif
    printf("Currently configuration is: ");
//    printf(  is_master == 1 ? "true" : "false", no_thread, compute_num);
    printf(
            "compute_num = %d, no_thread = %d, shared_ratio: %d, read_ratio: %d, "
            "space_locality: %d, time_locality: %d, op_type = %s, memory_type = %s, item_size = %d, cache_th = %f, result_file = %s\n",
            compute_num,
            no_thread,
//      remote_ratio,
            shared_ratio,
            read_ratio,
            space_locality,
            time_locality,
            op_type == 0 ?
            "read/write" :
            (op_type == 1 ?
             "rlock/wlock" :
             (op_type == 2 ? "rlock+read/wlock+write" : "try_rlock/try_wlock")),
            memory_type == 0 ? "local memory" : "global memory", item_size, cache_th,
            result_file);

    //srand(1);


    struct DSMEngine::config_t config = {
            NULL,  /* dev_name */
            NULL,  /* server_name */
            tcp_port, /* tcp_port */
            1,	 /* ib_port */ //physical
            1, /* gid_idx */
            4*10*1024*1024, /*initial local buffer size*/
            node_id,
//            cache_ptr
    };
//    DSMEngine::RDMA_Manager::node_id = ThisNodeID;

    auto rdma_mg = DSMEngine::RDMA_Manager::Get_Instance(&config);
    DSMEngine::Cache* cache_ptr = DSMEngine::NewLRUCache(cache_size);
    rdma_mg->set_page_cache(cache_ptr);
    assert(cache_ptr->GetCapacity()> 10000);
    DDSM ddsm = DDSM(cache_ptr, rdma_mg);
    compute_num = ddsm.rdma_mg->GetComputeNodeNum();
    memory_num = ddsm.rdma_mg->GetPhysicalMemNodeNum();
    NUMOFBLOCKS = allocated_mem_size/(kLeafPageSize);
    printf("number of blocks is %lu\n", NUMOFBLOCKS);
    SYNC_KEY = NUMOFBLOCKS;
#ifdef FULLY_SHARED_WITHIN_NODE
    STEPS = NUMOFBLOCKS;
#else
    STEPS = NUMOFBLOCKS/((no_thread - 1)*(100-shared_ratio)/100.00L + 1);
#endif
//    STEPS = NUMOFBLOCKS/((no_thread*compute_num - 1)*(100-shared_ratio)/100.00L + 1);

    printf("number of steps is %lu\n", STEPS);
    printf("workload is %d, zipfian_alpha is %f", workload, zipfian_param);
    ITERATION = ITERATION_TOTAL/no_thread;
    sleep(1);
    //sync with all the other workers
    //check all the workers are started
    int id;
    node_id = ddsm.GetID();
    printf("This node id is %d\n", node_id);
//    //synchronize here.
//    alloc->Put(SYNC_KEY + node_id, &node_id, sizeof(int));
//    for (int i = 1; i <= no_node; i++) {
//        alloc->Get(SYNC_KEY + i, &id);
//        epicAssert(id == i);
//    }

#ifdef PERF_GET
    int it = 1000000;
  alloc->Put(UINT_MAX, &it, sizeof(int));
  long start = get_time();
  int ib;
  for (int i = 0; i < it; i++) {
    alloc->Get(UINT_MAX, &ib);
    epicAssert(ib == it);
  }
  long end = get_time();
  long duration = end - start;
  epicLog(LOG_WARNING, "GET: throughput = %lf op/s, latency = %ld ns",
      (double )it / ((double )duration / 1000 / 1000 / 1000),
      duration / it);
#endif
    GlobalAddress *data = (GlobalAddress*) malloc(sizeof(GlobalAddress) * STEPS);

    std::thread* ths[no_thread];
    for (int i = 0; i < no_thread; i++) {
        ths[i] = new std::thread(Benchmark, i, &ddsm, data);
    }
    for (int i = 0; i < no_thread; i++) {
        ths[i]->join();
    }

    // print cache statistics
//    ddsm_->ReportCacheStatistics();
#ifdef TIMEPRINT
    printf("The average cache look up time elapse is %lu\n", Calculate_cache_counters());
#endif
    long t_thr = total_throughput;
    long a_thr = total_throughput;
    a_thr /= no_thread;
    uint64_t a_lat = avg_latency;
    a_lat /= no_thread;
    uint64_t invalidation_num = 0;
    uint64_t hit_valid_num = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        invalidation_num = cache_invalidation[i] + invalidation_num;
    }
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        hit_valid_num = cache_hit_valid[i][0] + hit_valid_num;
    }
    printf(
            "results for  node_id %d: workload: %d, no_thread: %d, zipfian_alpha: %f total_throughput: %ld, avg_throuhgput: %ld, avg_latency: %lu， operation need cache invalidation %lu, operation cache hit and valid is %lu,  total operation executed %ld, execution time elapse = %lf\n\n",
            node_id, workload, no_thread, zipfian_param, t_thr, a_thr, a_lat, invalidation_num, hit_valid_num, ITERATION_TOTAL, (double)ITERATION_TOTAL / (double)total_throughput);

    //sync with all the other workers
    //check all the benchmark are completed
    unsigned long res[5];
    res[0] = t_thr;  //total throughput for the current node
    res[1] = a_thr;  //avg throuhgput for the current node
    res[2] = a_lat;  //avg latency for the current node
    res[3] = invalidation_num;  //avg invalidated message number
    res[4] = hit_valid_num;  //avg latency for the current node
    int temp = SYNC_KEY + Memcache_offset + node_id;
    DEBUG_PRINT_arg("memset temp key %d\n", temp);
    ddsm.memSet((char*)&temp, sizeof(int), (char*)res, sizeof(long) * 5);
    t_thr = a_thr = a_lat = invalidation_num = hit_valid_num = 0;
    for (int i = 0; i < compute_num; i++) {
        memset(res, 0, sizeof(long) * 5);
        temp = SYNC_KEY + Memcache_offset + i * 2;
        size_t len;
        DEBUG_PRINT_arg("memGet temp key %d\n", temp);
        long* ret = (long*)ddsm.memGet((char*)&temp , sizeof(int), &len);
        assert(len == sizeof(long) * 5);
        t_thr += ret[0];
        a_thr += ret[1];
        a_lat += ret[2];
        invalidation_num += ret[3];
        hit_valid_num += ret[4];
    }
    a_thr /= compute_num;
    a_lat /= compute_num;
    invalidation_num /= compute_num;
    hit_valid_num /= compute_num;

    if (is_master) {
        std::ofstream result;
        result.open(result_file, std::ios::app);
        result << compute_num << "," << no_thread << ","  << ","
               << shared_ratio << "," << read_ratio << "," << space_locality << ","
               << time_locality << "," << op_type << "," << memory_type << ","
               << item_size << "," << t_thr << "," << a_thr << "," << a_lat << ","
               << cache_th << "\n";
        printf(
                "results for all the nodes: "
                "compute_num: %d, workload: %d, zipfian_alpha: %f no_thread: %d, shared_ratio: %d, read_ratio: %d, space_locality: %d, "
                "time_locality: %d, op_type = %d, memory_type = %d, item_size = %d, "
                "operation with cache invalidation message accounts for %f percents, average cache valid hit percents %f total_throughput: %ld, avg_throuhgput:%ld, avg_latency:%ld, \n\n",
                compute_num, workload, zipfian_param, no_thread, shared_ratio, read_ratio,
                space_locality, time_locality, op_type, memory_type, item_size, static_cast<double>(invalidation_num) / ITERATION_TOTAL, static_cast<double>(hit_valid_num) / ITERATION_TOTAL, t_thr,
                a_thr, a_lat);
#ifdef GETANALYSIS
        if (Prereadcounter.load() != 0){
            printf("Preread average time elapse is %lu ns, Postread average time elapse is %lu ns, Memcopy average time elapse is %lu ns, "
                   "prepare next step is %lu ns, counter is %lu, whole ops average time elapse is %lu ns, PostreadCOunter is %lu\n",
                   PrereadTotal.load()/Prereadcounter.load(), PostreadTotal.load()/Postreadcounter.load(), MemcopyTotal.load()/Memcopycounter.load(),
                   NextStepTotal.load()/NextStepcounter.load(), NextStepcounter.load(), WholeopTotal/Wholeopcounter, Postreadcounter.load());
        }

#endif
        result.close();
    }

#ifdef STATS_COLLECTION
    epicLog(LOG_WARNING, "shared_ratio = %lf, remote_ratio = %lf, read_ratio = %lf, space_locality = %lf, time_locality = %lf, "
      "total blocks touched %d, expected blocks touched %d\n",
      ((double)shared_access)/(ITERATION*no_thread)*100/2, ((double)remote_access)/(ITERATION*no_thread)*100/2,
      ((double)read_access)/(ITERATION*no_thread)*100/2,
      ((double)space_local_access)/(ITERATION*no_thread)*100/2, ((double)time_local_access)/(ITERATION*no_thread)*100/2,
      real_accesses.size(), gen_accesses.size());
#endif

//	long time = no_thread*compute_num*(double)(100-read_ratio)/100+1;
//	time /= 2;
//	if(time < 2) time += 1;
    long time = 1;
    printf( "sleep for %ld s\n\n", time);
    sleep(time);

    return 0;
}

