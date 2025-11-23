// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN_TRANSACTION_MANAGER_H__
#define __DATABASE_TXN_TRANSACTION_MANAGER_H__

#include <iostream>
#include <vector>

#include "CharArray.h"
#include "DeltaSection.h"
#include "Meta.h"
#include "Profiler.h"
#include "Record.h"
#include "Records.h"
#include "TableDirectory.h"
#include "TxnAccess.h"
#include "TxnContext.h"
#include "TxnParam.h"
#include "env_posix.h"
#include "RedoLogger.h"
#include "LogCodec.h"
#include <unordered_map>
//#include "log.h"
#define TWO_PHASE_COMMIT
#define EARLYABORT
// Enable optimization for retried transactions: use local_ts_next for snapshot and treat READ_ONLY as READ_WRITE



namespace DSMEngine {
    // extern TpccBenchmark::TpccScaleParams tpcc_scale_params;
    extern uint64_t delta_pull_num[MAX_APP_THREAD];
    extern uint64_t roll_back_num[MAX_APP_THREAD];
    enum ISOLATION_LEVEL {
        READ_COMMITTED     = 1,
        READ_UNCOMMITTED   = 2,
        REPEATABLE_READ    = 3,
        SNAPSHOT_ISOLATION = 4,
        SERIALIZABLE       = 5,
    };
    class TransactionManager {
    public:
        TransactionManager(TableDirectory* storage_manager, size_t thread_count, size_t thread_id, bool wal_log = false,
            bool sharding = false, int partition_start = 1, int partition_end = 1, int num_items_per_partition = 1,
            int partition_key_bits = 48)
            : storage_manager_(storage_manager), thread_id_(thread_id), thread_count_(thread_count),
              log_enabled_(wal_log), sharding_(sharding), partition_start_(partition_start),
              partition_end_(partition_end), num_items_per_partition_(num_items_per_partition),
              partition_key_bits_(partition_key_bits) {
            env_ = Env::Default();
#if defined(MVOCC)
            active_manager_count.fetch_add(1, std::memory_order_acq_rel);
#endif
            if (wal_log) {
                if (!log_file) {
                    //              Status ret = env_->NewWritableFile("/ssd_root/wang4996/logdump.txt", &log_file);
                    Status ret = env_->NewWritableFile("./logdump.txt", &log_file);

                    if (!ret.ok()) {
                        printf("cannot create log file\n");
                        fflush(stdout);
                    }
                }
            }
#if defined(MVOCC)
            auto rdma_mg = default_gallocator->rdma_mg;
            if (rdma_mg->message_handling_funcs_map.count(DeltaCreate) == 0) {
                //          auto func = std::bind(&TransactionManager::ProcessDeltaCreate,  std::placeholders::_1);
                rdma_mg->Set_message_handling_func(ProcessDeltaCreate, DeltaCreate);
                rdma_mg->Set_message_handling_func(ProcessDeltaPull, DeltaPull);
                rdma_mg->Set_message_handling_func(ProcessSnapshotPush, SnapshotPush);
            }

            uint8_t target_node_id    = 2 * ((rdma_mg->node_id / 2) % rdma_mg->GetLogicalMemNodeNum()) + 1;
            GlobalAddress remote_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Chunk_type::DeltaChunk, target_node_id);
            assert((remote_addr.offset % 128 * define::MB) % 10 * define::MB == 0);
            ibv_mr* local_mr = new ibv_mr{};
            rdma_mg->Allocate_Local_RDMA_Slot(*local_mr, DeltaChunk);
#ifdef SINGLE_DELTA_PER_NODE
            std::unique_lock<RWSpinMutex> lck1(delta_map_mtx);
            if (ds_for_write == nullptr) {
                ds_for_write =
                    new DeltaSectionWrap(rdma_mg->node_id, remote_addr, rdma_mg->delta_section_size, local_mr);

                delta_sections.insert(std::make_pair(remote_addr, ds_for_write));
                // todo: sync the delta sections to the other nodes.
                rdma_mg->Sync_Create_Delta_Section_RPC(remote_addr, rdma_mg->node_id);
            }
            lck1.unlock();
#else
            ds_for_write = new DeltaSectionWrap(rdma_mg->node_id, remote_addr, rdma_mg->delta_section_size, local_mr);
            std::unique_lock<std::shared_mutex> lck1(delta_map_mtx);
            delta_sections.insert(std::make_pair(remote_addr, ds_for_write));
            // todo: sync the delta sections to the other nodes.
            rdma_mg->Sync_Create_Delta_Section_RPC(remote_addr, rdma_mg->node_id);
            lck1.unlock();
#endif
            // std::unique_lock<SpinMutex> lck2(c_l_mtx);
            // // need to initialize the cluster_least_sp_.
            // if (cluster_least_sp_.empty()) {
            //     for (uint16_t i = 0; i < rdma_mg->GetComputeNodeNum(); i++) {
            //         cluster_least_sp_[2 * i] = 0;
            //     }
            // }

#endif
        }
        ~TransactionManager() {
            if (log_enabled_) {
                delete log_file;
            }
            // todo: exit the delta GC thread.
#if defined(MVOCC)
            uint32_t prev = active_manager_count.fetch_sub(1, std::memory_order_acq_rel);
            if (prev == 1) {
                gc_thread_control_mtx.lock();
                gc_should_run.store(false, std::memory_order_release);
                if (gc_thread != nullptr) {
                    if (gc_thread->joinable()) {
                        gc_thread->join();
                    }
                    delete gc_thread;
                    gc_thread = nullptr;
                }
                gc_thread_control_mtx.unlock();
            }
#endif
        }
        //    static void Two_phase_commit_worker(uint16_t targe){
        //        TransactionManager *txn_manager = new TransactionManager(nullptr, 0, 0);
        //    };
        //    Status NewWritableFile(const std::string& filename,
        //                           WritableFile** result)  {
        //        int fd = ::open(filename.c_str(),
        //                        O_TRUNC | O_WRONLY | O_CREAT | 0, 0644);
        //        if (fd < 0) {
        //            *result = nullptr;
        //            return PosixError(filename, errno);
        //        }
        //
        //        *result = new PosixWritableFile(filename, fd);
        //        return Status::OK();
        //    }
        bool AllocateNewRecord(size_t table_id, Cache::Handle*& handle, GlobalAddress& data_addr, Record*& tuple);

        bool InsertRecord(size_t table_id, const DynamicCompoundKey keys, size_t key_num, Record* record,
            Cache::Handle* handle, const GlobalAddress tuple_gaddr);
        // Merge the Latch and unlatch request for tuples within the same global cache line.
        bool AcquireLatchForTuple(char*& tuple_buffer, GlobalAddress tuple_gaddr, AccessType access_type);
        bool AcquireXLatchForTuple(char*& tuple_buffer, GlobalAddress tuple_gaddr, Cache::Handle*& handle);
        bool AcquireSLatchForTuple(char*& tuple_buffer, GlobalAddress tuple_gaddr, Cache::Handle*& handle);
        void ReleaseLatchForTuple(GlobalAddress tuple_addr, Cache::Handle* handle);
        void ReleaseLatchForGCL(GlobalAddress page_gaddr, Cache::Handle* handle);
        bool ClearAllLatches();
        bool IsRecordLocal(IndexKey primary_key, uint16_t& target_node_id) {
            // Extract partition ID from primary key (bit shift encoding)
            int partition_id = primary_key >> partition_key_bits_;
            target_node_id   = ((partition_id - 1) / num_items_per_partition_) * 2;
            return partition_id >= partition_start_ && partition_id <= partition_end_;
        }
        void EnableLog() {
            log_enabled_ = true;
        }
        void DisableLog() {
            log_enabled_ = false;
        }
        RecordSchema* GetRecordSchema(size_t table_id) {
            return storage_manager_->tables_[table_id]->GetSchema();
        }
        RecordSchema* GetPrimaryIndexSchema(size_t table_id) {
            return storage_manager_->tables_[table_id]->GetPrimaryIndexSchema();
        }
        bool SearchRecord(
            size_t table_id, const DynamicCompoundKey primary_key, Record*& record, AccessType access_type) {
            PROFILE_TIME_START(thread_id_, INDEX_READ);
            uint16_t target_node_id;
#if ACCESS_MODE == 2
            if (sharding_ && !IsRecordLocal(primary_key, target_node_id)) {
                RecordSchema* schema_ptr = storage_manager_->tables_[table_id]->GetSchema();

                char* tuple_buffer;
                // Send message to the corresponding node to search the record.
                if (default_gallocator->rdma_mg->Tuple_Read_2PC_RPC(target_node_id, primary_key, table_id,
                        schema_ptr->GetSchemaSize(), tuple_buffer, access_type, log_enabled_)) {
                    record                        = new Record(schema_ptr, tuple_buffer);
                    Access* access                = access_list_.NewAccess();
                    access->access_type_          = access_type;
                    access->access_global_record_ = record;
                    if (participants.find(target_node_id) == participants.end()) {
                        participants.insert(target_node_id);
                    }
                    return true;
                } else {
                    //              printf("Abort at remote tuple read\n");
                    //                fflush(stdout);
                    AbortTransaction();
                    return false;
                }
            }
#endif
            GlobalAddress data_addr = storage_manager_->tables_[table_id]->SearchPriIndex(primary_key);
            PROFILE_TIME_END(thread_id_, INDEX_READ);
            if (data_addr != GlobalAddress::Null()) {
                bool ret = SelectRecordCC(table_id, record, data_addr, access_type);
                if (ret) {
                    assert(buffer_is_not_all_zero(record->data_ptr_, record->GetRecordSize()));
                }
                return ret;
            } else {
                printf("table_id=%d cannot find the record with  key=%s\n",
                    table_id, primary_key.start);
                fflush(stdout);
                // Not found return true, and let the caller to handle check whetehr record is still null to figure out
                // whether the tuple is found or not.
                return true;
            }
        }

        bool SearchRecords(TxnContext* context, size_t table_id, size_t index_id, const IndexKey& secondary_key,
            Records* records, AccessType access_type) {
            printf("not supported for now\n");
            return true;
        }

        bool CommitTransaction(CharArray& ret_str);
        bool HasActiveTransaction() const {
            return access_list_.access_count_ > 0;
        }
        bool CoordinatorPrepare();
        void WritePrepareLog() {
            // TODO: WE can accumulate the REDO log in thread local buffer and then allocate a log buffer
            // by CPU fetch_and_add, and then write the log to the file. After that the thread will wait
            // for the commit signal. For more details: https://catkang.github.io/2020/02/27/mysql-redo.html
            if (log_enabled_) {
                std::string ret_str_temp("Prepare\n");
                Slice log_record = Slice(ret_str_temp.c_str(), ret_str_temp.size());
                log_file->Append(log_record);
                //            log_file->Flush();
                log_file->Sync();
            }
        }
        void WriteCommitLog() {
            if (log_enabled_) {
                std::string ret_str_temp("Commit\n");
                Slice log_record = Slice(ret_str_temp.c_str(), ret_str_temp.size());
                log_file->Append(log_record);
                //            log_file->Flush();
                // if there is two phase commit, then this file sync is not necessary
                log_file->Sync();
            }
        }
        void WriteAbortLog() {

            std::string ret_str_temp("Abort\n");
            Slice log_record = Slice(ret_str_temp.c_str(), ret_str_temp.size());
            log_file->Append(log_record);
            //        log_file->Flush();
            log_file->Sync();
        }
        void AbortTransaction();
        
        // Direct redo logging per page update
        void LogDataUpdateOperation(Access* access, uint64_t commit_ts);
        void LogIndexInsertOperation(Access* access, const DynamicCompoundKey& primary_key, uint64_t commit_ts);
        
        // Page version management for redo logging (using already locked pages)
        uint64_t GetCurrentPageVersion(void* page_buffer);
        void SetCurrentPageVersion(void* page_buffer, uint64_t version);

        size_t GetThreadId() const {
            return thread_id_;
        }

    private:
        bool SelectRecordCC(size_t table_id, Record*& record, const GlobalAddress& tuple_gaddr, AccessType access_type);
#ifdef USE_SNAPSHOT_MANAGER
        void RegisterSeenTs(uint64_t ts);
#else
        inline void RegisterSeenTs(uint64_t) {}
#endif

    public:
        TableDirectory* storage_manager_;
        Env* env_;
        static WritableFile* log_file;
        
        // RedoLogger is now shared across all threads via DDSM (singleton)
        // Access it through: default_gallocator->GetRedoLogger(log_enabled_)
        static std::atomic<uint64_t> largest_sp_acquired;

#if defined(MVOCC)
        static RWSpinMutex delta_map_mtx;
        static std::map<GlobalAddress, DeltaSectionWrap*, std::greater<GlobalAddress>> delta_sections;
        static RWSpinMutex garb_mtx;
        static SpinMutex pin_sp_mtx;
        static std::map<uint64_t, uint16_t> pined_snapshot_this_node; // <snapshotid, count>
                                                                      //    static uint64_t last_broadcasted_sp;
        static SpinMutex c_l_mtx;
        static std::map<uint16_t, uint64_t> cluster_least_sp_; // <node id, least snapshot id>
        static SpinMutex gc_thread_control_mtx;
        static std::atomic<bool> gc_should_run;
        static std::atomic<uint32_t> active_manager_count;
        void GetSnapshot();
        void ReleaseSnapshot();
        static std::thread* gc_thread;
        static void ProcessDeltaCreate(void* args);
        static void ProcessDeltaPull(void* args);
        static void ProcessSnapshotPush(void* args);
        static void ProcessSnapshotPull(void* args);
        static void GarbageCollection();
        static void BroadCastLeastSP(uint64_t least_sp);
        void ClearStates() {
            locked_handles_.clear();
            ReleaseSnapshot();
            is_first_access_ = true;
            pure_read_txn    = true;
            have_rolled_back = false;
            snapshot_ts      = 0;
            plain_occ        = false;
#ifdef ENABLE_MVOCC_RETRY_OPTIMIZATION
            is_retry_        = false;
#endif
        }
#endif
    protected:
        //  Env* env_;
        size_t thread_id_;
        size_t thread_count_;
        AccessList access_list_;
#ifdef USE_SNAPSHOT_MANAGER
        uint64_t max_seen_ts_ = 0;
#endif

        bool log_enabled_ = false;
        bool sharding_    = false;
        //    bool require_2pc = false;
        int partition_start_         = 0;
        int partition_end_           = 0;
        int num_items_per_partition_ = 0;
        int partition_key_bits_      = 48; // Bit shift for extracting partition ID from key
        std::set<uint16_t> participants;
        ISOLATION_LEVEL isolation_level = SNAPSHOT_ISOLATION;

//    std::map<uint64_t, Access*> access_list_;
#if defined(TO)
        uint64_t start_timestamp_ = 0;
        bool is_first_access_     = true;
        std::map<uint64_t, std::pair<Cache::Handle*, int>> locked_handles_;
#endif
#if defined(TIMESTAMP)
        uint64_t start_timestamp_ = 0;
        bool is_first_access_     = true;
#endif
        // lock handles shall also be used for non-lock based algorithm to avoid acquire the same latch twice during the
        // execution.
#if defined(LOCK) || defined(OCC) || defined(MVOCC) || defined(TIMESTAMP)
        std::unordered_map<uint64_t, std::pair<Cache::Handle*, AccessType>> locked_handles_;
#endif

#if defined(MVOCC)
    public: // todo: make it private after debugging.
        uint64_t snapshot_ts  = 0;
        bool is_first_access_ = true;
        bool pure_read_txn    = true;
        bool have_rolled_back = false;
        bool plain_occ        = false;
#ifdef ENABLE_MVOCC_RETRY_OPTIMIZATION
        bool is_retry_        = false;
#endif
#ifdef SINGLE_DELTA_PER_NODE
        static DeltaSectionWrap* ds_for_write;
#else
        DeltaSectionWrap* ds_for_write = nullptr;

#endif


#endif
    };
} // namespace DSMEngine

#endif
