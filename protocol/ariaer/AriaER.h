#pragma once

#include <chrono>
#include <barrier>
#include <thread>
#include <glog/logging.h>
#include <loom/utils/Ulock.h>
#include <loom/protocol/common.h>
#include <loom/common/Block.h>
#include <loom/common/Transaction.h>
#include <loom/utils/Statistic/Statistics.h>

using namespace std;

namespace loom {

#define K std::string
#define T AriaERTransaction

/// @brief ariaER tranaction with local read and write set.
struct AriaERTransaction: public Transaction {
    size_t      id;
    size_t      batch_id;
    bool        flag_conflict{false};
    std::atomic<bool>   committed{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<string, string> local_get;
    std::unordered_map<string, string> local_put;
    AriaERTransaction(Transaction&& inner, size_t id, size_t batch_id);
    AriaERTransaction(AriaERTransaction&& tx) noexcept; // move constructor
    AriaERTransaction(const AriaERTransaction& other); // copy constructor
};

/// @brief ariaER table entry for first round execution
struct AriaEREntry {
    string  value           = "";
    size_t  batch_id_get    = 0;
    size_t  batch_id_put    = 0;
    T*      reserved_get_tx = nullptr;
    T*      reserved_put_tx = nullptr;
};

/// @brief ariaER table for first round execution
struct AriaERTable: public Table<K, AriaEREntry, KeyHasher> {
    void ReserveGet(T* tx, const K& k);
    void ReservePut(T* tx, const K& k);
    void ReservePutAgain(T* tx, const K& k, const std::vector<bool>& abortList);
    bool CompareReservedGet(T* tx, const K& k);
    bool CompareReservedPut(T* tx, const K& k, const std::vector<bool>& abortList);
};

/// @brief ariaER table entry for fallback pessimistic execution
struct AriaERLockEntry {
    std::vector<T*>     deps_get;
    std::vector<T*>     deps_put;
};

/// @brief ariaER table for fallback pessimistic execution
struct AriaERLockTable: public Table<K, AriaERLockEntry, KeyHasher> {
    AriaERLockTable(size_t partitions);
};

/// @brief ariaER protocol master class
class AriaER: public Protocol {

public:
    AriaER(vector<Block::Ptr> blocks, Statistics& statistics, size_t num_threads, size_t table_partitions = 1, bool enable_reordering = true);
    void Start() override;
    void Stop() override;

private:
    Statistics&                             statistics;
    vector<Block::Ptr>                      blocks;
    AriaERTable                             table;
    AriaERLockTable                         lock_table;
    bool                                    enable_reordering;
    size_t                                  num_threads;
    std::atomic<size_t>                     confirm_exit{0};
    std::atomic<bool>                       stop_flag{false};
    std::barrier<std::function<void()>>     barrier;
    std::atomic<size_t>                     counter{0};
    std::atomic<bool>                       has_conflict{false};
    std::vector<std::thread>                workers;
    std::vector<bool>                       abortList;
    friend class AriaERExecutor;
};

/// @brief routines to be executed in various execution stages
class AriaERExecutor {

public:
    AriaERExecutor(AriaER& ariaER, size_t worker_id, vector<vector<T>> batchTxs);
    void Run();
    void Execute(T* tx);
    void WriteReserve(T* tx);
    void ReadReserve(T* tx);
    void VerifyWrite(T* tx);
    void Verify(T* tx);
    void Commit(T* tx);
    void PrepareLockTable(T* tx);
    void Fallback(T* tx);
    void CleanLockTable(T* tx);

private:
    Statistics&                             statistics;
    vector<vector<T>>                       batchTxs;
    AriaERTable&                            table;
    AriaERLockTable&                        lock_table;
    bool                                    enable_reordering;
    size_t                                  num_threads;
    std::atomic<size_t>&                    confirm_exit;
    std::atomic<bool>&                      stop_flag;
    std::barrier<std::function<void()>>&    barrier;
    std::atomic<size_t>&                    counter;
    std::atomic<bool>&                      has_conflict;
    std::vector<bool>&                      abortList;
    size_t                                  worker_id;
};

#undef T
#undef K

}
