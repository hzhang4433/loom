#pragma once

#include <chrono>
#include <barrier>
#include <thread>
#include <glog/logging.h>
#include <loom/utils/Ulock.h>
#include <loom/protocol/common.h>
#include <loom/common/Block.h>
#include <loom/common/Transaction.h>

using namespace std;

namespace loom {

#define K std::string
#define T HarmonyTransaction

/// @brief harmony tranaction with local read and write set.
struct HarmonyTransaction: public Transaction {
    size_t      id;
    size_t      batch_id;
    bool        flag_conflict{false};
    std::atomic<bool>   committed{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<string, string> local_get;
    std::unordered_map<string, string> local_put;
    size_t      min_out;
    size_t      max_in;
    HarmonyTransaction(Transaction&& inner, size_t id, size_t batch_id);
    HarmonyTransaction(HarmonyTransaction&& tx) noexcept; // move constructor
    HarmonyTransaction(const HarmonyTransaction& other); // copy constructor
};

/// @brief harmony table entry for first round execution
struct HarmonyEntry {
    string value                = "";
    vector<T*> reserved_get_txs = {};
    vector<T*> reserved_put_txs = {};
};

/// @brief harmony table for first round execution
struct HarmonyTable: public Table<K, HarmonyEntry, KeyHasher> {
    void on_seeing_rw_dependency(T* Ti, T* Tj);
};

/// @brief harmony table entry for fallback pessimistic execution
struct HarmonyLockEntry {
    std::vector<T*>     deps_get;
    std::vector<T*>     deps_put;
};

/// @brief harmony table for fallback pessimistic execution
struct HarmonyLockTable: public Table<K, HarmonyLockEntry, KeyHasher> {
    HarmonyLockTable(size_t partitions);
};

/// @brief harmony protocol master class
class Harmony: public Protocol {

public:
    Harmony(vector<Block::Ptr> blocks, size_t num_threads, bool enable_inter_block, size_t table_partitions = 1);
    void Start() override;
    void Stop() override;

private:
    vector<Block::Ptr>                      blocks;
    HarmonyTable                            table;
    HarmonyLockTable                        lock_table;
    bool                                    enable_inter_block;
    size_t                                  num_threads;
    std::atomic<size_t>                     confirm_exit{0};
    std::atomic<bool>                       stop_flag{false};
    std::barrier<std::function<void()>>     barrier;
    std::atomic<size_t>                     counter{0};
    std::atomic<bool>                       has_conflict{false};
    std::vector<std::thread>                workers;
    friend class HarmonyExecutor;
};

/// @brief routines to be executed in various execution stages
class HarmonyExecutor {

public:
    HarmonyExecutor(Harmony& harmony, size_t worker_id, vector<vector<T>> batchTxs);
    void Run();
    void Execute(T* tx);
    void Reserve(T* tx);
    void Verify(T* tx);
    void Commit(T* tx);
    void PrepareLockTable(T* tx);
    void Fallback(T* tx);
    void CleanLockTable(T* tx);

private:
    vector<vector<T>>                       batchTxs;
    HarmonyTable&                           table;
    HarmonyLockTable&                       lock_table;
    bool                                    enable_inter_block;
    size_t                                  num_threads;
    std::atomic<size_t>&                    confirm_exit;
    std::atomic<bool>&                      stop_flag;
    std::barrier<std::function<void()>>&    barrier;
    std::atomic<size_t>&                    counter;
    std::atomic<bool>&                      has_conflict;
    size_t                                  worker_id;
};

#undef T
#undef K

}
