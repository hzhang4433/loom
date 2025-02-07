#pragma once

#include <chrono>
#include <loom/common/Transaction.h>
#include <loom/utils/Ulock.h>
#include <loom/utils/thread/ThreadPool.h>
#include <loom/common/Block.h>
#include <loom/utils/Statistic/Statistics.h>

namespace loom {

#define K string
#define T shared_ptr<OptMETransaction>

class AddressBasedConflictGraph;

/// @brief optme tranaction.
struct OptMETransaction: public Transaction {
    size_t      id;
    size_t      block_id;
    size_t      sequence;
    std::atomic<bool>   committed{false};
    std::atomic<bool>   aborted{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<string, string> local_get;
    std::unordered_map<string, string> local_put;
    void Execute() override;
    void set_sequence(size_t seq) { sequence = seq; }
    size_t get_sequence() const { return sequence; }
    OptMETransaction(Transaction&& inner, size_t id, size_t block_id);
    OptMETransaction(OptMETransaction&& tx) noexcept; // move constructor
    OptMETransaction(const OptMETransaction& other); // copy constructor
};

/// @brief optme table entry for execution
struct OptMEEntry {
    string              value               = "";
    size_t              block_id_get        = 0;
    unordered_set<T>    reserved_get_txs    = {};
    size_t              block_id_put        = 0;
    size_t              reserved_put_num    = 0;
    size_t              next_reserved_put   = 0;
};

/// @brief optme table for execution
struct OptMETable: public Table<K, OptMEEntry, KeyHasher> {
    OptMETable(size_t partitions);
    void ReserveGet(T tx, const K& k);
    void ReservePut(T tx, const K& k);
};

/// @brief optme protocol master class
class OptME: public Protocol {
public:
    OptME(vector<Block::Ptr>& blocks, Statistics& statistics, size_t num_threads, size_t table_partitions = 1, bool enable_parallel = true);
    void Start() override;
    void Stop() override;
    void Run();
    void Simulate(vector<T>& batch);
    void Reorder(vector<T>& simulation_result, vector<T>& aborted_txs);
    void Reorder(shared_ptr<AddressBasedConflictGraph>& acg, vector<T>& aborted_txs);
    void IntraEpochReordering(vector<T>& simulation_result, vector<T>& aborted_txs, vector<T>& tx_list);
    void IntraEpochReordering(shared_ptr<AddressBasedConflictGraph>& acg, vector<T>& aborted_txs, vector<T>& tx_list);
    void InterEpochReordering(vector<vector<T>>& schedule, vector<T>& aborted_txs); // rescedule txs
    void ParallelExecute(vector<vector<T>>& schedule, vector<T>& aborted_txs);
    void Finalize(T tx);
    void ReExecute(T tx);

private:
    Statistics&                     statistics;
    vector<Block::Ptr>&             blocks;
    vector<vector<T>>               batches;
    vector<shared_ptr<AddressBasedConflictGraph>> acgs;
    size_t                          num_threads;
    OptMETable                      table;
    bool                            enable_parallel;
    std::atomic<size_t>             committed_block;
    std::shared_ptr<ThreadPool>     pool;
    size_t                          block_idx;
    std::mutex                      mtx;
    std::condition_variable         cv;

    // AddressBasedConflictGraph       acg; // Address-based conflict graph
    // vector<vector<T>>               schedule; // schedule for parallel execution
};

#undef T
#undef K

}
