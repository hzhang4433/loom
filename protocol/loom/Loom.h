#pragma once

#include <chrono>
#include <loom/common/Transaction.h>
#include <loom/protocol/loom/common.h>
#include <loom/utils/Ulock.h>
#include <loom/utils/thread/ThreadPool.h>
#include <loom/common/Block.h>
#include <loom/utils/Statistic/Statistics.h>

namespace loom {

#define K string
#define T shared_ptr<LoomTransaction>

/// @brief loom tranaction.
struct LoomTransaction: public Transaction {
    size_t      id;
    size_t      block_id;
    std::atomic<bool>   committed{false};
    std::atomic<bool>   aborted{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<string, string> local_get;
    std::unordered_map<string, string> local_put;
    void Execute() override;
    LoomTransaction(Transaction&& inner, size_t id, size_t block_id);
    LoomTransaction(LoomTransaction&& tx) noexcept; // move constructor
    LoomTransaction(const LoomTransaction& other); // copy constructor
};

/// @brief loom table entry for execution
struct LoomEntry {
    string              value               = "";
    size_t              block_id_get        = 0;
    unordered_set<T>    reserved_get_txs    = {};
    size_t              block_id_put        = 0;
    size_t              reserved_put_num    = 0;
    size_t              next_reserved_put   = 0;
};

/// @brief loom table for execution
struct LoomTable: public Table<K, LoomEntry, KeyHasher> {
    LoomTable(size_t partitions);
    void ReserveGet(T tx, const K& k);
    void ReservePut(T tx, const K& k);
};

/// @brief loom protocol master class
class Loom: public Protocol {
public:
    Loom(vector<Block::Ptr> blocks, Statistics& statistics, size_t num_threads, size_t table_partitions = 1, bool enable_nested_reExecution = true, bool enable_inter_block = true);
    void Start() override;
    void Stop() override;
    void NormalMode(Block::Ptr block, vector<T>& batch);
    void InterBlockMode(Block::Ptr block, vector<T> batch);
    void PostExecuteBlock(Block::Ptr block, vector<T> batch);
    void PreExecute(vector<T>& batch, const size_t& block_id);
    void PreExecuteInterBlock(vector<T>& batch, const size_t& block_id);
    void MinWRollBack(vector<T>& batch, Block::Ptr block, vector<Vertex::Ptr>& rbList, vector<vector<int>>& serialOrders);
    void ReExecute(Block::Ptr block, vector<Vertex::Ptr>& rbList, vector<vector<int>>& serialOrders);
    void Finalize(T tx);
    void ClearTable(T tx);
    void notifyRetry();
    void resetRetry();

private:
    Statistics&                     statistics;
    vector<Block::Ptr>              blocks;
    vector<vector<T>>               batches;
    size_t                          num_threads;
    LoomTable                       table;
    std::atomic<size_t>             committed_block;
    bool                            enable_inter_block;
    bool                            enable_nested_reExecution;
    std::shared_ptr<ThreadPool>     pool;
    size_t                          block_idx;
    std::mutex                      mtx;
    std::condition_variable         cv;
    std::atomic<bool>               canRetry;
};

#undef T
#undef K

}
