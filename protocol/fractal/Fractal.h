#pragma once

#include <chrono>
#include <barrier>
#include <glog/logging.h>
#include <loom/utils/Ulock.h>
#include <loom/protocol/common.h>
#include <loom/common/Block.h>
#include <loom/common/Transaction.h>
#include <unordered_set>
#include <loom/thread/ThreadPool.h>


using namespace std;

namespace loom {

#define K std::string
#define V FractalEntry
#define T FractalTransaction

/// @brief fractal transaction with local read and write set.
struct FractalTransaction: public Transaction {
    size_t id;
    atomic<bool> flag_conflict{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<K, std::string> local_get;
    std::unordered_map<K, std::string> local_put;
    void Execute() override;
    FractalTransaction(Transaction&& inner, size_t id);
    FractalTransaction(FractalTransaction&& tx) noexcept; // move constructor
    FractalTransaction(const FractalTransaction& other); // copy constructor
};

/// @brief fractal table entry for execution
struct FractalEntry {
    string value;
    T* writer = nullptr;
    unordered_set<T*> readers;
};

/// @brief fractal table for execution
struct FractalTable: private Table<K, FractalEntry, KeyHasher> {
    FractalTable(size_t partitions);
    void Get(T* tx, const K& k, std::string& v);
    void Put(T* tx, const K& k, const std::string& v);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k);
    void ClearPut(T* tx, const K& k);
};

/// @brief fractal protocol master class
class Fractal: public Protocol {

public:
    Fractal(vector<Block::Ptr> blocks, size_t num_threads, size_t table_partitions = 1);
    void Start() override;
    void Stop() override;
    void Execute(T* tx);
    void ReExecute(T* tx);
    void Finalize(T* tx);

private:
    vector<Block::Ptr>                      blocks;
    size_t                                  num_threads;
    FractalTable                            table;
    std::atomic<size_t>                     last_finalized{0};
    std::shared_ptr<ThreadPool>             pool;
};

/// @brief routines to be executed in various execution stages
class FractalExecutor {

public:
    FractalExecutor(Fractal& fractal, size_t worker_id, vector<vector<T>> batchTxs);
    void Run();
    void Execute(T* tx);
    void ReExecute(T* tx);
    void Finalize(T* tx);

private:
    vector<vector<T>>                       batchTxs;
    FractalTable&                           table;
    std::atomic<size_t>&                    last_executed;
    std::atomic<size_t>&                    last_finalized;
    std::atomic<bool>&                      stop_flag;
    std::barrier<std::function<void()>>&    stop_latch;
    std::atomic<bool>&                      has_conflict;
    size_t                                  worker_id;
};

#undef T
#undef K

}
