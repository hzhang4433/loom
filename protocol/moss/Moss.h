#pragma once

#include <chrono>
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
#define V MossEntry
#define T MossTransaction

/// @brief moss transaction with local read and write set.
struct MossTransaction: public Transaction {
    size_t id;
    atomic<bool> flag_conflict{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<K, std::string> local_get;
    std::unordered_map<K, std::string> local_put;
    void Execute() override;
    MossTransaction(Transaction&& inner, size_t id);
    MossTransaction(MossTransaction&& tx) noexcept; // move constructor
    MossTransaction(const MossTransaction& other); // copy constructor
};

/// @brief moss table entry for execution
struct MossEntry {
    string value;
    T* writer = nullptr;
    unordered_set<T*> readers;
    SpinLock r_mu;
    SpinLock w_mu;
};

/// @brief moss table for execution
struct MossTable: private Table<K, MossEntry, KeyHasher> {
    MossTable(size_t partitions);
    void Get(T* tx, const K& k, std::string& v);
    void Put(T* tx, const K& k, const std::string& v);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k);
    void ClearPut(T* tx, const K& k);
};

/// @brief moss protocol master class
class Moss: public Protocol {

public:
    Moss(vector<Block::Ptr> blocks, size_t num_threads, size_t table_partitions = 1);
    void Start() override;
    void Stop() override;
    void Execute(T* tx);
    void ReExecute(T* tx);
    void Finalize(T* tx);

private:
    vector<Block::Ptr>                      blocks;
    size_t                                  num_threads;
    MossTable                            table;
    std::atomic<size_t>                     last_finalized{0};
    std::shared_ptr<ThreadPool>             pool;
};

#undef T
#undef K

}
