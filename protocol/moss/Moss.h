#pragma once

#include <chrono>
#include <glog/logging.h>
#include <loom/utils/Ulock.h>
#include <loom/protocol/common.h>
#include <loom/common/Block.h>
#include <loom/common/Transaction.h>
#include <unordered_set>
#include <loom/thread/ThreadPool.h>
#include <loom/utils/Statistic/Statistics.h>


using namespace std;

namespace loom {

#define K string
#define V MossEntry
#define T shared_ptr<MossTransaction>
#define ST shared_ptr<MossSubTransaction>

struct MossSubTransaction;

/// @brief moss transaction with local read and write set.
struct MossTransaction: public Transaction {
    size_t id;
    SpinLock rerun_txs_mu;
    std::unordered_set<ST> rerun_txs;
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    ST root_tx = nullptr;
    void Execute() override;
    bool HasWAR();
    bool HasWAR(const ST stx);
    void SetWAR(const ST stx);
    MossTransaction(Transaction&& inner, size_t id);
    MossTransaction(MossTransaction&& tx) noexcept; // move constructor
    MossTransaction(const MossTransaction& other); // copy constructor
};

struct MossSubTransaction: public Vertex {
    T ftx;
    vector<ST> children_txs{};
    std::unordered_map<K, std::string> local_get{};
    std::unordered_map<K, std::string> local_put{};
    MossSubTransaction(Vertex&& inner, T ftx);
    void Execute() override;
};

/// @brief moss table entry for execution
struct MossEntry {
    string value;
    ST writer = nullptr;
    unordered_set<ST> readers;
    SpinLock r_mu;
    SpinLock w_mu;
};

/// @brief moss table for execution
struct MossTable: public Table<K, MossEntry, KeyHasher> {
    MossTable(size_t partitions);
    void Get(ST stx, const K& k, std::string& v);
    void Put(ST stx, const K& k, const std::string& v);
    void ClearGet(ST stx, const K& k);
    void ClearPut(ST stx, const K& k);
};

/// @brief moss protocol master class
class Moss: public Protocol {

public:
    Moss(vector<Block::Ptr> blocks, Statistics& statistics, size_t num_threads, size_t table_partitions = 1);
    void Start() override;
    void Stop() override;
    void Preparation(vector<vector<T>>& m_blocks);
    void BuildRoot(ST stx, Vertex::Ptr v, T ftx);
    void Execute(T tx);
    void Execute(ST stx);
    void ReExecute(T tx);
    void Finalize(T tx);
    void ClearTable(ST tx);

private:
    Statistics&                             statistics;
    vector<Block::Ptr>                      blocks;
    size_t                                  num_threads;
    MossTable                               table;
    std::atomic<size_t>                     last_finalized{0};
    std::shared_ptr<ThreadPool>             pool;
};

#undef T
#undef K
#undef V
#undef ST
}
