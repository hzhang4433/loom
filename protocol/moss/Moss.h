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
#define T shared_ptr<MossTransaction>
#define ST shared_ptr<MossSubTransaction>

struct MossSubTransaction;

/// @brief moss transaction with local read and write set.
struct MossTransaction: public Transaction {
    size_t id;
    SpinLock rerun_txs_mu;
    std::vector<ST> rerun_txs;
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<K, std::string> local_get{};
    std::unordered_map<K, std::string> local_put{};
    ST root_tx = nullptr;
    void Execute() override;
    bool HasWAR();
    bool HasWAR(const ST stx);
    void SetWAR(const ST stx);
    void SetWAR(const T tx);
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
    std::size_t tx_id_put;
    std::string detail_id_put;
    ST writer = nullptr;
    unordered_set<ST> readers;
    SpinLock r_mu;
    SpinLock w_mu;
};

/// @brief moss table for execution
struct MossTable: private Table<K, MossEntry, KeyHasher> {
    MossTable(size_t partitions);
    void Get(ST stx, const K& k, std::string& v);
    void Put(ST stx, const K& k, const std::string& v);
    void RegretGet(T tx, const K& k, size_t version);
    void RegretPut(T tx, const K& k);
    void ClearGet(T tx, const K& k);
    void ClearPut(T tx, const K& k);
};

/// @brief moss protocol master class
class Moss: public Protocol {

public:
    Moss(vector<Block::Ptr> blocks, size_t num_threads, size_t table_partitions = 1);
    void Start() override;
    void Stop() override;
    void Preparation(vector<vector<T>>& m_blocks);
    void BuildRoot(ST stx, Vertex::Ptr v, T ftx);
    void Execute(T tx);
    void Execute(T tx, ST stx);
    void ReExecute(T tx);
    void Finalize(T tx);

private:
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
