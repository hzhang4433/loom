#pragma once

#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <loom/utils/Ulock.h>
#include <loom/protocol/common.h>
#include <loom/common/Block.h>
#include <loom/common/Transaction.h>
#include <loom/utils/Statistic/Statistics.h>


using namespace std;

namespace loom {

#define K string
#define V string
#define T SerialTransaction

/// @brief aria tranaction with local read and write set.
struct SerialTransaction: public Transaction {
    size_t      id;
    size_t      batch_id;
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<string, string> local_get;
    std::unordered_map<string, string> local_put;
    SerialTransaction(Transaction&& inner, size_t id, size_t batch_id);
    SerialTransaction(SerialTransaction&& tx) noexcept; // move constructor
    SerialTransaction(const SerialTransaction& other); // copy constructor
};

/// @brief aria table entry for first round execution
struct SerialEntry {
    string value = "";
};

/// @brief aria table for first round execution
struct SerialTable: public Table<K, SerialEntry, KeyHasher> {
    SerialTable(size_t partitions);
};

class Serial: public Protocol {

public:
    Serial(vector<Block::Ptr> blocks, Statistics& statistics, size_t table_partitions = 1);
    void Start() override;
    void Stop()  override;

private:
    vector<Block::Ptr>  blocks;
    SerialTable         table;
    std::thread*        thread{nullptr};
    std::atomic<bool>   stop_flag{false};
    Statistics&         statistics;
};

#undef K
#undef V
#undef T

}