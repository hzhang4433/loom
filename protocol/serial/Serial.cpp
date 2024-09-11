#include <loom/protocol/serial/Serial.h>
#include <fmt/core.h>
#include <glog/logging.h>

#define K string
#define V string
#define T SerialTransaction

/// @brief initialize serial protocol
/// @param blocks blocks to execute
/// @param statistics statistics to record
/// @param table_partitions number of partitions for table
Serial::Serial(
    vector<Block::Ptr> blocks, Statistics& statistics, 
    size_t table_partitions, size_t repeat
): 
    blocks(blocks), 
    statistics(statistics), 
    table(table_partitions),
    repeat(repeat)
{
    LOG(INFO) << fmt::format("Serial(table_partitions={})", table_partitions) << std::endl;
}

/// @brief start serial protocol
void Serial::Start() {
    LOG(INFO) << "Serial Start";
    vector<T> workloads;
    for (auto& block: blocks) {
        size_t block_id = block->getBlockId();
        for (auto& tx: block->getTxs()) {
            size_t txid = tx->GetTx()->m_hyperId;
            workloads.emplace_back(std::move(*tx), txid, block_id);
        }
    }

    thread = new std::thread([this, txs = std::move(workloads)]() {
        size_t idx = 0;
        size_t block_id = 0;
        LOG(INFO) << "Execution Start";
        while (!stop_flag.load() && idx < txs.size()) {
            auto tx = std::move(txs[idx]);
            if (tx.block_id != block_id) {
                block_id = tx.block_id;
                statistics.JournalBlock();
            }
            auto start_time = std::chrono::steady_clock::now();
            tx.InstallGetStorageHandler([&](
                const std::unordered_set<string>& readSet
            ) {
                string keys;
                for (auto& key: readSet) {
                    keys += key + " ";
                    string value;
                    table.Put(key, [&](auto& entry) {
                        value = entry.value;
                    });
                    tx.local_get[key] = value;
                }
                DLOG(INFO) << "tx " << tx.id << " read: " << keys;
            });
            tx.InstallSetStorageHandler([&](
                const std::unordered_set<string>& writeSet, 
                const string& value
            ) {
                string keys;
                for (auto& key: writeSet) {
                    keys += key + " ";
                    tx.local_put[key] = value;
                }
                DLOG(INFO) << "tx " << tx.id << " write: " << keys;
            });
            // execute transaction
            tx.Execute();
            // record statistics
            statistics.JournalExecute();
            statistics.JournalCommit(chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start_time).count());
            statistics.JournalOverheads(tx.m_tx->m_rootVertex->m_cost);
            idx++;
        }
        LOG(INFO) << "Execution Finish";
    });
}

/// @brief stop serial protocol
void Serial::Stop() {
    stop_flag.store(true);
    thread->join();
    delete thread;
    thread = nullptr;
    LOG(INFO) << "Serial Stop";
}

/// @brief construct an empty serial transaction
SerialTransaction::SerialTransaction(
    Transaction&& inner, size_t id, size_t block_id
):
    Transaction{std::move(inner)},
    id{id},
    block_id{block_id}
{
    // initial readSet and writeSet empty
    local_get = std::unordered_map<string, string>();
    local_put = std::unordered_map<string, string>();
}

/// @brief move constructor
SerialTransaction::SerialTransaction(
    SerialTransaction&& tx
) noexcept:
    Transaction{std::move(tx)},
    id{tx.id},
    block_id{tx.block_id},
    start_time{tx.start_time},
    local_get{std::move(tx.local_get)},
    local_put{std::move(tx.local_put)}
{}

/// @brief copy constructor
SerialTransaction::SerialTransaction(
    const SerialTransaction& other
):
    Transaction{other},
    id{other.id},
    block_id{other.block_id},
    start_time{other.start_time},
    local_get{other.local_get},
    local_put{other.local_put}
{}

/// @brief initialize serial table
/// @param partitions number of partitions
SerialTable::SerialTable(size_t partitions):
    Table::Table(partitions)
{}

#undef K
#undef V
#undef T