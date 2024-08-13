#include <loom/protocol/fractal/Fractal.h>
#include <fmt/core.h>
#include <glog/logging.h>


#define K std::string
#define T FractalTransaction


/// @brief initialize aria protocol
/// @param blocks the blocks to be executed
/// @param num_threads the number of threads
/// @param table_partitions the number of partitions for the table
Fractal::Fractal(
    vector<Block::Ptr> blocks, 
    size_t num_threads, 
    size_t table_partitions
): 
    blocks(std::move(blocks)), 
    num_threads(num_threads),
    table(table_partitions),
    pool(std::make_shared<ThreadPool>(num_threads))
{
    LOG(INFO) << fmt::format("Fractal(num_threads={}, table_partitions={})", num_threads, table_partitions) << std::endl;
}

/// @brief start the protocol
void Fractal::Start() {
    // transform Transaction to FractalTransaction
    vector<vector<T>> m_blocks;
    for (size_t i = 0; i < blocks.size(); i++) {
        auto txs = blocks[i]->getTxs();
        vector<T> m_txs;
        for (size_t j = 0; j < txs.size(); j++) {
            auto tx = txs[j];
            auto txid = tx->GetTx()->m_hyperId;
            m_txs.emplace_back(std::move(*tx), txid);
        }
        m_blocks.push_back(m_txs);
    }

    // execute all transactions in the blocks
    LOG(INFO) << "start" << std::endl;
    for (size_t i = 0; i < m_blocks.size(); i++) {
        auto m_txs = m_blocks[i];
        // execute all transactions in the block
        std::vector<std::future<void>> futures;
        for (size_t j = 0; j < m_txs.size(); j++) {
            auto tx = m_txs[j];
            futures.push_back(pool->enqueue([this, &tx] {
                DLOG(INFO) << "enqueue tx " << tx.id << std::endl;
                tx.Execute();
            }));
        }
        // wait for all transactions in the block to complete
        for (auto& future : futures) {
            future.get();
        }
    }
}

/// @brief stop the protocol
void Fractal::Stop() {
    pool->shutdown();
    LOG(INFO) << "aria stop";
}

/// @brief construct an empty FractalTransaction
FractalTransaction::FractalTransaction(
    Transaction&& inner, size_t id
): 
    Transaction(std::move(inner)), 
    id(id),
    start_time(std::chrono::steady_clock::now())
{
    // initial readSet and writeSet empty
    local_get = std::unordered_map<string, string>();
    local_put = std::unordered_map<string, string>();
}

/// @brief move constructor for FractalTransaction
FractalTransaction::FractalTransaction(
    FractalTransaction&& tx
) noexcept: 
    Transaction(std::move(tx)), 
    id(tx.id),
    flag_conflict(tx.flag_conflict),
    start_time(tx.start_time),
    local_get{std::move(tx.local_get)},
    local_put{std::move(tx.local_put)}
{}

/// @brief copy constructor for FractalTransaction
FractalTransaction::FractalTransaction(
    const FractalTransaction& other
): 
    Transaction(other), 
    id(other.id),
    flag_conflict(other.flag_conflict),
    start_time(other.start_time),
    local_get(other.local_get),
    local_put(other.local_put)
{}

/// @brief initialize the fractal table
/// @param partitions the number of partitions
FractalTable::FractalTable(
    size_t partitions
): 
    Table::Table(partitions)
{}

