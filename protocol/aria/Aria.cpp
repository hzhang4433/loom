#include <Loom/protocol/aria/Aria.h>
#include <fmt/core.h>
#include <glog/logging.h>
#include <Loom/thread/ThreadPool.h>



#define T loom::AriaTransaction
#define K std::string

/// @brief initialize aria protocol
/// @param workload an evm transaction workload
/// @param batch_size batch size
/// @param num_threads the number of threads in thread pool
/// @param table_partitions the number of partitions in table
Aria::Aria(
    vector<Block::Ptr> blocks, size_t num_threads, 
    bool enable_reordering, size_t table_partitions
):
    blocks(blocks),
    barrier(num_threads, []{ DLOG(INFO) << "batch complete" << std::endl; }),
    table{table_partitions},
    lock_table{table_partitions},
    enable_reordering{enable_reordering},
    num_threads{num_threads}
{
    LOG(INFO) << fmt::format("Aria(num_threads={}, table_partitions={}, enable_reordering={})", num_threads, table_partitions, enable_reordering) << std::endl;
}

/// @brief start aria protocol
void Aria::Start() {
    DLOG(INFO) << "aria start";
    
    // split blocks into batches
    vector<vector<vector<T>>> batches;
    for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        auto& txs = block->getTxs();
        // calculate the number of transactions per thread
        auto tx_per_thread = (txs.size() + num_threads - 1) / num_threads;
        size_t index = 0;
        vector<vector<T>> batch;
        batch.reserve(num_threads);
        // get all batch of one block
        for (size_t j = 0; j < num_threads; j++) {
            size_t end = std::min(index + tx_per_thread, txs.size());
            size_t batch_id = i + 1;
            vector<T> subBatch;
            // get one thread batch
            for (; index < end; ++index) {
                auto tx = txs[index];
                Transaction tx_inner = *tx;
                subBatch.emplace_back(std::move(tx_inner), batch_id);
            }
            // store thread batch
            // batch.push_back(subBatch);
            batch.push_back(std::move(subBatch));
        }
        // store block batch
        // batches.push_back(batch);
        batches.push_back(std::move(batch));
    }

    for (size_t i = 0; i < num_threads; ++i) {
        vector<vector<T>> thread_batches;
        for (size_t j = 0; j < blocks.size(); ++j) {
            // thread_batches.push_back(batches[j][i]);
            thread_batches.push_back(std::move(batches[j][i]));
        }
        workers.push_back(std::thread([this, i, thread_batches]() {
            AriaExecutor(*this, i, std::move(thread_batches)).Run();
        }));
        ThreadPool::PinRoundRobin(workers[i], i);
    }
}

/// @brief stop aria protocol and return statistics
/// @return statistics of current execution
void Aria::Stop() {
    stop_flag.store(true);
    for (size_t i = 0; i < num_threads; ++i) {
        workers[i].join();
    }
    DLOG(INFO) << "aria stop";
}

/// @brief construct an empty aria transaction
AriaTransaction::AriaTransaction(
    Transaction&& inner, size_t batch_id
):
    Transaction{std::move(inner)},
    batch_id{batch_id},
    start_time{std::chrono::steady_clock::now()}
{
    // initial readSet and writeSet empty
    readSet = std::unordered_set<string>();
    writeSet = std::unordered_set<string>();
}

AriaTransaction::AriaTransaction(AriaTransaction&& tx) noexcept:
    Transaction{std::move(tx)},
    batch_id{tx.batch_id},
    start_time{tx.start_time},
    readSet{std::move(tx.readSet)},
    writeSet{std::move(tx.writeSet)}
{}

AriaTransaction::AriaTransaction(const AriaTransaction& other) : 
    Transaction(other), // 调用基类拷贝构造函数
    batch_id(other.batch_id),
    flag_conflict(other.flag_conflict),
    committed(other.committed.load()), // 对于 std::atomic，使用 load
    start_time(other.start_time),
    readSet(other.readSet),
    writeSet(other.writeSet)
{}

/// @brief initialize an aria lock table
/// @param partitions the number of partitions used in parallel hash table
AriaLockTable::AriaLockTable(size_t partitions): 
    Table::Table(partitions)
{}

/// @brief initialize an aria executor
/// @param aria the aria configuration object
AriaExecutor::AriaExecutor(Aria& aria, size_t worker_id, vector<vector<T>> batchTxs):
    batchTxs(std::move(batchTxs)),
    table{aria.table},
    lock_table{aria.lock_table},
    enable_reordering{aria.enable_reordering},
    stop_flag{aria.stop_flag},
    barrier{aria.barrier},
    counter{aria.counter},
    has_conflict{aria.has_conflict},
    num_threads{aria.num_threads},
    confirm_exit{aria.confirm_exit},
    worker_id{worker_id}
{}

/// @brief run transactions
void AriaExecutor::Run() {
    while (true) {
        // stage 1: execute

    }
}

/// @brief reserve the smallest transaction to table
/// @param tx the transaction to be reserved
/// @param table the table
void AriaExecutor::Reserve(T* tx) {

}

/// @brief verify transaction by checking dependencies
/// @param tx the transaction, flag_conflict will be altered
void AriaExecutor::Verify(T* tx) {
    
}

/// @brief commit written values into table
/// @param tx the transaction
void AriaExecutor::Commit(T* tx) {

}

/// @brief put transaction id (local id) into table
/// @param tx the transaction
void AriaExecutor::PrepareLockTable(T* tx) {

}

/// @brief fallback execution without constant
/// @param tx the transaction
void AriaExecutor::Fallback(T* tx) {

}

/// @brief clean up the lock table
/// @param tx the transaction to clean up
void AriaExecutor::CleanLockTable(T* tx) {

}

#undef T
#undef K