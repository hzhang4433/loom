#include <loom/protocol/harmony/Harmony.h>
#include <fmt/core.h>
#include <glog/logging.h>
#include <loom/utils/thread/ThreadPool.h>
#include <limits>


#define T loom::HarmonyTransaction
#define K std::string

using namespace std::chrono;

/// @brief initialize harmony protocol
/// @param workload an evm transaction workload
/// @param batch_size batch size
/// @param num_threads the number of threads in thread pool
/// @param table_partitions the number of partitions in table
Harmony::Harmony(
    vector<Block::Ptr> blocks, Statistics& statistics,
    size_t num_threads, bool enable_inter_block, size_t table_partitions
):
    blocks(blocks),
    statistics(statistics),
    barrier(num_threads, []{ LOG(INFO) << "batch complete" << std::endl; }),
    table{table_partitions},
    lock_table{table_partitions},
    enable_inter_block{enable_inter_block},
    num_threads{num_threads}
{
    LOG(INFO) << fmt::format("Harmony(num_threads={}, table_partitions={}, enable_inter_block={})", num_threads, table_partitions, enable_inter_block) << std::endl;
}

/// @brief start harmony protocol
void Harmony::Start() {
    LOG(INFO) << "harmony start";
    
    // split blocks into batches
    vector<vector<vector<T>>> batches;
    for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        auto& txs = block->getTxs();
        // calculate the number of transactions per thread
        // auto tx_per_thread = (txs.size() + num_threads - 1) / num_threads;
        auto tx_per_thread = 1;
        size_t index = 0;
        vector<vector<T>> batch;
        size_t batch_id = i + 1;
        batch.resize(num_threads);
        // get all batch of one block
        for (size_t j = 0; j < txs.size(); j += tx_per_thread) {
            size_t batch_idx = index % num_threads;
            for (size_t k = 0; k < tx_per_thread && j + k < txs.size(); ++k) {
                auto tx = txs[j + k];
                size_t txid = tx->GetTx()->m_hyperId;
                Transaction tx_inner = *tx;
                batch[batch_idx].emplace_back(std::move(tx_inner), txid, batch_id);
            }
            index++;
        }
        // store block batch
        batches.push_back(std::move(batch));
    }

    for (size_t i = 0; i < num_threads; ++i) {
        vector<vector<T>> thread_batches;
        for (size_t j = 0; j < blocks.size(); ++j) {
            thread_batches.push_back(std::move(batches[j][i]));
        }
        workers.push_back(std::thread([this, i, thread_batches]() {
            HarmonyExecutor(*this, i, std::move(thread_batches)).Run();
        }));
        ThreadPool::PinRoundRobin(workers[i], i);
    }
}

/// @brief stop harmony protocol and return statistics
/// @return statistics of current execution
void Harmony::Stop() {
    stop_flag.store(true);
    for (size_t i = 0; i < num_threads; ++i) {
        workers[i].join();
    }
    LOG(INFO) << "harmony stop";
}

/// @brief construct an empty harmony transaction
HarmonyTransaction::HarmonyTransaction(
    Transaction&& inner, size_t id, size_t batch_id
):
    Transaction{std::move(inner)},
    id{id},
    batch_id{batch_id},
    min_out{id + 1},
    max_in{std::numeric_limits<size_t>::min()},
    out_batch_id{batch_id},
    in_batch_id{batch_id}
{
    // initial readSet and writeSet empty
    local_get = std::unordered_map<string, string>();
    local_put = std::unordered_map<string, string>();
}

HarmonyTransaction::HarmonyTransaction(HarmonyTransaction&& tx) noexcept:
    Transaction{std::move(tx)},
    id{tx.id},
    batch_id{tx.batch_id},
    start_time{tx.start_time},
    min_out{tx.min_out},
    max_in{tx.max_in},
    out_batch_id(tx.out_batch_id),
    in_batch_id(tx.in_batch_id),
    local_get{std::move(tx.local_get)},
    local_put{std::move(tx.local_put)}
{}

HarmonyTransaction::HarmonyTransaction(const HarmonyTransaction& other) : 
    Transaction(other), // 调用基类拷贝构造函数
    id(other.id),
    batch_id(other.batch_id),
    flag_conflict(other.flag_conflict),
    committed(other.committed.load()), // 对于 std::atomic，使用 load
    start_time(other.start_time),
    min_out(other.min_out),
    max_in(other.max_in),
    out_batch_id(other.out_batch_id),
    in_batch_id(other.in_batch_id),
    local_get(other.local_get),
    local_put(other.local_put)
{}

/// @brief handle a r-w denpendency
/// @param Ti the transaction write key
/// @param Tj the transaction read key
void HarmonyTable::on_seeing_rw_dependency(T* Ti, T* Tj) {
    DLOG(INFO) << "handle r-w dependency: " << Tj->id << " -> " << Ti->id << std::endl;
    if (Ti->id < Tj->min_out) {
        Tj->min_out = Ti->id;
        Tj->out_batch_id = Ti->batch_id;
    }
    if (Tj->id > Ti->max_in) {
        Ti->max_in = Tj->id;
        Ti->in_batch_id = Tj->batch_id;
    }
}

/// @brief initialize an harmony lock table
/// @param partitions the number of partitions used in parallel hash table
HarmonyLockTable::HarmonyLockTable(size_t partitions): 
    Table::Table(partitions)
{}

/// @brief initialize an harmony executor
/// @param harmony the harmony configuration object
HarmonyExecutor::HarmonyExecutor(Harmony& harmony, size_t worker_id, vector<vector<T>> batchTxs):
    batchTxs(std::move(batchTxs)),
    statistics(harmony.statistics),
    table{harmony.table},
    lock_table{harmony.lock_table},
    enable_inter_block{harmony.enable_inter_block},
    stop_flag{harmony.stop_flag},
    barrier{harmony.barrier},
    counter{harmony.counter},
    num_threads{harmony.num_threads},
    confirm_exit{harmony.confirm_exit},
    worker_id{worker_id},
    batchIdx{0}
{}

/// @brief run transactions
void HarmonyExecutor::Run() {
    if (enable_inter_block) {
        DLOG(INFO) << "worker " << worker_id << " size of batchTxs " << batchTxs.size() << std::endl;
        // Processing Block Streamly
        barrier.arrive_and_wait();
        InterBlockExecute(NextBatch());
    } else {
        // Processing Block One by One
        for (auto& batch : batchTxs) {
            #define LATENCY duration_cast<microseconds>(steady_clock::now() - tx.start_time).count()
            // stage 1: execute
            auto _stop = confirm_exit.load() == num_threads;
            barrier.arrive_and_wait();
            if (_stop) {return;}
            if (stop_flag.load()) { confirm_exit.compare_exchange_weak(worker_id, worker_id + 1);}
            DLOG(INFO) << "worker " << worker_id << " executing" << std::endl;
            for (auto& tx : batch) {
                // record start time
                tx.start_time = steady_clock::now();
                // execute transaction and handle r-w dependency
                this->Execute(&tx);
                statistics.JournalExecute();
                statistics.JournalOverheads(tx.CountOverheads());
            }
            // stage 2: verify + commit
            barrier.arrive_and_wait();
            DLOG(INFO) << "worker " << worker_id << " verifying" << std::endl;
            for (auto& tx : batch) {
                this->Verify(&tx);
                if (tx.flag_conflict) {
                    this->PrepareLockTable(&tx);
                } else {
                    this->Commit(&tx);
                    statistics.JournalCommit(LATENCY);
                }
            }
            // stage 3: fallback
            barrier.arrive_and_wait();
            DLOG(INFO) << "worker " << worker_id << " fallbacking" << std::endl;
            for (auto& tx : batch) {
                if (tx.flag_conflict) {
                    this->Fallback(&tx);
                    statistics.JournalExecute();
                    statistics.JournalCommit(LATENCY);
                    statistics.JournalOverheads(tx.CountOverheads());
                    statistics.JournalRollback(tx.CountOverheads());
                }
            }
            // stage 4: clean up
            barrier.arrive_and_wait();
            DLOG(INFO) << "worker " << worker_id << " cleaning up" << std::endl;
            for (auto& tx : batch) {
                this->CleanLockTable(&tx);
            }
            statistics.JournalBlock();
            #undef LATENCY
        }
    }
}

/// @brief get next batch txs of executor
vector<T> HarmonyExecutor::NextBatch() {
    if (batchIdx >= batchTxs.size()) {
        DLOG(INFO) << "worker " << worker_id << " no more batch" << std::endl;
        return {};
    }
    return batchTxs[batchIdx++];
}

/// @brief execute transactions in inter-block mode
/// @param batch the batch of transactions
void HarmonyExecutor::InterBlockExecute(vector<T> batch) {
    #define LATENCY duration_cast<microseconds>(steady_clock::now() - tx.start_time).count()
    // stage 1: execute
    auto _stop = confirm_exit.load() == num_threads;
    if (_stop) {return;}
    if (stop_flag.load()) { confirm_exit.compare_exchange_weak(worker_id, worker_id + 1);}
    DLOG(INFO) << "worker " << worker_id << " executing batch " << batchIdx << " size " << batch.size() << std::endl;
    for (auto& tx : batch) {
        // execute transaction and handle r-w dependency
        tx.start_time = steady_clock::now();
        this->Execute(&tx);
        statistics.JournalExecute();
        statistics.JournalOverheads(tx.CountOverheads());
    }
    // stage 2: verify + commit
    barrier.arrive_and_wait();
    DLOG(INFO) << "worker " << worker_id << " verifying batch " << batchIdx << std::endl;
    for (auto& tx : batch) {
        this->Verify(&tx);
        if (tx.flag_conflict) {
            this->PrepareLockTable(&tx);
        } else {
            this->Commit(&tx);
            statistics.JournalCommit(LATENCY);
        }
    }
    // stage 3: fallback
    barrier.arrive_and_wait();
    if (worker_id == 0) {
        LOG(INFO) << "committed num " << counter.load() << endl;
    }
    DLOG(INFO) << "worker " << worker_id << " fallbacking batch " << batchIdx << std::endl;
    for (auto& tx : batch) {
        if (tx.flag_conflict) {
            this->Fallback(&tx);
            statistics.JournalExecute();
            statistics.JournalCommit(LATENCY);
            statistics.JournalOverheads(tx.CountOverheads());
            statistics.JournalRollback(tx.CountOverheads());
        }
    }
    // stage 4: streamly execute next block
    auto batch_next = NextBatch();
    if (batch_next.size() > 0) {
        DLOG(INFO) << "worker " << worker_id << " streamly next block" << std::endl;
        InterBlockExecute(batch_next);
    } else {
    // stage 5: clean up
        barrier.arrive_and_wait();
        DLOG(INFO) << "worker " << worker_id << " cleaning up batch " << batchIdx << std::endl;
        for (auto& tx : batch) {
            this->CleanLockTable(&tx);
        }
    }
    statistics.JournalBlock();
    #undef LATENCY
}

/// @brief execute a transaction and journal write operations locally
/// @param tx the transaction
void HarmonyExecutor::Execute(T* tx) {
    // read from the public table
    tx->InstallGetStorageHandler([&](
        const std::unordered_set<string>& readSet
    ) {
        string keys;
        for (auto& key : readSet) {
            keys += key + " ";
            string value;
            table.Put(key, [&](auto& entry){
                value = entry.value;
                // update put_txs' max_in and tx's min_out
                for (T* _tx: entry.reserved_put_txs) {
                    if (_tx == tx) { continue;}
                    table.on_seeing_rw_dependency(_tx, tx);
                }
                entry.reserved_get_txs.push_back(tx);
            });
            tx->local_get[key] = value;
        }
        DLOG(INFO) << "tx " << tx->id << " read: " << keys << std::endl;
    });
    // write locally to local storage
    tx->InstallSetStorageHandler([&](
        const std::unordered_set<string>& writeSet,
        const std::string& value
    ) {
        string keys;
        for (auto& key : writeSet) {
            keys += key + " ";
            tx->local_put[key] = value;
            table.Put(key, [&](auto& entry){
                // update get_txs' min_out and tx's max_in
                for (T* _tx: entry.reserved_get_txs) {
                    if (_tx == tx) { continue;}
                    table.on_seeing_rw_dependency(tx, _tx);
                }
                entry.reserved_put_txs.push_back(tx);
            });
        }
        DLOG(INFO) << "tx " << tx->id << " write: " << keys << std::endl;
    });
    // execute the transaction
    tx->Execute();
}

/// @brief verify transaction by checking min_out and max_in
/// @param tx the transaction, flag_conflict will be altered
void HarmonyExecutor::Verify(T* tx) {
    if (enable_inter_block) {
        // when inter block in enabled, then 
        // if tx.min_out < tx.id and tx.min_out <= tx.max_in, then conflict
        //   if tx.batch_id = tx.in_batch_id, then abort
        // meanwhile, if tx.min_out < tx.id and tx.out_batch_id < tx.batch_id, then abort
        if (tx->min_out < tx->id && tx->min_out <= tx->max_in && tx->batch_id == tx->in_batch_id) {
            tx->flag_conflict = true;
        } else if (tx->min_out < tx->id && tx->out_batch_id < tx->batch_id) {
            tx->flag_conflict = true;
        }
    } else {
        // when inter block in disabled, then
        // if tx.min_out < tx.id and tx.min_out <= tx.max_in, then conflict
        if (tx->min_out < tx->id && tx->min_out <= tx->max_in) {
            tx->flag_conflict = true;
        }
    }
    if (tx->flag_conflict) {
        DLOG(INFO) << "abort " << tx->batch_id << ":" << tx->id << std::endl;
    }
}

/// @brief commit written values into table
/// @param tx the transaction
void HarmonyExecutor::Commit(T* tx) {
    for (auto& tup: tx->local_put) {
        table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.value = std::get<1>(tup);
        });
    }
    counter.fetch_add(1, std::memory_order_relaxed);
    DLOG(INFO) << "tx " << tx->id << " committed" << std::endl;
}

/// @brief put transaction id (local id) into table
/// @param tx the transaction
void HarmonyExecutor::PrepareLockTable(T* tx) {
    for (auto& tup: tx->local_get) {
        lock_table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.deps_get.push_back(tx);
        });
    }
    for (auto& tup: tx->local_put) {
        lock_table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.deps_put.push_back(tx);
        });
    }
}

/// @brief fallback execution without constant
/// @param tx the transaction
void HarmonyExecutor::Fallback(T* tx) {
    // read from the public table
    tx->InstallGetStorageHandler([&](
        const std::unordered_set<string>& readSet
    ) {
        string keys;
        for (auto& key : readSet) {
            keys += key + " ";
            string value;
            table.Get(key, [&](auto& entry){
                value = entry.value;
            });
            tx->local_get[key] = value;
        }
        DLOG(INFO) << "tx " << tx->id << " fallbacking, read: " << keys << std::endl;
    });
    // write directly into the public table
    tx->InstallSetStorageHandler([&](
        const std::unordered_set<string>& writeSet,
        const std::string& value
    ) {
        string keys;
        for (auto& key : writeSet) {
            keys += key + " ";
            table.Put(key, [&](auto& entry){
                entry.value = value;
            });
        }
        DLOG(INFO) << "tx " << tx->id << " fallbacking, write: " << keys << std::endl;
    });

    // get the latest dependency and wait on it
    T* should_wait = nullptr;
    #define COND (_tx->id < tx->id && (should_wait == nullptr || _tx->id > should_wait->id))
    for (auto& tup: tx->local_put) {
        lock_table.Get(std::get<0>(tup), [&](auto& entry) {
            for (auto _tx: entry.deps_get) { if (COND) { should_wait = _tx; } }
            for (auto _tx: entry.deps_put) { if (COND) { should_wait = _tx; } }
        });
    }
    for (auto& tup: tx->local_get) {
        lock_table.Get(std::get<0>(tup), [&](auto& entry) {
            for (auto _tx: entry.deps_put) { if (COND) { should_wait = _tx; } }
        });
    }
    #undef COND
    while(should_wait && !should_wait->committed.load()) {}
    tx->Execute();
    tx->committed.store(true);
    counter.fetch_add(1, std::memory_order_relaxed);
    DLOG(INFO) << "tx " << tx->id << " committed" << std::endl;
}

/// @brief clean up the lock table
/// @param tx the transaction to clean up
void HarmonyExecutor::CleanLockTable(T* tx) {
    for (auto& tup: tx->local_put) {
        lock_table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.deps_put.clear();
        });
    }
    for (auto& tup: tx->local_get) {
        lock_table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.deps_get.clear();
        });
    }
}

#undef T
#undef K