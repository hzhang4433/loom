#include <loom/protocol/aria/Aria.h>
#include <fmt/core.h>
#include <glog/logging.h>
#include <loom/thread/ThreadPool.h>



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
    barrier(num_threads, []{ LOG(INFO) << "batch complete" << std::endl; }),
    table{table_partitions},
    lock_table{table_partitions},
    enable_reordering{enable_reordering},
    num_threads{num_threads}
{
    LOG(INFO) << fmt::format("Aria(num_threads={}, table_partitions={}, enable_reordering={})", num_threads, table_partitions, enable_reordering) << std::endl;
}

/// @brief start aria protocol
void Aria::Start() {
    LOG(INFO) << "aria start";
    
    // split blocks into batches
    vector<vector<vector<T>>> batches;
    for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        auto& txs = block->getTxs();
        // calculate the number of transactions per thread
        auto tx_per_thread = (txs.size() + num_threads - 1) / num_threads;
        // auto tx_per_thread = 15;
        size_t index = 0;
        vector<vector<T>> batch;
        size_t batch_id = i + 1;
        batch.resize(num_threads);
        // get all batch of one block
        for (size_t j = 0; j < txs.size(); j += tx_per_thread) {
            size_t batch_idx = j % num_threads;
            for (size_t k = 0; k < tx_per_thread && j + k < txs.size(); ++k) {
                auto tx = txs[j + k];
                size_t txid = tx->GetTx()->m_hyperId;
                Transaction tx_inner = *tx;
                batch[batch_idx].emplace_back(std::move(tx_inner), txid, batch_id);
            }
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
    LOG(INFO) << "aria stop";
}

/// @brief construct an empty aria transaction
AriaTransaction::AriaTransaction(
    Transaction&& inner, size_t id, size_t batch_id
):
    Transaction{std::move(inner)},
    id{id},
    batch_id{batch_id},
    start_time{std::chrono::steady_clock::now()}
{
    // initial readSet and writeSet empty
    local_get = std::unordered_map<string, string>();
    local_put = std::unordered_map<string, string>();
}

AriaTransaction::AriaTransaction(AriaTransaction&& tx) noexcept:
    Transaction{std::move(tx)},
    id{tx.id},
    batch_id{tx.batch_id},
    start_time{tx.start_time},
    local_get{std::move(tx.local_get)},
    local_put{std::move(tx.local_put)}
{}

AriaTransaction::AriaTransaction(const AriaTransaction& other) : 
    Transaction(other), // 调用基类拷贝构造函数
    id(other.id),
    batch_id(other.batch_id),
    flag_conflict(other.flag_conflict),
    committed(other.committed.load()), // 对于 std::atomic，使用 load
    start_time(other.start_time),
    local_get(other.local_get),
    local_put(other.local_put)
{}

/// @brief reserved a get entry
/// @param tx the transaction
/// @param k the reserved key
void AriaTable::ReserveGet(T* tx, const K& k) {
    Table::Put(k, [&](AriaEntry& entry) {
        DLOG(INFO) << tx->batch_id << ":" <<  tx->id << " reserve get " << k << ", current tx(" << entry.batch_id_get << ":" << entry.reserved_get_tx << ")" << std::endl;
        if (entry.batch_id_get != tx->batch_id || entry.reserved_get_tx == nullptr || entry.reserved_get_tx->id > tx->id) {
            entry.reserved_get_tx = tx;
            entry.batch_id_get = tx->batch_id;
            DLOG(INFO) << tx->batch_id << ":" << tx->id << " reserve get " << k << " ok" << std::endl;
        }
    });
}

/// @brief reserve a put entry
/// @param tx the transaction
/// @param k the reserved key
void AriaTable::ReservePut(T* tx, const K& k) {
    Table::Put(k, [&](AriaEntry& entry) {
        DLOG(INFO) << tx->batch_id << ":" <<  tx->id << " reserve put " << k << ", current tx(" << entry.batch_id_put << ":" << entry.reserved_put_tx << ")" << std::endl;
        if (entry.batch_id_put != tx->batch_id || entry.reserved_put_tx == nullptr || entry.reserved_put_tx->id > tx->id) {
            entry.reserved_put_tx = tx;
            entry.batch_id_put = tx->batch_id;
            DLOG(INFO) << tx->batch_id << ":" << tx->id << " reserve put " << k << " ok" << std::endl;
        }
    });
}

/// @brief compare reserved get transaction
/// @param tx the transaction
/// @param k the key to compare
/// @return if current transaction reserved this entry successfully
bool AriaTable::CompareReservedGet(T* tx, const K& k) {
    bool eq = true;
    Table::Get(k, [&](auto entry) {
        eq = entry.batch_id_get != tx->batch_id || (
            entry.reserved_get_tx == nullptr || 
            entry.reserved_get_tx->id == tx->id
        );
    });
    return eq;
}

/// @brief compare reserved put transaction
/// @param tx the transaction
/// @param k the compared key
/// @return if current transaction reserved this entry successfully
bool AriaTable::CompareReservedPut(T* tx, const K& k) {
    bool eq = true;
    Table::Get(k, [&](auto entry) {
        eq = entry.batch_id_put != tx->batch_id || (
            entry.reserved_put_tx == nullptr || 
            entry.reserved_put_tx->id == tx->id
        );
    });
    return eq;
}

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
    for (auto& batch : batchTxs) {
        #define LATENCY duration_cast<microseconds>(steady_clock::now() - tx.start_time).count()
        // stage 1: execute
        auto _stop = confirm_exit.load() == num_threads;
        barrier.arrive_and_wait();
        if (_stop) {return;}
        if (stop_flag.load()) { confirm_exit.compare_exchange_weak(worker_id, worker_id + 1);}
        has_conflict.store(false);
        DLOG(INFO) << "worker " << worker_id << " executing" << std::endl;
        for (auto& tx : batch) {
            // execute transaction
            this->Execute(&tx);
            // reserve read/write set
            this->Reserve(&tx);
        }
        // stage 2: verify + commit
        barrier.arrive_and_wait();
        DLOG(INFO) << "worker " << worker_id << " verifying" << std::endl;
        for (auto& tx : batch) {
            this->Verify(&tx);
            if (tx.flag_conflict) {
                has_conflict.store(true);
                this->PrepareLockTable(&tx);
            } else {
                this->Commit(&tx);
            }
        }
        // stage 3: fallback
        barrier.arrive_and_wait();
        DLOG(INFO) << "worker " << worker_id << " fallbacking" << std::endl;
        if (!has_conflict.load()) {
            continue;
        }
        for (auto& tx : batch) {
            if (tx.flag_conflict) {
                this->Fallback(&tx);
            }
        }
        // stage 4: clean up
        barrier.arrive_and_wait();
        DLOG(INFO) << "worker " << worker_id << " cleaning up" << std::endl;
        for (auto& tx : batch) {
            this->CleanLockTable(&tx);
        }
        #undef LATENCY
    }
}

/// @brief execute a transaction and journal write operations locally
/// @param tx the transaction
/// @param table the table
void AriaExecutor::Execute(T* tx) {
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
        }
        DLOG(INFO) << "tx " << tx->id << " write: " << keys << std::endl;
    });
    // execute the transaction
    tx->Execute();
}

/// @brief reserve the smallest transaction to table
/// @param tx the transaction to be reserved
/// @param table the table
void AriaExecutor::Reserve(T* tx) {
    // journal all entries to the reservation table
    for (auto& tup: tx->local_get) {
        table.ReserveGet(tx, std::get<0>(tup));
    }
    for (auto& tup: tx->local_put) {
        table.ReservePut(tx, std::get<0>(tup));
    }
    DLOG(INFO) << "tx " << tx->id << " reserved" << std::endl;
}

/// @brief verify transaction by checking dependencies
/// @param tx the transaction, flag_conflict will be altered
void AriaExecutor::Verify(T* tx) {
    // conceptually, we take a snapshot on the database before we execute a batch
    //  , and all transactions are executed viewing the snapshot. 
    // however, we want the global state transitioned 
    // as if we executed some of these transactions sequentially. 
    // therefore, we have to pick some transactions and arange them into a sequence. 
    // this algorithm implicitly does it for us. 
    bool war = false, waw = false, raw = false;
    for (auto& tup : tx->local_get) {
        // the value is updated, snapshot contains out-dated value
        raw |= !table.CompareReservedPut(tx, std::get<0>(tup));
    }
    for (auto& tup : tx->local_put) {
        // the value is read before, therefore we should not update it
        war |= !table.CompareReservedGet(tx, std::get<0>(tup));
    }
    for (auto& tup : tx->local_put) {
        // if some write happened after write
        waw |= !table.CompareReservedPut(tx, std::get<0>(tup));
    }
    if (enable_reordering) {
        tx->flag_conflict = waw || (raw && war);
    } else {
        tx->flag_conflict = waw || raw;
    }
    if (tx->flag_conflict) {
        DLOG(INFO) << "abort " << tx->batch_id << ":" << tx->id << " raw: " << raw << " war: " << war << " waw: " << waw << std::endl;
    }
}

/// @brief commit written values into table
/// @param tx the transaction
void AriaExecutor::Commit(T* tx) {
    for (auto& tup: tx->local_put) {
        table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.value = std::get<1>(tup);
        });
    }
    DLOG(INFO) << "tx " << tx->id << " committed" << std::endl;
}

/// @brief put transaction id (local id) into table
/// @param tx the transaction
void AriaExecutor::PrepareLockTable(T* tx) {
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
void AriaExecutor::Fallback(T* tx) {
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
    DLOG(INFO) << "tx " << tx->id << " committed" << std::endl;
}

/// @brief clean up the lock table
/// @param tx the transaction to clean up
void AriaExecutor::CleanLockTable(T* tx) {
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