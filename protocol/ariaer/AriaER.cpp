#include <loom/protocol/ariaer/AriaER.h>
#include <fmt/core.h>
#include <glog/logging.h>
#include <loom/utils/thread/ThreadPool.h>



#define T loom::AriaERTransaction
#define K std::string

using namespace std::chrono;

/// @brief initialize ariaER protocol
/// @param blocks the blocks to be executed
/// @param num_threads the number of threads in thread pool
/// @param enable_reordering the flag of reordering
/// @param table_partitions the number of partitions in table
AriaER::AriaER(
    vector<Block::Ptr> blocks, Statistics& statistics, 
    size_t num_threads, size_t table_partitions, bool enable_reordering
):
    blocks(blocks),
    statistics(statistics),
    barrier(num_threads, []{ LOG(INFO) << "batch complete" << std::endl; }),
    table{table_partitions},
    lock_table{table_partitions},
    enable_reordering{enable_reordering},
    num_threads{num_threads}
{
    LOG(INFO) << fmt::format("AriaER(num_threads={}, table_partitions={}, enable_reordering={})", num_threads, table_partitions, enable_reordering) << std::endl;
}

/// @brief start ariaER protocol
void AriaER::Start() {
    LOG(INFO) << "ariaER start";
    
    // split blocks into batches
    vector<vector<vector<T>>> batches;
    for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        auto& txs = block->getTxs();
        abortList.resize(txs.size() + 1, false);
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
        statistics.JournalBlock();
    }

    for (size_t i = 0; i < num_threads; ++i) {
        vector<vector<T>> thread_batches;
        for (size_t j = 0; j < blocks.size(); ++j) {
            thread_batches.push_back(std::move(batches[j][i]));
        }
        workers.push_back(std::thread([this, i, thread_batches]() {
            AriaERExecutor(*this, i, std::move(thread_batches)).Run();
        }));
        ThreadPool::PinRoundRobin(workers[i], i);
    }
}

/// @brief stop ariaER protocol
void AriaER::Stop() {
    stop_flag.store(true);
    for (size_t i = 0; i < num_threads; ++i) {
        workers[i].join();
    }
    LOG(INFO) << "ariaER stop";
}

/// @brief construct an empty ariaER transaction
AriaERTransaction::AriaERTransaction(
    Transaction&& inner, size_t id, size_t batch_id
):
    Transaction{std::move(inner)},
    id{id},
    batch_id{batch_id}
{
    // initial readSet and writeSet empty
    local_get = std::unordered_map<string, string>();
    local_put = std::unordered_map<string, string>();
}

/// @brief move constructor for AriaERTransaction
AriaERTransaction::AriaERTransaction(
    AriaERTransaction&& tx
) noexcept:
    Transaction{std::move(tx)},
    id{tx.id},
    batch_id{tx.batch_id},
    start_time{tx.start_time},
    local_get{std::move(tx.local_get)},
    local_put{std::move(tx.local_put)}
{}

/// @brief copy constructor for AriaERTransaction
AriaERTransaction::AriaERTransaction(
    const AriaERTransaction& other
): 
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
void AriaERTable::ReserveGet(T* tx, const K& k) {
    Table::Put(k, [&](AriaEREntry& entry) {
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
void AriaERTable::ReservePut(T* tx, const K& k) {
    Table::Put(k, [&](AriaEREntry& entry) {
        DLOG(INFO) << tx->batch_id << ":" <<  tx->id << " reserve put " << k << ", current tx(" << entry.batch_id_put << ":" << entry.reserved_put_tx << ")" << std::endl;
        if (entry.batch_id_put != tx->batch_id || entry.reserved_put_tx == nullptr || entry.reserved_put_tx->id > tx->id) {
            entry.reserved_put_tx = tx;
            entry.batch_id_put = tx->batch_id;
            DLOG(INFO) << tx->batch_id << ":" << tx->id << " reserve put " << k << " ok" << std::endl;
        }
    });
}

/// @brief reserve a put entry after the WAW check
/// @param tx the transaction
/// @param k the reserved key
/// @param abortList the list of transaction ids to abort
void AriaERTable::ReservePutAgain(T* tx, const K& k, const std::vector<bool>& abortList) {
    Table::Put(k, [&](AriaEREntry& entry) {
        DLOG(INFO) << tx->batch_id << ":" <<  tx->id << " reserve put again" << k << ", current tx(" << entry.batch_id_put << ":" << entry.reserved_put_tx << ")" << std::endl;
        
        if (entry.batch_id_put != tx->batch_id || entry.reserved_put_tx == nullptr || entry.reserved_put_tx->id > tx->id || abortList[entry.reserved_put_tx->id] == true) {
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
bool AriaERTable::CompareReservedGet(T* tx, const K& k) {
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
bool AriaERTable::CompareReservedPut(T* tx, const K& k, const std::vector<bool>& abortList) {
    bool eq = true;
    Table::Get(k, [&](auto entry) {
        eq = entry.batch_id_put != tx->batch_id || 
            abortList.at(entry.reserved_put_tx->id) || (
            entry.reserved_put_tx == nullptr || 
            entry.reserved_put_tx->id == tx->id
        );
    });
    return eq;
}

/// @brief initialize an ariaER lock table
/// @param partitions the number of partitions used in parallel hash table
AriaERLockTable::AriaERLockTable(size_t partitions): 
    Table::Table(partitions)
{}

/// @brief initialize an ariaER executor
/// @param ariaER the ariaER configuration object
AriaERExecutor::AriaERExecutor(AriaER& ariaER, size_t worker_id, vector<vector<T>> batchTxs):
    batchTxs(std::move(batchTxs)),
    statistics{ariaER.statistics},
    table{ariaER.table},
    lock_table{ariaER.lock_table},
    enable_reordering{ariaER.enable_reordering},
    stop_flag{ariaER.stop_flag},
    barrier{ariaER.barrier},
    counter{ariaER.counter},
    has_conflict{ariaER.has_conflict},
    abortList(ariaER.abortList),
    num_threads{ariaER.num_threads},
    confirm_exit{ariaER.confirm_exit},
    worker_id{worker_id}
{}

/// @brief run transactions
void AriaERExecutor::Run() {
    for (auto& batch : batchTxs) {
        #define LATENCY duration_cast<microseconds>(steady_clock::now() - tx.start_time).count()
        #define PHASE_TIME duration_cast<microseconds>(steady_clock::now() - begin_time).count()
        // stage 1: execute and write reserve
        auto _stop = confirm_exit.load() == num_threads;
        barrier.arrive_and_wait();
        if (_stop) {return;}
        if (stop_flag.load()) { confirm_exit.compare_exchange_weak(worker_id, worker_id + 1);}
        has_conflict.store(false);
        DLOG(INFO) << "worker " << worker_id << " executing" << std::endl;
        for (auto& tx : batch) {
            // record start time
            tx.start_time = steady_clock::now();
            // execute transaction
            this->Execute(&tx);
            statistics.JournalExecute();
            statistics.JournalOverheads(tx.CountOverheads());
            // reserve write set
            this->WriteReserve(&tx);
        }
        barrier.arrive_and_wait();
        // stage 2: write verify + read reserve
        DLOG(INFO) << "worker " << worker_id << " WAW verifying" << std::endl;
        time_point<steady_clock> begin_time;
        if (worker_id == 0) begin_time = steady_clock::now();
        for (auto& tx : batch) {
            this->VerifyWrite(&tx);
            if (tx.flag_conflict) {
                has_conflict.store(true);
                this->abortList[tx.id] = true;
                this->PrepareLockTable(&tx);
            }
        }
        barrier.arrive_and_wait();
        for (auto& tx : batch) {
            if (!tx.flag_conflict) {
              // reserve read set
              this->ReadReserve(&tx);
            }
        }
        // stage 3: re-verify + commit
        barrier.arrive_and_wait();
        DLOG(INFO) << "worker " << worker_id << " re-verifying" << std::endl;
        for (auto& tx : batch) {
            if (!tx.flag_conflict) { // 防止二次lock
                this->Verify(&tx);
                if (tx.flag_conflict) {
                    has_conflict.store(true);
                    this->PrepareLockTable(&tx);
                } else {
                    this->Commit(&tx);
                    statistics.JournalCommit(LATENCY);
                }
            }
        }
        // stage 3: fallback
        barrier.arrive_and_wait();
        if (worker_id == 0) statistics.JournalRollbackExecution(PHASE_TIME);
        DLOG(INFO) << "worker " << worker_id << " fallbacking" << std::endl;
        if (worker_id == 0) begin_time = steady_clock::now();
        if (!has_conflict.load()) {
            continue;
        }
        for (auto& tx : batch) {
            if (tx.flag_conflict) {
                this->Fallback(&tx);
                statistics.JournalExecute();
                statistics.JournalCommit(LATENCY);
                statistics.JournalRollback(tx.CountOverheads());
            }
        }
        // stage 4: clean up
        barrier.arrive_and_wait();
        if (worker_id == 0) {
            statistics.JournalReExecution(PHASE_TIME);
            // reset all the elements of abortList to false
            std::fill(abortList.begin(), abortList.end(), false);
        }
        DLOG(INFO) << "worker " << worker_id << " cleaning up" << std::endl;
        for (auto& tx : batch) {
            this->CleanLockTable(&tx);
        }
        #undef LATENCY
        #undef PHASE_TIME
    }
}

/// @brief execute a transaction and journal write operations locally
/// @param tx the transaction
/// @param table the table
void AriaERExecutor::Execute(T* tx) {
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

/// @brief reserve the write set of transactions to table
/// @param tx the transaction to be reserved
/// @param table the table
void AriaERExecutor::WriteReserve(T* tx) {
    // journal all write entries to the reservation table
    for (auto& tup: tx->local_put) {
        table.ReservePut(tx, std::get<0>(tup));
    }
    DLOG(INFO) << "tx " << tx->id << " reserved" << std::endl;
}

/// @brief reserve the read set of transactions to table
/// @param tx the transaction to be reserved
/// @param table the table
void AriaERExecutor::ReadReserve(T* tx) {
    // journal all entries to the reservation table
    for (auto& tup: tx->local_get) {
        table.ReserveGet(tx, std::get<0>(tup));
    }
    for (auto& tup: tx->local_put) {
        table.ReservePutAgain(tx, std::get<0>(tup), this->abortList);
    }
    DLOG(INFO) << "tx " << tx->id << " read reserved" << std::endl;
}

/// @brief verify transaction by checking dependencies
/// @param tx the transaction, flag_conflict will be altered
void AriaERExecutor::Verify(T* tx) {
    // conceptually, we take a snapshot on the database before we execute a batch
    //  , and all transactions are executed viewing the snapshot. 
    // however, we want the global state transitioned 
    // as if we executed some of these transactions sequentially. 
    // therefore, we have to pick some transactions and arange them into a sequence. 
    // this algorithm implicitly does it for us. 
    bool war = false, waw = false, raw = false;
    for (auto& tup : tx->local_get) {
        // the value is updated, snapshot contains out-dated value
        raw |= !table.CompareReservedPut(tx, std::get<0>(tup), this->abortList);
    }
    for (auto& tup : tx->local_put) {
        // the value is read before, therefore we should not update it
        war |= !table.CompareReservedGet(tx, std::get<0>(tup));
    }
    for (auto& tup : tx->local_put) {
        // if some write happened after write
        waw |= !table.CompareReservedPut(tx, std::get<0>(tup), this->abortList);
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

/// @brief verify transaction by checking waw dependencies
/// @param tx the transaction, flag_conflict will be altered
void AriaERExecutor::VerifyWrite(T* tx) {
    bool waw = false;
    for (auto& tup : tx->local_put) {
        // if some write happened after write
        waw |= !table.CompareReservedPut(tx, std::get<0>(tup), this->abortList);
    }
    tx->flag_conflict = waw;
    if (tx->flag_conflict) {
        DLOG(INFO) << "abort " << tx->batch_id << ":" << tx->id << " due to waw." << std::endl;
    }
}

/// @brief commit written values into table
/// @param tx the transaction
void AriaERExecutor::Commit(T* tx) {
    for (auto& tup: tx->local_put) {
        table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.value = std::get<1>(tup);
        });
    }
    DLOG(INFO) << "tx " << tx->id << " committed" << std::endl;
}

/// @brief put transaction id (local id) into table
/// @param tx the transaction
void AriaERExecutor::PrepareLockTable(T* tx) {
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
void AriaERExecutor::Fallback(T* tx) {
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
void AriaERExecutor::CleanLockTable(T* tx) {
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