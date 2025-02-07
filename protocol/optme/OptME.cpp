#include <loom/protocol/optme/utils.hpp>
#include <glog/logging.h>
#include <fmt/core.h>

using namespace std;
using namespace loom;

#define T shared_ptr<OptMETransaction>
#define K string

using namespace std::chrono;

/// @brief initialize optme protocol
/// @param blocks the blocks to be executed
/// @param num_threads the number of threads
/// @param table_partitions the number of partitions for the table
OptME::OptME(
    vector<Block::Ptr>& blocks,
    Statistics& statistics,
    size_t num_threads, 
    size_t table_partitions
):
    blocks(blocks),
    statistics(statistics),
    batches(), // empty
    table(table_partitions),
    num_threads(num_threads),
    pool(make_shared<ThreadPool>(num_threads)),
    block_idx(0),
    committed_block(0)
{
    LOG(INFO) << fmt::format("OptME(num_threads={}, table_partitions={})", num_threads, table_partitions) << endl;
}

/// @brief start optme protocol
void OptME::Start() {
    LOG(INFO) << "OptME started";
    
    // split blocks into batches
    for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        auto& txs = block->getTxs();
        vector<T> batch;
        batch.reserve(txs.size());
        size_t batch_id = i + 1;
        // get all batch of one block
        for (size_t j = 0; j < txs.size(); j++) {
            auto& tx = txs[j];
            size_t txid = tx->GetTx()->m_hyperId;
            batch.emplace_back(make_shared<OptMETransaction>(std::move(*tx), txid, batch_id));
        }
        // store block batch
        acgs.push_back(make_shared<AddressBasedConflictGraph>(pool, batch));
        batches.push_back(std::move(batch));
    }

    Run();
}

/// @brief run optme protocol
void OptME::Run() {
    // execute each batch
    for (int i = 0; i < batches.size(); ++i) {
        auto& batch = batches[i];
        auto& acg = acgs[i];
        vector<vector<T>> schedules;
        vector<T> aborted_txs;
        Simulate(batch);
        // Reorder(batch, aborted_txs);
        Reorder(acg, aborted_txs);
        ParallelExecute(schedules, aborted_txs);
        statistics.JournalBlock();
        LOG(INFO) << "Block " << block_idx << " finalize done";
    }
}

/// @brief stop optme protocol
void OptME::Stop() {
    pool->shutdown();
    LOG(INFO) << "OptME stopped";
}

/// @brief simulate transactions
/// @param batch the transactions to be simulated
/// @param block_id the block id
void OptME::Simulate(vector<T>& batch) {
    LOG(INFO) << "Simulate block " << ++block_idx;
    vector<future<void>> simulateFutures;
    for (T tx : batch) {
        simulateFutures.push_back(pool->enqueue([this, tx] {
            // read locally from local storage
            tx->InstallGetStorageHandler([&](
                const unordered_set<string>& readSet
            ) {
                string keys;
                for (auto& key : readSet) {
                    keys += key + " ";
                    string value = "";
                    table.ReserveGet(tx, key);
                    tx->local_get[key] = value;
                }
                DLOG(INFO) << "tx " << tx->id << " read: " << keys << std::endl;
            });
            // write locally to local storage
            tx->InstallSetStorageHandler([&](
                const unordered_set<string>& writeSet,
                const string& value
            ) {
                string keys;
                for (auto& key : writeSet) {
                    keys += key + " ";
                    table.ReservePut(tx, key);
                    tx->local_put[key] = value;
                }
                DLOG(INFO) << "tx " << tx->id << " write: " << keys << std::endl;
            });
            
            tx->start_time = chrono::steady_clock::now();
            tx->Execute();
            statistics.JournalExecute();
            statistics.JournalOverheads(tx->CountOverheads());
        }));
    }
    // wait for all futures to finish
    for (auto& future : simulateFutures) {
        future.get();
    }
    LOG(INFO) << "Simulate block " << block_idx << " done";
}

/// @brief reorder transactions
void OptME::Reorder(vector<T>& simulation_result, vector<T>& aborted_txs) {
    #define LATENCY duration_cast<microseconds>(steady_clock::now() - tx->start_time).count()
    #define PHASE_TIME duration_cast<microseconds>(steady_clock::now() - begin_time).count()
    LOG(INFO) << "Reorder block " << block_idx;
    auto begin_time = chrono::steady_clock::now();

    // construct acg and rollback
    vector<T> tx_list;

    IntraEpochReordering(simulation_result, aborted_txs, tx_list);
    // concurrent commit
    for (auto tx : tx_list) {
        statistics.JournalCommit(LATENCY);
    }

    statistics.JournalRollbackExecution(PHASE_TIME);
    LOG(INFO) << "Reorder block " << block_idx << " done";
    #undef LATENCY
    #undef PHASE_TIME
}

/// @brief reorder transactions
void OptME::Reorder(shared_ptr<AddressBasedConflictGraph>& acg, vector<T>& aborted_txs) {
    #define LATENCY duration_cast<microseconds>(steady_clock::now() - tx->start_time).count()
    #define PHASE_TIME duration_cast<microseconds>(steady_clock::now() - begin_time).count()
    LOG(INFO) << "Reorder block " << block_idx;
    auto begin_time = chrono::steady_clock::now();

    // construct acg and rollback
    vector<T> tx_list;

    IntraEpochReordering(acg, aborted_txs, tx_list);
    // concurrent commit
    for (auto tx : tx_list) {
        statistics.JournalCommit(LATENCY);
    }

    statistics.JournalRollbackExecution(PHASE_TIME);
    LOG(INFO) << "Reorder block " << block_idx << " done";
    #undef LATENCY
    #undef PHASE_TIME
}

/// @brief intra epoch reordering
void OptME::IntraEpochReordering(vector<T>& simulation_result, vector<T>& aborted_txs, vector<T>& tx_list) {    
    // construct acg and rollback
    AddressBasedConflictGraph acg(pool);
    
    auto begin_time = chrono::steady_clock::now();
    
    // acg.construct(simulation_result);
    acg.parallel_construct(simulation_result);
    auto construct_time = chrono::steady_clock::now();
    LOG(INFO) << "Construct ACG time: " << duration_cast<microseconds>(construct_time - begin_time).count() / 1000.0 << "ms";

    acg.hierarchical_sort();
    auto sort_time = chrono::steady_clock::now();
    LOG(INFO) << "Sort ACG time: " << duration_cast<microseconds>(sort_time - construct_time).count() / 1000.0 << "ms";

    acg.reorder();
    auto reorder_time = chrono::steady_clock::now();
    LOG(INFO) << "Reorder ACG time: " << duration_cast<microseconds>(reorder_time - sort_time).count() / 1000.0 << "ms";

    // extract the aborted transactions and tx_list
    aborted_txs = acg.extract_aborted_txs();
    tx_list = acg.extract_tx_list();
}

void OptME::IntraEpochReordering(shared_ptr<AddressBasedConflictGraph>& acg, vector<T>& aborted_txs, vector<T>& tx_list) {    
    auto begin_time = chrono::steady_clock::now();
    
    acg->hierarchical_sort();
    auto sort_time = chrono::steady_clock::now();
    LOG(INFO) << "Sort time: " << duration_cast<microseconds>(sort_time - begin_time).count() / 1000.0 << "ms";

    acg->reorder();
    auto reorder_time = chrono::steady_clock::now();
    LOG(INFO) << "Reorder time: " << duration_cast<microseconds>(reorder_time - sort_time).count() / 1000.0 << "ms";

    // extract the aborted transactions and tx_list
    aborted_txs = acg->extract_aborted_txs();
    tx_list = acg->extract_tx_list();
}

/// @brief inter epoch reordering
void OptME::InterEpochReordering(vector<vector<T>>& schedules, vector<T>& aborted_txs) {
    // rescedule aborted txs
    vector<unordered_set<K>> epoch_map;
    for (const auto& tx : aborted_txs) {
        int epoch = 0;
        while (epoch < epoch_map.size() && (loom::hasContain(tx->local_get, epoch_map[epoch]) || loom::hasContain(tx->local_put, epoch_map[epoch]))) {
            epoch++;
        }

        if (epoch >= epoch_map.size()) {
            epoch_map.resize(epoch + 1);
            schedules.resize(epoch + 1);
        }

        schedules[epoch].push_back(tx);
        // for (const auto& kv : tx->local_put) {
        //     epoch_map[epoch].insert(kv.first);
        // }
        std::transform(tx->local_put.begin(), tx->local_put.end(), std::inserter(epoch_map[epoch], epoch_map[epoch].end()), [](const auto& kv) { return kv.first; });
    }
}

/// @brief parallel execute transactions
/// @param schedule the schedule for transactions
void OptME::ParallelExecute(vector<vector<T>>& schedules, vector<T>& aborted_txs) {
    #define LATENCY duration_cast<microseconds>(steady_clock::now() - tx->start_time).count()
    #define PHASE_TIME duration_cast<microseconds>(steady_clock::now() - begin_time).count()
    LOG(INFO) << "ReExecute block " << block_idx;
    auto begin_time = chrono::steady_clock::now();
    
    // rescedule txs
    InterEpochReordering(schedules, aborted_txs);
    
    // concurrent re-execute
    for (auto schedule : schedules) {
        std::vector<std::future<void>> reexecuteFutures;
        for (auto tx : schedule) {
            reexecuteFutures.emplace_back(pool->enqueue([this, tx] {
                ReExecute(tx);
                Finalize(tx);
            }));
            statistics.JournalExecute();
            statistics.JournalCommit(LATENCY);
            statistics.JournalRollback(tx->CountOverheads());
        }
        for (auto& future : reexecuteFutures) {
            future.get();
        }
    }

    statistics.JournalReExecution(PHASE_TIME);
    LOG(INFO) << "ReExecute block " << block_idx << " done";
    #undef LATENCY
    #undef PHASE_TIME
}

/// @brief finalize a transaction
/// @param tx the transaction to be finalized
void OptME::Finalize(T tx) {
    DLOG(INFO) << "Finalize tx " << tx->id;
    tx->committed.store(true);
    // release reserved put
    for (auto& kv : tx->local_put) {
        table.Put(std::get<0>(kv), [&](OptMEEntry& entry) {
            if (entry.block_id_put == tx->block_id) {
                // update reserved put number
                entry.reserved_put_num--;
                if (entry.reserved_put_num == 0) {
                    // update block id put
                    if (entry.next_reserved_put > 0) {
                        entry.block_id_put++;
                        entry.reserved_put_num = entry.next_reserved_put;
                        entry.next_reserved_put = 0;
                    } else {
                        entry.block_id_put = 0;
                    }
                } else if (entry.reserved_put_num < 0) {
                    LOG(ERROR) << kv.first << " reserved put num = " << entry.reserved_put_num;
                }
            }
        });
    }
}

/// @brief re-execute a transaction
/// @param tx the transaction to be re-executed
void OptME::ReExecute(T tx) {
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

    tx->Execute();
}

/// @brief construct an empty OptMETransaction
OptMETransaction::OptMETransaction(
    Transaction&& inner, size_t id, size_t block_id
): 
    Transaction(std::move(inner)), 
    id(id),
    block_id(block_id),
    sequence(0)
{
    // initial readSet and writeSet empty
    local_get = unordered_map<string, string>();
    local_put = unordered_map<string, string>();
}

/// @brief move constructor for OptMETransaction
OptMETransaction::OptMETransaction(
    OptMETransaction&& tx
) noexcept: 
    Transaction(std::move(tx)), 
    id(tx.id),
    block_id(tx.block_id),
    committed(tx.committed.load()),
    aborted(tx.aborted.load()),
    local_get{std::move(tx.local_get)},
    local_put{std::move(tx.local_put)}
{}

/// @brief copy constructor for OptMETransaction
OptMETransaction::OptMETransaction(
    const OptMETransaction& other
): 
    Transaction(other),
    id(other.id),
    block_id(other.block_id),
    committed(other.committed.load()),
    aborted(other.aborted.load()),
    start_time(other.start_time),
    local_get(other.local_get),
    local_put(other.local_put)
{}

/// @brief execute the transaction
void OptMETransaction::Execute() {
    DLOG(INFO) << "Execute transaction: " << m_tx << " txid: " << m_tx->m_hyperId << std::endl;
    if (getHandler) {
        getHandler(m_tx->m_rootVertex->allReadSet);
    }
    if (setHandler) {
        setHandler(m_tx->m_rootVertex->allWriteSet, "value");
    }
    auto& tx = m_tx->m_rootVertex->m_cost;
    DLOG(INFO) << "tx " << id << ": " << tx << std::endl;
    loom::Exec(tx);
}

/// @brief initialize the optme table
/// @param partitions the number of partitions
OptMETable::OptMETable(
    size_t partitions
): 
    Table::Table(partitions)
{}

/// @brief reserve a get operation
/// @param tx the transaction
/// @param k the key
void OptMETable::ReserveGet(T tx, const K& k) {
    Table::Put(k, [&](OptMEEntry& entry) {
        DLOG(INFO) << tx->block_id << ":" <<  tx->id << " reserve get " << k << ", current tx(" << entry.block_id_get << ")" << endl;
        // don't have war conflict
        if (entry.block_id_put == 0 || entry.block_id_put == tx->block_id) {
            entry.block_id_get = max(entry.block_id_get, tx->block_id);
            DLOG(INFO) << tx->block_id << ":" << tx->id << " reserve get " << k << " ok" << std::endl;
        } else {
            tx->aborted.store(true);
            DLOG(INFO) << tx->block_id << ":" << tx->id << " reserve get " << k << " failed: put id = " << entry.block_id_put << std::endl;
        }
    });
}

/// @brief reserve a put operation
/// @param tx the transaction
/// @param k the key
void OptMETable::ReservePut(T tx, const K& k) {
    Table::Put(k, [&](OptMEEntry& entry) {
        DLOG(INFO) << tx->block_id << ":" <<  tx->id << " reserve put " << k << ", current tx(" << entry.block_id_get << ")" << endl;
        if (entry.block_id_put == 0) {
            // reserve put
            entry.block_id_put = tx->block_id;
            // flag have a bigger block_id put
            entry.reserved_put_num = 1;
        } else if (entry.block_id_put == tx->block_id) {
            // store reserved put
            entry.reserved_put_num++;
        } else if (entry.block_id_put < tx->block_id) {
            // restore next reserved put
            entry.next_reserved_put++;
        }
        DLOG(INFO) << tx->block_id << ":" << tx->id << " reserve put " << k << " ok" << std::endl;
    });
}

// /// @brief construct an empty OptMETransaction
// /// @param pool the thread pool
// AddressBasedConflictGraph::AddressBasedConflictGraph(
//     std::shared_ptr<ThreadPool>& pool
// ): 
//     pool(pool){}

// /// @brief add units to address
// /// @param write_units the write units
// void AddressBasedConflictGraph::add_units_to_address(T tx) {
//     // add read units
//     for (auto& tup: tx->local_get) {
//         auto key = std::get<0>(tup);
//         if (addresses.find(key) == addresses.end()) {
//             addresses[key] = make_shared<Address>();
//         }
//         addresses[key]->add_read_unit(tx);
//     }
//     // add write units
//     for (auto& tup: tx->local_put) {
//         auto key = std::get<0>(tup);
//         if (addresses.find(key) == addresses.end()) {
//             addresses[key] = make_shared<Address>();
//         }
//         addresses[key]->add_write_unit(tx);
//     }
// }

// /// @brief construct an AddressBasedConflictGraph
// /// @param simulation_result the simulation result
// void AddressBasedConflictGraph::construct(vector<T>& simulation_result) {
//     for (auto& tx : simulation_result) {
//         // first judge waw conflict
//         if (check_updater_conflict(tx->local_put)) {
//             tx->aborted.store(true);
//             aborted_txs.push_back(tx);
//             continue;
//         }

//         tx_list.push_back(tx);
//         add_units_to_address(tx);
//     }
// }

// /// @brief parallel construct an AddressBasedConflictGraph
// /// @param simulation_result the simulation result
// void AddressBasedConflictGraph::parallel_construct(vector<T>& simulation_result) {
//     vector<future<void>> futures;
//     for (auto& tx : simulation_result) {
//         futures.push_back(pool->enqueue([this, tx] {
//             auto tx_ptr = make_shared<Transaction>(tx);
//             if (check_updater_conflict(tx_ptr)) {
//                 tx_ptr->abort();
//                 aborted_txs.push_back(tx_ptr);
//                 return;
//             }
//             tx_list[tx.id] = tx_ptr;
//         }));
//     }
//     for (auto& future : futures) {
//         future.get();
//     }
// }

// /// @brief check if there is an updater conflict
// /// @param tx the transaction
// bool AddressBasedConflictGraph::check_updater_conflict(unordered_map<string, string>& write_units) {
//     for (auto& tup: write_units) {
//         auto key = std::get<0>(tup);
//         if (addresses.find(key) != addresses.end() && !addresses.at(key)->write_units.empty()) {
//             return true;
//         }
//     }
//     return false;
// }

// /// @brief extract the aborted transactions
// /// @return the list of aborted transactions
// vector<T> AddressBasedConflictGraph::extract_abortList() {
//     return aborted_txs;
// }

#undef T
#undef K