#include <loom/protocol/loom/Loom.h>
#include <loom/protocol/loom/MinWRollback.h>
#include <loom/protocol/loom/DeterReExecute.h>
#include <glog/logging.h>
#include <fmt/core.h>

using namespace std;
using namespace loom;

#define T shared_ptr<LoomTransaction>
#define K string

using namespace std::chrono;

/// @brief initialize loom protocol
/// @param blocks the blocks to be executed
/// @param num_threads the number of threads
/// @param table_partitions the number of partitions for the table
Loom::Loom(
    vector<Block::Ptr> blocks,
    Statistics& statistics,
    size_t num_threads, 
    bool enable_inter_block,
    bool enable_nested_reExecution,
    size_t table_partitions
):
    blocks(std::move(blocks)),
    statistics(statistics),
    batches(), // empty
    enable_inter_block(enable_inter_block),
    enable_nested_reExecution(enable_nested_reExecution),
    table(table_partitions),
    num_threads(num_threads),
    pool(make_shared<ThreadPool>(num_threads)),
    block_idx(0),
    committed_block(0),
    canRetry(false)
{
    LOG(INFO) << fmt::format("Loom(num_threads={}, table_partitions={}, enable_inter_block={}, enable_nested_reExecution={})", num_threads, table_partitions, enable_inter_block, enable_nested_reExecution) << endl;
}

/// @brief start loom protocol
void Loom::Start() {
    LOG(INFO) << "Loom started";
    
    // split blocks into batches
    for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        auto& txs = block->getTxs();
        vector<T> batch;
        size_t batch_id = i + 1;
        // get all batch of one block
        for (size_t j = 0; j < txs.size(); j++) {
            auto tx = txs[j];
            size_t txid = tx->GetTx()->m_hyperId;
            batch.emplace_back(make_shared<LoomTransaction>(std::move(*tx), txid, batch_id));
        }
        // store block batch
        batches.push_back(std::move(batch));
    }

    // execute each batch
    if (enable_inter_block) {
        InterBlockMode(blocks[block_idx], std::move(batches[block_idx]));
    } else {
        for (auto& block : blocks) {
            NormalMode(block, batches[block_idx++]);
        }
    }

    // // wait for all blocks to be committed
    // while (committed_block.load() < blocks.size()) {
    //     this_thread::sleep_for(chrono::milliseconds(100));
    // }
}

/// @brief stop loom protocol
void Loom::Stop() {
    pool->shutdown();
    LOG(INFO) << "Loom stopped";
}

/// @brief execute transactions in normal mode
/// @param block the block to be executed
/// @param batch the transactions to be executed
void Loom::NormalMode(Block::Ptr block, vector<T>& batch) {
    #define LATENCY duration_cast<microseconds>(tx->GetTx()->m_commit_time - tx->start_time).count()
    #define COMMIT_TIME tx->GetTx()->m_commit_time

    vector<Vertex::Ptr> rbList;
    vector<vector<int>> serialOrders;
    vector<future<void>> finalFutures;
    // pre-execute
    PreExecute(batch, block->getBlockId());

    // minW-rollback
    MinWRollBack(batch, block, rbList, serialOrders);

    // re-execute
    ReExecute(block, rbList, serialOrders);

    // finalize
    for (auto tx : batch) {
        if (!tx->committed.load()) {
            statistics.JournalCommit(LATENCY, COMMIT_TIME);
            finalFutures.push_back(pool->enqueue([this, tx] {
                ClearTable(tx);
            }));
        }
    }
    for (auto& future : finalFutures) {
        future.get();
    }

    // mark block as committed
    committed_block.fetch_add(1, memory_order_relaxed);
    statistics.JournalBlock();
    LOG(INFO) << "Block " << block->getBlockId() << " finalize done";
    #undef LATENCY
    #undef COMMIT_TIME
}

/// @brief execute transactions in inter-block mode
/// @param block the block to be executed
/// @param batch the transactions to be executed
void Loom::InterBlockMode(Block::Ptr block, vector<T> batch) {
    DLOG(INFO) << "InterBlockMode block " << block->getBlockId();
    // pre-execute
    PreExecuteInterBlock(batch, block->getBlockId());

    // Start post-execution in a separate thread
    thread postExecThread(&Loom::PostExecuteBlock, this, block, std::move(batch));
    // Allow post-execution to run independently
    postExecThread.detach();
    
    // concurrent next block
    if (++block_idx < blocks.size()) {
        InterBlockMode(blocks[block_idx], std::move(batches[block_idx]));
    }
}

/// @brief run rest phrase of loom protocol
/// @param block the block to be executed
/// @param batch the transactions to be executed
void Loom::PostExecuteBlock(Block::Ptr block, vector<T> batch) {
    #define LATENCY duration_cast<microseconds>(tx->GetTx()->m_commit_time - tx->start_time).count()
    #define COMMIT_TIME tx->GetTx()->m_commit_time

    DLOG(INFO) << "PostExecute block " << block->getBlockId();
    vector<future<void>> finalFutures;
    vector<Vertex::Ptr> rbList;
    vector<vector<int>> serialOrders;

    // minW-rollback
    MinWRollBack(batch, block, rbList, serialOrders);
    // re-execute
    ReExecute(block, rbList, serialOrders);
    // finalize
    for (auto tx : batch) {
        if (!tx->committed.load()) {
            statistics.JournalCommit(LATENCY, COMMIT_TIME);
            finalFutures.push_back(pool->enqueue([this, tx] {
                ClearTable(tx);
            }));
        }
    }
    for (auto& future : finalFutures) {
        future.get();
    }
    // notify retry
    notifyRetry();
    // mark block as committed
    committed_block.fetch_add(1, memory_order_relaxed);
    statistics.JournalBlock();
    LOG(INFO) << "InterBlockMode block " << block->getBlockId() << " finalize done";
    #undef LATENCY
    #undef COMMIT_TIME
}

/// @brief pre-execute transactions
/// @param batch the transactions to be executed
/// @param block_id the block id
void Loom::PreExecute(vector<T>& batch, const size_t& block_id) {
    LOG(INFO) << "PreExecute block " << block_id;
    vector<future<void>> preExecFutures;
    for (T tx : batch) {
        preExecFutures.push_back(pool->enqueue([this, tx] {
            // read locally from local storage
            tx->InstallGetStorageHandler([&](
                const unordered_set<string>& readSet
            ) {
                string keys;
                for (auto& key : readSet) {
                    keys += key + " ";
                    string value = "";
                    table.ReserveGet(tx, key);
                    if (tx->aborted.load()) return;
                    tx->local_get[key] = value;
                }
            });
            // write locally to local storage
            tx->InstallSetStorageHandler([&](
                const unordered_set<string>& writeSet,
                const string& value
            ) {
                if (tx->aborted.load()) return;
                string keys;
                for (auto& key : writeSet) {
                    keys += key + " ";
                    table.ReservePut(tx, key);
                    tx->local_put[key] = value;
                }
            });
            tx->start_time = chrono::steady_clock::now();
            tx->Execute();
            statistics.JournalExecute();
            statistics.JournalOverheads(tx->CountOverheads());
        }));
    }
    // wait for all futures to finish
    for (auto& future : preExecFutures) {
        future.get();
    }
    LOG(INFO) << "PreExecute block " << block_id << " done";
}

/// @brief pre-execute transactions in inter-block mode
/// @param batch the transactions to be executed
/// @param block_id the block id
void Loom::PreExecuteInterBlock(vector<T>& batch, const size_t& block_id) {
    LOG(INFO) << "PreExecute inter block " << block_id;
    vector<future<void>> preExecFutures;
    tbb::concurrent_vector<T> retryTxs;
    // Lambda for executing a single transaction
    std::function<void(T)> executeTx = [&](T tx) {
        // Reset aborted state before each execution
        tx->aborted.store(false);
        // read locally from local storage
        tx->InstallGetStorageHandler([&](
            const unordered_set<string>& readSet
        ) {
            string keys;
            for (auto& key : readSet) {
                keys += key + " ";
                string value = "";
                table.ReserveGet(tx, key);
                if (tx->aborted.load()) return;
                tx->local_get[key] = value;
            }
        });
        // write locally to local storage
        tx->InstallSetStorageHandler([&](
            const unordered_set<string>& writeSet,
            const string& value
        ) {
            if (tx->aborted.load()) return;
            string keys;
            for (auto& key : writeSet) {
                keys += key + " ";
                table.ReservePut(tx, key);
                tx->local_put[key] = value;
            }
        });
        // Execute transaction
        tx->start_time = chrono::steady_clock::now();
        tx->Execute();
        statistics.JournalExecute();
        statistics.JournalOverheads(tx->CountOverheads());
        // Check if transaction was aborted
        if (tx->aborted.load()) {
            DLOG(WARNING) << "Transaction " << tx->id << " aborted, queuing for retry...";
            // enqueue the task for retry and exit to avoid continuing the loop
            retryTxs.push_back(tx);
        }
    };

    // Enqueue all transactions for initial execution
    for (T tx : batch) {
        preExecFutures.push_back(pool->enqueue([this, executeTx, tx] {
            executeTx(tx);
        }));
    }
    // wait for all futures to finish
    for (auto& future : preExecFutures) {
        future.get();
    }

    // Continue processing retry transactions until all are complete
    while (!retryTxs.empty()) {
        LOG(INFO) << "Block " << block_id << " wait to retry...";
        // wait retry signal to be true
        std::unique_lock<std::mutex> lk(mtx);
        while (!canRetry.load()) {
            cv.wait(lk);
        }
        // cv.wait(lk, [this] { return canRetry.load(); });
        // set retry signal to false
        resetRetry();
        // unlock mutex
        lk.unlock();

        LOG(INFO) << "Block " << block_id << " begin to retry...";
        // Swap retryTxs with currentTxs
        tbb::concurrent_vector<T> currentRetryTxs = std::move(retryTxs);
        // Clear retryTxs
        retryTxs.clear();
        // retry transactions
        vector<future<void>> retryFutures;
        for (T tx : currentRetryTxs) {
            retryFutures.push_back(pool->enqueue([this, executeTx, tx] { 
                executeTx(tx);
            }));
        }
        // Wait for all retry transactions to finish
        for (auto& future : retryFutures) {
            future.get();
        }
    }

    LOG(INFO) << "PreExecute block " << block_id << " done";
}

/// @brief minW-rollback
/// @param batch the transactions to be executed
/// @param block the block to be executed
/// @param rbList the list of transactions to be rolled back
/// @param serialOrders the serial order of transactions to be rolled back
void Loom::MinWRollBack(vector<T>& batch, Block::Ptr block, vector<Vertex::Ptr>& rbList, vector<vector<int>>& serialOrders) {
    #define LATENCY duration_cast<microseconds>(tx->GetTx()->m_commit_time - tx->start_time).count()
    #define COMMIT_TIME tx->GetTx()->m_commit_time

    LOG(INFO) << "MinWRollBack block " << block->getBlockId();
    vector<future<void>> graphFutures, finalFutures;
    std::vector<std::future<loom::ReExecuteInfo>> rollbackFutures;
    MinWRollback minw(block->getTxList(), block->getRWIndex());
    // build graph
    minw.buildGraphNoEdgeC(pool, graphFutures);
    DLOG(INFO) << "build block " << block->getBlockId() << " graph done";
    // recognize scc
    minw.onWarm2SCC();
    // rollback transactions
    minw.fastRollback(block->getRBIndex(), rbList);
    for (auto& scc : minw.m_sccs) {
        rollbackFutures.emplace_back(pool->enqueue([this, &scc, &minw] {
            // 回滚事务
            auto reExecuteInfo = minw.rollbackNoEdge(scc, true);
            // 获得回滚事务并根据事务顺序排序
            set<Vertex::Ptr, loom::customCompare> rollbackTxs(loom::customCompare(reExecuteInfo.m_serialOrder));            
            rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
            // 更新m_orderedRollbackTxs
            reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
            return std::move(reExecuteInfo);
        }));
    }
    // wait for all futures to finish
    for (auto& future : rollbackFutures) {
        auto res = future.get();
        // 获得回滚事务顺序
        serialOrders.push_back(std::move(res.m_serialOrder));
        // 将排序后的交易插入rbList
        rbList.insert(rbList.end(), std::make_move_iterator(res.m_orderedRollbackTxs.begin()), std::make_move_iterator(res.m_orderedRollbackTxs.end()));
    }

    // abort all the tx in rbList
    for (auto& tx : rbList) {
        tx->m_hyperVertex->m_aborted = true;
    }

    // finalize all txs (maybe async latter)
    for (auto tx : batch) {
        if (!tx->GetTx()->m_aborted) {
            statistics.JournalCommit(LATENCY, COMMIT_TIME);
            finalFutures.push_back(pool->enqueue([this, tx] {
                Finalize(tx);
            }));
        }
    }
    for (auto& future : finalFutures) {
        future.get();
    }

    // notify retry
    notifyRetry();

    LOG(INFO) << "MinWRollBack block " << block->getBlockId() << " done";
    #undef LATENCY
    #undef COMMIT_TIME
}

/// @brief re-execute transactions
void Loom::ReExecute(Block::Ptr block, vector<Vertex::Ptr>& rbList, vector<vector<int>>& serialOrders) {
    LOG(INFO) << "ReExecute block " << block->getBlockId();
    std::vector<std::future<void>> reExecuteFutures;

    if (enable_nested_reExecution) {
        // re-execute using nested structure
        DeterReExecute reExecute(rbList, serialOrders, block->getConflictIndex());
        reExecute.buildAndReSchedule();
        reExecute.reExcution(pool, reExecuteFutures, statistics);
    } else {
        // re-execute using flat structure
        vector<Vertex::Ptr> normalList;
        DeterReExecute::setNormalList(rbList, normalList);
        DeterReExecute reExecute(normalList, serialOrders, block->getConflictIndex());
        reExecute.buildAndReScheduleFlat();
        reExecute.reExcution(pool, reExecuteFutures, statistics);
    }
    
    LOG(INFO) << "ReExecute block " << block->getBlockId() << " done";
}

/// @brief finalize a transaction
/// @param tx the transaction to be finalized
void Loom::Finalize(T tx) {
    DLOG(INFO) << "Finalize tx " << tx->id;
    tx->committed.store(true);
    // release reserved put
    for (auto& kv : tx->local_put) {
        table.Put(std::get<0>(kv), [&](LoomEntry& entry) {
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

/// @brief clear table
/// @param tx the transaction to be finalized
void Loom::ClearTable(T tx) {
    DLOG(INFO) << "Clear table of tx " << tx->id;
    tx->committed.store(true);
    // release reserved put
    for (auto& kv : tx->local_put) {
        table.Put(std::get<0>(kv), [&](LoomEntry& entry) {
            if (entry.block_id_put == tx->block_id) {
                // update block id put
                if (entry.next_reserved_put > 0) {
                    entry.block_id_put++;
                    entry.reserved_put_num = entry.next_reserved_put;
                    entry.next_reserved_put = 0;
                } else {
                    entry.block_id_put = 0;
                    entry.reserved_put_num = 0;
                }
            }
        });
    }
}

/* old version
/// @brief finalize a transaction
/// @param tx the transaction to be finalized
void Loom::Finalize(T tx) {
    DLOG(INFO) << "Finalize tx " << tx->id;
    tx->committed.store(true);
    // release reserved get
    for (auto& kv : tx->local_get) {
        table.Put(std::get<0>(kv), [&](LoomEntry& entry) {
            if (entry.block_id_get == tx->block_id) {
                entry.reserved_get_txs.erase(tx);
                if (entry.reserved_get_txs.empty()) {
                    entry.block_id_get = 0;
                }
            }
        });
    }
    // release reserved put
    for (auto& kv : tx->local_put) {
        table.Put(std::get<0>(kv), [&](LoomEntry& entry) {
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
*/

/// @brief notify retry
void Loom::notifyRetry() {
    DLOG(INFO) << "notifyRetry";
    // if (canRetry.load()) return;
    std::lock_guard<std::mutex> lk(mtx);
    canRetry.store(true);
    cv.notify_all();
}

/// @brief reset retry
void Loom::resetRetry() {
    DLOG(INFO) << "resetRetry";
    canRetry.store(false);
}

/// @brief construct an empty LoomTransaction
LoomTransaction::LoomTransaction(
    Transaction&& inner, size_t id, size_t block_id
): 
    Transaction(std::move(inner)), 
    id(id),
    block_id(block_id)
{
    // initial readSet and writeSet empty
    local_get = unordered_map<string, string>();
    local_put = unordered_map<string, string>();
}

/// @brief move constructor for LoomTransaction
LoomTransaction::LoomTransaction(
    LoomTransaction&& tx
) noexcept: 
    Transaction(std::move(tx)), 
    id(tx.id),
    block_id(tx.block_id),
    committed(tx.committed.load()),
    aborted(tx.aborted.load()),
    local_get{std::move(tx.local_get)},
    local_put{std::move(tx.local_put)}
{}

/// @brief copy constructor for LoomTransaction
LoomTransaction::LoomTransaction(
    const LoomTransaction& other
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
void LoomTransaction::Execute() {
    DLOG(INFO) << "Execute transaction: " << m_tx << " txid: " << m_tx->m_hyperId << std::endl;
    if (getHandler) {
        getHandler(m_tx->m_rootVertex->allReadSet);
    }
    if (aborted.load()) return;
    if (setHandler) {
        setHandler(m_tx->m_rootVertex->allWriteSet, "value");
    }
    if (aborted.load()) return;
    auto& tx = m_tx->m_rootVertex->m_cost;
    loom::Exec(tx);
    m_tx->m_commit_time = chrono::steady_clock::now();
}

/// @brief initialize the loom table
/// @param partitions the number of partitions
LoomTable::LoomTable(
    size_t partitions
): 
    Table::Table(partitions)
{}

/// @brief reserve a get operation
/// @param tx the transaction
/// @param k the key
void LoomTable::ReserveGet(T tx, const K& k) {
    Table::Put(k, [&](LoomEntry& entry) {
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
void LoomTable::ReservePut(T tx, const K& k) {
    Table::Put(k, [&](LoomEntry& entry) {
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

/* old version
/// @brief reserve a get operation
/// @param tx the transaction
/// @param k the key
void LoomTable::ReserveGet(T tx, const K& k) {
    Table::Put(k, [&](LoomEntry& entry) {
        DLOG(INFO) << tx->block_id << ":" <<  tx->id << " reserve get " << k << ", current tx(" << entry.block_id_get << ")" << endl;
        // don't have war conflict
        if (entry.block_id_put == 0 || entry.block_id_put >= tx->block_id) {
            if (entry.block_id_get < tx->block_id) {
                entry.block_id_get = tx->block_id;
                entry.reserved_get_txs.clear();
                entry.reserved_get_txs.insert(tx);
            } else if (entry.block_id_get == tx->block_id) {
                entry.reserved_get_txs.insert(tx);
            }
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
void LoomTable::ReservePut(T tx, const K& k) {
    Table::Put(k, [&](LoomEntry& entry) {
        DLOG(INFO) << tx->block_id << ":" <<  tx->id << " reserve put " << k << ", current tx(" << entry.block_id_get << ")" << endl;
        if (entry.block_id_get > tx->block_id) {
            for (auto rtx : entry.reserved_get_txs) {
                rtx->aborted.store(true);
            }
        }
        if (entry.block_id_put == 0) {
            // reserve put
            entry.block_id_put = tx->block_id;
            // flag have a bigger block_id put
            entry.reserved_put_num = 1;
        } else if (entry.block_id_put > tx->block_id) {
            // reserve put
            entry.block_id_put = tx->block_id;
            // restore next reserved put
            entry.next_reserved_put = entry.reserved_put_num;
            // store reserved put
            entry.reserved_put_num = 1;
        } else if (entry.block_id_put == tx->block_id) {
            entry.reserved_put_num++;
        } else if (entry.block_id_put < tx->block_id) {
            entry.next_reserved_put++;
        }

        DLOG(INFO) << tx->block_id << ":" << tx->id << " reserve put " << k << " ok" << std::endl;
    });
}
*/

#undef T
#undef K