#include <loom/protocol/moss/Moss.h>
#include <fmt/core.h>
#include <glog/logging.h>


using namespace std::chrono;

#define K std::string
#define V MossEntry
#define T shared_ptr<MossTransaction>
#define ST shared_ptr<MossSubTransaction>


/// @brief initialize aria protocol
/// @param blocks the blocks to be executed
/// @param num_threads the number of threads
/// @param table_partitions the number of partitions for the table
Moss::Moss(
    vector<Block::Ptr> blocks, 
    size_t num_threads, 
    size_t table_partitions
): 
    blocks(std::move(blocks)), 
    num_threads(num_threads),
    table(table_partitions),
    pool(std::make_shared<ThreadPool>(num_threads))
{
    LOG(INFO) << fmt::format("Moss(num_threads={}, table_partitions={})", num_threads, table_partitions) << std::endl;
}

/// @brief prepare the transactions
/// @param m_blocks the transactions to be transformed
void Moss::Preparation(vector<vector<T>>& m_blocks) {
    for (size_t i = 0; i < blocks.size(); i++) {
        auto txs = blocks[i]->getTxs();
        vector<T> m_txs;
        for (size_t j = 0; j < txs.size(); j++) {
            auto tx = txs[j];
            auto txid = tx->GetTx()->m_hyperId;
            auto rootVertex = tx->GetTx()->m_rootVertex;
            T m_tx = make_shared<MossTransaction>(std::move(*tx), txid);
            ST root_tx = make_shared<MossSubTransaction>(std::move(*rootVertex), m_tx);
            BuildRoot(root_tx, rootVertex, m_tx);
            m_tx->root_tx = root_tx;
            m_txs.push_back(m_tx);
        }
        m_blocks.push_back(m_txs);
    }
}

/// @brief build the root sub-transaction
/// @param stx the root sub-transaction
/// @param v the vertex to be transformed
/// @param ftx the parent transaction
void Moss::BuildRoot(ST stx, Vertex::Ptr v, T ftx) {
    if (!v->m_children.empty()) {
        for (auto& child : v->m_children) {
            auto sub_tx = make_shared<MossSubTransaction>(std::move(*child.vertex), ftx);
            BuildRoot(sub_tx, child.vertex, ftx);
            stx->children_txs.push_back(sub_tx);
        }
    }
}

/// @brief start the protocol
void Moss::Start() {
    // transform Transaction to MossTransaction
    vector<vector<T>> m_blocks;
    Preparation(m_blocks);
    
    // execute all transactions in the blocks
    LOG(INFO) << "start" << std::endl;
    for (size_t i = 0; i < m_blocks.size(); i++) {
        auto m_txs = m_blocks[i];
        // execute all transactions in the block
        std::vector<std::future<void>> futures;
        for (size_t j = 0; j < m_txs.size(); j++) {
            auto tx = m_txs[j];
            futures.push_back(pool->enqueue([this, tx]() mutable {
                DLOG(INFO) << "enqueue tx " << tx->id << endl;
                // execute the transaction
                Execute(tx->root_tx);
                while (true) {
                    if (tx->HasWAR()) {
                        // if there are war, re-execute entire transaction
                        ReExecute(tx);
                    } else if (last_finalized.load() + 1 == tx->id && !tx->HasWAR()) {
                        // if last transaction has finalized, and currently i don't have to re-execute, 
                        // then i can final commit and do another transaction. 
                        Finalize(tx);
                        break;
                    }
                }
            }));
        }
        // wait for all transactions in the block to complete
        for (auto& future : futures) {
            future.get();
        }
        LOG(INFO) << "block " << i + 1 << " done, " << last_finalized.load() << " txs committed" << endl;
    }
}

/// @brief stop the protocol
void Moss::Stop() {
    pool->shutdown();
    LOG(INFO) << "moss stop";
}


/// @brief execute the sub-transaction
/// @param tx the transaction to be executed
void Moss::Execute(ST stx) {
    // set the handlers
    stx->InstallGetStorageHandler([this, stx](
        const std::unordered_set<string>& readSet
    ) {
        string keys;
        // no multi-version
        for (auto& key : readSet) {
            DLOG(INFO) << "stx " << stx->m_id << " read " << key << std::endl;
            keys += key + " ";
            string value;
            table.Get(stx, key, value);
            if (stx->ftx->HasWAR(stx)) { return; }
            stx->local_get[key] = value;
        }
        DLOG(INFO) << "stx " << stx->m_id << " all reads: " << keys << std::endl;
    });

    stx->InstallSetStorageHandler([this, stx](
        const std::unordered_set<string>& writeSet,
        const std::string& value
    ) {
        string keys;
        for (auto& key : writeSet) {
            DLOG(INFO) << "stx " << stx->m_id << " write " << key << std::endl;
            keys += key + " ";
            table.Put(stx, key, value);
            if (stx->ftx->HasWAR(stx)) { return; }
            stx->local_put[key] = value;
        }
        DLOG(INFO) << "stx " << stx->m_id << " all writes: " << keys << std::endl;
    });

    // execute sub-transactions
    auto children = stx->children_txs;
    for (auto& child : children) {
        Execute(child);
    }
    // execute the current transaction
    stx->Execute();
}

/// @brief re-execute the transaction
/// @param tx the transaction to be re-executed
void Moss::ReExecute(T tx) {
    DLOG(INFO) << "moss re-execute " << tx->id;
    // get current rerun keys
    std::unordered_set<ST> rerun_txs{};
    {
        auto guard = Guard{tx->rerun_txs_mu}; 
        std::swap(tx->rerun_txs, rerun_txs);
    }
    // rollback the rerun_txs
    for (auto& rerun_tx: rerun_txs) {
        // clear the local read and write set
        rerun_tx->local_get.clear();
        rerun_tx->local_put.clear();
        // re-execute the transaction
        Execute(rerun_tx);
    }
}

/// @brief finalize the transaction
/// @param tx the transaction to be finalized
void Moss::Finalize(T tx) {
    // finalize the transaction
    DLOG(INFO) << "moss finalize " << tx->id << endl;
    ClearTable(tx->root_tx);
    last_finalized.fetch_add(1, std::memory_order_seq_cst);
    // auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
}

void Moss::ClearTable(ST stx) {
    for (auto entry: stx->local_get) {
        table.ClearGet(stx, std::get<0>(entry));
    }
    for (auto entry: stx->local_put) {
        table.ClearPut(stx, std::get<0>(entry));
    }
}

/// @brief construct an empty MossTransaction
MossTransaction::MossTransaction(
    Transaction&& inner, size_t id
): 
    Transaction(std::move(inner)), 
    id(id)
{
}

/// @brief move constructor for MossTransaction
MossTransaction::MossTransaction(
    MossTransaction&& tx
) noexcept: 
    Transaction(std::move(tx)), 
    id(tx.id),
    start_time(tx.start_time),
    rerun_txs(tx.rerun_txs)
{}

/// @brief copy constructor for MossTransaction
MossTransaction::MossTransaction(
    const MossTransaction& other
): 
    Transaction(other), 
    id(other.id),
    start_time(other.start_time),
    rerun_txs(other.rerun_txs)
{}

/// @brief check if there is a write-after-read conflict
bool MossTransaction::HasWAR() {
    auto guard = Guard{rerun_txs_mu};
    return !rerun_txs.empty();
}

/// @brief check if there is a write-after-read conflict
bool MossTransaction::HasWAR(const ST stx) {
    auto guard = Guard{rerun_txs_mu};
    // if rerun_txs if empty return false
    if (rerun_txs.empty()) return false;
    // check if the rerun_txs contains ancestors of stx
    for (auto& rerun_tx: rerun_txs) {
        // if the m_id of rerun_tx is the prefix of m_id of the stx
        // then rerun_tx is the ancestor of stx, return true
        if (stx->m_id.find(rerun_tx->m_id) == 0) {
            return true;
        }
    }
    return false;
}

/// @brief set a write-after-read conflict
/// @param stx the transaction that causes the conflict
void MossTransaction::SetWAR(const ST stx) {
    vector<ST> wait_to_remove;
    auto guard = Guard{rerun_txs_mu};
    // set and combine same ancestors of rerun_txs
    for (auto& rerun_tx: rerun_txs) {
        // if rerun_tx is the ancestor of stx, return
        if (stx->m_id.find(rerun_tx->m_id) == 0) {
            return;
        } else if (rerun_tx->m_id.find(stx->m_id) == 0) {
            // if stx is the ancestor of rerun_tx, remove the rerun_tx
            wait_to_remove.push_back(rerun_tx);
        }
    }
    // remove the rerun_txs that are descendant of stx
    for (auto& rerun_tx: wait_to_remove) {
        rerun_txs.erase(rerun_tx);
    }
    rerun_txs.insert(stx);
}

/// @brief execute the transaction
void MossTransaction::Execute() {
    DLOG(INFO) << "Execute transaction: " << m_tx->m_hyperId << std::endl;
    if (getHandler) {
        getHandler(m_tx->m_rootVertex->allReadSet);
    }
    if (setHandler) {
        setHandler(m_tx->m_rootVertex->allWriteSet, "value");
    }
    auto& tx = m_tx->m_rootVertex->m_cost;
    loom::Exec(tx);
}

/// @brief construct an empty MossSubTransaction
MossSubTransaction::MossSubTransaction(
    Vertex&& inner, T ftx
): 
    Vertex(std::move(inner)), 
    ftx(ftx)
{
    if (!ftx) {
        throw std::invalid_argument("ftx cannot be null");
    }
}

/// @brief execute the transaction
void MossSubTransaction::Execute() {
    DLOG(INFO) << "Execute transaction: " << m_tx << " txid: " << m_id << std::endl;
    if (getHandler) {
        getHandler(readSet);
    }
    if (setHandler) {
        setHandler(writeSet, "value");
    }
    auto& tx = m_self_cost;
    loom::Exec(tx);
}

/// @brief initialize the moss table
/// @param partitions the number of partitions
MossTable::MossTable(
    size_t partitions
): 
    Table::Table(partitions)
{}


/// @brief get a value
/// @param stx the (sub)transaction that reads the value
/// @param k the key of the read entry
/// @param v the value of read entry
void MossTable::Get(ST stx, const K& k, std::string& v) {
    DLOG(INFO) << stx->m_id << "is reading..." << endl;
    Table::Put(k, [&](V& _v) {
        // see a war
        {
            auto guard = Guard{_v.w_mu};
            if (_v.writer != nullptr && _v.writer->ftx->id < stx->ftx->id) {
                DLOG(INFO) << stx->m_id << " see a war by " << _v.writer->m_id << ", aborted." << endl;
                stx->ftx->SetWAR(stx);
                return;
            }
        }
        v = _v.value;
        {
            auto guard = Guard{_v.r_mu};
            // DLOG(INFO) << stx->m_id << " add read record" << endl;
            _v.readers.insert(stx);
        } 
    });
}

/// @brief put a value
/// @param stx the (sub)transaction that writes the value
/// @param k the key of the written entry
/// @param v the value to write
void MossTable::Put(ST stx, const K& k, const string& v) {
    DLOG(INFO) << stx->m_id << "is writing..." << endl;
    Table::Put(k, [&](V& _v) {
        
        // abort transactions that read outdated keys
        {
            auto guard = Guard{_v.r_mu};
            for (auto _tx: _v.readers) {
                if (_tx->ftx->id > stx->ftx->id || (_tx->ftx->id == stx->ftx->id && _tx->m_id > stx->m_id)) {
                    DLOG(INFO) << stx->m_id << " abort war on " << _tx->m_id << endl;
                    _tx->ftx->SetWAR(_tx);
                }
            }
        }
        
        // handle duplicated write on the same key
        {
            auto guard = Guard{_v.w_mu};
            if (_v.writer != nullptr) {
                if (_v.writer->m_id == stx->m_id) {
                    // same transaction write the same key
                    _v.value = v;
                } else if (_v.writer->ftx->id == stx->ftx->id && _v.writer->m_id < stx->m_id) {
                    // same transaction but different sub-transaction write the same key
                    // stx is later sub-transaction, aborted
                    stx->ftx->SetWAR(stx);
                } else if (_v.writer->ftx->id == stx->ftx->id && _v.writer->m_id > stx->m_id) {
                    // same transaction but different sub-transaction write the same key
                    // stx is earlier sub-transaction, save
                    _v.writer->ftx->SetWAR(_v.writer);
                    _v.writer = stx;
                } else if (_v.writer->ftx->id > stx->ftx->id) {
                    _v.writer->ftx->SetWAR(_v.writer);
                    _v.writer = stx;
                } else if (_v.writer->ftx->id < stx->ftx->id) {
                    stx->ftx->SetWAR(stx);
                }
            } else {
                _v.writer = stx;
            }
        }
    });
}


/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void MossTable::ClearGet(ST stx, const K& k) {
    DLOG(INFO) << "remove read record from " << stx->m_id << endl;
    Table::Put(k, [&](V& _v) {
        auto guard = Guard{_v.r_mu};
        _v.readers.erase(stx);
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void MossTable::ClearPut(ST stx, const K& k) {
    DLOG(INFO) << "remove write record from " << stx->m_id << endl;
    Table::Put(k, [&](V& _v) {
        auto guard = Guard{_v.w_mu};
        if (_v.writer == stx) {
            _v.writer = nullptr;
        } else {
            DLOG(INFO) << "something must be wrong in " << stx->m_id << endl;
        }
    });
}

#undef T
#undef K
#undef V
#undef ST