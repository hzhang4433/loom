#include <loom/protocol/moss/Moss.h>
#include <fmt/core.h>
#include <glog/logging.h>


using namespace std::chrono;

#define K std::string
#define V MossEntry
#define T MossTransaction


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

/// @brief start the protocol
void Moss::Start() {
    // transform Transaction to MossTransaction
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
            futures.push_back(pool->enqueue([this, tx]() mutable {
                DLOG(INFO) << "enqueue tx " << tx.id << endl;
                // execute the transaction
                Execute(&tx);
                while (true) {
                    if (tx.flag_conflict.load()) {
                        // if there are war, re-execute entire transaction
                        ReExecute(&tx);
                    } else if (last_finalized.load() + 1 == tx.id && !tx.flag_conflict.load()) {
                        // if last transaction has finalized, and currently i don't have to re-execute, 
                        // then i can final commit and do another transaction. 
                        Finalize(&tx);
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

/// @brief execute the transaction
/// @param tx the transaction to be executed
void Moss::Execute(T* tx) {
    // execute the transaction
    tx->start_time = steady_clock::now();
    tx->InstallGetStorageHandler([this, tx](
        const std::unordered_set<string>& readSet
    ) {
        string keys;
        // no multi-version
        for (auto& key : readSet) {
            DLOG(INFO) << "tx " << tx->id << " read " << key << std::endl;
            keys += key + " ";
            string value;
            table.Get(tx, key, value);
            if (tx->flag_conflict.load()) { return; }
            tx->local_get[key] = value;
        }
        DLOG(INFO) << "tx " << tx->id << " all reads: " << keys << std::endl;
    });

    tx->InstallSetStorageHandler([this, tx](
        const std::unordered_set<string>& writeSet,
        const std::string& value
    ) {
        string keys;
        for (auto& key : writeSet) {
            DLOG(INFO) << "tx " << tx->id << " write " << key << std::endl;
            keys += key + " ";
            table.Put(tx, key, value);
            if (tx->flag_conflict.load()) { return; }
            tx->local_put[key] = value;
        }
        DLOG(INFO) << "tx " << tx->id << " all writes: " << keys << std::endl;
    });
    // execute the transaction
    tx->Execute();
    DLOG(INFO) << "moss executed " << tx->id;
}

/// @brief re-execute the transaction
/// @param tx the transaction to be re-executed
void Moss::ReExecute(T* tx) {
    DLOG(INFO) << "moss re-execute " << tx->id;
    tx->flag_conflict.store(false);
    tx->local_get.clear();
    tx->local_put.clear();
    // re-execute the transaction
    tx->Execute();
}

/// @brief finalize the transaction
/// @param tx the transaction to be finalized
void Moss::Finalize(T* tx) {
    // finalize the transaction
    DLOG(INFO) << "moss finalize " << tx->id << endl;
    for (auto entry: tx->local_get) {
        table.ClearGet(tx, std::get<0>(entry));
    }
    for (auto entry: tx->local_put) {
        table.ClearPut(tx, std::get<0>(entry));
    }
    last_finalized.fetch_add(1, std::memory_order_seq_cst);
    // auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
}

/// @brief construct an empty MossTransaction
MossTransaction::MossTransaction(
    Transaction&& inner, size_t id
): 
    Transaction(std::move(inner)), 
    id(id),
    flag_conflict(false)
{
    // initial readSet and writeSet empty
    local_get = std::unordered_map<string, string>();
    local_put = std::unordered_map<string, string>();
}

/// @brief move constructor for MossTransaction
MossTransaction::MossTransaction(
    MossTransaction&& tx
) noexcept: 
    Transaction(std::move(tx)), 
    id(tx.id),
    flag_conflict(tx.flag_conflict.exchange(false)),
    start_time(tx.start_time),
    local_get{std::move(tx.local_get)},
    local_put{std::move(tx.local_put)}
{}

/// @brief copy constructor for MossTransaction
MossTransaction::MossTransaction(
    const MossTransaction& other
): 
    Transaction(other), 
    id(other.id),
    flag_conflict(other.flag_conflict.load()),
    start_time(other.start_time),
    local_get(other.local_get),
    local_put(other.local_put)
{}

void MossTransaction::Execute() {
    DLOG(INFO) << "Execute transaction: " << m_tx << " txid: " << m_tx->m_hyperId << std::endl;
    if (getHandler) {
        getHandler(m_tx->m_rootVertex->allReadSet);
    }
    if (setHandler) {
        setHandler(m_tx->m_rootVertex->allWriteSet, "value");
    }
    auto& tx = m_tx->m_rootVertex->m_cost;
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
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
/// @param v the value of read entry
void MossTable::Get(T* tx, const K& k, std::string& v) {
    DLOG(INFO) << tx->id << "(" << tx << ")" << " read " << endl;
    Table::Put(k, [&](V& _v) {
        // see a war
        {
            auto guard = Guard{_v.w_mu};
            if (_v.writer != nullptr && _v.writer->id < tx->id) {
                DLOG(INFO) << tx->id << " see a war by " << _v.writer->id << " aborted." << endl;
                tx->flag_conflict.store(true);
                return;
            }  
        }
        v = _v.value;
        {
            auto guard = Guard{_v.r_mu};
            DLOG(INFO) << tx->id << " add read record" << endl;
            _v.readers.insert(tx);
        } 
    });
}

/// @brief put a value
/// @param tx the transaction that writes the value
/// @param k the key of the written entry
/// @param v the value to write
void MossTable::Put(T* tx, const K& k, const string& v) {
    DLOG(INFO) << tx->id << "(" << tx << ")" << " write " << endl;
    Table::Put(k, [&](V& _v) {
        
        // abort transactions that read outdated keys
        {
            auto guard = Guard{_v.r_mu};
            for (auto _tx: _v.readers) {
                if (_tx->id > tx->id) {
                    DLOG(INFO) << tx->id << " abort war on" << _tx->id << endl;
                    _tx->flag_conflict.store(true);
                }
            }
        }
        
        // handle duplicated write on the same key
        {
            auto guard = Guard{_v.w_mu};
            if (_v.writer != nullptr) {
                if (_v.writer->id == tx->id) {
                    _v.value = v;
                } else if (_v.writer->id > tx->id) {
                    _v.writer->flag_conflict.store(true);
                    _v.writer = tx;
                } else if (_v.writer->id < tx->id) {
                    tx->flag_conflict.store(true);
                }
            } else {
                _v.writer = tx;
            }
        }
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void MossTable::ClearGet(T* tx, const K& k) {
    DLOG(INFO) << "remove read record from " << tx->id << endl;
    Table::Put(k, [&](V& _v) {
        auto guard = Guard{_v.r_mu};
        _v.readers.erase(tx);
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void MossTable::ClearPut(T* tx, const K& k) {
    DLOG(INFO) << "remove write record from " << tx->id << endl;
    Table::Put(k, [&](V& _v) {
        auto guard = Guard{_v.w_mu};
        if (_v.writer == tx) {
            _v.writer = nullptr;
        } else {
            DLOG(INFO) << "something must be wrong in " << tx->id << endl;
        }
    });
}