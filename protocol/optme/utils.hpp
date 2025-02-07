#include <vector>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <set>
#include <algorithm>
#include <loom/protocol/optme/OptME.h>

using namespace std;

namespace loom {

#define K string
#define T shared_ptr<OptMETransaction>
#define U shared_ptr<Unit>

enum class UnitType { Read, Write };

class Unit {
public:
    T tx;
    UnitType unit_type;
    K address;
    mutable shared_mutex lock;
    uint32_t wr_dependencies = 0;
    bool co_located;

    Unit(T tx, UnitType unit_type, K address, bool co_located)
        : tx(tx), unit_type(unit_type), address(address), co_located(co_located) {}

    UnitType get_unit_type() const { return unit_type; }
    K get_address() const { return address; }
    uint32_t get_degree() const { shared_lock l(lock); return wr_dependencies; }
    void add_dependency() { unique_lock l(lock); ++wr_dependencies; }
    bool is_sorted() const { return tx->get_sequence() > 0; }
    void abort_tx() { tx->aborted.store(true); }
};

class ReadUnits {
public:
    vector<U> units;
    uint32_t max_seq = 0;

    ReadUnits() = default;
    ReadUnits(vector<U> units) : units(units) {}
    void push(U unit) { units.push_back(unit); }
    void sort() {
        vector<U> sorted, remaining;
        partition_copy(units.begin(), units.end(), back_inserter(sorted), back_inserter(remaining),
                       [](const auto& unit) { return unit->is_sorted(); });
        if (sorted.empty()) {
            this->max_seq = 1;
        } else {
            auto [min_seq, max_seq] = minmax_element(sorted.begin(), sorted.end(),
                                                      [](const auto& a, const auto& b) { return a->tx->get_sequence() < b->tx->get_sequence(); });
            this->max_seq = (*max_seq)->tx->get_sequence();
        }
        for (auto& unit : remaining) {
            unit->tx->set_sequence(max_seq);
        }
        units.clear();
        move(sorted.begin(), sorted.end(), back_inserter(units));
        move(remaining.begin(), remaining.end(), back_inserter(units));
    }
};

class WriteUnits {
public:
    vector<U> units;
    uint32_t max_seq = 0;
    bool first_updater_flag = false;

    WriteUnits() = default;
    WriteUnits(vector<U> units) : units(units) {}
    void push(U unit) { units.push_back(unit); }
    void sort(ReadUnits& read_units) {
        vector<U> sorted, remaining;
        partition_copy(units.begin(), units.end(), back_inserter(sorted), back_inserter(remaining),
                       [](const auto& unit) { return unit->is_sorted(); });

        for (auto& unit : sorted) {
            if (!unit->tx->aborted && unit->co_located) {
                if (!first_updater_flag) {
                    unit->tx->set_sequence(read_units.max_seq + 1);
                    first_updater_flag = true;
                } else {
                    DLOG(INFO) << "abort tx by unit: " << unit->tx->id;
                    unit->abort_tx();
                }
            }
        }

        for (auto& unit : sorted) {
            if (!unit->tx->aborted && unit->tx->get_sequence() < read_units.max_seq) {
                DLOG(INFO) << "abort tx by unit: " << unit->tx->id;
                unit->abort_tx();
            }
        }

        set<uint32_t> write_seq_set;
        for (const auto& unit : sorted) {
            write_seq_set.insert(unit->tx->get_sequence());
        }

        uint32_t write_seq = read_units.max_seq + 1;
        for (auto& unit : remaining) {
            while (write_seq_set.count(write_seq)) {
                ++write_seq;
            }
            unit->tx->set_sequence(write_seq);
            write_seq_set.insert(write_seq);
        }

        max_seq = write_seq;
        units.clear();
        move(sorted.begin(), sorted.end(), back_inserter(units));
        move(remaining.begin(), remaining.end(), back_inserter(units));
    }
};

class Address {
public:
    K address;
    uint32_t in_degree = 0;
    uint32_t out_degree = 0;
    ReadUnits read_units;
    WriteUnits write_units;
    bool first_updater_flag = false;

    Address(K address) : address(address) {}

    void add_unit(U unit) {
        if (unit->get_unit_type() == UnitType::Read) {
            in_degree += unit->get_degree();
            read_units.push(unit);
        } else {
            if (unit->co_located) {
                first_updater_flag = true;
            }
            out_degree += unit->get_degree();
            write_units.push(unit);
        }
    }

    void sort_read_units() { read_units.sort(); }
    void sort_write_units() { write_units.sort(read_units); }

    void merge(Address& other) {
        if (first_updater_flag && other.first_updater_flag) {
            for (auto& unit : other.read_units.units) {
                if (unit->co_located) {
                    DLOG(INFO) << "abort tx by unit: " << unit->tx->id;
                    unit->abort_tx();
                    in_degree += other.in_degree - unit->get_degree();
                    break;
                }
            }
            for (auto& unit : other.write_units.units) {
                if (unit->co_located) {
                    DLOG(INFO) << "abort tx by unit: " << unit->tx->id;
                    unit->abort_tx();
                    in_degree += other.in_degree - unit->get_degree();
                    break;
                }
            }
        } else {
            in_degree += other.in_degree;
            out_degree += other.out_degree;
            read_units.units.insert(read_units.units.end(), other.read_units.units.begin(), other.read_units.units.end());
            write_units.units.insert(write_units.units.end(), other.write_units.units.begin(), other.write_units.units.end());
            first_updater_flag = first_updater_flag || other.first_updater_flag;
        }
    }
};

class AddressBasedConflictGraph {
public:
    unordered_map<K, shared_ptr<Address>> addresses;
    vector<T> tx_list;
    vector<T> aborted_txs;
    std::shared_ptr<ThreadPool>& pool;

    AddressBasedConflictGraph(std::shared_ptr<ThreadPool>& pool): pool(pool) {}

    AddressBasedConflictGraph& operator=(AddressBasedConflictGraph&& other) noexcept {
        if (this != &other) {
            addresses = std::move(other.addresses);
            tx_list = std::move(other.tx_list);
            aborted_txs = std::move(other.aborted_txs);
        }
        return *this;
    }

    AddressBasedConflictGraph(const AddressBasedConflictGraph&) = delete;
    AddressBasedConflictGraph& operator=(const AddressBasedConflictGraph&) = delete;

    void construct(const vector<T>& simulation_result) {
        for (auto& tx : simulation_result) {
            WriteUnits write_units(convert_to_units(tx, UnitType::Write, tx->local_put, tx->local_get));
            
            if (check_updater_already_exist_in_same_address(write_units.units)) {
                tx->aborted.store(true);
                aborted_txs.push_back(tx);
                continue;
            }

            ReadUnits read_units(convert_to_units(tx, UnitType::Read, tx->local_get));
            set_wr_dependencies(read_units.units, write_units.units);

            tx_list.push_back(tx);
            add_units_to_address(read_units.units);
            add_units_to_address(write_units.units);
        }
    }

    /// @brief parallel construct an AddressBasedConflictGraph initial version
    void parallel_construct(vector<T>& simulation_result) {
        size_t num_of_txn = simulation_result.size();
        size_t ncpu = pool->getThreadNum();
        size_t chunk_size = max(num_of_txn / ncpu, size_t(1));

        vector<future<shared_ptr<AddressBasedConflictGraph>>> futures;
        mutex merge_mutex;
        
        for (size_t i = 0; i < num_of_txn; i += chunk_size) {
            vector<T> chunk(simulation_result.begin() + i,
                            simulation_result.begin() + min(i + chunk_size, num_of_txn));
            
            futures.push_back(pool->enqueue([this, chunk]() {
                shared_ptr<AddressBasedConflictGraph> sub_graph = make_shared<AddressBasedConflictGraph>(this->pool);
                sub_graph->construct(chunk);
                return sub_graph;
            }));
        }
        
        vector<shared_ptr<AddressBasedConflictGraph>> sub_graphs;
        for (auto& f : futures) {
            sub_graphs.push_back(f.get());
        }

        while (sub_graphs.size() > 1) {
            vector<shared_ptr<AddressBasedConflictGraph>> merged_graphs;
            for (size_t i = 0; i < sub_graphs.size(); i += 2) {
                if (i + 1 < sub_graphs.size()) {
                    lock_guard<mutex> lock(merge_mutex);
                    sub_graphs[i]->merge(*sub_graphs[i + 1]);
                }
                merged_graphs.push_back(sub_graphs[i]);
            }
            sub_graphs = std::move(merged_graphs);
        }
        
        *this = std::move(*sub_graphs.front());
    }

    void parallel_construct2(vector<T>& simulation_result) {
        size_t num_of_txn = simulation_result.size();
        size_t ncpu = pool->getThreadNum();
        size_t chunk_size = max(num_of_txn / (2 * ncpu), size_t(1));  // ✅ 改进任务切分
        
        vector<future<shared_ptr<AddressBasedConflictGraph>>> futures;
        mutex merge_mutex;
        
        for (size_t i = 0; i < num_of_txn; i += chunk_size) {
            vector<T> chunk(simulation_result.begin() + i,
                            simulation_result.begin() + min(i + chunk_size, num_of_txn));
            
            futures.push_back(pool->enqueue([this, chunk]() {
                shared_ptr<AddressBasedConflictGraph> sub_graph = make_shared<AddressBasedConflictGraph>(this->pool);
                sub_graph->construct(chunk);
                return sub_graph;
            }));
        }
        
        vector<shared_ptr<AddressBasedConflictGraph>> sub_graphs;
        sub_graphs.reserve(futures.size());
        for (auto& f : futures) {
            f.wait();
        }
        for (auto& f : futures) {
            sub_graphs.push_back(f.get()); 
        }

        while (sub_graphs.size() > 1) {
            vector<future<shared_ptr<AddressBasedConflictGraph>>> merge_futures;

            for (size_t i = 0; i < sub_graphs.size(); i += 2) {
                if (i + 1 < sub_graphs.size()) {
                    merge_futures.push_back(pool->enqueue([g1 = sub_graphs[i], g2 = sub_graphs[i + 1]]() {
                        g1->merge(*g2);
                        return g1;
                    }));
                } else {
                    merge_futures.push_back(async(launch::async, [g = sub_graphs[i]]() { return g; }));
                }
            }

            sub_graphs.clear();
            for (auto& f : merge_futures) {
                sub_graphs.push_back(f.get());
            }
        }

        *this = std::move(*sub_graphs.front());
    }


    /// @brief convert read/write set to read/write units
    vector<U> convert_to_units(T tx, UnitType unit_type, unordered_map<K, string> read_or_write_set, unordered_map<K, string> read_set = {}) {    
        vector<U> units;
        for (const auto& [key, value] : read_or_write_set) {
            bool co_locate = false;
            if (!read_set.empty()) {
                auto it = read_set.find(key);
                // read/write set co-location
                if (it != read_set.end()) {
                    co_locate = true;
                }
            }
            units.push_back(make_shared<Unit>(tx, unit_type, key, co_locate));
        }
        return units;
    }

    bool check_updater_already_exist_in_same_address(const vector<U>& write_units) {
        for (const auto& unit : write_units) {
            if (unit->co_located) {
                auto it = addresses.find(unit->get_address());
                if (it != addresses.end() && it->second->first_updater_flag) {
                    return true;
                }
            }
        }
        return false;
    }

    void set_wr_dependencies(vector<U>& read_units, vector<U>& write_units) {
        for (auto& write_unit : write_units) {
            K address = write_unit->get_address();
            for (auto& read_unit : read_units) {
                if (read_unit->get_address() != address) {
                    read_unit->add_dependency();
                    write_unit->add_dependency();
                }
            }
        }
    }

    void add_units_to_address(vector<U> units) {
        for (auto& unit : units) {
            K raw_address = unit->get_address();
            if (addresses.find(raw_address) == addresses.end()) {
                addresses[raw_address] = make_shared<Address>(raw_address);
            }
            addresses[raw_address]->add_unit(unit);
        }
    }

    void hierarchical_sort() {
        for (const auto& key : address_rank()) {
            // sort read units and write units
            auto address = addresses[key];
            address->sort_read_units();
            address->sort_write_units();
        }
    }

    /// @brief sort addresses
    vector<K> address_rank() {
        vector<shared_ptr<Address>> address_list;
        for (const auto& [key, addr] : addresses) {
            address_list.push_back(addr);
        }
        sort(address_list.begin(), address_list.end(), [](const shared_ptr<Address>& a, const shared_ptr<Address>& b) {
            if (a->in_degree != b->in_degree) {
                return a->in_degree > b->in_degree;
            } else if (a->out_degree != b->out_degree) {
                return a->out_degree < b->out_degree;
            } else {
                return a->address < b->address;
            }
        });

        vector<K> ranked_addresses;
        for (const auto& addr : address_list) {
            ranked_addresses.push_back(addr->address);
        }
        return ranked_addresses;
    }

    void reorder() {
        vector<T> all_aborted = extract_abortList();
        // vector<T> reorder_targets;
        // vector<T> aborted;

        // partition_copy(all_aborted.begin(), all_aborted.end(), back_inserter(reorder_targets), back_inserter(aborted),
        //                [](const T& tx) { return tx->local_get.size() == 0 && tx->local_put.size() > 1; });

        // aborted_txs.insert(aborted_txs.end(), aborted.begin(), aborted.end());
        // std::sort(aborted_txs.begin(), aborted_txs.end(), [](const auto& a, const auto& b) {
        //     return a->id < b->id;
        // });

        // for (const auto& tx : reorder_targets) {
        //     uint32_t seq = 0;
        //     set<K> unique_addresses;
        //     for (const auto& [key, value] : tx->local_put) {
        //         unique_addresses.insert(key);
        //     }
            
        //     for (K address : unique_addresses) {
        //         auto it = addresses.find(address);
        //         if (it != addresses.end()) {
        //             seq = max(seq, max(it->second->write_units.max_seq, it->second->read_units.max_seq));
        //         }
        //     }
            
        //     tx->set_sequence(seq);
        //     tx_list.push_back(tx);
        // }
    }

    vector<T> extract_abortList() {
        vector<T> aborted_list;
        std::vector<decltype(tx_list)::value_type> to_erase;
        DLOG(INFO) << "tx_list size in extract_abortList: " << tx_list.size();

        for (auto tx : tx_list) {
            if (tx->aborted.load()) {
                // aborted_list.push_back(tx);
                aborted_txs.push_back(tx);
                to_erase.push_back(tx);
            }
        }

        for (auto tx : to_erase) {
            auto it = std::find(tx_list.begin(), tx_list.end(), tx);
            if (it != tx_list.end()) {
                tx_list.erase(it);
            }
        }
        return aborted_list;
    }

    void merge(AddressBasedConflictGraph& other) {
        for (auto& [addr, address] : other.addresses) {
            if (addresses.find(addr) != addresses.end()) {
                addresses[addr]->merge(*address);
            } else {
                addresses[addr] = address;
            }
        }
        tx_list.insert(tx_list.end(), other.tx_list.begin(), other.tx_list.end());
        aborted_txs.insert(aborted_txs.end(), other.aborted_txs.begin(), other.aborted_txs.end());
    }

    vector<T> extract_aborted_txs() { return aborted_txs; }

    vector<T> extract_tx_list() { return tx_list; }
};

#undef T
#undef K
#undef U

}