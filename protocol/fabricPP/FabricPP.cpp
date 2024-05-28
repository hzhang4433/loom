#include "FabricPP.h"
#include "protocol/common.h"

using namespace std;

void FabricPP::execute(const Transaction::Ptr& tx) {
    // 构建超节点
    int txid = getId();
    HyperVertex::Ptr hyperVertex = make_shared<HyperVertex>(txid);
    
    // 构建事务节点
    Vertex::Ptr rootVertex = make_shared<Vertex>(hyperVertex, txid, to_string(txid));
    hyperVertex->buildVertexs(tx, rootVertex);
    
    // 设置超节点状态
    // 添加回滚代价
    rootVertex->m_cost = rootVertex->m_self_cost;
    // 设置节点集
    hyperVertex->m_vertices.insert(rootVertex);
    hyperVertex->m_rootVertex = rootVertex;

    // 记录超节点
    m_hyperVertices.insert(hyperVertex);
}

void FabricPP::buildGraph() {
    // 遍历超节点
    for (auto& hyperVertex : m_hyperVertices) {
        // 获取超节点根节点，即对应事务
        Vertex::Ptr newV = hyperVertex->m_rootVertex;
        // 遍历现有事务集，依次判断依赖关系
        for (auto& oldV : m_vertices) {
            // // 0. 获取超节点
            // auto& newHyperVertex = newV->m_hyperVertex;
            // auto& oldHyperVertex = oldV->m_hyperVertex;
            // 1. 判断rw冲突
            if (protocol::hasConflict(newV->readSet, oldV->writeSet)) {
                // 添加出边
                newV->m_out_edges.insert(oldV);
                // 更新度数
                newV->m_degree++;
                // 添加入边
                oldV->m_in_edges.insert(newV);
                // 更新度数
                oldV->m_degree++;
            }
            // 2. 判断wr冲突
            if (protocol::hasConflict(newV->writeSet, oldV->readSet)) {
                // 添加入边
                newV->m_in_edges.insert(oldV);
                // 更新度数
                newV->m_degree++;
                // 添加出边
                oldV->m_out_edges.insert(newV);
                // 更新度数
                oldV->m_degree++;
            }
        }
        // 将本节点加入事务集
        m_vertices.insert(newV);
    }
}

void FabricPP::rollback() {
    // 1. 使用Tarjan算法找到所有的scc
    vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>> sccs;
    if (!Tarjan(m_vertices, sccs)) {
        return;
    }
    
    // 2. 使用Johnson算法找到所有scc中的所有环路，记录每个vertex所处环路数和每个环路中的节点
    // 定义有序集合，按照节点环路数和id排序
    set<Vertex::Ptr, Vertex::VertexCmpCycle> pq;

    for (auto& scc : sccs) {
        // 输出scc信息
        cout << "get one scc, size: " << scc.size() << endl;
        for (auto& v : scc) {
            cout << v->m_id << " ";
        }
        cout << endl;

        vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>> cycles;
        Johnson(scc, cycles);
        
        // 记录每个vertex所处环路数
        set<Vertex::Ptr> waitToInsert;
        for (auto& cycle : cycles) {
            for (auto& v : cycle) {
                v->m_cycle_num++;
                waitToInsert.insert(v);
            }
        }

        // 插入优先队列
        for (auto& v : waitToInsert) {
            pq.insert(v);
        }

        // 记录所有环路
        m_cycles.insert(m_cycles.end(), cycles.begin(), cycles.end());
    }

    // 输出所有环路信息
    cout << "get all cycles, size: " << m_cycles.size() << endl;
    for (auto& cycle : m_cycles) {
        cout << "cycle size: " << cycle.size() << endl;
        for (auto& v : cycle) {
            cout << v->m_id << " ";
        }
        cout << endl;
    }
    
    // 3. 从环路数最大，id最小的vertex开始回滚
    while (!m_cycles.empty()) {
        // 3.1 获取当前节点
        Vertex::Ptr v = *pq.begin();
        pq.erase(v);
        // 3.2 回滚当前节点
        m_rollbackTxs.insert(v);
        // 3.3 删除对应环路
        m_cycles.erase(std::remove_if(m_cycles.begin(), m_cycles.end(),
        [&v, &pq](tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& cycle) {
            if (cycle.find(v) != cycle.end()) {
                for (auto& w : cycle) {
                    // 更新优先队列
                    pq.erase(w);
                    // 更新相关节点的环路数
                    if (w->m_cycle_num > 1) {
                        w->m_cycle_num--;
                        pq.insert(w);
                    }
                }
                return true;
            }
            return false;
        }), m_cycles.end());

    // 4. 若还存在环路，则递归回滚
    }
    
}

bool FabricPP::Tarjan(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& vertices, vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& sccs) {
    // 初始化
    int index = 0;
    stack<Vertex::Ptr> s;
    unordered_map<Vertex::Ptr, int> indexMap;
    unordered_map<Vertex::Ptr, int> lowLinkMap;
    unordered_map<Vertex::Ptr, bool> onStackMap;
    vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>> components;

    // 初始化
    for (auto& v : vertices) {
        indexMap[v] = -1;
        lowLinkMap[v] = -1;
        onStackMap[v] = false;
    }

    // 遍历所有节点
    for (auto& v : vertices) {
        if (indexMap[v] == -1) {
            strongConnect(v, index, s, indexMap, lowLinkMap, onStackMap, components);
        }
    }

    // 记录size大于1的scc
    for (auto& component : components) {
        if (component.size() > 1) {
            sccs.push_back(component);
        }
    }

    return !sccs.empty();
}

void FabricPP::strongConnect(Vertex::Ptr& v, int& index, stack<Vertex::Ptr>& s, unordered_map<Vertex::Ptr, int>& indexMap, unordered_map<Vertex::Ptr, int>& lowLinkMap, unordered_map<Vertex::Ptr, bool>& onStackMap, vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& components) {
    // 设置index和lowLink
    indexMap[v] = lowLinkMap[v] = index++;
    s.push(v);
    onStackMap[v] = true;

    // 遍历出边
    for (auto& w : v->m_out_edges) {
        if (indexMap[w] == -1) {
            strongConnect(w, index, s, indexMap, lowLinkMap, onStackMap, components);
            lowLinkMap[v] = min(lowLinkMap[v], lowLinkMap[w]);
        } else if (onStackMap[w]) {
            lowLinkMap[v] = min(lowLinkMap[v], indexMap[w]);
        }
    }

    // 判断是否为scc
    if (lowLinkMap[v] == indexMap[v]) {
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> component;
        Vertex::Ptr w;
        do {
            w = s.top();
            s.pop();
            onStackMap[w] = false;
            component.insert(w);
        } while (w != v);
        components.push_back(component);
    }
}

void FabricPP::Johnson(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& scc, vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& cycles) {
    // 初始化
    tbb::concurrent_unordered_map<Vertex::Ptr, int, Vertex::VertexHash> blockedMap;
    tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> blockedSet;
    vector<Vertex::Ptr> stack;
    // 记录所有环路
    for (auto& v : scc) {
        blockedMap[v] = false;
        blockedSet[v] = tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>();
    }

    // 从每个节点开始寻找环路
    for (auto& start : scc) {
        stack.push_back(start);
        blockedMap[start] = true;
        findCycles(start, start, stack, blockedMap, blockedSet, cycles);
        stack.pop_back();
    }
}

bool FabricPP::findCycles(Vertex::Ptr& start, Vertex::Ptr& v, vector<Vertex::Ptr>& stack, tbb::concurrent_unordered_map<Vertex::Ptr, int, Vertex::VertexHash>& blockedMap, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& blockedSet, vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& cycles) {
    bool foundCycle = false;
    
    for (auto& w : v->m_out_edges) {
        if (w == start) {
            foundCycle = true;
            cycles.push_back(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>(stack.begin(), stack.end()));
        } else if (!blockedMap[w]) {
            stack.push_back(w);
            blockedMap[w] = true;
            if (findCycles(start, w, stack, blockedMap, blockedSet, cycles)) {
                foundCycle = true;
            }
            stack.pop_back();
        }
    }

    if (foundCycle) {
        unblock(v, blockedMap, blockedSet);
    } else {
        for (auto& w : v->m_out_edges) {
            blockedSet[w].insert(v);
        }
    }
}

void FabricPP::unblock(Vertex::Ptr& u, tbb::concurrent_unordered_map<Vertex::Ptr, int, Vertex::VertexHash>& blockedMap, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& blockedSet) {
    blockedMap[u] = false;
    auto& set = blockedSet[u];
    while (!set.empty()) {
        Vertex::Ptr unblockNode = *set.begin();
        set.unsafe_erase(unblockNode);
        if (blockedMap[unblockNode]) {
            unblock(unblockNode, blockedMap, blockedSet);
        }
    }
}

int FabricPP::getId() {
    return id_counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

int FabricPP::printRollbackTxs() {
    cout << "====================Rollback Transactions====================" << endl;
    cout << "total size: " << m_rollbackTxs.size() << endl;
    int totalRollbackCost = 0;
    for (auto& tx : m_rollbackTxs) {
        totalRollbackCost += tx->m_self_cost;
        cout << tx->m_id << endl;
    }
    cout << "rollback cost: " << totalRollbackCost << endl;
    cout << "=============================================================" << endl;
    return totalRollbackCost;
}