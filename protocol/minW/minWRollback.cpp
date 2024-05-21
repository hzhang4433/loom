#include <algorithm>
#include <tbb/tbb.h>
#include <boost/heap/fibonacci_heap.hpp>
#include "minWRollback.h"
#include <iostream>
#include <chrono>

using namespace std;

/*  执行算法: 将transaction转化为hyperVertex
    详细算法流程: 
        1. 多线程并发执行所有事务，获得事务执行信息（读写集，事务结构，执行时间 => 随机生成）
        2. 判断执行完成的事务与其它执行完成事务间的rw依赖，构建rw依赖图
    状态: 待测试...
*/
void minWRollback::execute(const Transaction::Ptr& tx, bool isNest) {
    int txid = getId();
    HyperVertex::Ptr hyperVertex = make_shared<HyperVertex>(txid);
    Vertex::Ptr rootVertex = make_shared<Vertex>(hyperVertex, txid, to_string(txid));
    // 根据事务结构构建超节点
    string txid_str = to_string(txid);
    
    if (isNest) {
        hyperVertex->buildVertexs(tx, hyperVertex, rootVertex, txid_str);
        // 记录超节点包含的所有节点
        hyperVertex->m_vertices = rootVertex->cascadeVertices;
        // 根据子节点依赖更新回滚代价和级联子事务
        hyperVertex->m_rootVertex = rootVertex;
        hyperVertex->recognizeCascades(rootVertex);
    } else {
        hyperVertex->buildVertexs(tx, rootVertex);
        // 添加回滚代价
        rootVertex->m_cost = rootVertex->m_self_cost;

        hyperVertex->m_vertices.insert(rootVertex);
        hyperVertex->m_rootVertex = rootVertex;
    }
    
    // for test print hyperVertex tree
    hyperVertex->printVertexTree();

    // // 构建超图
    // auto start = std::chrono::high_resolution_clock::now();
    // buildGraph(rootVertex->cascadeVertices);
    // auto end = std::chrono::high_resolution_clock::now();
    // cout << "tx" << txid << " build time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    // // 记录节点
    // m_vertices.insert(rootVertex->cascadeVertices.begin(), rootVertex->cascadeVertices.end());
    // // 记录超节点
    // m_hyperVertices.insert(hyperVertex);
}

/*  构图算法（考虑多线程扩展性）: 将hyperVertex转化为hyperGraph
    详细算法流程: 依次遍历超节点中的所有节点与现有节点进行依赖分析，构建超图
        1. 若与节点存在rw依赖(出边)
            1.1 在v1.m_out_edges数组（记录所有out边）中新增一条边（v2），并尝试更新v1.m_hyperVertex.m_min_out
                为min_out = min(v2.m_hyperVertex.m_min_out, v2.m_hyperId)
                1. 若v1.m_hyperVertex.m_min_out更新，且m_in_edges存在记录，则依次遍历其中的节点v_in【out更新 => out更新】
                    1. 尝试更新v_in.m_min_out为min(v_in.m_min_out, min_out)
                    2. 若v_in.m_min_out更新成功，则尝试递归更新
                    3. 若更新失败，则返回
            1.2 在v2.m_in_edges数组（记录所有in边）中新增一条边（v1），并尝试更新v2.m_hyperVertex.m_min_in
                为min_in = min(v1.m_hyperVertex.m_min_in，v1.m_hyperId)
                1. 若v2.m_hyperVertex.m_min_in更新，且m_out_edges存在记录，则依次遍历其中的节点v_out【in更新 => in更新】
                    1. 尝试更新v_out.m_min_in为min(v_out.m_min_in, min_in)
                    2. v_out.m_min_in更新成功，则尝试递归更新
                    3. 若更新失败，则返回
        2. 若与节点存在wr依赖（入边）与1相反
    状态: 待测试...
 */
void minWRollback::buildGraph(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& vertices) {
    for (auto& newV: vertices) {
        for (auto& oldV: m_vertices) {
            // 获取超节点
            auto& newHyperVertex = newV->m_hyperVertex;
            auto& oldHyperVertex = oldV->m_hyperVertex;
            // 存在rw依赖
            if (hasConflict(newV->readSet, oldV->writeSet)) {
                // newV新增一条出边
                newV->m_out_edges.insert(oldV);

                // 更新newV对应的hyperVertex的out_edges
                handleNewEdge(newV, oldV, newHyperVertex->m_out_edges[oldHyperVertex]);

                // 更新依赖数
                newV->m_degree++;
                // 尝试更新newV对应的hyperVertex的min_out
                int min_out = min(oldHyperVertex->m_min_out, oldV->m_hyperId);
                if (min_out < newHyperVertex->m_min_out) {
                    recursiveUpdate(newHyperVertex, min_out, minw::EdgeType::OUT);
                }

                // oldV新增一条入边
                oldV->m_in_edges.insert(newV);

                // 更新oldV对应的hyperVertex的in_edges
                handleNewEdge(oldV, newV, oldHyperVertex->m_in_edges[newHyperVertex]);

                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_in
                int min_in = min(newHyperVertex->m_min_in, newV->m_hyperId);
                if (min_in < oldHyperVertex->m_min_in) {
                    recursiveUpdate(oldHyperVertex, min_in, minw::EdgeType::IN);
                }
            
            // 存在wr依赖
            } else if (hasConflict(newV->writeSet, oldV->readSet)) {
                // newV新增一条入边
                newV->m_in_edges.insert(oldV);

                // 更新newV对应的hyperVertex的in_edges
                handleNewEdge(newV, oldV, newHyperVertex->m_in_edges[oldHyperVertex]);

                // 更新依赖数
                newV->m_degree++;
                // 尝试更新newV对应的hyperVertex的min_in
                int min_in = min(oldHyperVertex->m_min_in, oldV->m_hyperId);
                if (min_in < newHyperVertex->m_min_in) {
                    recursiveUpdate(newHyperVertex, min_in, minw::EdgeType::IN);
                }

                // oldV新增一条出边                
                oldV->m_out_edges.insert(newV);

                // 更新oldV对应的hyperVertex的out_edges
                handleNewEdge(oldV, newV, oldHyperVertex->m_out_edges[newHyperVertex]);

                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_out
                int min_out = min(newHyperVertex->m_min_out, newV->m_hyperId);
                if (min_out < oldHyperVertex->m_min_out) {
                    recursiveUpdate(oldHyperVertex, min_out, minw::EdgeType::OUT);
                }
            }
        }
    }
}

// 判断读写集是否冲突
bool minWRollback::hasConflict(tbb::concurrent_unordered_set<std::string>& set1, tbb::concurrent_unordered_set<std::string>& set2) {
    for (auto item : set2) {
        if (set1.find(item) != set1.end()) {
            return true;
        }
    }
    return false;
    
/* 并行化冲突检测，带测试，对比效率 
    std::atomic<bool> found(false);

    tbb::parallel_for(tbb::blocked_range<size_t>(0, set2.size()), [&](const tbb::blocked_range<size_t>& r) {
        for (size_t i = r.begin(); i != r.end() && !found; ++i) {
            auto it = set2.begin();
            std::advance(it, i);
            if (set1.find(*it) != set1.end()) {
                found = true;
            }
        }
    });

    return found;
*/ 
}

// 判断两个set是否有交集
template <typename T, typename Hash>
bool hasIntersection(const tbb::concurrent_unordered_set<T, Hash>& set1, const tbb::concurrent_unordered_set<T, Hash>& set2) {
    // for (const auto& item : set1) {
    //     if (set2.find(item) != set2.end()) {
    //         return true;
    //     }
    // }
    // return false;

    std::atomic<bool> found(false);

    tbb::parallel_for(tbb::blocked_range<size_t>(0, set2.size()), [&](const tbb::blocked_range<size_t>& r) {
        for (size_t i = r.begin(); i != r.end() && !found; ++i) {
            auto it = set2.begin();
            std::advance(it, i);
            if (set1.find(*it) != set1.end()) {
                found = true;
            }
        }
    });

    return found;
}

// 处理新边, 尝试将v2插入到v1为起点的边中
void minWRollback::handleNewEdge(Vertex::Ptr& v1, Vertex::Ptr& v2, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges) {
    
    if (edges.count(v1)) {
        edges.at(v1).insert(v2);
    } else {
        // edges.insert({v1, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>{v2}});
        edges.emplace(v1, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>());
        edges.at(v1).insert(v2);
    }

/*
    bool needInsert = true;
    std::vector<Vertex::Ptr> to_erase;
    
    if (edges.count(v1)) {
        for (auto& v: edges.at(v1)) {
            if (isAncester(v->m_id, v2->m_id)) { 
                // 已经存在要插入节点的祖先，则不插入
                needInsert = false;
                break;
            } else if (isAncester(v2->m_id, v->m_id)) {
                // 要插入的节点是已经存在的祖先，则删除已经存在的节点
                to_erase.push_back(v);
            }
        }
    } else {
        edges.emplace(v1, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>());
    }

    for (auto edge : to_erase) {
        edges.at(v1).unsafe_erase(edge);
    }

    if (needInsert) {
        edges.at(v1).insert(v2);
    }
*/
}

// 判断v1是否是v2的祖先
bool minWRollback::isAncester(const string& v1, const string& v2) {
    // 子孙节点id的前缀包含祖先节点id
    return v2.find(v1) == 0;
}

// 递归更新超节点min_in和min_out
void minWRollback::recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, minw::EdgeType type) {
// 或者后续不在这里记录强连通分量，后续根据开销进行调整。。。
    
    // 更新前需要把这个Hypervertex从原有m_min2HyperVertex中删除 (前提:他们都不是初始值)
    if (hyperVertex->m_min_in != INT_MAX && hyperVertex->m_min_out != INT_MAX) {
        m_min2HyperVertex[combine(hyperVertex->m_min_in, hyperVertex->m_min_out)].unsafe_erase(hyperVertex);
    }
    
    if (type == minw::EdgeType::OUT) {
        if (hyperVertex->m_min_in != INT_MAX) {
            // cout << hyperVertex->m_hyperId << " insert scc: min_in: " << hyperVertex->m_min_in << " min_value: " << min_value << endl;
            m_min2HyperVertex[combine(hyperVertex->m_min_in, min_value)].insert(hyperVertex);
        }
        hyperVertex->m_min_out = min_value;        
        // 若min_out更新成功，且hyperVertex的in_edges存在记录
        if (!hyperVertex->m_in_edges.empty()) {
            // 依次遍历其中的节点v_in
            for (auto& v_in: hyperVertex->m_in_edges) {
                // 尝试更新v_in对应的hyperVertex的m_min_out 
                if (min_value < v_in.first->m_min_out) {
                    // 递归更新
                    recursiveUpdate(v_in.first, min_value, type);
                } else {
                    return;
                }
            }
        }
    } else {
        if (hyperVertex->m_min_out != INT_MAX) {
            // cout << hyperVertex->m_hyperId << " insert scc: min_value: " << min_value << " min_out: " << hyperVertex->m_min_out << endl;
            m_min2HyperVertex[combine(min_value, hyperVertex->m_min_out)].insert(hyperVertex);
        }
        hyperVertex->m_min_in = min_value;
        // 若min_in更新成功，且hyperVertex的out_edges存在记录
        if (!hyperVertex->m_out_edges.empty()) {
            // 依次遍历其中的节点v_out
            for (auto& v_out: hyperVertex->m_out_edges) {
                // 尝试更新v_out对应的hyperVertex的m_min_in
                if (min_value < v_out.first->m_min_in) {
                    // 递归更新
                    recursiveUpdate(v_out.first, min_value, type);
                } else {
                    return;
                }
            }
        }
    }
}

// 构建强连通分量的key，合并两个int为long long
long long minWRollback::combine(int a, int b) {
    return a * 1000001LL + b;
}

/* 确定性回滚: 从hyperGraph中找到最小的回滚子事务集合
   状态: 待测试...
*/
void minWRollback::rollback() {
    // 遍历所有强连通分量
    for (auto scc : m_min2HyperVertex) {
        // cout << "scc: " << scc.first << endl;

// 可尝试使用fibonacci堆优化更新效率，带测试，看效果
        // 定义有序集合，以hyperVertex.m_cost为key从小到大排序
        set<HyperVertex::Ptr, cmp> pq;

        // 遍历强连通分量中每个超节点，计算scc超节点间边权
        calculateHyperVertexWeight(scc.second, pq);

        // printHyperGraph();

        // 输出pq
        cout << "init pq: " << endl;
        for (auto v : pq) {
            cout << "hyperId: " << v->m_hyperId << " cost: " << v->m_cost << " in_cost: " << v->m_in_cost << " out_cost: " << v->m_out_cost << endl;
        }

        // 贪心获取最小回滚代价的节点集
        GreedySelectVertex(scc.second, pq, minWRollbackTxs);
    }
}

/* 计算强连通分量中每个超节点间回滚代价和回滚子事务集
    1. 计算scc超节点间边权
    2. 计算每个超节点的最小回滚代价
    3. 将超节点放入优先队列
*/
void minWRollback::calculateHyperVertexWeight(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq) {
    for (auto& hyperVertex : scc) {
        // 遍历超节点的出边
        for (auto out_edge : hyperVertex->m_out_edges) {
            // 同属于一个scc
            if (scc.find(out_edge.first) != scc.cend()) {
                // 计算超节点间边权, 获得超节点出边回滚代价
                calculateVertexRollback(hyperVertex, out_edge.first, minw::EdgeType::OUT);
            }
        }
        // 计算m_out_cost
        int out_degree = 0;
        for (auto v : hyperVertex->m_out_allRB) {
            hyperVertex->m_out_cost += v->m_self_cost;
            out_degree += v->m_degree;
        }
        hyperVertex->m_out_cost /= out_degree;
        // 遍历超节点的入边
        for (auto in_edge : hyperVertex->m_in_edges) {
            // 同属于一个scc
            if (scc.find(in_edge.first) != scc.cend()) {
                // 计算超节点间边权, 获得超节点入边回滚代价
                calculateVertexRollback(hyperVertex, in_edge.first, minw::EdgeType::IN); 
            }
        }
        // 计算m_in_cost
        int in_degree = 0;
        for (auto v : hyperVertex->m_in_allRB) {
            hyperVertex->m_in_cost += v->m_self_cost;
            in_degree += v->m_degree;
        }
        hyperVertex->m_in_cost /= in_degree;
        // 计算超节点的最小回滚代价
        if (hyperVertex->m_in_cost < hyperVertex->m_out_cost) {
            hyperVertex->m_rollback_type = minw::EdgeType::IN;
            hyperVertex->m_cost = hyperVertex->m_in_cost;
        } else {
            hyperVertex->m_rollback_type = minw::EdgeType::OUT;
            hyperVertex->m_cost = hyperVertex->m_out_cost;
        }
        
        pq.insert(hyperVertex);
    }
}

/* 计算两个超节点间的总回滚子事务集
    1. 判断边类型
    2. 判断是否已经存在回滚集
        2.1 若存在则直接返回
    3. 若不存在则统计超节点间回滚子事务集
    4. 记录节点间回滚子事务集
*/
void minWRollback::calculateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, minw::EdgeType type) {
    double min_cost = 0;
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> rollbackVertexs;
    
    if (type == minw::EdgeType::OUT) {
        // 如果已经存在边权，则直接返回
        if (hv1->m_out_rollback.find(hv2) != hv1->m_out_rollback.end()) {
            return;
        }

        // 逐边判断两个超节点hv1和hv2间的最小回滚代价 => 获取最终回滚子事务集
        calculateEdgeRollback(hv1->m_out_edges[hv2], min_cost, rollbackVertexs);

        // // 记录边权
        // hv1->m_out_weights[hv2] = min_cost;
        // hv2->m_in_weights[hv1] = min_cost;

        // 记录回滚子事务
        hv1->m_out_rollback[hv2] = rollbackVertexs;
        hv1->m_out_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        hv2->m_in_rollback[hv1] = rollbackVertexs;
        hv2->m_in_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        
    } else {
        // 如果已经存在边权，则直接返回
        if (hv1->m_in_rollback.find(hv2) != hv1->m_in_rollback.end()) {
            return;
        }
        
        // 逐边判断两个超节点hv1和hv2间的最小回滚代价 => 获取最终回滚代价和回滚子事务集
        calculateEdgeRollback(hv1->m_in_edges[hv2], min_cost, rollbackVertexs);

        // // 记录边权
        // hv1->m_in_weights[hv2] = min_cost;
        // hv2->m_out_weights[hv1] = min_cost;

        // 记录回滚子事务
        hv1->m_in_rollback[hv2] = rollbackVertexs;
        hv1->m_in_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        hv2->m_out_rollback[hv1] = rollbackVertexs;
        hv2->m_out_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
    }
}

/* 遍历边集，统计两超节点间一条边的回滚子事务集
    1. 计算边起点tailV的回滚权重
    2. 计算边终点headV的回滚权重
    3. 获取min(tailV回滚权重，headV回滚权重), 并更新总回滚代价和度
    4. 记录回滚子事务
*/
void minWRollback::calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges,
                                        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex) {
    // 遍历每条边
    for (auto& edge : edges) {
        // 初始化边起点和终点的回滚代价和度
        double tailV_weight = 0, headV_weight = 0;
        int tailV_degree = 0, headV_degree = 0;

        // 起点节点tailV
        auto tailV = edge.first;
        if (rollbackVertex.find(tailV) != rollbackVertex.cend()) {
            continue;
        }

        // 统计边起点tailV的度
// 可对比差集版本，需测试看效率，后续版本优化
        auto vertexs = tailV->cascadeVertices;
        for (auto& v : vertexs) {
            tailV_degree += v->m_degree;
        }
/* 效率有提高吗？待测试 
        // 并行计算边起点tailV的度
        tailV_degree = tbb::parallel_reduce(
            tbb::blocked_range<size_t>(0, vertexs.size()), 0,
            [&](const tbb::blocked_range<size_t>& r, int init) -> int {
                for (size_t i = r.begin(); i != r.end(); ++i) {
                    auto it = vertexs.begin();
                    std::advance(it, i);
                    init += (*it)->m_degree;
                }
                return init;
            },
            std::plus<int>()
        );
*/

        // 计算边起点tailV的回滚权重
        tailV_weight = tailV->m_cost / tailV_degree;

        // 尾部节点s
        auto headVs = edge.second;
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> headVSet;
        for (auto& headV : headVs) {
            // 判断边起点和终点是否已经在回滚子事务集中
            if (rollbackVertex.find(headV) != rollbackVertex.cend()) {
                continue;
            }

            headVSet.insert(headV->cascadeVertices.cbegin(), headV->cascadeVertices.cend());            
        }
        
        // 尾部节点都在回滚子事务集中
        if (headVSet.empty()) {
            continue;
        }

        for (auto& v : headVSet) {
            // 计算边终点headV的回滚代价
            headV_weight += v->m_self_cost;
            // 统计边终点headV的度
            headV_degree += v->m_degree;
        }

        // 计算边终点headV的回滚权重
        headV_weight /= headV_degree;
        
        // 比较tailV回滚代价和headV回滚代价
        if (tailV_weight < headV_weight) {
            // 记录回滚子事务
            rollbackVertex.insert(tailV->cascadeVertices.cbegin(), tailV->cascadeVertices.cend());
        } else {
            // 记录回滚子事务
            rollbackVertex.insert(headVSet.cbegin(), headVSet.cend());
        }
    }
}

/* old version of calculateEdgeRollback
*/
void minWRollback::calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, double& weight, 
                                        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex) {
    // 记录全局回滚代价和度
    double total_cost = 0;
    int total_degree = 0;

    // 遍历每条边
    for (auto& edge : edges) {
        // 初始化边起点和终点的回滚代价和度
        double tailV_cost = 0, headV_cost = 0, tailV_weight = 0, headV_weight = 0;
        int tailV_degree = 0, headV_degree = 0;

        // 起点节点
        auto tailV = edge.first;
        if (rollbackVertex.find(tailV) != rollbackVertex.cend()) {
            continue;
        }

        // 获取边起点tailV的回滚代价
        tailV_cost = tailV->m_cost;
        // 统计边起点tailV的度
// 可对比差集版本，需测试看效率，后续版本优化
        auto vertexs = tailV->cascadeVertices;
        for (auto& v : vertexs) {
            tailV_degree += v->m_degree;
        }
/* 效率有提高吗？待测试 
        // 并行计算边起点tailV的度
        tailV_degree = tbb::parallel_reduce(
            tbb::blocked_range<size_t>(0, vertexs.size()), 0,
            [&](const tbb::blocked_range<size_t>& r, int init) -> int {
                for (size_t i = r.begin(); i != r.end(); ++i) {
                    auto it = vertexs.begin();
                    std::advance(it, i);
                    init += (*it)->m_degree;
                }
                return init;
            },
            std::plus<int>()
        );
*/

        // 计算边起点tailV的回滚权重
        tailV_weight = tailV_cost / tailV_degree;


        // 尾部节点s
        auto headVs = edge.second;
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> headVSet;
        for (auto& headV : headVs) {
            // 判断边起点和终点是否已经在回滚子事务集中
            if (rollbackVertex.find(headV) != rollbackVertex.cend()) {
                continue;
            }

            headVSet.insert(headV->cascadeVertices.cbegin(), headV->cascadeVertices.cend());            
        }
        
        // 尾部节点都在回滚子事务集中
        if (headVSet.empty()) {
            continue;
        }

        for (auto& v : headVSet) {
            // 计算边终点headV的回滚代价
            headV_cost += v->m_self_cost;
            // 统计边终点headV的度
            headV_degree += v->m_degree;
        }

        // 计算边终点headV的回滚权重
        headV_weight = headV_cost / headV_degree;
        
        // 比较tailV回滚代价和headV回滚代价
        if (tailV_weight < headV_weight) {
            // 更新总回滚代价和度
            total_cost += tailV_cost;
            total_degree += tailV_degree;
            // 记录回滚子事务
            rollbackVertex.insert(tailV->cascadeVertices.cbegin(), tailV->cascadeVertices.cend());
        } else {
            // 更新总回滚代价和度
            total_cost += headV_cost;
            total_degree += headV_degree;
            // 记录回滚子事务
            rollbackVertex.insert(headVSet.cbegin(), headVSet.cend());
        }
        
        // 计算边权
        weight = total_cost / total_degree;
    }
}


// 计算两个集合的差集
tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> minWRollback::diff(
            const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& cascadeVertices, 
            const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex) {
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> diff;
    
    tbb::parallel_for(tbb::blocked_range<size_t>(0, cascadeVertices.size()), [&](const tbb::blocked_range<size_t>& r) {
        for (size_t i = r.begin(); i != r.end(); ++i) {
            auto it = cascadeVertices.begin();
            std::advance(it, i);
            if (rollbackVertex.find(*it) == rollbackVertex.end()) {
                diff.insert(*it);
            }
        }
    });
    return diff;
}

/* 回滚最小代价的超节点
    1. 选择回滚代价最小的超节点
    2. 更新scc中与它相邻的超节点的回滚代价
    3. 若存在一条边只有出度或入度，则递归更新
    4. 更新scc
*/
void minWRollback::GreedySelectVertex(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, 
                                      set<HyperVertex::Ptr, cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result) {
    /* 1. 选择回滚代价最小的超节点
        1.1 取出优先队列队头超节点rb
        1.2 判断是否在scc中
        1.3 若不存在则pop，直到找到一个在scc中的超节点
    */
    // 拿到pq第一个元素
    auto rb = *pq.begin();

    /* 2. 更新scc
        2.1 拿出优先队列队头超节点rb，回滚该笔事务对应的（子）事务
        2.2 遍历与该超节点直接依赖的超节点di并更新回滚代价
            2.1 若它不存在出边或入边，则从scc中删除
                2.2.1 递归更新相关超节点回滚代价
            2.2 若它存在出边或入边，则更新回滚代价
        2.3 从scc中删除该超节点rb
    */
    // 记录rb需要回滚的（子）事务
    if (rb->m_rollback_type == minw::EdgeType::OUT) {
        result.insert(rb->m_out_allRB.begin(), rb->m_out_allRB.end());
    } else {
        result.insert(rb->m_in_allRB.begin(), rb->m_in_allRB.end());
    }
    // 递归更新scc超节点和scc中依赖节点状态
    updateSCCandDependency(scc, rb, pq, result);

    /* 3. 递归贪心回滚节点
    */
    if (scc.size() > 1) {
        // 递归调用GreedySelectVertex获取回滚节点集合
        GreedySelectVertex(scc, pq, result);
    }
}

/*  递归更新scc超节点和scc中依赖节点状态
        1. 从scc中删除rb
        2. 遍历rb在scc中的出边节点，更新对应超节点的权重与依赖关系
        3. 遍历rb在scc中的入边节点，更新对应超节点的权重与依赖关系
    状态: 待测试...
*/
void minWRollback::updateSCCandDependency(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, 
                                            set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs) {
    // if (scc.find(rb) == scc.cend()) {
    //     return;
    // }

    cout << "delete: " << rb->m_hyperId << endl;
    // 从scc中删除rb
    scc.unsafe_erase(rb);
    // 若scc中只剩下一个超节点，则返回
    if (scc.size() <= 1) {
        return;
    }
    pq.erase(rb);

    // 记录遍历过的要更新的超节点
    tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> waitToUpdate;

    // 记录要删除的超节点
    tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> waitToDelete;

    // 记录更新过的边
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> udVertexs;


    // 遍历rb在scc中的出边节点，更新对应超节点的权重与依赖关系
    for (auto out_edge : rb->m_out_edges) {
        // 记录出边超节点
        auto out_vertex = out_edge.first;
        // 超节点在scc中
        if (scc.find(out_vertex) != scc.cend()) {            
            // 标记是否更新过后不存在入边
            bool canDelete = true;
            // 遍历目标超节点的所有入边
            auto& in_edges = out_vertex->m_in_edges;
            for (auto in_edge : in_edges) {
                // 记录出边超节点的入边超节点
                auto& in_vertex = in_edge.first;
                // 仍然存在入边，则更新超节点信息
                if (scc.find(in_vertex) != scc.cend()) {
                    // 更新超节点入边回滚子事务
                    for (auto& vertex : rb->m_out_rollback[out_vertex]) {
                        out_vertex->m_in_allRB.unsafe_erase(vertex);
                    }
                    // tbb::parallel_for_each(rb->m_out_rollback[out_vertex].begin(), rb->m_out_rollback[out_vertex].end(), [&](const Vertex::Ptr& vertex) {
                    //     out_vertex->m_in_allRB.unsafe_erase(vertex);
                    // });


                    // 遍历并记录这个超节点out_vertex与rb存在依赖的子事务udVertexs，更新子事务的度
                    for (auto& out_edges : rb->m_out_edges[out_vertex]) {
                        for (auto& udVertex : out_edges.second) {
                            udVertex->m_degree--;
                            udVertexs.insert(udVertex);
                        }
                    }

                    // // 更新超节点权重与回滚子事务集合
                    // updateHyperVertexWeight(scc, out_vertex, pq, udVertexs, rbVertexs);
                    
                    // 等待统一更新
                    waitToUpdate.insert(out_vertex);

                    /*
                    // 更新超节点入边回滚代价
                    out_vertex->m_in_cost -= out_vertex->m_in_weights[rb];
                    // 更新超节点入边回滚子事务
                    out_vertex->m_in_allRB.unsafe_erase(rb->m_out_rollback[out_vertex].begin(), rb->m_out_rollback[out_vertex].end());
                    // 判断整体回滚代价是否更新
                    if (out_vertex->m_in_cost < out_vertex->m_cost) {
                        // 更新超节点回滚代价和回滚类型
                        out_vertex->m_cost = out_vertex->m_in_cost;
                        out_vertex->m_rollback_type = minw::EdgeType::IN;
                        // 更新pq优先级
                        pq.erase(out_vertex);
                        pq.insert(out_vertex);
                    }*/
                    // 标记
                    canDelete = false;
                    break;
                }
            }
            // 不存在入边，则删除该超节点，递归更新scc
            if (canDelete) {
                cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                waitToDelete.insert(out_vertex);
                // 递归更新
                // updateSCCandDependency(scc, out_vertex, pq, rbVertexs);
            }
        }
    }
    // 遍历rb在scc中的入边节点，更新对应超节点的权重与依赖关系
    for (auto in_edge : rb->m_in_edges) {
        // 记录入边超节点
        auto in_vertex = in_edge.first;
        // 不需要删除且在scc中
        if (waitToDelete.find(in_vertex) == waitToDelete.cend() && scc.find(in_vertex) != scc.cend()) {
            // 标记是否更新过后不存在出边
            bool canDelete = true;
            // 遍历目标超节点的所有出边
            auto& out_edges = in_vertex->m_out_edges;
            for (auto out_edge : out_edges) {
                // 记录入边超节点的出边超节点
                auto& out_vertex = out_edge.first;
                // 仍然存在出边，则更新超节点信息
                if (scc.find(out_vertex) != scc.cend()) {
                    // 更新超节点出边回滚子事务
                    for (auto& vertex : rb->m_in_rollback[in_vertex]) {
                        in_vertex->m_out_allRB.unsafe_erase(vertex);
                    }
                    // tbb::parallel_for_each(rb->m_in_rollback[in_vertex].begin(), rb->m_in_rollback[in_vertex].end(), [&](const Vertex::Ptr& vertex) {
                    //     in_vertex->m_out_allRB.unsafe_erase(vertex);
                    // });

                    
                    for (auto& in_edges : rb->m_in_edges[in_vertex]) {
                        for (auto& udVertex : in_edges.second) {
                            udVertex->m_degree--;
                            udVertexs.insert(udVertex);
                        }
                    }

                    // // 更新超节点权重与回滚子事务集合
                    // updateHyperVertexWeight(scc, in_vertex, pq, udVertexs, rbVertexs);

                    // 等待统一更新
                    waitToUpdate.insert(in_vertex);

                    /*
                    // 更新超节点出边回滚代价
                    in_vertex->m_out_cost -= in_vertex->m_out_weights[rb];
                    // 更新超节点出边回滚子事务
                    in_vertex->m_out_allRB.unsafe_erase(rb->m_in_rollback[in_vertex].begin(), rb->m_in_rollback[in_vertex].end());
                    // 判断整体回滚代价是否更新
                    if (in_vertex->m_out_cost < in_vertex->m_cost) {
                        // 更新超节点回滚代价和回滚类型
                        in_vertex->m_cost = in_vertex->m_out_cost;
                        in_vertex->m_rollback_type = minw::EdgeType::OUT;
                        // 更新pq优先级
                        pq.erase(in_vertex);
                        pq.insert(in_vertex);
                    }*/
                    // 标记
                    canDelete = false;
                    break;
                }
            }
            // 不存在出边，则删除该超节点，递归更新scc
            if (canDelete) {
                cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                waitToDelete.insert(in_vertex);

                // // 递归更新
                // updateSCCandDependency(scc, in_vertex, pq, rbVertexs);
            }
        }
    }

    // 定义变量记录更新过的超节点间的边
    tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> calculated;
    // 更新超节点权重
    for (auto& v : waitToUpdate) {
        updateHyperVertexWeight(scc, v, rb, pq, udVertexs, rbVertexs, calculated);
    }

    // 删除超节点（如果存在的话）
    for (auto& v : waitToDelete) {
        updateSCCandDependency(scc, v, pq, rbVertexs);
    }

    // 遍历pq
    cout << "now pq: " << endl;
    for (auto v : pq) {
        cout << "hyperId: " << v->m_hyperId << " cost: " << v->m_cost << " in_cost: " << v->m_in_cost << " out_cost: " << v->m_out_cost << endl;
    }
}

/* 由于超节点中某些子事务的度数发生更新，尝试更新相关超节点回滚代价
    遍历超节点与其它在scc中的超节点间的回滚子事务集
    若与udVertexs有交集，则尝试更新这两个超节点间的回滚代价
    边回滚集合要更新，总回滚集合要更新，超节点回滚代价要更新, 存在原本需要回滚的已经回滚了的现象
*/
void minWRollback::updateHyperVertexWeight(const tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, HyperVertex::Ptr& hyperVertex, const HyperVertex::Ptr& rb, 
                                            set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& udVertexs, 
                                            const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated) {
    // 记录更新过回滚集的超节点
    tbb::concurrent_unordered_map<HyperVertex::Ptr, minw::EdgeType, HyperVertex::HyperVertexHash> updated;
    bool inUpdated = false;
    bool outUpdated = false;

    // 遍历出边需要回滚的子事务
    for (auto& rollbackSet : hyperVertex->m_out_rollback) {
        // 判断是否是当前删除的超节点
        if (rollbackSet.first == rb) {
            // 更新超节点回滚代价
            outUpdated = true;
            continue;
        // 判断是否在scc中 && 已经被计算过
        } else if (scc.find(rollbackSet.first) == scc.cend() || calculated.find(rollbackSet.first) != calculated.cend()) {
            continue;
        // 判断是否与udVertexs有交集
        } else if (hasIntersection(rollbackSet.second, udVertexs)) {
            // 更新超节点间边权
            if(updateVertexRollback(hyperVertex, rollbackSet.first, rbVertexs, minw::EdgeType::OUT)) {
                // 记录需要重新计算权重的超节点
                updated.insert(std::make_pair(rollbackSet.first, minw::EdgeType::IN));
                outUpdated = true;
            }
        }
    }
    
    // 遍历入边需要回滚的子事务
    for (auto& rollbackSet : hyperVertex->m_in_rollback) {
        if (rollbackSet.first == rb) {
            // 更新超节点回滚代价
            inUpdated = true;
            continue;
        // 判断是否在scc中 && 已经被计算过
        } else if (calculated.find(rollbackSet.first) != calculated.cend() || scc.find(rollbackSet.first) == scc.cend()) {
            continue;
        // 判断是否与udVertexs有交集
        } else if (hasIntersection(rollbackSet.second, udVertexs)) {
            // 更新超节点间边权
            if(updateVertexRollback(hyperVertex, rollbackSet.first, rbVertexs, minw::EdgeType::IN)) {
                // 记录需要重新计算权重的超节点
                if (updated.count(rollbackSet.first) == 0) {
                    updated.insert(std::make_pair(rollbackSet.first, minw::EdgeType::OUT));
                } else {
                    updated.at(rollbackSet.first) = minw::EdgeType::BOTH;
                }
                inUpdated = true;
            }
        }
    }

    // 若存在需要更新的超节点，则增加本节点并更新权重
    if (inUpdated || outUpdated) {
        minw::EdgeType type;
        if (inUpdated && outUpdated) {
            type = minw::EdgeType::BOTH;
        } else if (inUpdated) {
            type = minw::EdgeType::IN;
        } else {
            type = minw::EdgeType::OUT;
        }
        updated.insert(std::make_pair(hyperVertex, type));
        
        // 更新节点权重
        for (auto& v : updated) {
            auto copy = v.first;
            pq.erase(copy);
            calculateWeight(copy, v.second);
            pq.insert(copy);
        }

        // // updated较小时这个方法可能并不比串行方法快
        // tbb::parallel_for_each(updated.begin(), updated.end(),
        //     [&](HyperVertex::Ptr& vertex) {
        //         calculateWeight(vertex);
        //     }
        // );
    }
    calculated.insert(hyperVertex);
}

// 计算超节点权重m_cost
void minWRollback::calculateWeight(HyperVertex::Ptr& hyperVertex, minw::EdgeType& type) {
    // cout << "======= now hyperVertex " << hyperVertex->m_hyperId << " is calculating weight =======" << endl;
    
    if (type == minw::EdgeType::OUT) {
        // 计算m_out_cost
        int out_degree = 0;
        hyperVertex->m_out_cost = 0;
        for (auto v : hyperVertex->m_out_allRB) {
            hyperVertex->m_out_cost += v->m_self_cost;
            out_degree += v->m_degree;
        }
        
        // cout << "out weight: " << hyperVertex->m_out_cost << " out degree: " << out_degree << endl;

        hyperVertex->m_out_cost /= out_degree;
        
        // 更新超节点回滚代价
        if (hyperVertex->m_out_cost < hyperVertex->m_cost) {
            hyperVertex->m_rollback_type = minw::EdgeType::OUT;
            hyperVertex->m_cost = hyperVertex->m_out_cost;
        }
    } else if (type == minw::EdgeType::IN) {
        // 计算m_in_cost
        int in_degree = 0;
        hyperVertex->m_in_cost = 0;
        for (auto v : hyperVertex->m_in_allRB) {
            hyperVertex->m_in_cost += v->m_self_cost;
            in_degree += v->m_degree;
        }

        // cout << "in weight: " << hyperVertex->m_in_cost << " in degree: " << in_degree << endl;

        hyperVertex->m_in_cost /= in_degree;

        // 更新超节点回滚代价
        if (hyperVertex->m_in_cost < hyperVertex->m_cost) {
            hyperVertex->m_rollback_type = minw::EdgeType::IN;
            hyperVertex->m_cost = hyperVertex->m_in_cost;
        }
    } else {
        // 计算m_out_cost
        int out_degree = 0;
        hyperVertex->m_out_cost = 0;
        for (auto v : hyperVertex->m_out_allRB) {
            hyperVertex->m_out_cost += v->m_self_cost;
            out_degree += v->m_degree;
        }
        hyperVertex->m_out_cost /= out_degree;
        
        // 计算m_in_cost
        int in_degree = 0;
        hyperVertex->m_in_cost = 0;
        for (auto v : hyperVertex->m_in_allRB) {
            hyperVertex->m_in_cost += v->m_self_cost;
            in_degree += v->m_degree;
        }
        hyperVertex->m_in_cost /= in_degree;
        
        // 计算超节点的最小回滚代价
        if (hyperVertex->m_in_cost < hyperVertex->m_out_cost) {
            hyperVertex->m_rollback_type = minw::EdgeType::IN;
            hyperVertex->m_cost = hyperVertex->m_in_cost;
        } else {
            hyperVertex->m_rollback_type = minw::EdgeType::OUT;
            hyperVertex->m_cost = hyperVertex->m_out_cost;
        }
    }
}

// 比较两个concurrent_unordered_set是否相等
bool areEqual(const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& set1,
              const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& set2) {
    if (set1.size() != set2.size()) {
        return false;
    }

    tbb::atomic<bool> result = true;

    tbb::parallel_for_each(set1.begin(), set1.end(),
        [&](const Vertex::Ptr& vertex) {
            if (set2.find(vertex) == set2.end()) {
                result = false;
            }
        }
    );

    return result;
}

/* 尝试更新超节点间回滚事务集
    1. 判断边类型
    2. 判断是否需要更新
    3. 更新回滚事务集
        3.1 先删除allRB中的原有值
        3.2 再更新边回滚集合
        3.3 再向allRB中添加新值
*/
bool minWRollback::updateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, minw::EdgeType type) {
    double min_cost = 0;
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> rollbackVertexs;
    
    if (type == minw::EdgeType::OUT) {
        // 尝试更新两个超节点hv1和hv2间的最小回滚代价 => 获取最终回滚子事务集
        updateEdgeRollback(hv1->m_out_edges[hv2], rollbackVertexs, rbVertexs);

        // 如果rollbackVertexs和原来的回滚子事务集合相等，则不更新
        if (areEqual(rollbackVertexs, hv1->m_out_rollback[hv2])) {
            return false;
        }

        // 更新回滚子事务
        // 先删除
        // hv1->m_out_allRB.unsafe_erase(hv1->m_out_rollback[hv2].begin(), hv1->m_out_rollback[hv2].end());
        for (auto& vertex : hv1->m_out_rollback[hv2]) {
            hv1->m_out_allRB.unsafe_erase(vertex);
        }
        // tbb::parallel_for_each(hv1->m_out_rollback[hv2].begin(), hv1->m_out_rollback[hv2].end(), [&](const Vertex::Ptr& vertex) {
        //     hv1->m_out_allRB.unsafe_erase(vertex);
        // });

        // 再添加
        // hv1->m_out_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        for (auto& vertex : rollbackVertexs) {
            hv1->m_out_allRB.insert(vertex);
        }
        // tbb::parallel_for_each(rollbackVertexs.cbegin(), rollbackVertexs.cend(), [&](const Vertex::Ptr& vertex) {
        //     hv1->m_out_allRB.insert(vertex);
        // });

        // 再更新
        hv1->m_out_rollback[hv2] = rollbackVertexs;


        // 一样的操作
        // hv2->m_in_allRB.unsafe_erase(hv2->m_in_rollback[hv1].begin(), hv2->m_in_rollback[hv1].end());
        for (auto& vertex : hv2->m_in_rollback[hv1]) {
            hv2->m_in_allRB.unsafe_erase(vertex);
        }
        // tbb::parallel_for_each(hv2->m_in_rollback[hv1].begin(), hv2->m_in_rollback[hv1].end(), [&](const Vertex::Ptr& vertex) {
        //     hv2->m_in_allRB.unsafe_erase(vertex);
        // });

        // hv2->m_in_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        for (auto& vertex : rollbackVertexs) {
            hv2->m_in_allRB.insert(vertex);
        }
        // tbb::parallel_for_each(rollbackVertexs.cbegin(), rollbackVertexs.cend(), [&](const Vertex::Ptr& vertex) {
        //     hv2->m_in_allRB.insert(vertex);
        // });

        hv2->m_in_rollback[hv1] = rollbackVertexs;

    } else {
        // 尝试更新两个超节点hv1和hv2间的最小回滚代价 => 获取最终回滚代价和回滚子事务集
        updateEdgeRollback(hv1->m_in_edges[hv2], rollbackVertexs, rbVertexs);

        // 如果rollbackVertexs和原来的回滚子事务集合相等，则不更新
        if (areEqual(rollbackVertexs, hv1->m_in_rollback[hv2])) {
            return false;
        }

        // 更新回滚子事务
        // hv1->m_in_allRB.unsafe_erase(hv1->m_in_rollback[hv2].begin(), hv1->m_in_rollback[hv2].end());
        for (auto& vertex : hv1->m_in_rollback[hv2]) {
            hv1->m_in_allRB.unsafe_erase(vertex);
        }
        // tbb::parallel_for_each(hv1->m_in_rollback[hv2].begin(), hv1->m_in_rollback[hv2].end(), [&](const Vertex::Ptr& vertex) {
        //     hv1->m_in_allRB.unsafe_erase(vertex);
        // });

        // hv1->m_in_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        for (auto& vertex : rollbackVertexs) {
            hv1->m_in_allRB.insert(vertex);
        }
        // tbb::parallel_for_each(rollbackVertexs.cbegin(), rollbackVertexs.cend(), [&](const Vertex::Ptr& vertex) {
        //     hv1->m_in_allRB.insert(vertex);
        // });

        hv1->m_in_rollback[hv2] = rollbackVertexs;
        
        // hv2->m_out_allRB.unsafe_erase(hv2->m_out_rollback[hv1].begin(), hv2->m_out_rollback[hv1].end());
        for (auto& vertex : hv2->m_out_rollback[hv1]) {
            hv2->m_out_allRB.unsafe_erase(vertex);
        }
        // tbb::parallel_for_each(hv2->m_out_rollback[hv1].begin(), hv2->m_out_rollback[hv1].end(), [&](const Vertex::Ptr& vertex) {
        //     hv2->m_out_allRB.unsafe_erase(vertex);
        // });

        // hv2->m_out_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        for (auto& vertex : rollbackVertexs) {
            hv2->m_out_allRB.insert(vertex);
        }
        // tbb::parallel_for_each(rollbackVertexs.cbegin(), rollbackVertexs.cend(), [&](const Vertex::Ptr& vertex) {
        //     hv2->m_out_allRB.insert(vertex);
        // });

        hv2->m_out_rollback[hv1] = rollbackVertexs;
    }
    return true;
}

/* 更新两超节点间一条边的回滚子事务集
*/
void minWRollback::updateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges,
                                      tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbacked) {
    // 遍历每条边
    for (auto& edge : edges) {
        // 初始化边起点和终点的回滚代价和度
        double tailV_weight = 0, headV_weight = 0;
        int tailV_degree = 0, headV_degree = 0;

        // 起点节点tailV
        auto tailV = edge.first;
        // 判断边起点是否已经在回滚子事务集中或者已经被回滚！
        if (rollbacked.find(tailV) != rollbacked.cend() || rollbackVertex.find(tailV) != rollbackVertex.cend()) {
            continue;
        }

        // 统计边起点tailV的度
        auto vertexs = tailV->cascadeVertices;
        // for (auto& v : vertexs) {
        //     if (rollbacked.find(v) != rollbacked.cend()) {
        //         continue;
        //     }
        //     tailV_weight += v->m_self_cost;
        //     tailV_degree += v->m_degree;
        // }

        tbb::combinable<double> tailV_weight_comb;
        tbb::combinable<int> tailV_degree_comb;

        tbb::parallel_for_each(vertexs.begin(), vertexs.end(), [&](const Vertex::Ptr& v) {
            if (rollbacked.find(v) == rollbacked.cend()) {
                tailV_weight_comb.local() += v->m_self_cost;
                tailV_degree_comb.local() += v->m_degree;
            }
        });

        tailV_weight = tailV_weight_comb.combine(std::plus<double>());
        tailV_degree = tailV_degree_comb.combine(std::plus<int>());

        // 计算边起点tailV的回滚权重
        tailV_weight /= tailV_degree;

        // 尾部节点s
        auto headVs = edge.second;
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> headVSet;
        for (auto& headV : headVs) {
            // 判断边起点和终点是否已经在回滚子事务集中
            if (rollbacked.find(headV) != rollbacked.cend() || rollbackVertex.find(headV) != rollbackVertex.cend()) {
                continue;
            }

            headVSet.insert(headV->cascadeVertices.cbegin(), headV->cascadeVertices.cend());            
        }
        
        // 尾部节点都在回滚子事务集中
        if (headVSet.empty()) {
            continue;
        }

        // for (auto& v : headVSet) {
        //     if (rollbacked.find(v) != rollbacked.cend()) {
        //         continue;
        //     }
        //     // 计算边终点headV的回滚代价
        //     headV_weight += v->m_self_cost;
        //     // 统计边终点headV的度
        //     headV_degree += v->m_degree;
        // }
        
        tbb::combinable<double> headV_weight_comb;
        tbb::combinable<int> headV_degree_comb;

        tbb::parallel_for_each(headVSet.begin(), headVSet.end(), [&](const Vertex::Ptr& v) {
            if (rollbacked.find(v) == rollbacked.cend()) {
                headV_weight_comb.local() += v->m_self_cost;
                headV_degree_comb.local() += v->m_degree;
            }
        });

        headV_weight = headV_weight_comb.combine(std::plus<double>());
        headV_degree = headV_degree_comb.combine(std::plus<int>());

        // 计算边终点headV的回滚权重
        headV_weight /= headV_degree;
        
        // 比较tailV回滚代价和headV回滚代价
        if (tailV_weight < headV_weight) {
            // 记录回滚子事务
            rollbackVertex.insert(tailV->cascadeVertices.cbegin(), tailV->cascadeVertices.cend());
        } else {
            // 记录回滚子事务
            rollbackVertex.insert(headVSet.cbegin(), headVSet.cend());
        }
    }
}

// 打印超图
void minWRollback::printHyperGraph() {
    cout << "====================HyperGraph now====================" << endl;
    for (auto& hyperVertex : m_hyperVertices) {
        // 输出超节点回滚代价
        cout << "HyperVertex: " << hyperVertex->m_hyperId << " min_in: " << hyperVertex->m_min_in << " min_out: " << hyperVertex->m_min_out << endl;
        // 输出超节点包含的所有节点
        cout << "Vertexs: ";
        for (auto& vertex : hyperVertex->m_vertices) {
            cout << vertex->m_id << " ";
        }
        cout << endl;
        // 输出超节点的出边和入边
        cout << "==========Edges==========" << endl;
        for (auto& out_edge : hyperVertex->m_out_edges) {
            cout << "OutEdge: " << out_edge.first->m_hyperId << " ";
            for (auto& edge : out_edge.second) {
                for (auto& out_edge : edge.second) {
                    cout << edge.first->m_id << " -> " << out_edge->m_id << " ";
                }
            }
            cout << endl;
        }
        for (auto& in_edge : hyperVertex->m_in_edges) {
            cout << "InEdge: " << in_edge.first->m_hyperId << " ";
            for (auto& edge : in_edge.second) {
                for (auto& in_edge : edge.second) {
                    cout << edge.first->m_id << " <- " << in_edge->m_id << " ";
                }
            }
            cout << endl;
        }
        // 输出超节点的出边和入边权重
        cout << "==========Weights==========" << endl;
        cout << "m_cost: " << hyperVertex->m_cost << " m_out_cost: " << hyperVertex->m_out_cost << " m_in_cost: " << hyperVertex->m_in_cost << endl; 
        for (auto& out_weight : hyperVertex->m_out_weights) {
            cout << "OutWeight: " << out_weight.first->m_hyperId << " " << out_weight.second << endl;
        }
        for (auto& in_weight : hyperVertex->m_in_weights) {
            cout << "InWeight: " << in_weight.first->m_hyperId << " " << in_weight.second << endl;
        }
        // 输出超节点的出边和入边回滚子事务
        cout << "==========Rollback==========" << endl;
        for (auto& out_rollback : hyperVertex->m_out_rollback) {
            cout << "OutRollback: " << out_rollback.first->m_hyperId << " ";
            for (auto& rollback : out_rollback.second) {
                cout << rollback->m_id << " ";
            }
            cout << endl;
        }
        for (auto& in_rollback : hyperVertex->m_in_rollback) {
            cout << "InRollback: " << in_rollback.first->m_hyperId << " ";
            for (auto& rollback : in_rollback.second) {
                cout << rollback->m_id << " ";
            }
            cout << endl;
        }
        // 输出超节点的所有出边和入边回滚子事务
        cout << "==========AllRollback==========" << endl;
        cout << "OutAllRollback: ";
        for (auto& rollback : hyperVertex->m_out_allRB) {
            cout << rollback->m_id << " ";
        }
        cout << endl;
        cout << "InAllRollback: ";
        for (auto& rollback : hyperVertex->m_in_allRB) {
            cout << rollback->m_id << " ";
        }
        cout << endl;
        cout << "========================== end =========================" << endl;
    }
}

// 输出回滚子事务
void minWRollback::printRollbackTxs() {
    cout << "====================Rollback Transactions====================" << endl;
    cout << "total size: " << minWRollbackTxs.size() << endl;
    for (auto& tx : minWRollbackTxs) {
        cout << tx->m_id << endl;
    }
}