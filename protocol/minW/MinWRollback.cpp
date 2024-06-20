#include <algorithm>
#include <tbb/tbb.h>
#include <boost/heap/fibonacci_heap.hpp>
#include "MinWRollback.h"
#include <iostream>
#include <chrono>
#include <future>
#include "protocol/common.h"


using namespace std;

/*  执行算法: 将transaction转化为hyperVertex
    详细算法流程: 
        1. 多线程并发执行所有事务，获得事务执行信息（读写集，事务结构，执行时间 => 随机生成）
        2. 判断执行完成的事务与其它执行完成事务间的rw依赖，构建rw依赖图
    状态: 测试完成，待优化...
*/
void MinWRollback::execute(const Transaction::Ptr& tx, bool isNest) {
    int txid = getId();
    HyperVertex::Ptr hyperVertex = make_shared<HyperVertex>(txid);
    Vertex::Ptr rootVertex = make_shared<Vertex>(hyperVertex, txid, to_string(txid));
    // 根据事务结构构建超节点
    string txid_str = to_string(txid);
    
    if (isNest) {
        hyperVertex->buildVertexs(tx, hyperVertex, rootVertex, txid_str, m_invertedIndex);
        // 记录超节点包含的所有节点
        hyperVertex->m_vertices = rootVertex->cascadeVertices;
        // 根据子节点依赖更新回滚代价和级联子事务
        hyperVertex->m_rootVertex = rootVertex;
        hyperVertex->recognizeCascades(rootVertex);
    } else {
        hyperVertex->buildVertexs(tx, rootVertex, m_invertedIndex);
        // 添加回滚代价
        rootVertex->m_cost = rootVertex->m_self_cost;
        // 添加自己
        rootVertex->cascadeVertices.insert(rootVertex);

        hyperVertex->m_vertices.insert(rootVertex);
        hyperVertex->m_rootVertex = rootVertex;
    }
    
    // for test print hyperVertex tree
    // hyperVertex->printVertexTree();

    /*
    // 构建超图
    auto start = std::chrono::high_resolution_clock::now();
    buildGraph(rootVertex->cascadeVertices);
    auto end = std::chrono::high_resolution_clock::now();
    cout << "tx" << txid << " build time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    // 记录节点
    m_vertices.insert(rootVertex->cascadeVertices.begin(), rootVertex->cascadeVertices.end());
    */

    // 记录超节点
    m_hyperVertices.insert(hyperVertex);
}

void MinWRollback::onWarm() {
    // 利用倒排索引构建RWIndex
    for (auto& kv : m_invertedIndex) {
        auto& readTxs = kv.second.readSet;
        auto& writeTxs = kv.second.writeSet;
        for (auto& rTx : readTxs) {
            m_RWIndex[rTx].insert(writeTxs.begin(), writeTxs.end());
            m_RWIndex[rTx].erase(rTx);
        }
    }
}

/* 构建超图
*/
void MinWRollback::buildGraph() {
    edgeCounter = 0;

    for (auto& hyperVertex : m_hyperVertices) {
        auto rootVertex = hyperVertex->m_rootVertex;
        // cout << "tx" << hyperVertex->m_hyperId << " start build, m_vertices size: " << m_vertices.size() 
        //      << " cascadeVertices size: " << rootVertex->cascadeVertices.size() << endl;
        
        // auto start = std::chrono::high_resolution_clock::now();
        build(rootVertex->cascadeVertices);
        // auto end = std::chrono::high_resolution_clock::now();
       
        // 记录节点
        m_vertices.insert(rootVertex->cascadeVertices.begin(), rootVertex->cascadeVertices.end());
        
        // cout <<" build time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }

    return;
}

/* 索引构建超图 */
void MinWRollback::buildGraph2() {
    edgeCounter = 0;

    // // 遍历倒排索引
    // for (auto& kv : m_invertedIndex) {
    //     auto& readTxs = kv.second.readSet;
    //     auto& writeTxs = kv.second.writeSet;
    //     // 遍历读集
    //     for (auto& rTx : readTxs) {
    //         // 遍历读集
    //         for (auto& wTx : writeTxs) {
    //             if (rTx != wTx) {
    //                 // 构建超图
    //                 onRWC(rTx, wTx);
    //             }
    //         }
    //     }
    // }

    // 遍历rw索引
    for (auto& rTxs : m_RWIndex) {
        auto& rTx = rTxs.first;
        auto& wTxs = rTxs.second;
        for (auto& wTx : wTxs) {
            // 构建超图
            onRWC(rTx, wTx);
        }
    }
    return;
}

/* 基于倒排索引并发构图 */
void MinWRollback::buildGraphConcurrent(ThreadPool::Ptr Pool) {
    edgeCounter = 0;
    std::vector<std::future<void>> futures;

    // 将多个onRW的处理作为一个任务
    std::vector<std::pair<Vertex::Ptr, Vertex::Ptr>> rwPairs;
    // for (auto& kv : m_invertedIndex) {
    //     auto& readTxs = kv.second.readSet;
    //     auto& writeTxs = kv.second.writeSet;
    //     for (auto& rTx : readTxs) {
    //         for (auto& wTx : writeTxs) {
    //             if (rTx != wTx) {
    //                 // onRW(rTx, wTx);
    //                 rwPairs.emplace_back(rTx, wTx);
    //             }
    //         }
    //     }
    // }

    for (auto& rTxs : m_RWIndex) {
        auto& rTx = rTxs.first;
        auto& wTxs = rTxs.second;
        for (auto& wTx : wTxs) {
            // 构建超图
            rwPairs.emplace_back(rTx, wTx);
        }
    }

    size_t totalPairs = rwPairs.size();
    size_t chunkSize;
    if (minw::BLOCK_SIZE == 50) {
        chunkSize = 20;
    } else if (minw::BLOCK_SIZE == 100) {
        chunkSize = 50;
    } else {
        chunkSize = minw::BLOCK_SIZE / 2;
        // chunkSize = (totalPairs + CGRAPH_DEFAULT_THREAD_SIZE - 1) / (CGRAPH_DEFAULT_THREAD_SIZE * 1);
    }
    cout << "totalPairs: " << totalPairs << " chunkSize: " << chunkSize << endl;

    for (size_t i = 0; i < totalPairs; i += chunkSize) {
        futures.emplace_back(Pool->enqueue([this, &rwPairs, i, chunkSize, totalPairs] {
            size_t end = std::min(i + chunkSize, totalPairs);
            for (size_t j = i; j < end; ++j) {
                onRWC(rwPairs[j].first, rwPairs[j].second);
            }
        }));
    }

    // 等待所有任务完成
    for (auto &future : futures) {
        future.get();
    }

    // const auto& threadDurations = Pool->getThreadDurations();
    // const auto& taskCounts = Pool->getTaskCounts();
    // for (size_t i = 0; i < threadDurations.size(); ++i) {
    //     std::cout << "Thread " << i << " duration: " << (double)threadDurations[i].count() / 1000 << " ms "
    //               << "Task count: " << taskCounts[i] << std::endl;
    // }
}

void MinWRollback::buildGraphConcurrent(UThreadPoolPtr Pool) {
    edgeCounter = 0;
    std::vector<std::future<void>> futures;

    // 将多个onRW的处理作为一个任务
    std::vector<std::pair<Vertex::Ptr, Vertex::Ptr>> rwPairs;

    for (auto& rTxs : m_RWIndex) {
        auto& rTx = rTxs.first;
        auto& wTxs = rTxs.second;
        for (auto& wTx : wTxs) {
            // 构建超图
            rwPairs.emplace_back(rTx, wTx);
            // futures.emplace_back(Pool->commit([this, rTx, wTx] {
            //     onRWC(rTx, wTx);
            // }));
        }
    }

    size_t totalPairs = rwPairs.size();
    size_t chunkSize;
    if (minw::BLOCK_SIZE == 50) {
        chunkSize = 20;
    } else if (minw::BLOCK_SIZE == 100) {
        chunkSize = 50;
    } else {
        chunkSize = minw::BLOCK_SIZE / 2;
        // chunkSize = (totalPairs + CGRAPH_DEFAULT_THREAD_SIZE - 1) / (CGRAPH_DEFAULT_THREAD_SIZE * 1);
    }
    cout << "totalPairs: " << totalPairs << " chunkSize: " << chunkSize << endl;

    for (size_t i = 0; i < totalPairs; i += chunkSize) {
        futures.emplace_back(Pool->commit([this, &rwPairs, i, chunkSize, totalPairs] {
            size_t end = std::min(i + chunkSize, totalPairs);
            for (size_t j = i; j < end; ++j) {
                onRWC(rwPairs[j].first, rwPairs[j].second);
            }
        }));
    }

    // 等待所有任务完成
    for (auto &future : futures) {
        future.get();
    }
}

void MinWRollback::buildGraphConcurrent(threadpool::Ptr& Pool) {
    edgeCounter = 0;
    std::vector<std::future<void>> futures;

    // 将多个onRW的处理作为一个任务
    std::vector<std::pair<Vertex::Ptr, Vertex::Ptr>> rwPairs;

    for (auto& rTxs : m_RWIndex) {
        auto& rTx = rTxs.first;
        auto& wTxs = rTxs.second;
        for (auto& wTx : wTxs) {
            // 构建超图
            rwPairs.emplace_back(rTx, wTx);
        }
    }

    size_t totalPairs = rwPairs.size();
    size_t chunkSize;
    if (minw::BLOCK_SIZE == 50) {
        chunkSize = 20;
    } else if (minw::BLOCK_SIZE == 100) {
        chunkSize = 50;
    } else {
        chunkSize = minw::BLOCK_SIZE / 2;
        // chunkSize = (totalPairs + CGRAPH_DEFAULT_THREAD_SIZE - 1) / (CGRAPH_DEFAULT_THREAD_SIZE * 1);
    }

    cout << "totalPairs: " << totalPairs << " chunkSize: " << chunkSize << endl;

    for (size_t i = 0; i < totalPairs; i += chunkSize) {
        futures.emplace_back(Pool->commit([this, &rwPairs, i, chunkSize, totalPairs] {
            size_t end = std::min(i + chunkSize, totalPairs);
            for (size_t j = i; j < end; ++j) {
                onRWC(rwPairs[j].first, rwPairs[j].second);
            }
        }));
    }

    // 等待所有任务完成
    for (auto &future : futures) {
        future.get();
    }

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
void MinWRollback::build(unordered_set<Vertex::Ptr, Vertex::VertexHash>& vertices) {
    
    for (auto& newV: vertices) {
        for (auto& oldV: m_vertices) {
            // 获取超节点
            auto& newHyperVertex = newV->m_hyperVertex;
            auto& oldHyperVertex = oldV->m_hyperVertex;
            int newHyperIdx = newV->m_hyperId;
            int oldHyperIdx = oldV->m_hyperId;

            // 存在rw依赖
            if (protocol::hasConflict(newV->readSet, oldV->writeSet)) {
                edgeCounter++;
                
                // 更新newV对应的hyperVertex的out_edges
                // handleNewEdge(newV, oldV, newHyperVertex->m_out_edges[oldHyperVertex]);
                newHyperVertex->m_out_hv.insert(oldHyperVertex);
                newHyperVertex->m_out_edges[oldHyperIdx].insert(oldV);

                // 更新依赖数
                newV->m_degree++;
                // 尝试更新newV对应的hyperVertex的min_out
                int min_out = min(oldHyperVertex->m_min_out, oldV->m_hyperId);
                if (min_out < newHyperVertex->m_min_out) {
                    recursiveUpdate(newHyperVertex, min_out, minw::EdgeType::OUT);
                }
                // 更新回滚集合
                newHyperVertex->m_out_rollback[oldHyperIdx].insert(newV->cascadeVertices.cbegin(), newV->cascadeVertices.cend());


                // 更新oldV对应的hyperVertex的in_edges
                // handleNewEdge(oldV, newV, oldHyperVertex->m_in_edges[newHyperVertex]);
                oldHyperVertex->m_in_hv.insert(newHyperVertex);
                oldHyperVertex->m_in_edges[newHyperIdx].insert(newV);
                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_in
                int min_in = min(newHyperVertex->m_min_in, newV->m_hyperId);
                if (min_in < oldHyperVertex->m_min_in) {
                    recursiveUpdate(oldHyperVertex, min_in, minw::EdgeType::IN);
                }
                // 更新回滚集合
                oldHyperVertex->m_in_allRB.insert(newV->cascadeVertices.cbegin(), newV->cascadeVertices.cend());
            }
            // 存在wr依赖
            if (protocol::hasConflict(newV->writeSet, oldV->readSet)) {
                edgeCounter++;

                // 更新newV对应的hyperVertex的in_edges
                // handleNewEdge(newV, oldV, newHyperVertex->m_in_edges[oldHyperVertex]);
                newHyperVertex->m_in_hv.insert(oldHyperVertex);
                newHyperVertex->m_in_edges[oldHyperIdx].insert(oldV);
                // 更新依赖数
                newV->m_degree++;
                // 尝试更新newV对应的hyperVertex的min_in
                int min_in = min(oldHyperVertex->m_min_in, oldV->m_hyperId);
                if (min_in < newHyperVertex->m_min_in) {
                    recursiveUpdate(newHyperVertex, min_in, minw::EdgeType::IN);
                }
                // 更新回滚集合
                newHyperVertex->m_in_allRB.insert(oldV->cascadeVertices.cbegin(), oldV->cascadeVertices.cend());


                // 更新oldV对应的hyperVertex的out_edges
                // handleNewEdge(oldV, newV, oldHyperVertex->m_out_edges[newHyperVertex]);
                oldHyperVertex->m_out_hv.insert(newHyperVertex);
                oldHyperVertex->m_out_edges[newHyperIdx].insert(newV);
                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_out
                int min_out = min(newHyperVertex->m_min_out, newV->m_hyperId);
                if (min_out < oldHyperVertex->m_min_out) {
                    recursiveUpdate(oldHyperVertex, min_out, minw::EdgeType::OUT);
                }
                // 更新回滚集合
                oldHyperVertex->m_out_rollback[newHyperIdx].insert(oldV->cascadeVertices.cbegin(), oldV->cascadeVertices.cend());
            }
        }
    }
}

void MinWRollback::onRWC(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx) {
    // 获取超节点
    auto& rHyperVertex = rTx->m_hyperVertex;
    auto& wHyperVertex = wTx->m_hyperVertex;
    auto rhvIdx = rTx->m_hyperId;
    auto whvIdx = wTx->m_hyperId;

    // // 若该rw依赖已经存在，则直接返回
    // if (rHyperVertex->m_out_map[whvIdx].count(wTx) && 
    //     wHyperVertex->m_in_map[rhvIdx].count(rTx)) {
    //     return;
    // }

    edgeCounter.fetch_add(1);

    // 更新newV对应的hyperVertex的out_edges
    rHyperVertex->m_out_hv.insert(wHyperVertex);
    rHyperVertex->m_out_edges[whvIdx].insert(wTx);

    // 更新依赖数
    rTx->m_degree++;
    // 更新回滚集合
    rHyperVertex->m_out_rollback[whvIdx].insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());

    // 更新oldV对应的hyperVertex的in_edges
    wHyperVertex->m_in_hv.insert(rHyperVertex);
    wHyperVertex->m_in_edges[rhvIdx].insert(rTx);
    // 更新依赖数
    wTx->m_degree++;
    // 更新回滚集合
    wHyperVertex->m_in_allRB.insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
}

void MinWRollback::onRW(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx) {
    // 获取超节点
    auto& rHyperVertex = rTx->m_hyperVertex;
    auto& wHyperVertex = wTx->m_hyperVertex;

    edgeCounter++;

    // 更新newV对应的hyperVertex的out_edges
    // rHyperVertex->m_out_edgesS[wHyperVertex].insert(wTx);
    // 能快好多
    rHyperVertex->m_out_hvS.insert(wHyperVertex);
    rHyperVertex->m_out_mapS[wTx->m_hyperId].insert(wTx);

    // 更新依赖数
    rTx->m_degree++;
    // 尝试更新newV对应的hyperVertex的min_out -- 100us
    int min_out = min(wHyperVertex->m_min_out, wTx->m_hyperId);
    if (min_out < rHyperVertex->m_min_out) {
        recursiveUpdate(rHyperVertex, min_out, minw::EdgeType::OUT);
    }
    // 更新回滚集合
    // rHyperVertex->m_out_rollbackS[wHyperVertex].insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
    rHyperVertex->m_out_rollbackMS[wTx->m_hyperId].insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());


    // 更新oldV对应的hyperVertex的in_edges -- 3ms
    // wHyperVertex->m_in_edgesS[rHyperVertex].insert(rTx);
    wHyperVertex->m_in_hvS.insert(rHyperVertex);
    wHyperVertex->m_in_mapS[rTx->m_hyperId].insert(rTx);

    // 更新依赖数
    wTx->m_degree++;
    // 尝试更新oldV对应的hyperVertex的min_in -- 100us
    int min_in = min(rHyperVertex->m_min_in, rTx->m_hyperId);
    if (min_in < wHyperVertex->m_min_in) {
        recursiveUpdate(wHyperVertex, min_in, minw::EdgeType::IN);
    }
    // 更新回滚集合 -- 1.77ms
    wHyperVertex->m_in_allRB.insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
}

/* 构图算法：遍历超节点
*/
void MinWRollback::build(Vertex::Ptr& rootVertex) {
    for (auto& hv : m_hyperVertices) {
        auto& vertex = hv->m_rootVertex;
        auto& parent = vertex;

        // 自上到下遍历已有超节点中的所有节点
        do {
            // // 比较已有节点和新节点间依赖
            // compare(vertex, rootVertex);
            // // 获得超节点下一个子事务
            // vertex = getNextVertex(parent, vertex);
        } while (vertex != nullptr);
        
    }
}

// 处理新边, 尝试将v2插入到v1为起点的边中
void MinWRollback::handleNewEdge(const Vertex::Ptr& v1, const Vertex::Ptr& v2, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges) {
    
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
bool MinWRollback::isAncester(const string& v1, const string& v2) {
    // 子孙节点id的前缀包含祖先节点id
    return v2.find(v1) == 0;
}

// 递归更新超节点min_in和min_out
void MinWRollback::recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, minw::EdgeType type) {
// 或者后续不在这里记录强连通分量，后续根据开销进行调整。。。

// 可以加个visited数组进行剪枝

    // cout << "Update hyperVertex: " << hyperVertex->m_hyperId << " min_value: " << min_value
    //      << " type: " << minw::edgeTypeToString(type) << endl;

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
        // 若min_out更新成功，依次遍历其中的节点v_in
        for (auto& v_in: hyperVertex->m_in_hv) {
            // 尝试更新v_in对应的hyperVertex的m_min_out 
            if (min_value < v_in->m_min_out) {
                // 递归更新
                recursiveUpdate(v_in, min_value, type);
            }
        }
    } else {
        if (hyperVertex->m_min_out != INT_MAX) {
            // cout << hyperVertex->m_hyperId << " insert scc: min_value: " << min_value << " min_out: " << hyperVertex->m_min_out << endl;
            m_min2HyperVertex[combine(min_value, hyperVertex->m_min_out)].insert(hyperVertex);
        }
        hyperVertex->m_min_in = min_value;
        // 若min_in更新成功，依次遍历其中的节点v_out
        for (auto& v_out: hyperVertex->m_out_hv) {
            // 尝试更新v_out对应的hyperVertex的m_min_in
            if (min_value < v_out->m_min_in) {
                // 递归更新
                recursiveUpdate(v_out, min_value, type);
            }
        }
        
    }
}

// 构建强连通分量的key，合并两个int为long long
long long MinWRollback::combine(int a, int b) {
    return a * 1000001LL + b;
}

/* 确定性回滚: 从hyperGraph中找到最小的回滚子事务集合
   状态: 测试完成，待优化...
*/
void MinWRollback::rollback(int mode) {
    // 遍历所有强连通分量
    for (auto hyperVertexs : m_min2HyperVertex) {
        
        if (hyperVertexs.second.size() <= 1) {
            continue;
        }

        cout << "hyperVertexs: " << hyperVertexs.first << ", size = " << hyperVertexs.second.size() << endl;

        // 拿到所有scc
        vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> sccs;
        if (!recognizeSCC(hyperVertexs.second, sccs)) {
            continue;
        }

        // 遍历scc回滚节点
        for (auto& scc : sccs) { 
            cout << "===== 输出scc中的每一个元素的详细信息 =====" << endl;
            cout << "size = " << scc.size() << ", all vertexs:"; 
            for (auto hv : scc) {
                cout << hv->m_hyperId << " ";
            }
            cout << endl;


            // 统计每个scc rollback耗时
            auto start = std::chrono::high_resolution_clock::now();

    // 可尝试使用fibonacci堆优化更新效率，带测试，看效果
            // 定义有序集合，以hyperVertex.m_cost为key从小到大排序
            set<HyperVertex::Ptr, cmp> pq;

            // 遍历强连通分量中每个超节点，计算scc超节点间边权
            calculateHyperVertexWeight(scc, pq);

            // // 输出pq
            // cout << "init pq: " << endl;
            // for (auto v : pq) {
            //     cout << "hyperId: " << v->m_hyperId << " cost: " << v->m_cost << " in_cost: " << v->m_in_cost << " out_cost: " << v->m_out_cost << endl;
            // }

            // 贪心获取最小回滚代价的节点集
            GreedySelectVertex(scc, pq, m_rollbackTxs, mode);
            auto end = std::chrono::high_resolution_clock::now();
            cout << "scc rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
        
        }
    }
}

/* 识别强连通分量
    1. 利用Tarjan算法识别强连通分量，并返回size大于1的强连通分量
*/
bool MinWRollback::recognizeSCC(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& hyperVertexs, vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>>& sccs) {
    int index = 0;
    stack<HyperVertex::Ptr> S;
    unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash> indices;
    unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash> lowlinks;
    unordered_map<HyperVertex::Ptr, bool, HyperVertex::HyperVertexHash> onStack;
    vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> components;

    for (const auto& hv : hyperVertexs) {
        if (indices.find(hv) == indices.end()) { //没有访问过
            strongconnect(hyperVertexs, hv, index, S, indices, lowlinks, onStack, components);
        }
    }

    // 只返回大小大于1的强连通分量
    for (const auto& component : components) {
        if (component.size() > 1) {
            sccs.push_back(component);
        }
    }

    return !sccs.empty();
}

void MinWRollback::strongconnect(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& hyperVertexs, const HyperVertex::Ptr& v, int& index, stack<HyperVertex::Ptr>& S, unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& indices,
                   unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& lowlinks, unordered_map<HyperVertex::Ptr, bool, HyperVertex::HyperVertexHash>& onStack,
                   vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>>& components) {
    indices[v] = lowlinks[v] = ++index;
    S.push(v);
    onStack[v] = true;

    for (const auto& w : v->m_out_hv) {
        if (hyperVertexs.find(w) == hyperVertexs.end()) {
            continue;
        }

        if (indices.find(w) == indices.end()) {
            strongconnect(hyperVertexs, w, index, S, indices, lowlinks, onStack, components);
            lowlinks[v] = min(lowlinks[v], lowlinks[w]);
        } else if (onStack[w]) {
            lowlinks[v] = min(lowlinks[v], indices[w]);
        }
    }

    if (lowlinks[v] == indices[v]) {
        unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> component;
        HyperVertex::Ptr w;
        do {
            w = S.top();
            S.pop();
            onStack[w] = false;
            component.insert(w);
        } while (w != v);
        components.push_back(component);
    }
}

/* 计算强连通分量中每个超节点间回滚代价和回滚子事务集
    1. 计算scc超节点间边权
    2. 计算每个超节点的最小回滚代价
    3. 将超节点放入优先队列
*/
void MinWRollback::calculateHyperVertexWeight(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq) {
    // 计算每个超节点的in和out边权重
    for (auto& hyperVertex : scc) {
        // 计算m_out_cost
        int out_degree = 0;
        // 遍历超节点的出边
        for (auto& out_hv : hyperVertex->m_out_hv) {
            // 同属于一个scc
            if (scc.find(out_hv) != scc.cend()) {
                // 统计m_out_allRB
                for (auto & v : hyperVertex->m_out_rollback[out_hv->m_hyperId]) {
                    // 如果之前没有这个节点，则新增map并设置值为1，否则值+1
                    if (hyperVertex->m_out_allRB.find(v) == hyperVertex->m_out_allRB.end()) {
                        hyperVertex->m_out_allRB[v] = 1;
                        hyperVertex->m_out_cost += v->m_self_cost;
                        out_degree += v->m_degree;
                    } else {
                        hyperVertex->m_out_allRB[v]++;
                    }
                }
            }
        }
        hyperVertex->m_out_cost /= out_degree;
        

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

/* 计算两个超节点间的总回滚子事务集 —— 不需要再算了，因为已经只会abort出边节点，因此回滚集早已确定
    1. 判断边类型
    2. 判断是否已经存在回滚集
        2.1 若存在则直接返回
    3. 若不存在则统计超节点间回滚子事务集
    4. 记录节点间回滚子事务集

void MinWRollback::calculateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, minw::EdgeType type) {
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> rollbackVertexs;
    
    if (type == minw::EdgeType::OUT) {
        // 如果已经存在边权，则直接返回
        if (hv1->m_out_rollback.find(hv2) != hv1->m_out_rollback.end()) {
            return;
        }

        // 逐边判断两个超节点hv1和hv2间的最小回滚代价 => 获取最终回滚子事务集
        calculateEdgeRollback(hv1->m_out_edges[hv2], rollbackVertexs);

        // 记录回滚子事务
        hv1->m_out_rollback[hv2] = rollbackVertexs;
        hv2->m_in_rollback[hv1] = rollbackVertexs;

        for (auto & v : rollbackVertexs) {
            // 如果之前没有这个节点，则新增map并设置值为1，否则值+1
            if (hv1->m_out_allRB.find(v) == hv1->m_out_allRB.end()) {
                hv1->m_out_allRB[v] = 1;
            } else {
                hv1->m_out_allRB[v]++;
            }

            if (hv2->m_in_allRB.find(v) == hv2->m_in_allRB.end()) {
                hv2->m_in_allRB[v] = 1;
            } else {
                hv2->m_in_allRB[v]++;
            }
        }
        // hv1->m_out_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        // hv2->m_in_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        
    } else {
        // 如果已经存在边权，则直接返回
        if (hv1->m_in_rollback.find(hv2) != hv1->m_in_rollback.end()) {
            return;
        }
        
        // 逐边判断两个超节点hv1和hv2间的最小回滚代价 => 获取最终回滚代价和回滚子事务集
        calculateEdgeRollback(hv1->m_in_edges[hv2], rollbackVertexs);

        // 记录回滚子事务
        hv1->m_in_rollback[hv2] = rollbackVertexs;
        hv2->m_out_rollback[hv1] = rollbackVertexs;
        for (auto& v : rollbackVertexs) {
            // 如果之前没有这个节点，则新增map并设置值为1，否则值+1
            if (hv1->m_in_allRB.find(v) == hv1->m_in_allRB.end()) {
                hv1->m_in_allRB[v] = 1;
            } else {
                hv1->m_in_allRB[v]++;
            }

            if (hv2->m_out_allRB.find(v) == hv2->m_out_allRB.end()) {
                hv2->m_out_allRB[v] = 1;
            } else {
                hv2->m_out_allRB[v]++;
            }
        }
        // hv1->m_in_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        // hv2->m_out_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
    }
}
*/

/* 遍历边集，统计两超节点间一条边的回滚子事务集
    1. 计算边起点tailV的回滚权重
    2. 计算边终点headV的回滚权重
    3. 获取min(tailV回滚权重，headV回滚权重), 并更新总回滚代价和度
    4. 记录回滚子事务
*/
void MinWRollback::calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges,
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
        tailV_weight = (double)tailV->m_cost / tailV_degree;

        // cout << "tailV: " << tailV->m_id 
        //      << " tailV_cost: " << tailV->m_cost
        //      << " tailV_degree: " << tailV_degree 
        //      << " tailV_weight: " << tailV_weight << endl;



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
        
        // cout << "headV: " ;
        for (auto& v : headVSet) {
            // cout << v->m_id << ", ";
            // 计算边终点headV的回滚代价
            headV_weight += v->m_self_cost;
            // 统计边终点headV的度
            headV_degree += v->m_degree;
        }

        // cout << "headV_cost: " << headV_weight;

        // 计算边终点headV的回滚权重
        headV_weight /= headV_degree;

        
        // cout << " headV_degree: " << headV_degree 
        //      << " headV_weight: " << headV_weight << endl;

        
        // 比较tailV回滚代价和headV回滚代价
        if (tailV_weight == headV_weight) {
            // cout << "choose less subscript" << endl;
            if (tailV->m_id < (*headVs.begin())->m_id) {
                rollbackVertex.insert(tailV->cascadeVertices.cbegin(), tailV->cascadeVertices.cend());
            } else {
                rollbackVertex.insert(headVSet.cbegin(), headVSet.cend());
            }
        } else if(tailV_weight < headV_weight) {
            // cout << "choose tailV" << endl;
            // 记录回滚子事务
            rollbackVertex.insert(tailV->cascadeVertices.cbegin(), tailV->cascadeVertices.cend());
        } else {
            // cout << "choose headV" << endl;
            // 记录回滚子事务
            rollbackVertex.insert(headVSet.cbegin(), headVSet.cend());
        }
    }
}

/* old version of calculateEdgeRollback */
void MinWRollback::calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, double& weight, 
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

/* 回滚最小代价的超节点
    1. 选择回滚代价最小的超节点
    2. 更新scc中与它相邻的超节点的回滚代价
    3. 若存在一条边只有出度或入度，则递归更新
    4. 更新scc
*/
void MinWRollback::GreedySelectVertex(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result, int mode) {
    /* 1. 选择回滚代价最小的超节点
        1.1 取出优先队列队头超节点rb
        1.2 判断是否在scc中
        1.3 若不存在则pop，直到找到一个在scc中的超节点
    */
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
        // result.insert(rb->m_out_allRB.begin(), rb->m_out_allRB.end());
        std::transform(rb->m_out_allRB.begin(), rb->m_out_allRB.end(), std::inserter(result, result.end()), [](const std::pair<const Vertex::Ptr, int>& pair) {
            return pair.first;
        });
    } else {
        result.insert(rb->m_in_allRB.begin(), rb->m_in_allRB.end());
    }
    
    cout << "Greedy delete: " << rb->m_hyperId << endl;
    // 从scc中删除rb
    scc.erase(rb);
    pq.erase(rb);

    // // 打印result
    // cout << "result now: ";
    // for (auto v : result) {
    //     cout << v->m_id << " ";
    // }
    // cout << endl;

    if (mode == 0) {
        // 定义变量记录更新过的超节点间的边
        tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> calculated;
        // 递归更新scc超节点和scc中依赖节点状态
        updateSCCandDependency(scc, rb, pq, result, calculated);      // old version
    } else if (mode == 1) {
        updateSCCandDependency(scc, rb, pq, result);                    // new version
    }
    
    // 若scc中只剩下一个超节点，则无需回滚
    if (scc.size() > 1) {
        // 递归调用GreedySelectVertex获取回滚节点集合
        GreedySelectVertex(scc, pq, result, mode);
    }
}

/*  递归更新scc超节点和scc中依赖节点状态 -- old version
        1. 从scc中删除rb
        2. 遍历rb在scc中的出边节点，更新对应超节点的权重与依赖关系
        3. 遍历rb在scc中的入边节点，更新对应超节点的权重与依赖关系
    状态: 待修改逻辑...
*/
void MinWRollback::updateSCCandDependency(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, 
                                          set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs,
                                          tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated) {
    if (scc.size() <= 1) {
        return;
    }
    
    // cout << "In updateSCCandDependency " << rb->m_hyperId << " is deleted, now begin to update scc, "
    //      << "scc size = " << scc.size() << endl;
    

    // 记录遍历过的要更新的超节点
    tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> waitToUpdate;

    // 记录要删除的超节点
    tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> waitToDelete;

    // 记录更新过的边
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> udVertexs;


    // 遍历rb在scc中的出边节点，更新对应超节点的权重与依赖关系
    for (auto out_vertex : rb->m_out_hv) {
        // 记录出边超节点索引
        auto out_vertex_idx = out_vertex->m_hyperId;
        // 超节点在scc中
        if (scc.find(out_vertex) != scc.cend()) {            
            // 标记是否更新过后不存在入边
            bool canDelete = true;
            // 遍历目标超节点的所有入边
            auto& in_hvs = out_vertex->m_in_hv;
            for (auto in_hv : in_hvs) {
                // 仍然存在入边，则更新超节点信息
                if (scc.find(in_hv) != scc.cend()) {
                    // 标记
                    canDelete = false;

                    // 判断是否可以无需代价直接删除 —— 感觉可以删除
                    if (protocol::hasContain(rbVertexs, out_vertex->m_out_allRB) || 
                        protocol::hasContain(rbVertexs, out_vertex->m_in_allRB)) {
                        cout << "can delete without cost out loop: " << out_vertex->m_hyperId << endl;
                        scc.erase(out_vertex);
                        pq.erase(out_vertex);
                        waitToDelete.insert(out_vertex);
                        break;
                    }

                    // 更新超节点入边回滚子事务
                    for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                        // in_allRB不存在当前rollback节点还是与其它超节点间的rollback节点的情况，因此直接删除
                        out_vertex->m_in_allRB.unsafe_erase(vertex);
                    }

                    // tbb::parallel_for_each(rb->m_out_rollback[out_vertex].begin(), rb->m_out_rollback[out_vertex].end(), [&](const Vertex::Ptr& vertex) {
                    //     out_vertex->m_in_allRB.unsafe_erase(vertex);
                    // });

                    /* 如果rb是out类型的abort，那么需要更新度
                       如果rb是in类型的abort，那么也需要更新度
                       ==> 必须更新度
                    */
                    // 遍历并记录这个超节点out_vertex与rb存在依赖的子事务udVertexs，更新子事务的度
                    for (auto& udVertex : rb->m_out_edges[out_vertex_idx]) {
                        udVertex->m_degree--;
                        udVertexs.insert(udVertex);
                    }
                    
                    // 等待统一更新
                    waitToUpdate.insert(out_vertex);  
                    break;
                }
            }
            // 不存在入边，则删除该超节点，递归更新scc
            if (canDelete) {
                cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                waitToDelete.insert(out_vertex);
            }
        }
    }
    // 遍历rb在scc中的入边节点，更新对应超节点的权重与依赖关系
    for (auto in_vertex : rb->m_in_hv) {
        // 记录入边超节点索引
        auto in_vertex_idx = in_vertex->m_hyperId;
        // 不需要删除且在scc中
        if (waitToDelete.find(in_vertex) == waitToDelete.cend() && scc.find(in_vertex) != scc.cend()) {
            // 标记是否更新过后不存在出边
            bool canDelete = true;
            // 遍历目标超节点的所有出边
            auto& out_hvs = in_vertex->m_out_hv;
            for (auto out_hv : out_hvs) {                
                // 仍然存在出边，则更新超节点信息
                if (scc.find(out_hv) != scc.cend()) {
                    // 标记
                    canDelete = false;

                    // 判断 是否没有判断过 && 是否可以无需代价直接删除
                    if (waitToUpdate.find(in_vertex) == waitToUpdate.cend() && 
                        (protocol::hasContain(rbVertexs, in_vertex->m_out_allRB) || 
                         protocol::hasContain(rbVertexs, in_vertex->m_in_allRB))) {
                        cout << "can delete without cost in loop: " << in_vertex->m_hyperId << endl;
                        scc.erase(in_vertex);
                        pq.erase(in_vertex);
                        waitToDelete.insert(in_vertex);
                        break;
                    }
                    
                    
                    // 更新超节点出边回滚子事务
                    // 若rb是out类型的abort，则尝试删除
                    if (rb->m_rollback_type == minw::EdgeType::OUT) {
                        for (auto& vertex : rb->m_in_rollback[in_vertex_idx]) {
                            // 尝试删除
                            if (in_vertex->m_out_allRB[vertex] == 1) {
                                in_vertex->m_out_allRB.unsafe_erase(vertex);
                            } else {
                                in_vertex->m_out_allRB[vertex]--;
                            }
                        }
                        
                        // 若rb是out类型的abort，则更新度
                        for (auto& udVertex : rb->m_in_edges[in_vertex_idx]) {
                            udVertex->m_degree--;
                            udVertexs.insert(udVertex);
                        }
                    } else { // 若rb是in类型的abort，则直接删除
                        for (auto& vertex : rb->m_in_rollback[in_vertex_idx]) {    
                            in_vertex->m_out_allRB.unsafe_erase(vertex);
                        }
                        // 若rb是in类型的abort，则不更新度
                    }

                    // tbb::parallel_for_each(rb->m_in_rollback[in_vertex].begin(), rb->m_in_rollback[in_vertex].end(), [&](const Vertex::Ptr& vertex) {
                    //     in_vertex->m_out_allRB.unsafe_erase(vertex);
                    // });


                    // 等待统一更新
                    waitToUpdate.insert(in_vertex);
                    break;
                }
            }
            // 不存在出边，则删除该超节点，递归更新scc
            if (canDelete) {
                cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                waitToDelete.insert(in_vertex);
            }
        }
    }

    if (scc.size() > 1) {
        // 利用删除后的超节点继续更新超图
        for (auto& v : waitToDelete) {
            updateSCCandDependency(scc, v, pq, rbVertexs, calculated);
        }
        
        // 更新超节点权重
        for (auto& v : waitToUpdate) {
            // 如果还在scc中，且没有被更新过
            if (scc.find(v) != scc.cend() && calculated.find(v) == calculated.cend()) {
                updateHyperVertexWeight(scc, v, rb, pq, udVertexs, rbVertexs, calculated);
            }
        }

        // // 遍历并输出更新过后的pq
        // cout << "update weight without " << rb->m_hyperId << ", now pq: " << endl;
        // for (auto v : pq) {
        //     cout << "hyperId: " << v->m_hyperId << " cost: " << v->m_cost << " in_cost: " << v->m_in_cost << " out_cost: " << v->m_out_cost << endl;
        // }
        
    }
}


/*  递归更新scc超节点和scc中依赖节点状态 -- new version
    1. 删除所有要删除的节点
    2. 更新删除节点关联节点的度
    3. 重新计算相关节点的回滚代价
*/
void MinWRollback::updateSCCandDependency(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, 
                                          set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs) {
    if (scc.size() <= 1) {
        return;
    }
    
    // cout << "In updateSCCandDependency " << rb->m_hyperId << " is deleted, now begin to update scc, " << "scc size = " << scc.size() << endl;

    
    // 记录要更新的超节点
    unordered_map<HyperVertex::Ptr, minw::EdgeType, HyperVertex::HyperVertexHash> waitToUpdate;
    
    // 递归删除并获取可以直接删除的节点 并 更新删除超节点关联超节点的子节点的度
    recursiveDelete(scc, pq, rb, rbVertexs, waitToUpdate);

    // 重新计算相关节点的回滚代价
    if (scc.size() > 1) {
        for (auto& v : waitToUpdate) {
            // 如果还在scc中
            if (scc.find(v.first) != scc.cend()) {
                // 更新超节点权重
                auto updateV = v.first;
                pq.erase(updateV);
                calculateWeight(updateV, v.second);
                pq.insert(updateV);
            }
        }
    }
    
    // // 遍历并输出更新过后的pq
    // cout << "now pq: " << endl;
    // for (auto v : pq) {
    //     cout << "hyperId: " << v->m_hyperId << " cost: " << v->m_cost << " in_cost: " << v->m_in_cost << " out_cost: " << v->m_out_cost << endl;
    // }
}

/* 递归删除scc中可删除的节点 */
void MinWRollback::recursiveDelete(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq, const HyperVertex::Ptr& rb,
                                   const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, unordered_map<HyperVertex::Ptr, minw::EdgeType, HyperVertex::HyperVertexHash>& waitToUpdate) {
    // 递归删除出边超节点
    for (auto& out_vertex : rb->m_out_hv) {
        // 记录出边超节点索引
        auto out_vertex_idx = out_vertex->m_hyperId;
        // 超节点在scc中
        if (scc.find(out_vertex) != scc.cend()) {
            // 判断是否可以无需代价直接删除
            if (rb->m_rollback_type == minw::EdgeType::OUT && protocol::hasContain(rbVertexs, out_vertex->m_in_allRB)) {
                cout << "can delete without cost out loop: " << out_vertex->m_hyperId << endl;
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                recursiveDelete(scc, pq, out_vertex, rbVertexs, waitToUpdate);
            }

            // 可能存在与不属于scc的超节点的入边，因此不能直接确定无法删除
            // 因此，先标记是否更新过后不存在入边
            bool canDelete = true;
            // 遍历目标超节点的所有入边
            auto& in_hvs = out_vertex->m_in_hv;
            for (auto& in_hv : in_hvs) {
                // 确定了：确实仍然存在入边，因此不能删除
                if (scc.find(in_hv) != scc.cend()) {
                    canDelete = false;

                    // 更新事务回滚事务集
                    for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                        out_vertex->m_in_allRB.unsafe_erase(vertex);
                    }

                    /* 更新waitToUpdate
                        1. 如果waitToUpdate[out_vertex]不存在，则设置为IN
                        2. 如果waitToUpdate[out_vertex]为OUT，则设置为BOTH
                        3. 如果waitToUpdate[out_vertex]为IN，则不更新
                        4. 如果waitToUpdate[out_vertex]为BOTH，则不更新
                    */
                    if (waitToUpdate.find(out_vertex) == waitToUpdate.cend()) {
                        waitToUpdate[out_vertex] = minw::EdgeType::IN;
                    } else if (waitToUpdate[out_vertex] == minw::EdgeType::OUT) {
                        waitToUpdate[out_vertex] = minw::EdgeType::BOTH;
                    }

                    // 更新依赖子事务(入)度
                    for (auto& udVertex : rb->m_out_edges[out_vertex_idx]) {
                        // // 只修改回滚集合相关边的度？
                        // if (out_vertex->m_in_allRB.find(udVertex) != out_vertex->m_in_allRB.cend()) {
                        //     udVertex->m_degree--;
                        // }
                        udVertex->m_degree--;   
                    }
                    
                    break;
                }
            }
            // 确定了：确实不存在入边，删除该超节点，递归更新scc
            if (canDelete) {
                cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                recursiveDelete(scc, pq, out_vertex, rbVertexs, waitToUpdate);
            }
        }
    }
    // 递归删除入边超节点
    for (auto& in_vertex : rb->m_in_hv) {
        // 记录入边超节点索引
        auto in_vertex_idx = in_vertex->m_hyperId;
        // 超节点在scc中
        if (scc.find(in_vertex) != scc.cend()) {
            // 判断是否可以无需代价直接删除 —— 可以删除
            if (rb->m_rollback_type == minw::EdgeType::IN && protocol::hasContain(rbVertexs, in_vertex->m_out_allRB)) {
                cout << "can delete without cost in loop: " << in_vertex->m_hyperId << endl;
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                recursiveDelete(scc, pq, in_vertex, rbVertexs, waitToUpdate);
            }
            
            // 可能存在与不属于scc的超节点的入边，因此不能直接确定无法删除
            // 因此，先标记是否更新过后不存在出边
            bool canDelete = true;
            // 遍历目标超节点的所有出边
            auto& out_hvs = in_vertex->m_out_hv;
            for (auto& out_hv : out_hvs) {
                // 确定了：确实仍然存在出边，因此不能删除
                if (scc.find(out_hv) != scc.cend()) {
                    canDelete = false; // 标记
                    
                    // 更新事务回滚事务集
                    if (rb->m_rollback_type == minw::EdgeType::OUT) {
                        bool needUpdate = false;
                        for (auto& vertex : rb->m_in_rollback[in_vertex_idx]) {
                            // 若value为1，代表只有本节点和该节点依赖，可直接删除
                            if (in_vertex->m_out_allRB[vertex] == 1) {
                                in_vertex->m_out_allRB.unsafe_erase(vertex);
                                needUpdate = true;
                            } else {
                                in_vertex->m_out_allRB[vertex]--;
                            }
                        }

                        // 更新依赖子事务(出)度
                        for (auto& udVertex : rb->m_in_edges[in_vertex_idx]) {
                            if (in_vertex->m_out_allRB.find(udVertex) != in_vertex->m_out_allRB.cend()) {
                                udVertex->m_degree--;
                                // 若度修改了，则需要更新权重
                                needUpdate = true;
                            }
                        }

                        /* 更新waitToUpdate
                            1. 如果waitToUpdate[in_vertex]不存在，则设置为OUT
                            2. 如果waitToUpdate[in_vertex]为IN，则设置为BOTH
                            3. 如果waitToUpdate[in_vertex]为OUT，则不更新
                            4. 如果waitToUpdate[in_vertex]为BOTH，则不更新
                        */
                        if (needUpdate) {
                            if (waitToUpdate.find(in_vertex) == waitToUpdate.cend()) {
                                waitToUpdate[in_vertex] = minw::EdgeType::OUT;
                            } else if (waitToUpdate[in_vertex] == minw::EdgeType::IN) {
                                waitToUpdate[in_vertex] = minw::EdgeType::BOTH;
                            }
                        }
                    } else {
                        for (auto& vertex : rb->m_in_rollback[in_vertex_idx]) {
                            in_vertex->m_out_allRB.unsafe_erase(vertex);
                        }
                        if (waitToUpdate.find(in_vertex) == waitToUpdate.cend()) {
                            waitToUpdate[in_vertex] = minw::EdgeType::OUT;
                        } else if (waitToUpdate[in_vertex] == minw::EdgeType::IN) {
                            waitToUpdate[in_vertex] = minw::EdgeType::BOTH;
                        }
                    }
                    
                    break;
                }
            }
            // 确定了：不存在出边，则删除该超节点，递归更新scc
            if (canDelete) {
                cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                recursiveDelete(scc, pq, in_vertex, rbVertexs, waitToUpdate);
            }

            /* 上面这段循环存在并行优化的可能 */
        }
    
    }
}

/* 由于超节点中某些子事务的度数发生更新，尝试更新相关超节点回滚代价
    遍历超节点与其它在scc中的超节点间的回滚子事务集
    若与udVertexs有交集，则尝试更新这两个超节点间的回滚代价
    边回滚集合要更新，总回滚集合要更新，超节点回滚代价要更新, 存在原本需要回滚的已经回滚了的现象
*/
void MinWRollback::updateHyperVertexWeight(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, HyperVertex::Ptr& hyperVertex, const HyperVertex::Ptr& rb, 
                                           set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& udVertexs, 
                                           const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs,
                                           tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated) {
    // cout << "want to update hyperVertex " << hyperVertex->m_hyperId << " and related hyperVertex" << endl;
        
    // 记录更新过回滚集的超节点
    tbb::concurrent_unordered_map<HyperVertex::Ptr, minw::EdgeType, HyperVertex::HyperVertexHash> updated;

    // 遍历出边需要回滚的子事务
    for (auto& out_hv : hyperVertex->m_out_hv) {
        auto& rollbackSet = hyperVertex->m_out_rollback[out_hv->m_hyperId];
        // 判断是否在scc中 && 已经被计算过
        if (scc.find(out_hv) == scc.cend() || calculated.find(out_hv) != calculated.cend()) {
            continue;
        // 判断是否与udVertexs有交集
        } else if (protocol::hasIntersection(rollbackSet, udVertexs)) {
            // 记录需要重新计算权重的超节点
            updated.insert(std::make_pair(out_hv, minw::EdgeType::IN));
        }
    }
    
    // cout << "hyperId: " << hyperVertex->m_hyperId << " inUpdated: " << inUpdated << " outUpdated: " << outUpdated << endl;
    // cout << "updated: ";
    // for (auto& v : updated) {
    //     cout  << v.first->m_hyperId << " type: " << minw::edgeTypeToString(v.second) << " | ";
    // }
    // cout << endl;


    // 增加本节点并更新权重
    updated.insert(std::make_pair(hyperVertex, minw::EdgeType::BOTH));
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
    
    calculated.insert(hyperVertex);
}

// 计算超节点权重m_cost
void MinWRollback::calculateWeight(HyperVertex::Ptr& hyperVertex, minw::EdgeType& type) {
    // cout << "======= now hyperVertex " << hyperVertex->m_hyperId << " is calculating weight =======" << endl;

    if (type == minw::EdgeType::OUT) {
        // 计算m_out_cost
        int out_degree = 0;
        hyperVertex->m_out_cost = 0;
        for (auto v : hyperVertex->m_out_allRB) {
            hyperVertex->m_out_cost += v.first->m_self_cost;
            out_degree += v.first->m_degree;
        }
        
        // cout << "out weight: " << hyperVertex->m_out_cost << " out degree: " << out_degree << endl;

        hyperVertex->m_out_cost /= out_degree;
        
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

    } else {
        // 计算m_out_cost
        int out_degree = 0;
        hyperVertex->m_out_cost = 0;
        for (auto v : hyperVertex->m_out_allRB) {
            hyperVertex->m_out_cost += v.first->m_self_cost;
            out_degree += v.first->m_degree;
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
    }

    // 更新超节点回滚代价
    if (hyperVertex->m_in_cost < hyperVertex->m_out_cost) {
        hyperVertex->m_rollback_type = minw::EdgeType::IN;
        hyperVertex->m_cost = hyperVertex->m_in_cost;
    } else {
        hyperVertex->m_rollback_type = minw::EdgeType::OUT;
        hyperVertex->m_cost = hyperVertex->m_out_cost;
    }
}

/* 尝试更新超节点间回滚事务集 —— 不再需要了
    1. 判断边类型
    2. 判断是否需要更新
    3. 更新回滚事务集
        3.1 先删除allRB中的原有值
        3.2 再更新边回滚集合
        3.3 再向allRB中添加新值

bool MinWRollback::updateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, minw::EdgeType type) {
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> rollbackVertexs;
    
    if (type == minw::EdgeType::OUT) {
        // 尝试更新两个超节点hv1和hv2间的最小回滚代价 => 获取最终回滚子事务集
        updateEdgeRollback(hv1->m_out_edges[hv2], rollbackVertexs, rbVertexs);

        // 如果rollbackVertexs和原来的回滚子事务集合相等，则不更新
        if (protocol::areEqual(rollbackVertexs, hv1->m_out_rollback[hv2])) {
            return false;
        }

        // 更新回滚子事务
        // 先删除
        // hv1->m_out_allRB.unsafe_erase(hv1->m_out_rollback[hv2].begin(), hv1->m_out_rollback[hv2].end());
        for (auto& vertex : hv1->m_out_rollback[hv2]) {
            // hv1->m_out_allRB.unsafe_erase(vertex);
            if (hv1->m_out_allRB[vertex] == 1) {
                hv1->m_out_allRB.unsafe_erase(vertex);
            } else {
                hv1->m_out_allRB[vertex]--;
            }
        }
        // tbb::parallel_for_each(hv1->m_out_rollback[hv2].begin(), hv1->m_out_rollback[hv2].end(), [&](const Vertex::Ptr& vertex) {
        //     hv1->m_out_allRB.unsafe_erase(vertex);
        // });

        // 再添加
        // hv1->m_out_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        for (auto& vertex : rollbackVertexs) {
            if (hv1->m_out_allRB.find(vertex) == hv1->m_out_allRB.end()) {
                hv1->m_out_allRB[vertex] = 1;
            } else {
                hv1->m_out_allRB[vertex]++;
            }
        }
        // tbb::parallel_for_each(rollbackVertexs.cbegin(), rollbackVertexs.cend(), [&](const Vertex::Ptr& vertex) {
        //     hv1->m_out_allRB.insert(vertex);
        // });

        // 再更新
        hv1->m_out_rollback[hv2] = rollbackVertexs;


        // 一样的操作
        // hv2->m_in_allRB.unsafe_erase(hv2->m_in_rollback[hv1].begin(), hv2->m_in_rollback[hv1].end());
        for (auto& vertex : hv2->m_in_rollback[hv1]) {
            // hv2->m_in_allRB.unsafe_erase(vertex);
            if (hv2->m_in_allRB[vertex] == 1) {
                hv2->m_in_allRB.unsafe_erase(vertex);
            } else {
                hv2->m_in_allRB[vertex]--;
            }
        }
        // tbb::parallel_for_each(hv2->m_in_rollback[hv1].begin(), hv2->m_in_rollback[hv1].end(), [&](const Vertex::Ptr& vertex) {
        //     hv2->m_in_allRB.unsafe_erase(vertex);
        // });

        // hv2->m_in_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        for (auto& vertex : rollbackVertexs) {
            if (hv2->m_in_allRB.find(vertex) == hv2->m_in_allRB.end()) {
                hv2->m_in_allRB[vertex] = 1;
            } else {
                hv2->m_in_allRB[vertex]++;
            }
        }
        // tbb::parallel_for_each(rollbackVertexs.cbegin(), rollbackVertexs.cend(), [&](const Vertex::Ptr& vertex) {
        //     hv2->m_in_allRB.insert(vertex);
        // });

        hv2->m_in_rollback[hv1] = rollbackVertexs;

    } else {
        // 尝试更新两个超节点hv1和hv2间的最小回滚代价 => 获取最终回滚代价和回滚子事务集
        updateEdgeRollback(hv1->m_in_edges[hv2], rollbackVertexs, rbVertexs);

        // 如果rollbackVertexs和原来的回滚子事务集合相等，则不更新
        if (protocol::areEqual(rollbackVertexs, hv1->m_in_rollback[hv2])) {
            return false;
        }

        // 更新回滚子事务
        // hv1->m_in_allRB.unsafe_erase(hv1->m_in_rollback[hv2].begin(), hv1->m_in_rollback[hv2].end());
        for (auto& vertex : hv1->m_in_rollback[hv2]) {
            if (hv1->m_in_allRB[vertex] == 1) {
                hv1->m_in_allRB.unsafe_erase(vertex);
            } else {
                hv1->m_in_allRB[vertex]--;
            }
        }
        // tbb::parallel_for_each(hv1->m_in_rollback[hv2].begin(), hv1->m_in_rollback[hv2].end(), [&](const Vertex::Ptr& vertex) {
        //     hv1->m_in_allRB.unsafe_erase(vertex);
        // });

        // hv1->m_in_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        for (auto& vertex : rollbackVertexs) {
            if (hv1->m_in_allRB.find(vertex) == hv1->m_in_allRB.end()) {
                hv1->m_in_allRB[vertex] = 1;
            } else {
                hv1->m_in_allRB[vertex]++;
            }
        }
        // tbb::parallel_for_each(rollbackVertexs.cbegin(), rollbackVertexs.cend(), [&](const Vertex::Ptr& vertex) {
        //     hv1->m_in_allRB.insert(vertex);
        // });

        hv1->m_in_rollback[hv2] = rollbackVertexs;
        
        // hv2->m_out_allRB.unsafe_erase(hv2->m_out_rollback[hv1].begin(), hv2->m_out_rollback[hv1].end());
        for (auto& vertex : hv2->m_out_rollback[hv1]) {
            if (hv2->m_out_allRB[vertex] == 1) {
                hv2->m_out_allRB.unsafe_erase(vertex);
            } else {
                hv2->m_out_allRB[vertex]--;
            }
        }
        // tbb::parallel_for_each(hv2->m_out_rollback[hv1].begin(), hv2->m_out_rollback[hv1].end(), [&](const Vertex::Ptr& vertex) {
        //     hv2->m_out_allRB.unsafe_erase(vertex);
        // });

        // hv2->m_out_allRB.insert(rollbackVertexs.cbegin(), rollbackVertexs.cend());
        for (auto& vertex : rollbackVertexs) {
            if (hv2->m_out_allRB.find(vertex) == hv2->m_out_allRB.end()) {
                hv2->m_out_allRB[vertex] = 1;
            } else {
                hv2->m_out_allRB[vertex]++;
            }
        }
        // tbb::parallel_for_each(rollbackVertexs.cbegin(), rollbackVertexs.cend(), [&](const Vertex::Ptr& vertex) {
        //     hv2->m_out_allRB.insert(vertex);
        // });

        hv2->m_out_rollback[hv1] = rollbackVertexs;
    }
    return true;
}
*/

/* 更新两超节点间一条边的回滚子事务集 —— 不再需要了

void MinWRollback::updateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges,
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
        if (tailV_weight == headV_weight) {
            if (tailV->m_id < (*headVs.begin())->m_id) {
                rollbackVertex.insert(tailV->cascadeVertices.cbegin(), tailV->cascadeVertices.cend());
            } else {
                rollbackVertex.insert(headVSet.cbegin(), headVSet.cend());
            }
        } else if (tailV_weight < headV_weight) {
            // 记录回滚子事务
            rollbackVertex.insert(tailV->cascadeVertices.cbegin(), tailV->cascadeVertices.cend());
        } else {
            // 记录回滚子事务
            rollbackVertex.insert(headVSet.cbegin(), headVSet.cend());
        }
    }
}
*/

// 打印超图
void MinWRollback::printHyperGraph() {
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
        for (auto& out_hv : hyperVertex->m_out_hv) {
            auto out_edges = hyperVertex->m_out_edges[out_hv->m_hyperId];
            cout << "OutEdge: " << out_hv->m_hyperId << ", size: " << out_edges.size() << " => ";
            for (auto& edge : out_edges) {
                cout << edge->m_id << " ";
            }
            cout << endl;
        }
        for (auto& in_hv : hyperVertex->m_in_hv) {
            auto in_edges = hyperVertex->m_in_edges[in_hv->m_hyperId];
            cout << "InEdge: " << in_hv->m_hyperId << ", size: " << in_edges.size() << " => ";
            for (auto& edge : in_edges) {
                cout << edge->m_id << " ";
            }
            cout << endl;
        }
        // // 输出超节点的出边和入边权重
        // cout << "==========Weights==========" << endl;
        // cout << "m_cost: " << hyperVertex->m_cost << " m_out_cost: " << hyperVertex->m_out_cost << " m_in_cost: " << hyperVertex->m_in_cost << endl; 
        // for (auto& out_weight : hyperVertex->m_out_weights) {
        //     cout << "OutWeight: " << out_weight.first->m_hyperId << " " << out_weight.second << endl;
        // }
        // for (auto& in_weight : hyperVertex->m_in_weights) {
        //     cout << "InWeight: " << in_weight.first->m_hyperId << " " << in_weight.second << endl;
        // }
        // // 输出超节点的出边和入边回滚子事务
        // cout << "==========Rollback==========" << endl;
        // for (auto& out_rollback : hyperVertex->m_out_rollback) {
        //     cout << "OutRollback: " << out_rollback.first->m_hyperId << " ";
        //     for (auto& rollback : out_rollback.second) {
        //         cout << rollback->m_id << " ";
        //     }
        //     cout << endl;
        // }
        // for (auto& in_rollback : hyperVertex->m_in_rollback) {
        //     cout << "InRollback: " << in_rollback.first->m_hyperId << " ";
        //     for (auto& rollback : in_rollback.second) {
        //         cout << rollback->m_id << " ";
        //     }
        //     cout << endl;
        // }
        // // 输出超节点的所有出边和入边回滚子事务
        // cout << "==========AllRollback==========" << endl;
        // cout << "OutAllRollback: ";
        // for (auto& rollback : hyperVertex->m_out_allRB) {
        //     cout << rollback.first->m_id << " ";
        // }
        // cout << endl;
        // cout << "InAllRollback: ";
        // for (auto& rollback : hyperVertex->m_in_allRB) {
        //     cout << rollback.first->m_id << " ";
        // }
        // cout << endl;
        // cout << "========================== end =========================" << endl;
    
        if (hyperVertex->m_hyperId == 5 || hyperVertex->m_hyperId == 8) {
            hyperVertex->printVertexTree();
        }
    }
    cout << endl;
}

// 输出回滚子事务
int MinWRollback::printRollbackTxs() {
    cout << "====================Rollback Transactions====================" << endl;
    cout << "total size: " << m_rollbackTxs.size() << endl;
    int totalRollbackCost = 0;
    for (auto& tx : m_rollbackTxs) {
        totalRollbackCost += tx->m_self_cost;
        // cout << tx->m_id << endl;
    }
    cout << "rollback cost: " << totalRollbackCost << endl;
    cout << "=============================================================" << endl;
    return totalRollbackCost;
}

// 输出超节点每条边对应回滚子事务集
void MinWRollback::printEdgeRollBack(HyperVertex::Ptr& hyperVertex, const tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc) {
    // 输出每条出边回滚子事务集
    cout << "========== Now print " << hyperVertex->m_hyperId << "'s out rollback ==========" << endl;
    // 输出每条边以及对应的回滚事务集
    for (auto& out_hv : hyperVertex->m_out_hv) {
        if (scc.find(out_hv) == scc.cend()) {
            continue;
        }
        cout << "out edge: " << out_hv->m_hyperId << " rollback: ";
        for (auto& vertex : hyperVertex->m_out_rollback[out_hv->m_hyperId]) {
            cout << vertex->m_id << " ";
        }
        cout << endl;
    }
    // 输出每条入边以及对应的回滚事务集
    for (auto& in_hv : hyperVertex->m_in_hv) {
        if (scc.find(in_hv) == scc.cend()) {
            continue;
        }
        cout << "in edge: " << in_hv->m_hyperId << " rollback: ";
        for (auto& vertex : hyperVertex->m_in_rollback[in_hv->m_hyperId]) {
            cout << vertex->m_id << " ";
        }
        cout << endl;
    }
    // // 输出所有出边回滚事务集
    // cout << "all out rollback: ";
    // for (auto& vertex : hyperVertex->m_out_allRB) {
    //     cout << vertex->m_id << " ";
    // }
    // cout << endl;
    // // 输出所有入边回滚事务集
    // cout << "all in rollback: ";
    // for (auto& vertex : hyperVertex->m_in_allRB) {
    //     cout << vertex->m_id << " ";
    // }
    // cout << endl;
}