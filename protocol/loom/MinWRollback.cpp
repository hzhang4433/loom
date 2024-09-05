#include <algorithm>
#include <tbb/tbb.h>
#include "MinWRollback.h"
#include <iostream>
#include <chrono>
#include <future>
#include <glog/logging.h>
#include "protocol/common.h"


using namespace std;

/*  执行算法: 将transaction转化为hyperVertex
    详细算法流程: 
        1. 多线程并发执行所有事务，获得事务执行信息（读写集，事务结构，执行时间 => 随机生成）
        2. 判断执行完成的事务与其它执行完成事务间的rw依赖，构建rw依赖图
    状态: 测试完成，待优化...
*/
HyperVertex::Ptr MinWRollback::execute(const TPCCTransaction::Ptr& tx, bool isNest) {
    int txid = getId();
    HyperVertex::Ptr hyperVertex = make_shared<HyperVertex>(txid, isNest);
    Vertex::Ptr rootVertex = make_shared<Vertex>(hyperVertex, txid, to_string(txid), 0, isNest);
    // 根据事务结构构建超节点
    string txid_str = to_string(txid);
    
    if (isNest) {
        hyperVertex->buildVertexs(tx, hyperVertex, rootVertex, txid_str, m_invertedIndex);
        // 记录超节点包含的所有节点
        hyperVertex->m_vertices.insert(rootVertex->cascadeVertices.begin(), rootVertex->cascadeVertices.end());
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

    // 记录超节点
    m_hyperVertices.push_back(hyperVertex);
    
    return hyperVertex;
}

/* 利用倒排索引进一步构建rw索引 */
void MinWRollback::onWarm2RWIndex() {
    // 利用倒排索引构建RWIndex
    for (auto& kv : m_invertedIndex) {
        auto& readTxs = kv.second.readSet;
        auto& writeTxs = kv.second.writeSet;
        // 判断是否有写事务，若没有写事务则跳过
        if (writeTxs.empty()) {
            continue;
        }
        for (auto& rTx : readTxs) {
            m_RWIndex[rTx].insert(writeTxs.begin(), writeTxs.end());
            m_RWIndex[rTx].erase(rTx);
        }
    }
}

/* 获取图中所有强连通分量 */
void MinWRollback::onWarm2SCC() {
    // // 拿到所有scc
    // auto start = std::chrono::high_resolution_clock::now();
    
    /* Tarjan
    int index = 0;
    stack<HyperVertex::Ptr> S;
    unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash> indices(m_hyperVertices.size());
    unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash> lowlinks(m_hyperVertices.size());
    unordered_map<HyperVertex::Ptr, bool, HyperVertex::HyperVertexHash> onStack(m_hyperVertices.size());

    for (const auto& hv : m_hyperVertices) {
        if (indices.find(hv) == indices.end()) { //没有访问过
            strongconnect(hv, index, S, indices, lowlinks, onStack);
        }
    }
    */

    /* Gabow */
    int index = 0;
    stack<HyperVertex::Ptr> S, B;
    unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash> indices(m_hyperVertices.size());
    unordered_map<HyperVertex::Ptr, bool, HyperVertex::HyperVertexHash> onStack(m_hyperVertices.size());

    for (const auto& hv : m_hyperVertices) {
        if (indices.find(hv) == indices.end()) {
            Gabow(hv, index, S, B, indices, onStack);
        }
    }

    // auto end = std::chrono::high_resolution_clock::now();
    // cout << "recognizeSCC time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
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
void MinWRollback::buildGraphSerial() {
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
    //                 onRW(rTx, wTx);
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
            onRW(rTx, wTx);
        }
    }
    return;
}

void MinWRollback::buildGraphNoEdge() {
    edgeCounter = 0;

    // 遍历倒排索引
    for (auto& kv : m_invertedIndex) {
        auto& readTxs = kv.second.readSet;
        auto& writeTxs = kv.second.writeSet;
        // 遍历读集
        for (auto& rTx : readTxs) {
            // 遍历读集
            for (auto& wTx : writeTxs) {
                if (rTx != wTx) {
                    // 构建超图
                    onRWNoEdge(rTx, wTx);
                }
            }
        }
    }
    return;
}

void MinWRollback::buildGraphNoEdgeC(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures) {
    // edgeCounter = 0;

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
    //                 onRWCNoEdge(rTx, wTx);
    //             }
    //         }
    //     }
    // }
    
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
    size_t chunkSize = (totalPairs + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 0.8);
    // chunkSize = 15;
    if (chunkSize == 0) {
        chunkSize = totalPairs;
    }
    cout << "totalPairs: " << totalPairs << " chunkSize: " << chunkSize << endl;

    for (size_t i = 0; i < totalPairs; i += chunkSize) {
        futures.emplace_back(Pool->commit([this, &rwPairs, i, chunkSize, totalPairs] {
            size_t end = std::min(i + chunkSize, totalPairs);
            for (size_t j = i; j < end; ++j) {
                onRWCNoEdge(rwPairs[j].first, rwPairs[j].second);
            }
        }));
    }

    // 等待所有任务完成
    for (auto &future : futures) {
        future.get();
    }
}

void MinWRollback::buildGraphNoEdgeC(ThreadPool::Ptr& Pool, std::vector<std::future<void>>& futures) {
    // edgeCounter = 0;
    
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
    // size_t chunkSize = (totalPairs + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 1);
    // // chunkSize = 20;
    // // cout << "totalPairs: " << totalPairs << " chunkSize: " << chunkSize << endl;

    // for (size_t i = 0; i < totalPairs; i += chunkSize) {
    //     futures.emplace_back(Pool->enqueue([this, &rwPairs, i, chunkSize, totalPairs] {
    //         size_t end = std::min(i + chunkSize, totalPairs);
    //         for (size_t j = i; j < end; ++j) {
    //             onRWCNoEdge(rwPairs[j].first, rwPairs[j].second);
    //         }
    //     }));
    // }

    size_t chunkSize = totalPairs / UTIL_DEFAULT_THREAD_SIZE;
    size_t remainder = totalPairs % UTIL_DEFAULT_THREAD_SIZE;
    // cout << "totalPairs: " << totalPairs << " chunkSize: " << chunkSize << endl;

    for (size_t i = 0; i < UTIL_DEFAULT_THREAD_SIZE; ++i) {
        size_t startIdx = i * chunkSize + std::min(i, remainder);
        size_t endIdx = startIdx + chunkSize + (i < remainder ? 1 : 0);
        endIdx = std::min(endIdx, totalPairs);
        // enqueue the threadpool
        futures.emplace_back(Pool->enqueue([this, &rwPairs, startIdx, endIdx] {
            for (size_t j = startIdx; j < endIdx; ++j) {
                onRWCNoEdge(rwPairs[j].first, rwPairs[j].second);
            }
        }));
    }

    // 等待所有任务完成
    for (auto &future : futures) {
        future.get();
    }

    // LOG(INFO) << "Graph futures size: " << futures.size();
    // try {
    //     for (auto& future : futures) {
    //         future.get();
    //         DLOG(INFO) << "Graph future completed.";
    //     }
    // } catch (const std::exception& e) {
    //     LOG(ERROR) << "Exception occurred in graph futures: " << e.what();
    // }
}

void MinWRollback::buildGraphNoEdgeC(threadpool::Ptr& Pool, std::vector<std::future<void>>& futures) {
    edgeCounter = 0;
    
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
    size_t chunkSize = (totalPairs + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 0.8);
    // chunkSize = 20;
    if (chunkSize == 0) {
        chunkSize = totalPairs;
    }
    cout << "totalPairs: " << totalPairs << " chunkSize: " << chunkSize << endl;
    
    for (size_t i = 0; i < totalPairs; i += chunkSize) {
        futures.emplace_back(Pool->commit([this, &rwPairs, i, chunkSize, totalPairs] {
            size_t end = std::min(i + chunkSize, totalPairs);
            for (size_t j = i; j < end; ++j) {
                onRWCNoEdge(rwPairs[j].first, rwPairs[j].second);
            }
        }));
    }

    // 等待所有任务完成
    for (auto &future : futures) {
        future.get();
    }
}

/* 基于倒排索引并发构图 */
void MinWRollback::buildGraphConcurrent(ThreadPool::Ptr& Pool) {
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
    if (loom::BLOCK_SIZE == 50) {
        chunkSize = 20;
    } else {
        chunkSize = loom::BLOCK_SIZE / 2;
        // chunkSize = (totalPairs + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 1);
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

void MinWRollback::buildGraphConcurrent(UThreadPoolPtr& Pool) {
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
    if (loom::BLOCK_SIZE == 50) {
        chunkSize = 20;
    } else {
        // chunkSize = loom::BLOCK_SIZE / 2;
        chunkSize = (totalPairs + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 2);
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

void MinWRollback::buildGraphConcurrent(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures) {
    edgeCounter = 0;

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
    // size_t chunkSize = (totalPairs + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 0.9);
    size_t chunkSize = 20;
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
    if (loom::BLOCK_SIZE == 50) {
        chunkSize = 20;
    } else {
        // chunkSize = loom::BLOCK_SIZE / 2;
        chunkSize = (totalPairs + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 1.5);
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

/*  构图算法（初始版）: 将hyperVertex转化为hyperGraph
    详细算法流程: 依次遍历超节点中的所有节点与现有节点进行依赖分析，构建超图
        1. 若与节点存在rw依赖(出边)
            1.1 在v1.m_out_edges数组（记录所有out边）中新增一条边（v2），并尝试更新v1.m_tx.m_min_out
                为min_out = min(v2.m_tx.m_min_out, v2.m_hyperId)
                1. 若v1.m_tx.m_min_out更新，且m_in_edges存在记录，则依次遍历其中的节点v_in【out更新 => out更新】
                    1. 尝试更新v_in.m_min_out为min(v_in.m_min_out, min_out)
                    2. 若v_in.m_min_out更新成功，则尝试递归更新
                    3. 若更新失败，则返回
            1.2 在v2.m_in_edges数组（记录所有in边）中新增一条边（v1），并尝试更新v2.m_tx.m_min_in
                为min_in = min(v1.m_tx.m_min_in，v1.m_hyperId)
                1. 若v2.m_tx.m_min_in更新，且m_out_edges存在记录，则依次遍历其中的节点v_out【in更新 => in更新】
                    1. 尝试更新v_out.m_min_in为min(v_out.m_min_in, min_in)
                    2. v_out.m_min_in更新成功，则尝试递归更新
                    3. 若更新失败，则返回
        2. 若与节点存在wr依赖（入边）与1相反
    状态: 待测试...
 */
void MinWRollback::build(set<Vertex::Ptr, Vertex::VertexCompare2>& vertices) {
    
    for (auto& newV: vertices) {
        for (auto& oldV: m_vertices) {
            // 获取超节点
            auto& newHyperVertex = newV->m_tx;
            auto& oldHyperVertex = oldV->m_tx;
            int newHyperIdx = newV->m_hyperId;
            int oldHyperIdx = oldV->m_hyperId;

            // 存在rw依赖
            if (loom::hasConflict(newV->readSet, oldV->writeSet)) {
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
                    recursiveUpdate(newHyperVertex, min_out, loom::EdgeType::OUT);
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
                    recursiveUpdate(oldHyperVertex, min_in, loom::EdgeType::IN);
                }
                // 更新回滚集合
                // oldHyperVertex->m_in_rollback[newHyperIdx].insert(newV->cascadeVertices.cbegin(), newV->cascadeVertices.cend());
            }
            // 存在wr依赖
            if (loom::hasConflict(newV->writeSet, oldV->readSet)) {
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
                    recursiveUpdate(newHyperVertex, min_in, loom::EdgeType::IN);
                }
                // 更新回滚集合
                // newHyperVertex->m_in_rollback[oldHyperIdx].insert(oldV->cascadeVertices.cbegin(), oldV->cascadeVertices.cend());


                // 更新oldV对应的hyperVertex的out_edges
                // handleNewEdge(oldV, newV, oldHyperVertex->m_out_edges[newHyperVertex]);
                oldHyperVertex->m_out_hv.insert(newHyperVertex);
                oldHyperVertex->m_out_edges[newHyperIdx].insert(newV);
                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_out
                int min_out = min(newHyperVertex->m_min_out, newV->m_hyperId);
                if (min_out < oldHyperVertex->m_min_out) {
                    recursiveUpdate(oldHyperVertex, min_out, loom::EdgeType::OUT);
                }
                // 更新回滚集合
                oldHyperVertex->m_out_rollback[newHyperIdx].insert(oldV->cascadeVertices.cbegin(), oldV->cascadeVertices.cend());
            }
        }
    }
}

void MinWRollback::onRWC(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx) {
    // 获取超节点
    auto& rHyperVertex = rTx->m_tx;
    auto& wHyperVertex = wTx->m_tx;
    auto rhvIdx = rTx->m_hyperId;
    auto whvIdx = wTx->m_hyperId;

    // edgeCounter.fetch_add(1);

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
    // // wHyperVertex->m_in_rollback[rhvIdx].insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
    // wHyperVertex->m_in_allRB.insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
}

void MinWRollback::onRW(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx) {
    // 获取超节点
    auto& rHyperVertex = rTx->m_tx;
    auto& wHyperVertex = wTx->m_tx;
    auto rhvIdx = rTx->m_hyperId;
    auto whvIdx = wTx->m_hyperId;

    edgeCounter++;

    // 更新newV对应的hyperVertex的out_edges
    rHyperVertex->m_out_hv.insert(wHyperVertex);
    rHyperVertex->m_out_edges[whvIdx].insert(wTx);

    // 更新依赖数
    rTx->m_degree++;
    // 尝试更新newV对应的hyperVertex的min_out -- 100us
    int min_out = min(wHyperVertex->m_min_out, whvIdx);
    if (min_out < rHyperVertex->m_min_out) {
        recursiveUpdate(rHyperVertex, min_out, loom::EdgeType::OUT);
    }
    // 更新回滚集合
    rHyperVertex->m_out_rollback[whvIdx].insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());


    // 更新oldV对应的hyperVertex的in_edges
    wHyperVertex->m_in_hv.insert(rHyperVertex);
    wHyperVertex->m_in_edges[rhvIdx].insert(rTx);

    // 更新依赖数
    wTx->m_degree++;
    // 尝试更新oldV对应的hyperVertex的min_in -- 100us
    int min_in = min(rHyperVertex->m_min_in, rhvIdx);
    if (min_in < wHyperVertex->m_min_in) {
        recursiveUpdate(wHyperVertex, min_in, loom::EdgeType::IN);
    }
    // 更新回滚集合 -- 1.77ms
    // wHyperVertex->m_in_rollback[rhvIdx].insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
    wHyperVertex->m_in_allRB.insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
}

void MinWRollback::onRWCNoEdge(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx) {
    // 获取超节点
    auto& rHyperVertex = rTx->m_tx;
    auto& wHyperVertex = wTx->m_tx;
    auto rhvIdx = rTx->m_hyperId;
    auto whvIdx = wTx->m_hyperId;

    // // 若该rw依赖已经存在，则直接返回
    // if (rHyperVertex->m_out_map[whvIdx].count(wTx) && 
    //     wHyperVertex->m_in_map[rhvIdx].count(rTx)) {
    //     return;
    // }

    // edgeCounter.fetch_add(1);

    // 更新newV对应的hyperVertex的out_edges
    rHyperVertex->m_out_hv.insert(wHyperVertex);

    // 更新回滚集合
    rHyperVertex->m_out_rollback[whvIdx].insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
    // 计算回滚代价
    rHyperVertex->m_out_weights[whvIdx] += rTx->m_cost;

    // 更新oldV对应的hyperVertex的in_edges
    wHyperVertex->m_in_hv.insert(rHyperVertex);
    
    // 更新回滚集合
    wHyperVertex->m_in_allRB.insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
    // 计算回滚代价
    wHyperVertex->m_in_cost += rTx->m_cost;
}

void MinWRollback::onRWNoEdge(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx) {
    // 获取超节点
    auto& rHyperVertex = rTx->m_tx;
    auto& wHyperVertex = wTx->m_tx;
    auto rhvIdx = rTx->m_hyperId;
    auto whvIdx = wTx->m_hyperId;

    edgeCounter++;

    // 更新newV对应的hyperVertex的out_edges
    rHyperVertex->m_out_hv.insert(wHyperVertex);
// 或许可以省去？
    // rHyperVertex->m_out_edges[whvIdx].insert(wTx);

    // 尝试更新newV对应的hyperVertex的min_out -- 100us
    int min_out = min(wHyperVertex->m_min_out, whvIdx);
    if (min_out < rHyperVertex->m_min_out) {
        recursiveUpdate(rHyperVertex, min_out, loom::EdgeType::OUT);
    }
    // 更新回滚集合
    rHyperVertex->m_out_rollback[whvIdx].insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
    rHyperVertex->m_out_weights[whvIdx] += rTx->m_cost;


    // 更新oldV对应的hyperVertex的in_edges
    wHyperVertex->m_in_hv.insert(rHyperVertex);
// 或许可以省去？
    // wHyperVertex->m_in_edges[rhvIdx].insert(rTx);

    // 尝试更新oldV对应的hyperVertex的min_in -- 100us
    int min_in = min(rHyperVertex->m_min_in, rhvIdx);
    if (min_in < wHyperVertex->m_min_in) {
        recursiveUpdate(wHyperVertex, min_in, loom::EdgeType::IN);
    }
    // 更新回滚集合
    wHyperVertex->m_in_allRB.insert(rTx->cascadeVertices.cbegin(), rTx->cascadeVertices.cend());
    wHyperVertex->m_in_cost += rTx->m_cost;
}

// 判断v1是否是v2的祖先
bool MinWRollback::isAncester(const string& v1, const string& v2) {
    // 子孙节点id的前缀包含祖先节点id
    return v2.find(v1) == 0;
}

// 递归更新超节点min_in和min_out
void MinWRollback::recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, loom::EdgeType type) {
// 或者后续不在这里记录强连通分量，后续根据开销进行调整。。。

// 可以加个visited数组进行剪枝

    // cout << "Update hyperVertex: " << hyperVertex->m_hyperId << " min_value: " << min_value
    //      << " type: " << loom::edgeTypeToString(type) << endl;

    // 更新前需要把这个Hypervertex从原有m_min2HyperVertex中删除 (前提:他们都不是初始值)
    if (hyperVertex->m_min_in != INT_MAX && hyperVertex->m_min_out != INT_MAX) {
        m_min2HyperVertex[combine(hyperVertex->m_min_in, hyperVertex->m_min_out)].erase(hyperVertex);
    }
    
    if (type == loom::EdgeType::OUT) {
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

/* 确定性回滚(初始版): 不支持多线程 */
void MinWRollback::rollback() {
    long totalSCC = 0;

    // 遍历所有强连通分量
    for (auto hyperVertexs : m_min2HyperVertex) {
        
        if (hyperVertexs.second.size() <= 1) {
            continue;
        }

        // cout << "hyperVertexs: " << hyperVertexs.first << ", size = " << hyperVertexs.second.size() << endl;

        // 拿到所有scc
        vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> sccs;
        auto start = std::chrono::high_resolution_clock::now();
        if (!recognizeSCC(hyperVertexs.second, sccs)) {
            auto end = std::chrono::high_resolution_clock::now();
            totalSCC += chrono::duration_cast<chrono::microseconds>(end - start).count();
            continue;
        }
        auto end = std::chrono::high_resolution_clock::now();
        totalSCC += chrono::duration_cast<chrono::microseconds>(end - start).count();

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
            set<HyperVertex::Ptr, loom::cmp> pq;

            // 遍历强连通分量中每个超节点，计算scc超节点间边权
            calculateHyperVertexWeight(scc, pq);

            // // 输出pq
            // cout << "init pq: " << endl;
            // for (auto v : pq) {
            //     cout << "hyperId: " << v->m_hyperId << " cost: " << v->m_cost << " in_cost: " << v->m_in_cost << " out_cost: " << v->m_out_cost << endl;
            // }

            // 贪心获取最小回滚代价的节点集
            GreedySelectVertex(scc, pq, m_rollbackTxs);
            auto end = std::chrono::high_resolution_clock::now();
            cout << "scc rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
        
        }
    }

    cout << "recognizeSCC time: " << (double)totalSCC / 1000 << "ms" << endl;
}

/* 确定性回滚(优化1:剪枝): 支持多线程 */
loom::ReExecuteInfo MinWRollback::rollbackOpt1(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc) {
    // 统计每个scc rollback耗时
    auto start = std::chrono::high_resolution_clock::now();
    
    cout << "===== 输出scc中的每一个元素的详细信息 =====" << endl;
    cout << "size = " << scc.size() << ", all vertexs:"; 
    for (auto hv : scc) {
        cout << hv->m_hyperId << " ";
    }
    cout << endl;

    loom::ReExecuteInfo reExecuteInfo;
    // 回滚事务集合
    unordered_set<Vertex::Ptr, Vertex::VertexHash> rollbackTxs;
    // 回滚嵌套事务序
    vector<int> queueOrder;
    // out回滚嵌套事务
    stack<int> stackOrder;

// 可尝试使用fibonacci堆优化更新效率，带测试，看效果
    // 定义有序集合，以hyperVertex.m_cost为key从小到大排序
    set<HyperVertex::Ptr, loom::cmp> pq;

    // 遍历强连通分量中每个超节点，计算scc超节点间边权    
    calculateHyperVertexWeight(scc, pq);

    // 贪心获取最小回滚代价的节点集
    GreedySelectVertexOpt1(scc, pq, rollbackTxs, queueOrder, stackOrder);

    if (!pq.empty()) {
        // 把pq中剩余的节点加入到回滚集合中
        queueOrder.push_back((*pq.begin())->m_hyperId);
    }

    // 将stackOrder中的元素放入queueOrder中
    while (!stackOrder.empty()) {
        queueOrder.push_back(stackOrder.top());
        stackOrder.pop();
    }

    reExecuteInfo.m_rollbackTxs = rollbackTxs;
    reExecuteInfo.m_serialOrder = queueOrder;

    auto end = std::chrono::high_resolution_clock::now();
    cout << "scc rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
    
    return reExecuteInfo;
}

/* 确定性并发回滚(优化1:剪枝): 多线程版 */
void MinWRollback::rollbackOpt1Concurrent(UThreadPoolPtr& Pool, std::vector<std::future<loom::ReExecuteInfo>>& futures) {
    // 拿到所有scc
    vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> sccs;
    
    auto start = std::chrono::high_resolution_clock::now();
    // recognizeSCC(m_hyperVertices, sccs);
    onWarm2SCC();
    auto end = std::chrono::high_resolution_clock::now();
    cout << "recognizeSCC time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;

    // 遍历所有scc回滚节点
    for (auto& scc : m_sccs) {
        futures.emplace_back(Pool->commit([this, scc] {
            auto scc_copy = scc;
            // 统计每个scc rollback耗时
            auto start = std::chrono::high_resolution_clock::now();

            loom::ReExecuteInfo reExecuteInfo;
            // 回滚事务集合
            unordered_set<Vertex::Ptr, Vertex::VertexHash> rollbackTxs;
            // 回滚嵌套事务序
            vector<int> queueOrder;
            // out回滚嵌套事务
            stack<int> stackOrder;

            // 定义有序集合，以hyperVertex.m_cost为key从小到大排序
            set<HyperVertex::Ptr, loom::cmp> pq;

            // 遍历强连通分量中每个超节点，计算scc超节点间边权
            calculateHyperVertexWeight(scc_copy, pq);

            // 贪心获取最小回滚代价的节点集
            GreedySelectVertexOpt1(scc_copy, pq, rollbackTxs, queueOrder, stackOrder);
            
            if (!pq.empty()) {
                // 把pq中剩余的节点加入到回滚集合中
                queueOrder.push_back((*pq.begin())->m_hyperId);
            }

            // 将stackOrder中的元素放入queueOrder中
            while (!stackOrder.empty()) {
                queueOrder.push_back(stackOrder.top());
                stackOrder.pop();
            }

            reExecuteInfo.m_rollbackTxs = rollbackTxs;
            reExecuteInfo.m_serialOrder = queueOrder;
            
            auto end = std::chrono::high_resolution_clock::now();
            cout << "scc size: " << scc.size() << " scc rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
            return reExecuteInfo;
        }));
    }
}

/* 确定性回滚(优化2:去边): 不支持多线程和fast模式 */
void MinWRollback::rollbackNoEdge() {
    long totalSCC = 0;

    for (auto hyperVertexs : m_min2HyperVertex) {
        
        if (hyperVertexs.second.size() <= 1) {
            continue;
        }
        
        // 拿到所有scc
        vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> sccs;
        auto start = std::chrono::high_resolution_clock::now();
        if (!recognizeSCC(hyperVertexs.second, sccs)) {
            auto end = std::chrono::high_resolution_clock::now();
            totalSCC += chrono::duration_cast<chrono::microseconds>(end - start).count();
            continue;
        }
        auto end = std::chrono::high_resolution_clock::now();
        totalSCC += chrono::duration_cast<chrono::microseconds>(end - start).count();


        // 遍历所有scc回滚节点
        for (auto& scc : sccs) {
            cout << "===== 输出scc中的每一个元素的详细信息 =====" << endl;
            cout << "size = " << scc.size() << ", all vertexs:"; 
            for (auto hv : scc) {
                cout << hv->m_hyperId << " ";
            }
            cout << endl;


            // 统计每个scc rollback耗时
            auto start = std::chrono::high_resolution_clock::now();

            // 定义有序集合，以hyperVertex.m_cost为key从小到大排序
            set<HyperVertex::Ptr, loom::cmp> pq;
            // 遍历强连通分量中每个超节点，计算scc超节点间边权
            calculateHyperVertexWeightNoEdge(scc, pq);
            // 贪心获取最小回滚代价的节点集
            // testFlag = true;
            GreedySelectVertexNoEdge(scc, pq, m_rollbackTxs, false);


    // // 可尝试使用fibonacci堆优化更新效率，带测试，看效果
    //         boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>> heap;
    //         std::unordered_map<HyperVertex::Ptr, decltype(heap)::handle_type, HyperVertex::HyperVertexHash> handles;
    //         calculateHyperVertexWeightNoEdge(scc, heap, handles);
    //         GreedySelectVertexNoEdge(scc, heap, handles, m_rollbackTxs);

            auto end = std::chrono::high_resolution_clock::now();
            cout << "scc rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
        }
    }

    cout << "recognizeSCC time: " << (double)totalSCC / 1000 << "ms" << endl;
}

/* 确定性回滚(优化2:去边): 不支持多线程，支持fast模式 */
void MinWRollback::rollbackNoEdge(bool fastMode) {
    // 拿到所有scc
    vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> sccs;
    
    auto start = std::chrono::high_resolution_clock::now();
    // recognizeSCC(m_hyperVertices, sccs);
    onWarm2SCC();
    auto end = std::chrono::high_resolution_clock::now();
    cout << "recognizeSCC time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;


    // 遍历所有scc回滚节点
    for (auto& scc : m_sccs) {
        // cout << "===== 输出scc中的每一个元素的详细信息 =====" << endl;
        // cout << "size = " << scc.size() << ", all vertexs:"; 
        // for (auto hv : scc) {
        //     cout << hv->m_hyperId << " ";
        // }
        // cout << endl;


        // 统计每个scc rollback耗时
        auto start = std::chrono::high_resolution_clock::now();

        // 定义有序集合，以hyperVertex.m_cost为key从小到大排序
        set<HyperVertex::Ptr, loom::cmp> pq;
        // 遍历强连通分量中每个超节点，计算scc超节点间边权
        calculateHyperVertexWeightNoEdge(scc, pq);
        // 贪心获取最小回滚代价的节点集
        // testFlag = true;
        GreedySelectVertexNoEdge(scc, pq, m_rollbackTxs, fastMode);


// // 可尝试使用fibonacci堆优化更新效率，带测试，看效果
//         boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>> heap;
//         std::unordered_map<HyperVertex::Ptr, decltype(heap)::handle_type, HyperVertex::HyperVertexHash> handles;
//         calculateHyperVertexWeightNoEdge(scc, heap, handles);
//         GreedySelectVertexNoEdge(scc, heap, handles, m_rollbackTxs);

        auto end = std::chrono::high_resolution_clock::now();
        cout << "scc rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
    }
}

/* 确定性回滚(优化2:去边): 支持多线程和fast模式 */
loom::ReExecuteInfo MinWRollback::rollbackNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, bool fastMode) {
    
    // auto start = std::chrono::high_resolution_clock::now();

    // cout << "===== 输出scc中的每一个元素的详细信息 =====" << endl;
    // cout << "size = " << scc.size() << ", all vertexs:"; 
    // for (auto hv : scc) {
    //     cout << hv->m_hyperId << " ";
    // }
    // cout << endl;

    loom::ReExecuteInfo reExecuteInfo;
    // 回滚事务集合
    unordered_set<Vertex::Ptr, Vertex::VertexHash> rollbackTxs;
    // 回滚嵌套事务序
    vector<int> queueOrder;
    // out回滚嵌套事务
    stack<int> stackOrder;

    // 定义有序集合，以hyperVertex.m_cost为key从小到大排序
    set<HyperVertex::Ptr, loom::cmp> pq;
    // 遍历强连通分量中每个超节点，计算scc超节点间边权
    calculateHyperVertexWeightNoEdge(scc, pq);
    // 贪心获取最小回滚代价的节点集
    GreedySelectVertexNoEdge(scc, pq, rollbackTxs, queueOrder, stackOrder, fastMode);

    if (!pq.empty()) {
        // 把pq的第一个元素放入queueOrder
        queueOrder.push_back((*pq.begin())->m_hyperId);
    }

    // 将stackOrder里的元素逐个取出放入queueOrder
    while (!stackOrder.empty()) {
        queueOrder.push_back(stackOrder.top());
        stackOrder.pop();
    }
    
    reExecuteInfo.m_rollbackTxs = rollbackTxs;
    reExecuteInfo.m_serialOrder = queueOrder;

    // auto end = std::chrono::high_resolution_clock::now();
    // cout << "scc rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
    
    return reExecuteInfo;
}

/* 确定性回滚(优化2:去边): 多线程版 */
void MinWRollback::rollbackNoEdgeConcurrent(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures, bool fastMode) {
    // 拿到所有scc
    vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> sccs;
    
    auto start = std::chrono::high_resolution_clock::now();
    // recognizeSCC(m_hyperVertices, sccs);
    onWarm2SCC();
    auto end = std::chrono::high_resolution_clock::now();
    cout << "recognizeSCC time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;


    // 遍历所有scc回滚节点
    for (auto& scc : m_sccs) {
        
        futures.emplace_back(Pool->commit([this, scc]{
            auto scc_copy = scc;
            // 统计每个scc rollback耗时
            auto start = std::chrono::high_resolution_clock::now();

            // 定义有序集合，以hyperVertex.m_cost为key从小到大排序
            set<HyperVertex::Ptr, loom::cmp> pq;
            // 遍历强连通分量中每个超节点，计算scc超节点间边权
            calculateHyperVertexWeightNoEdge(scc, pq);
            // 贪心获取最小回滚代价的节点集
            GreedySelectVertexNoEdge(scc_copy, pq, m_rollbackTxs, true);


    // // 可尝试使用fibonacci堆优化更新效率，带测试，看效果
    //         boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>> heap;
    //         std::unordered_map<HyperVertex::Ptr, decltype(heap)::handle_type, HyperVertex::HyperVertexHash> handles;
    //         calculateHyperVertexWeightNoEdge(scc, heap, handles);
    //         GreedySelectVertexNoEdge(scc, heap, handles, m_rollbackTxs);

            auto end = std::chrono::high_resolution_clock::now();
            // cout << "scc size: " << scc.size() << " scc rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;
        }));
        
    }
}

/* 识别强连通分量
    1. 利用Tarjan算法识别强连通分量，并返回size大于1的强连通分量
*/
bool MinWRollback::recognizeSCC(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& hyperVertexs, vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>>& sccs) {
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

void MinWRollback::strongconnect(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& hyperVertexs, const HyperVertex::Ptr& v, int& index, stack<HyperVertex::Ptr>& S, unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& indices,
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

void MinWRollback::strongconnect(const HyperVertex::Ptr& v, int& index, stack<HyperVertex::Ptr>& S, unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& indices,
                   unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& lowlinks, unordered_map<HyperVertex::Ptr, bool, HyperVertex::HyperVertexHash>& onStack) {
    indices[v] = lowlinks[v] = ++index;
    S.push(v);
    onStack[v] = true;

    if (v->m_out_hv.empty()) {
        S.pop();
        onStack[v] = false;
        return;
    }
    
    for (const auto& w : v->m_out_hv) {
        auto indices_it = indices.find(w);
        if (indices_it == indices.end()) {
            strongconnect(w, index, S, indices, lowlinks, onStack);
            lowlinks[v] = min(lowlinks[v], lowlinks[w]);
        } else if (onStack[w]) {
            lowlinks[v] = min(lowlinks[v], indices_it->second);
        }
    }

    if (lowlinks[v] == indices[v]) {
        unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> scc;
        HyperVertex::Ptr w;
        do {
            w = S.top();
            S.pop();
            onStack[w] = false;
            scc.insert(w);
        } while (w != v);

        if (scc.size() > 1) {
            m_sccs.push_back(scc);
        }
    }
}

void MinWRollback::Gabow(const HyperVertex::Ptr& v, int& index, stack<HyperVertex::Ptr>& S, stack<HyperVertex::Ptr>& B,
                    unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& indices, 
                    unordered_map<HyperVertex::Ptr, bool, HyperVertex::HyperVertexHash>& onStack) {
    indices[v] = index++;

    if (v->m_out_hv.empty()) {
        return;
    }

    S.push(v);
    B.push(v);
    onStack[v] = true;

    for (const auto& w : v->m_out_hv) {
        if (indices.find(w) == indices.end()) {
            // Successor w has not yet been visited; recurse on it
            Gabow(w, index, S, B, indices, onStack);
        } else if (onStack[w]) {
            // Successor w is in stack S and hence in the current SCC
            while (indices[B.top()] > indices[w]) {
                B.pop();
            }
        }
    }

    // If v is a root node, pop the stack and generate an SCC
    if (B.top() == v) {
        unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> scc;
        HyperVertex::Ptr w;
        do {
            w = S.top();
            S.pop();
            onStack[w] = false;
            scc.insert(w);
        } while (w != v);
        B.pop();

        if (scc.size() > 1) {
            m_sccs.push_back(scc);
        }
    }
}

/* 计算强连通分量中每个超节点间回滚代价和回滚子事务集
    1. 计算scc超节点间边权
    2. 计算每个超节点的最小回滚代价
    3. 将超节点放入优先队列
*/
void MinWRollback::calculateHyperVertexWeight(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq) {
    // 计算每个超节点的in和out边权重
    for (auto& hyperVertex : scc) {
        // 计算m_out_cost
        int out_degree = 0;
        // 遍历超节点的出边
        // auto waitToDel = vector<HyperVertex::Ptr>();
        for (auto& out_hv : hyperVertex->m_out_hv) {
            // 同属于一个scc
            if (scc.find(out_hv) != scc.cend()) {
                auto out_idx = out_hv->m_hyperId;
                // 统计m_out_allRB
                for (auto & v : hyperVertex->m_out_rollback[out_idx]) {
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
        for (auto& in_hv : hyperVertex->m_in_hv) {
            // 同属于一个scc
            if (scc.find(in_hv) != scc.cend()) {
                auto idx = hyperVertex->m_hyperId;
                auto rbs = in_hv->m_out_rollback[idx];
                for (auto& rb : rbs) {
                    hyperVertex->m_in_cost += rb->m_self_cost;
                    in_degree += rb->m_degree;
                }
                hyperVertex->m_in_allRB.insert(rbs.cbegin(), rbs.cend());
            }
        }

        // for (auto v : hyperVertex->m_in_allRB) {            
        //     hyperVertex->m_in_cost += v->m_self_cost;
        //     in_degree += v->m_degree;
        // }
        hyperVertex->m_in_cost /= in_degree;


        // 计算超节点的最小回滚代价
        if (hyperVertex->m_in_cost < hyperVertex->m_out_cost) {
            hyperVertex->m_rollback_type = loom::EdgeType::IN;
            hyperVertex->m_cost = hyperVertex->m_in_cost;
        } else {
            hyperVertex->m_rollback_type = loom::EdgeType::OUT;
            hyperVertex->m_cost = hyperVertex->m_out_cost;
        }
        
        pq.insert(hyperVertex);
    }
}


void MinWRollback::calculateHyperVertexWeightNoEdge(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq) {
    // 计算每个超节点的in和out边权重
    for (auto& hyperVertex : scc) {
        // 遍历超节点的出边, 计算m_out_cost
        for (auto& out_hv : hyperVertex->m_out_hv) {
            // 同属于一个scc
            if (scc.find(out_hv) != scc.cend()) {
                auto out_idx = out_hv->m_hyperId;
                // 统计m_out_allRB
                for (auto & v : hyperVertex->m_out_rollback[out_idx]) {
                    // 如果之前没有这个节点，则新增map并设置值为1，否则值+1
                    hyperVertex->m_out_allRB[v]++;                    
                }
                hyperVertex->m_out_cost += hyperVertex->m_out_weights[out_idx];
            }
        }
        
        // // 遍历超节点的入边, 计算m_in_cost
        // for (auto& in_hv : hyperVertex->m_in_hv) {
        //     // 同属于一个scc
        //     if (scc.find(in_hv) != scc.cend()) {
        //         auto hyper_idx = hyperVertex->m_hyperId;
        //         auto rbs = in_hv->m_out_rollback[hyper_idx];

        //         hyperVertex->m_in_cost += in_hv->m_out_weights[hyper_idx];
        //         hyperVertex->m_in_allRB.insert(rbs.begin(), rbs.end());
        //     }
        // }


        // 计算超节点的最小回滚代价
        if (hyperVertex->m_in_cost < hyperVertex->m_out_cost) {
            hyperVertex->m_rollback_type = loom::EdgeType::IN;
            hyperVertex->m_cost = hyperVertex->m_in_cost;
        } else {
            hyperVertex->m_rollback_type = loom::EdgeType::OUT;
            hyperVertex->m_cost = hyperVertex->m_out_cost;
        }
        
        pq.insert(hyperVertex);
    }
}


void MinWRollback::calculateHyperVertexWeightNoEdge(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles) {
    
    // 计算每个超节点的in和out边权重
    for (auto& hyperVertex : scc) {
        // 遍历超节点的出边, 计算m_out_cost
        for (auto& out_hv : hyperVertex->m_out_hv) {
            // 同属于一个scc
            if (scc.find(out_hv) != scc.cend()) {
                auto out_idx = out_hv->m_hyperId;
                // 统计m_out_allRB
                for (auto & v : hyperVertex->m_out_rollback[out_idx]) {
                    // 如果之前没有这个节点，则新增map并设置值为1，否则值+1
                    hyperVertex->m_out_allRB[v]++;                    
                }
                hyperVertex->m_out_cost += hyperVertex->m_out_weights[out_idx];
            }
        }
        

        // // 计算m_in_cost
        // for (auto& in_hv : hyperVertex->m_in_hv) {
        //     // 同属于一个scc
        //     if (scc.find(in_hv) != scc.cend()) {
        //         auto rbs = in_hv->m_out_rollback[hyperVertex->m_hyperId];
        //         hyperVertex->m_in_cost += in_hv->m_out_weights[hyperVertex->m_hyperId];
        //         hyperVertex->m_in_allRB.insert(rbs.cbegin(), rbs.cend());
        //     }
        // }


        // 计算超节点的最小回滚代价
        if (hyperVertex->m_in_cost < hyperVertex->m_out_cost) {
            hyperVertex->m_rollback_type = loom::EdgeType::IN;
            hyperVertex->m_cost = hyperVertex->m_in_cost;
        } else {
            hyperVertex->m_rollback_type = loom::EdgeType::OUT;
            hyperVertex->m_cost = hyperVertex->m_out_cost;
        }
        
        handles[hyperVertex] = heap.push(hyperVertex);
    }
}

/* 遍历边集，统计两超节点间一条边的回滚子事务集
    1. 计算边起点tailV的回滚权重
    2. 计算边终点headV的回滚权重
    3. 获取min(tailV回滚权重，headV回滚权重), 并更新总回滚代价和度
    4. 记录回滚子事务
*/
void MinWRollback::calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex) {
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

/* 回滚最小代价的超节点
    1. 选择回滚代价最小的超节点
    2. 更新scc中与它相邻的超节点的回滚代价
    3. 若存在一条边只有出度或入度，则递归更新
    4. 更新scc
*/
void MinWRollback::GreedySelectVertex(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result) {
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
    if (rb->m_rollback_type == loom::EdgeType::OUT) {
        // result.insert(rb->m_out_allRB.begin(), rb->m_out_allRB.end());
        std::transform(rb->m_out_allRB.begin(), rb->m_out_allRB.end(), std::inserter(result, result.end()), [](const std::pair<const Vertex::Ptr, int>& pair) {
            return pair.first;
        });
    } else {
        result.insert(rb->m_in_allRB.begin(), rb->m_in_allRB.end());
    }
    
    // cout << "Greedy delete: " << rb->m_hyperId << endl;

    // 从scc中删除rb
    scc.erase(rb);
    pq.erase(rb);

    // // 打印result
    // cout << "result now: ";
    // for (auto v : result) {
    //     cout << v->m_id << " ";
    // }
    // cout << endl;

    
    // 定义变量记录更新过的超节点间的边
    tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> calculated;
    // 递归更新scc超节点和scc中依赖节点状态
    updateSCCandDependency(scc, rb, pq, result, calculated);      // old version
    
    
    // 若scc中只剩下一个超节点，则无需回滚
    if (scc.size() > 1) {
        // 递归调用GreedySelectVertex获取回滚节点集合
        GreedySelectVertex(scc, pq, result);
    }
}

/* 贪心回滚节点,并记录回滚事务序 */
void MinWRollback::GreedySelectVertexOpt1(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, unordered_set<Vertex::Ptr, Vertex::VertexHash>& result, vector<int>& queueOrder, stack<int>& stackOrder) {
    auto rb = *pq.begin();

    // 记录rb需要回滚的（子）事务
    if (rb->m_rollback_type == loom::EdgeType::OUT) {
        std::transform(rb->m_out_allRB.begin(), rb->m_out_allRB.end(), std::inserter(result, result.end()), [](const std::pair<const Vertex::Ptr, int>& pair) {
            return pair.first;
        });
        stackOrder.push(rb->m_hyperId);
    } else {
        result.insert(rb->m_in_allRB.begin(), rb->m_in_allRB.end());
        queueOrder.push_back(rb->m_hyperId);
    }

    // 从scc中删除rb
    scc.erase(rb);
    pq.erase(rb);
    updateSCCandDependencyOpt1(scc, rb, pq, result, queueOrder, stackOrder);
    
    // 若scc中只剩下一个超节点，则无需回滚
    if (scc.size() > 1) {
        // 递归调用GreedySelectVertex获取回滚节点集合
        GreedySelectVertexOpt1(scc, pq, result, queueOrder, stackOrder);
    }
}

void MinWRollback::GreedySelectVertexNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result, bool fastMode) {
    auto rb = *pq.begin();

    // 记录rb需要回滚的（子）事务
    if (rb->m_rollback_type == loom::EdgeType::OUT) {
        // result.insert(rb->m_out_allRB.begin(), rb->m_out_allRB.end());
        std::transform(rb->m_out_allRB.begin(), rb->m_out_allRB.end(), std::inserter(result, result.end()), [](const std::pair<const Vertex::Ptr, int>& pair) {
            return pair.first;
        });
    } else {
        result.insert(rb->m_in_allRB.begin(), rb->m_in_allRB.end());
    }

    // 从scc中删除rb
    scc.erase(rb);
    pq.erase(rb);

    if (scc.size() > 1) {
        if (fastMode) {
            updateSCCandDependencyFastMode(scc, rb, pq);
        } else {
            updateSCCandDependencyNoEdge(scc, rb, pq);
        }
    }
    
    // 若scc中只剩下一个超节点，则无需回滚
    if (scc.size() > 1) {
        // 递归调用GreedySelectVertex获取回滚节点集合
        GreedySelectVertexNoEdge(scc, pq, result, fastMode);
    }
}

/* 贪心回滚节点,并记录回滚事务序 */
void MinWRollback::GreedySelectVertexNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, unordered_set<Vertex::Ptr, Vertex::VertexHash>& result, vector<int>& queueOrder, stack<int>& stackOrder, bool fastMode) {
    auto rb = *pq.begin();

    // 记录rb需要回滚的（子）事务
    if (rb->m_rollback_type == loom::EdgeType::OUT) {
        std::transform(rb->m_out_allRB.begin(), rb->m_out_allRB.end(), std::inserter(result, result.end()), [](const std::pair<const Vertex::Ptr, int>& pair) {
            return pair.first;
        });
        // if (rb->m_isNested) {
            stackOrder.push(rb->m_hyperId);
        // }
    } else {
        result.insert(rb->m_in_allRB.begin(), rb->m_in_allRB.end());
        // if (rb->m_isNested) {
            queueOrder.push_back(rb->m_hyperId);
        // }
    }

    // 从scc中删除rb
    // rb->m_aborted = true;
    scc.erase(rb);
    pq.erase(rb);

    if (scc.size() > 1) {
        if (fastMode) {
            updateSCCandDependencyFastMode(scc, rb, pq, queueOrder, stackOrder);
        } else {
            updateSCCandDependencyNoEdge(scc, rb, pq, queueOrder, stackOrder);
        }
    }
    
    // 若scc中只剩下一个超节点，则无需回滚
    if (scc.size() > 1) {
        // 递归调用GreedySelectVertex获取回滚节点集合
        GreedySelectVertexNoEdge(scc, pq, result, queueOrder, stackOrder, fastMode);
    }
}

void MinWRollback::GreedySelectVertexNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result) {
    auto rb = heap.top();

    // 记录rb需要回滚的（子）事务
    if (rb->m_rollback_type == loom::EdgeType::OUT) {
        // result.insert(rb->m_out_allRB.begin(), rb->m_out_allRB.end());
        std::transform(rb->m_out_allRB.begin(), rb->m_out_allRB.end(), std::inserter(result, result.end()), [](const std::pair<const Vertex::Ptr, int>& pair) {
            return pair.first;
        });
    } else {
        result.insert(rb->m_in_allRB.begin(), rb->m_in_allRB.end());
    }

    // 从scc中删除rb
    scc.erase(rb);
    heap.pop();
    handles.erase(rb);

    updateSCCandDependencyNoEdge(scc, rb, heap, handles, result);
    
    // 若scc中只剩下一个超节点，则无需回滚
    if (scc.size() > 1) {
        // 递归调用GreedySelectVertex获取回滚节点集合
        GreedySelectVertexNoEdge(scc, heap, handles, result);
    }
}

/*  递归更新scc超节点和scc中依赖节点状态 -- old version
        1. 从scc中删除rb
        2. 遍历rb在scc中的出边节点，更新对应超节点的权重与依赖关系
        3. 遍历rb在scc中的入边节点，更新对应超节点的权重与依赖关系
    状态: 待修改逻辑...
*/
void MinWRollback::updateSCCandDependency(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated) {
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
                    if (loom::hasContain(rbVertexs, out_vertex->m_out_allRB) || 
                        loom::hasContain(rbVertexs, out_vertex->m_in_allRB)) {
                        // cout << "can delete without cost out loop: " << out_vertex->m_hyperId << endl;
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
                // cout << "can delete without in: " << out_vertex->m_hyperId << endl;
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
        auto rbIdx = rb->m_hyperId;
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
                        (loom::hasContain(rbVertexs, in_vertex->m_out_allRB) || 
                         loom::hasContain(rbVertexs, in_vertex->m_in_allRB))) {
                        // cout << "can delete without cost in loop: " << in_vertex->m_hyperId << endl;
                        scc.erase(in_vertex);
                        pq.erase(in_vertex);
                        waitToDelete.insert(in_vertex);
                        break;
                    }
                    
                    
                    // 更新超节点出边回滚子事务
                    // 若rb是out类型的abort，则尝试删除
                    if (rb->m_rollback_type == loom::EdgeType::OUT) {
                        for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
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
                        for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {    
                            in_vertex->m_out_allRB.unsafe_erase(vertex);
                        }
                        // 若rb是in类型的abort，则不更新度
                    }

                    // 等待统一更新
                    waitToUpdate.insert(in_vertex);
                    break;
                }
            }
            // 不存在出边，则删除该超节点，递归更新scc
            if (canDelete) {
                // cout << "can delete without out: " << in_vertex->m_hyperId << endl;
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

/*  递归更新scc超节点和scc中依赖节点状态，并记录回滚事务序  -- new version
    1. 删除所有要删除的节点
    2. 更新删除节点关联节点的度
    3. 重新计算相关节点的回滚代价
*/
void MinWRollback::updateSCCandDependencyOpt1(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, const unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, vector<int>& queueOrder, stack<int>& stackOrder) {
    if (scc.size() <= 1) {
        return;
    }
        
    // 记录要更新的超节点
    unordered_map<HyperVertex::Ptr, loom::EdgeType, HyperVertex::HyperVertexHash> waitToUpdate;
    
    // 递归删除并获取可以直接删除的节点 并 更新删除超节点关联超节点的子节点的度
    recursiveDelete(scc, pq, rb, rbVertexs, waitToUpdate, queueOrder, stackOrder);

    // 重新计算相关节点的回滚代价
    if (scc.size() > 1) {
        for (auto& v : waitToUpdate) {
            // 如果还在scc中
            if (scc.find(v.first) != scc.cend()) {
                // cout << "wait to update: " << v.first->m_hyperId << endl;

                // 更新超节点权重
                auto updateV = v.first;
                pq.erase(updateV);
                calculateWeight(updateV, v.second);
                pq.insert(updateV);
            }
        }
    }
}


/* 递归删除scc中可删除的节点 */
void MinWRollback::recursiveDelete(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, const HyperVertex::Ptr& rb,
                                   const unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, unordered_map<HyperVertex::Ptr, loom::EdgeType, HyperVertex::HyperVertexHash>& waitToUpdate, 
                                   vector<int>& queueOrder, stack<int>& stackOrder) {
    auto rbIdx = rb->m_hyperId;
    // 递归删除出边超节点
    for (auto& out_vertex : rb->m_out_hv) {
        // 记录出边超节点索引
        auto out_vertex_idx = out_vertex->m_hyperId;
        // 超节点在scc中
        if (scc.find(out_vertex) != scc.end()) {
            // 判断是否可以无需代价直接删除
            if (rb->m_rollback_type == loom::EdgeType::OUT && loom::hasContain(rbVertexs, out_vertex->m_in_allRB)) {
                // cout << "can delete without cost out loop: " << out_vertex->m_hyperId << endl;
                queueOrder.push_back(out_vertex_idx);
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                recursiveDelete(scc, pq, out_vertex, rbVertexs, waitToUpdate, queueOrder, stackOrder);
            }

            // 更新事务回滚事务集
            for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                out_vertex->m_in_allRB.unsafe_erase(vertex);
            }

            // 不存在入边，删除该超节点，递归更新scc
            if (out_vertex->m_in_allRB.size() == 0) { 
                // cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                queueOrder.push_back(out_vertex_idx);
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                recursiveDelete(scc, pq, out_vertex, rbVertexs, waitToUpdate, queueOrder, stackOrder);
            }

            /* 更新waitToUpdate
                1. 如果waitToUpdate[out_vertex]不存在，则设置为IN
                2. 如果waitToUpdate[out_vertex]为OUT，则设置为BOTH
                3. 如果waitToUpdate[out_vertex]为IN，则不更新
                4. 如果waitToUpdate[out_vertex]为BOTH，则不更新
            */
            if (waitToUpdate.find(out_vertex) == waitToUpdate.cend()) {
                waitToUpdate[out_vertex] = loom::EdgeType::IN;
            } else if (waitToUpdate[out_vertex] == loom::EdgeType::OUT) {
                waitToUpdate[out_vertex] = loom::EdgeType::BOTH;
            }

            // 更新依赖子事务(入)度
            for (auto& udVertex : rb->m_out_edges[out_vertex_idx]) {
                udVertex->m_degree--;   
            }
        }
    }
    // 递归删除入边超节点
    for (auto& in_vertex : rb->m_in_hv) {
        // 记录入边超节点索引
        auto in_vertex_idx = in_vertex->m_hyperId;
        // 超节点在scc中
        if (scc.find(in_vertex) != scc.end()) {
            // 判断是否可以无需代价直接删除 —— 可以删除
            if (rb->m_rollback_type == loom::EdgeType::IN && loom::hasContain(rbVertexs, in_vertex->m_out_allRB)) {
                // cout << "can delete without cost in loop: " << in_vertex->m_hyperId << endl;
                stackOrder.push(in_vertex_idx);
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                recursiveDelete(scc, pq, in_vertex, rbVertexs, waitToUpdate, queueOrder, stackOrder);
            }
            
            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::OUT) {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    // 若value为1，代表只有本节点和该节点依赖，可直接删除
                    if (in_vertex->m_out_allRB[vertex] == 1) {
                        in_vertex->m_out_allRB.unsafe_erase(vertex);
                    } else {
                        in_vertex->m_out_allRB[vertex]--;
                    }
                }

                if (in_vertex->m_out_allRB.size() == 0) {
                    // cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                    stackOrder.push(in_vertex_idx);
                    scc.erase(in_vertex);
                    pq.erase(in_vertex);
                    // 递归删除
                    recursiveDelete(scc, pq, in_vertex, rbVertexs, waitToUpdate, queueOrder, stackOrder);
                }

                // 更新依赖子事务(出)度
                for (auto& udVertex : rb->m_in_edges[in_vertex_idx]) {
                    udVertex->m_degree--;
                }

            } else {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    in_vertex->m_out_allRB.unsafe_erase(vertex);
                }
            }

            /* 更新waitToUpdate
                1. 如果waitToUpdate[in_vertex]不存在，则设置为OUT
                2. 如果waitToUpdate[in_vertex]为IN，则设置为BOTH
                3. 如果waitToUpdate[in_vertex]为OUT，则不更新
                4. 如果waitToUpdate[in_vertex]为BOTH，则不更新
            */
            if (waitToUpdate.find(in_vertex) == waitToUpdate.cend()) {
                waitToUpdate[in_vertex] = loom::EdgeType::OUT;
            } else if (waitToUpdate[in_vertex] == loom::EdgeType::IN) {
                waitToUpdate[in_vertex] = loom::EdgeType::BOTH;
            }
            /* 上面这段循环存在并行优化的可能 */
        }
    
    }
}

/* 不考虑度数更新scc */
void MinWRollback::updateSCCandDependencyNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq) { 
    auto rbIdx = rb->m_hyperId;
    // 递归删除出边超节点
    for (auto& out_vertex : rb->m_out_hv) {
        // 超节点在scc中
        if (scc.find(out_vertex) != scc.end()) {
            // 记录出边超节点索引
            auto out_vertex_idx = out_vertex->m_hyperId;

            out_vertex->m_in_cost -= rb->m_out_weights[out_vertex_idx];
            if (out_vertex->m_in_cost < out_vertex->m_cost) {
                pq.erase(out_vertex);
                out_vertex->m_cost = out_vertex->m_in_cost;
                out_vertex->m_rollback_type = loom::EdgeType::IN;
                pq.insert(out_vertex);
            } else if (out_vertex->m_in_cost == 0) {
                cout << "now get zero by out loop" << endl;
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                updateSCCandDependencyNoEdge(scc, out_vertex, pq);
                continue;
            }

            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::IN) {
                for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                    out_vertex->m_in_allRB.unsafe_erase(vertex);
                }
            }

            // 不存在入边，删除该超节点，递归更新scc
            if (out_vertex->m_in_allRB.size() == 0) { 
                // cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                updateSCCandDependencyNoEdge(scc, out_vertex, pq);
            }

            
        }
    }
    // 递归删除入边超节点
    for (auto& in_vertex : rb->m_in_hv) {
        // 超节点在scc中
        if (scc.find(in_vertex) != scc.end()) {
            in_vertex->m_out_cost -= in_vertex->m_out_weights[rbIdx];
            if (in_vertex->m_out_cost < in_vertex->m_cost) {
                pq.erase(in_vertex);
                in_vertex->m_cost = in_vertex->m_out_cost;
                in_vertex->m_rollback_type = loom::EdgeType::OUT;
                pq.insert(in_vertex);
            } else if (in_vertex->m_out_cost == 0) {
                cout << "now get zero by in loop" << endl;
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                updateSCCandDependencyNoEdge(scc, in_vertex, pq);
                continue;
            }


            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::OUT) {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    // 若value为1，代表只有本节点和该节点依赖，可直接删除
                    if (in_vertex->m_out_allRB[vertex] == 1) {
                        in_vertex->m_out_allRB.unsafe_erase(vertex);
                    } else {
                        in_vertex->m_out_allRB[vertex]--;
                    }
                }
            } else {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    in_vertex->m_out_allRB.unsafe_erase(vertex);
                }
            }
            // 不存在入边，删除该超节点，递归更新scc
            if (in_vertex->m_out_allRB.size() == 0) {
                // cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                updateSCCandDependencyNoEdge(scc, in_vertex, pq);
            }

        }
    
    }
}

/* 递归更新scc超节点并记录回滚事务序 */
void MinWRollback::updateSCCandDependencyNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, vector<int>& queueOrder, stack<int>& stackOrder) { 
    auto rbIdx = rb->m_hyperId;
    // 递归删除出边超节点
    for (auto& out_vertex : rb->m_out_hv) {
        // 超节点在scc中
        if (scc.find(out_vertex) != scc.end()) {
            // 记录出边超节点索引
            auto out_vertex_idx = out_vertex->m_hyperId;

            out_vertex->m_in_cost -= rb->m_out_weights[out_vertex_idx];
            if (out_vertex->m_in_cost < out_vertex->m_cost) {
                pq.erase(out_vertex);
                out_vertex->m_cost = out_vertex->m_in_cost;
                out_vertex->m_rollback_type = loom::EdgeType::IN;
                pq.insert(out_vertex);
            } else if (out_vertex->m_in_cost == 0) {
                // cout << "now get zero by out loop" << endl;
                queueOrder.push_back(out_vertex_idx);
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                updateSCCandDependencyNoEdge(scc, out_vertex, pq, queueOrder, stackOrder);
                continue;
            }

            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::IN) {
                for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                    out_vertex->m_in_allRB.unsafe_erase(vertex);
                }
            }

            // 不存在入边，删除该超节点，递归更新scc
            if (out_vertex->m_in_allRB.size() == 0) { 
                // cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                queueOrder.push_back(out_vertex_idx);
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                updateSCCandDependencyNoEdge(scc, out_vertex, pq, queueOrder, stackOrder);
            }

            
        }
    }
    // 递归删除入边超节点
    for (auto& in_vertex : rb->m_in_hv) {
        // 超节点在scc中
        if (scc.find(in_vertex) != scc.end()) {
            in_vertex->m_out_cost -= in_vertex->m_out_weights[rbIdx];
            if (in_vertex->m_out_cost < in_vertex->m_cost) {
                pq.erase(in_vertex);
                in_vertex->m_cost = in_vertex->m_out_cost;
                in_vertex->m_rollback_type = loom::EdgeType::OUT;
                pq.insert(in_vertex);
            } else if (in_vertex->m_out_cost == 0) {
                // cout << "now get zero by in loop" << endl;
                stackOrder.push(in_vertex->m_hyperId);
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                updateSCCandDependencyNoEdge(scc, in_vertex, pq, queueOrder, stackOrder);
                continue;
            }


            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::OUT) {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    // 若value为1，代表只有本节点和该节点依赖，可直接删除
                    if (in_vertex->m_out_allRB[vertex] == 1) {
                        in_vertex->m_out_allRB.unsafe_erase(vertex);
                    } else {
                        in_vertex->m_out_allRB[vertex]--;
                    }
                }
            } else {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    in_vertex->m_out_allRB.unsafe_erase(vertex);
                }
            }
            // 不存在入边，删除该超节点，递归更新scc
            if (in_vertex->m_out_allRB.size() == 0) {
                // cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                stackOrder.push(in_vertex->m_hyperId);
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                updateSCCandDependencyNoEdge(scc, in_vertex, pq, queueOrder, stackOrder);
            }
        }
    }
}

void MinWRollback::updateSCCandDependencyFastMode(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq) {
    auto rbIdx = rb->m_hyperId;
    // 递归删除出边超节点
    for (auto& out_vertex : rb->m_out_hv) {
        // 超节点在scc中
        if (scc.find(out_vertex) != scc.end()) {
            // 记录出边超节点索引
            auto out_vertex_idx = out_vertex->m_hyperId;
            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::IN) {
                for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                    out_vertex->m_in_allRB.unsafe_erase(vertex);
                }
            }

            // 不存在入边，删除该超节点，递归更新scc
            if (out_vertex->m_in_allRB.size() == 0) { 
                // cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                updateSCCandDependencyFastMode(scc, out_vertex, pq);
            }   
        }
    }
    // 递归删除入边超节点
    for (auto& in_vertex : rb->m_in_hv) {
        // 超节点在scc中
        if (scc.find(in_vertex) != scc.end()) {
            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::OUT) {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    // 若value为1，代表只有本节点和该节点依赖，可直接删除
                    if (in_vertex->m_out_allRB[vertex] == 1) {
                        in_vertex->m_out_allRB.unsafe_erase(vertex);
                    } else {
                        in_vertex->m_out_allRB[vertex]--;
                    }
                }
            } else {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    in_vertex->m_out_allRB.unsafe_erase(vertex);
                }
            }
            // 不存在入边，删除该超节点，递归更新scc
            if (in_vertex->m_out_allRB.size() == 0) {
                // cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                updateSCCandDependencyFastMode(scc, in_vertex, pq);
            }
        }
    
    }

    /*
    if (rb->m_rollback_type == loom::EdgeType::OUT) {
        // 递归删除入边超节点
        for (auto& in_vertex : rb->m_in_hv) {
            // 超节点在scc中
            if (scc.find(in_vertex) != scc.end()) {
                // in_vertex->m_out_cost -= in_vertex->m_out_weights[rbIdx];
                // if (in_vertex->m_out_cost < in_vertex->m_cost) {
                //     pq.erase(in_vertex);
                //     in_vertex->m_cost = in_vertex->m_out_cost;
                //     in_vertex->m_rollback_type = loom::EdgeType::OUT;
                //     pq.insert(in_vertex);
                // } else if (in_vertex->m_out_cost == 0) {
                //     cout << "now get zero by in loop" << endl;
                //     scc.erase(in_vertex);
                //     pq.erase(in_vertex);
                //     // 递归更新
                //     updateSCCandDependencyFastMode(scc, in_vertex, pq);
                //     continue;
                // }


                // 更新事务回滚事务集
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    // 若value为1，代表只有本节点和该节点依赖，可直接删除
                    if (in_vertex->m_out_allRB[vertex] == 1) {
                        in_vertex->m_out_allRB.unsafe_erase(vertex);
                    } else {
                        in_vertex->m_out_allRB[vertex]--;
                    }
                }
                
                // // 不存在入边，删除该超节点，递归更新scc
                // if (in_vertex->m_out_allRB.size() == 0) {
                //     cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                //     scc.erase(in_vertex);
                //     pq.erase(in_vertex);
                //     // 递归删除
                //     updateSCCandDependencyFastMode(scc, in_vertex, pq);
                // }

            }
        
        }
    }
    
    if (rb->m_rollback_type == loom::EdgeType::IN) {
        // 递归删除出边超节点
        for (auto& out_vertex : rb->m_out_hv) {
            // 超节点在scc中
            if (scc.find(out_vertex) != scc.end()) {
                // 记录出边超节点索引
                auto out_vertex_idx = out_vertex->m_hyperId;

                // out_vertex->m_in_cost -= rb->m_out_weights[out_vertex_idx];
                // if (out_vertex->m_in_cost < out_vertex->m_cost) {
                //     pq.erase(out_vertex);
                //     out_vertex->m_cost = out_vertex->m_in_cost;
                //     out_vertex->m_rollback_type = loom::EdgeType::IN;
                //     pq.insert(out_vertex);
                // } else if (out_vertex->m_in_cost == 0) {
                //     cout << "now get zero by out loop" << endl;
                //     scc.erase(out_vertex);
                //     pq.erase(out_vertex);
                //     // 递归更新
                //     updateSCCandDependencyFastMode(scc, out_vertex, pq);
                //     continue;
                // }

                // 更新事务回滚事务集
                for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                    out_vertex->m_in_allRB.unsafe_erase(vertex);
                }
                

                // // 不存在入边，删除该超节点，递归更新scc
                // if (out_vertex->m_in_allRB.size() == 0) { 
                //     cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                //     scc.erase(out_vertex);
                //     pq.erase(out_vertex);
                //     // 递归删除
                //     updateSCCandDependencyFastMode(scc, out_vertex, pq);
                // }
            }
        }

        for (auto& in_vertex : rb->m_in_hv) {
            // 超节点在scc中
            if (scc.find(in_vertex) != scc.end()) {
                // in_vertex->m_out_cost -= in_vertex->m_out_weights[rbIdx];
                // if (in_vertex->m_out_cost < in_vertex->m_cost) {
                //     pq.erase(in_vertex);
                //     in_vertex->m_cost = in_vertex->m_out_cost;
                //     in_vertex->m_rollback_type = loom::EdgeType::OUT;
                //     pq.insert(in_vertex);
                // } else if (in_vertex->m_out_cost == 0) {
                //     cout << "now get zero by in loop" << endl;
                //     scc.erase(in_vertex);
                //     pq.erase(in_vertex);
                //     // 递归更新
                //     updateSCCandDependencyFastMode(scc, in_vertex, pq);
                //     continue;
                // }


                // 更新事务回滚事务集
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    in_vertex->m_out_allRB.unsafe_erase(vertex);
                }
                
                // // 不存在入边，删除该超节点，递归更新scc
                // if (in_vertex->m_out_allRB.size() == 0) {
                //     cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                //     scc.erase(in_vertex);
                //     pq.erase(in_vertex);
                //     // 递归删除
                //     updateSCCandDependencyFastMode(scc, in_vertex, pq);
                // }
            }
        }    
    }
    */ 
    
}

void MinWRollback::updateSCCandDependencyFastMode(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, vector<int>& queueOrder, stack<int>& stackOrder) {
    auto rbIdx = rb->m_hyperId;
    // auto flag = false;
    // // rb is evicted by abort but rollback type is in
    // if (rb->m_aborted && rb->m_rollback_type == loom::EdgeType::IN){
    //     rb->m_aborted = false;
    //     flag = true;
    // }
    // 递归删除出边超节点
    for (auto& out_vertex : rb->m_out_hv) {
        // 超节点在scc中
        if (scc.find(out_vertex) != scc.end()) {
            // 记录出边超节点索引
            auto out_vertex_idx = out_vertex->m_hyperId;
            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::IN) {
                for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                    out_vertex->m_in_allRB.unsafe_erase(vertex);
                }
            }

            // 不存在入边，删除该超节点，递归更新scc
            if (out_vertex->m_in_allRB.size() == 0) { 
                // cout << "can delete without in: " << out_vertex_idx << endl;
                queueOrder.push_back(out_vertex_idx);
                scc.erase(out_vertex);
                pq.erase(out_vertex);
                // 递归删除
                updateSCCandDependencyFastMode(scc, out_vertex, pq, queueOrder, stackOrder);
            }   
        }
    }
    // 递归删除入边超节点
    for (auto& in_vertex : rb->m_in_hv) {
        // if (flag) in_vertex->m_aborted = true;
        // 超节点在scc中
        if (scc.find(in_vertex) != scc.end()) {
            // 更新事务回滚事务集
            if (rb->m_rollback_type == loom::EdgeType::OUT) {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    // 若value为1，代表只有本节点和该节点依赖，可直接删除
                    if (in_vertex->m_out_allRB[vertex] == 1) {
                        in_vertex->m_out_allRB.unsafe_erase(vertex);
                    } else {
                        in_vertex->m_out_allRB[vertex]--;
                    }
                }
            } else {
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    in_vertex->m_out_allRB.unsafe_erase(vertex);
                }
            }
            // 不存在出边，删除该超节点，递归更新scc
            if (in_vertex->m_out_allRB.size() == 0) {
                // cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                stackOrder.push(in_vertex->m_hyperId);
                scc.erase(in_vertex);
                pq.erase(in_vertex);
                // 递归删除
                updateSCCandDependencyFastMode(scc, in_vertex, pq, queueOrder, stackOrder);
            }
        }
    }

    /*
    if (rb->m_rollback_type == loom::EdgeType::OUT) {
        // 递归删除入边超节点
        for (auto& in_vertex : rb->m_in_hv) {
            // 超节点在scc中
            if (scc.find(in_vertex) != scc.end()) {
                // in_vertex->m_out_cost -= in_vertex->m_out_weights[rbIdx];
                // if (in_vertex->m_out_cost < in_vertex->m_cost) {
                //     pq.erase(in_vertex);
                //     in_vertex->m_cost = in_vertex->m_out_cost;
                //     in_vertex->m_rollback_type = loom::EdgeType::OUT;
                //     pq.insert(in_vertex);
                // } else if (in_vertex->m_out_cost == 0) {
                //     cout << "now get zero by in loop" << endl;
                //     scc.erase(in_vertex);
                //     pq.erase(in_vertex);
                //     // 递归更新
                //     updateSCCandDependencyFastMode(scc, in_vertex, pq);
                //     continue;
                // }


                // 更新事务回滚事务集
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    // 若value为1，代表只有本节点和该节点依赖，可直接删除
                    if (in_vertex->m_out_allRB[vertex] == 1) {
                        in_vertex->m_out_allRB.unsafe_erase(vertex);
                    } else {
                        in_vertex->m_out_allRB[vertex]--;
                    }
                }
                
                // // 不存在入边，删除该超节点，递归更新scc
                // if (in_vertex->m_out_allRB.size() == 0) {
                //     cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                //     scc.erase(in_vertex);
                //     pq.erase(in_vertex);
                //     // 递归删除
                //     updateSCCandDependencyFastMode(scc, in_vertex, pq);
                // }

            }
        
        }
    }
    
    if (rb->m_rollback_type == loom::EdgeType::IN) {
        // 递归删除出边超节点
        for (auto& out_vertex : rb->m_out_hv) {
            // 超节点在scc中
            if (scc.find(out_vertex) != scc.end()) {
                // 记录出边超节点索引
                auto out_vertex_idx = out_vertex->m_hyperId;

                // out_vertex->m_in_cost -= rb->m_out_weights[out_vertex_idx];
                // if (out_vertex->m_in_cost < out_vertex->m_cost) {
                //     pq.erase(out_vertex);
                //     out_vertex->m_cost = out_vertex->m_in_cost;
                //     out_vertex->m_rollback_type = loom::EdgeType::IN;
                //     pq.insert(out_vertex);
                // } else if (out_vertex->m_in_cost == 0) {
                //     cout << "now get zero by out loop" << endl;
                //     scc.erase(out_vertex);
                //     pq.erase(out_vertex);
                //     // 递归更新
                //     updateSCCandDependencyFastMode(scc, out_vertex, pq);
                //     continue;
                // }

                // 更新事务回滚事务集
                for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                    out_vertex->m_in_allRB.unsafe_erase(vertex);
                }
                

                // // 不存在入边，删除该超节点，递归更新scc
                // if (out_vertex->m_in_allRB.size() == 0) { 
                //     cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                //     scc.erase(out_vertex);
                //     pq.erase(out_vertex);
                //     // 递归删除
                //     updateSCCandDependencyFastMode(scc, out_vertex, pq);
                // }
            }
        }

        for (auto& in_vertex : rb->m_in_hv) {
            // 超节点在scc中
            if (scc.find(in_vertex) != scc.end()) {
                // in_vertex->m_out_cost -= in_vertex->m_out_weights[rbIdx];
                // if (in_vertex->m_out_cost < in_vertex->m_cost) {
                //     pq.erase(in_vertex);
                //     in_vertex->m_cost = in_vertex->m_out_cost;
                //     in_vertex->m_rollback_type = loom::EdgeType::OUT;
                //     pq.insert(in_vertex);
                // } else if (in_vertex->m_out_cost == 0) {
                //     cout << "now get zero by in loop" << endl;
                //     scc.erase(in_vertex);
                //     pq.erase(in_vertex);
                //     // 递归更新
                //     updateSCCandDependencyFastMode(scc, in_vertex, pq);
                //     continue;
                // }


                // 更新事务回滚事务集
                for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                    in_vertex->m_out_allRB.unsafe_erase(vertex);
                }
                
                // // 不存在入边，删除该超节点，递归更新scc
                // if (in_vertex->m_out_allRB.size() == 0) {
                //     cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                //     scc.erase(in_vertex);
                //     pq.erase(in_vertex);
                //     // 递归删除
                //     updateSCCandDependencyFastMode(scc, in_vertex, pq);
                // }
            }
        }    
    }
    */ 
    
}

/* fibonacci heap 看起来并不高效*/
void MinWRollback::updateSCCandDependencyNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs) {
    if (scc.size() <= 1) {
        return;
    }
    
    auto rbIdx = rb->m_hyperId;
    
    // if (rb->m_rollback_type == loom::EdgeType::OUT) {

        // 遍历出边
        for (auto& out_vertex : rb->m_out_hv) {
            if (scc.find(out_vertex) != scc.end()) {
                auto out_vertex_idx = out_vertex->m_hyperId;
                out_vertex->m_in_cost -= rb->m_out_weights[out_vertex_idx];
                if (out_vertex->m_in_cost == 0) {
                    cout << "can delete without in: " << out_vertex->m_hyperId << endl;
                    scc.erase(out_vertex);
                    heap.erase(handles[out_vertex]);
                    handles.erase(out_vertex);
                    // 递归更新scc
                    // recursiveUpdateNoEdge(scc, pq, out_vertex, rbVertexs);
                    updateSCCandDependencyNoEdge(scc, out_vertex, heap, handles, rbVertexs);
                    continue;
                } else if (out_vertex->m_in_cost < out_vertex->m_cost) {
                    cout << "heap size: " << heap.size() << " id: " << out_vertex->m_hyperId
                            << " new cost: " << out_vertex->m_in_cost << " cost: " << out_vertex->m_cost << endl;
                    
                    out_vertex->m_cost = out_vertex->m_in_cost;
                    out_vertex->m_rollback_type = loom::EdgeType::IN;
                    heap.decrease(handles[out_vertex]);
                }
                if (rb->m_rollback_type == loom::EdgeType::IN) {
                    // 更新事务回滚事务集
                    for (auto& vertex : rb->m_out_rollback[out_vertex_idx]) {
                        out_vertex->m_in_allRB.unsafe_erase(vertex);
                    } 
                }
            }
        }

        // 遍历入边
        for (auto& in_vertex : rb->m_in_hv) {
            if (scc.find(in_vertex) != scc.end()) {                
                in_vertex->m_out_cost -= in_vertex->m_out_weights[rbIdx];
                if (in_vertex->m_out_cost == 0) {
                    cout << "can delete without out: " << in_vertex->m_hyperId << endl;
                    scc.erase(in_vertex);
                    heap.erase(handles[in_vertex]);
                    handles.erase(in_vertex);
                    // 递归更新scc
                    // recursiveUpdateNoEdge(scc, pq, in_vertex, rbVertexs);
                    updateSCCandDependencyNoEdge(scc, in_vertex, heap, handles, rbVertexs);
                    continue;
                } else if (in_vertex->m_out_cost < in_vertex->m_cost) {
                    cout << "heap size: " << heap.size() << " id: " << in_vertex->m_hyperId
                            << " new cost: " << in_vertex->m_in_cost << " cost: " << in_vertex->m_cost << endl;
                    
                    in_vertex->m_cost = in_vertex->m_out_cost;
                    in_vertex->m_rollback_type = loom::EdgeType::OUT;
                    heap.decrease(handles[in_vertex]);
                }
                if (rb->m_rollback_type == loom::EdgeType::OUT) {
                    // 更新事务回滚事务集
                    for (auto& vertex : in_vertex->m_out_rollback[rbIdx]) {
                        // 若value为1，代表只有本节点和该节点依赖，可直接删除
                        if (in_vertex->m_out_allRB[vertex] == 1) {
                            in_vertex->m_out_allRB.unsafe_erase(vertex);
                        } else {
                            in_vertex->m_out_allRB[vertex]--;
                        }
                    }
                }
            }
        }
    
    // }
}


/* 由于超节点中某些子事务的度数发生更新，尝试更新相关超节点回滚代价
    遍历超节点与其它在scc中的超节点间的回滚子事务集
    若与udVertexs有交集，则尝试更新这两个超节点间的回滚代价
    边回滚集合要更新，总回滚集合要更新，超节点回滚代价要更新, 存在原本需要回滚的已经回滚了的现象
*/
void MinWRollback::updateHyperVertexWeight(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, HyperVertex::Ptr& hyperVertex, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& udVertexs, 
                                           const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated) {
    // cout << "want to update hyperVertex " << hyperVertex->m_hyperId << " and related hyperVertex" << endl;
        
    // 记录更新过回滚集的超节点
    tbb::concurrent_unordered_map<HyperVertex::Ptr, loom::EdgeType, HyperVertex::HyperVertexHash> updated;

    // 遍历出边需要回滚的子事务
    for (auto& out_hv : hyperVertex->m_out_hv) {
        auto& rollbackSet = hyperVertex->m_out_rollback[out_hv->m_hyperId];
        // 判断是否在scc中 && 已经被计算过
        if (scc.find(out_hv) == scc.cend() || calculated.find(out_hv) != calculated.cend()) {
            continue;
        // 判断是否与udVertexs有交集
        } else if (loom::hasIntersection(rollbackSet, udVertexs)) {
            // 记录需要重新计算权重的超节点
            updated.insert(std::make_pair(out_hv, loom::EdgeType::IN));
        }
    }
    
    // cout << "hyperId: " << hyperVertex->m_hyperId << " inUpdated: " << inUpdated << " outUpdated: " << outUpdated << endl;
    // cout << "updated: ";
    // for (auto& v : updated) {
    //     cout  << v.first->m_hyperId << " type: " << loom::edgeTypeToString(v.second) << " | ";
    // }
    // cout << endl;


    // 增加本节点并更新权重
    updated.insert(std::make_pair(hyperVertex, loom::EdgeType::BOTH));
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
void MinWRollback::calculateWeight(HyperVertex::Ptr& hyperVertex, loom::EdgeType& type) {
    // cout << "======= now hyperVertex " << hyperVertex->m_hyperId << " is calculating weight =======" << endl;

    if (type == loom::EdgeType::OUT) {
        // 计算m_out_cost
        int out_degree = 0;
        hyperVertex->m_out_cost = 0;
        for (auto v : hyperVertex->m_out_allRB) {
            hyperVertex->m_out_cost += v.first->m_self_cost;
            out_degree += v.first->m_degree;
        }
        
        // cout << "out weight: " << hyperVertex->m_out_cost << " out degree: " << out_degree << endl;

        hyperVertex->m_out_cost /= out_degree;
        
    } else if (type == loom::EdgeType::IN) {
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
        hyperVertex->m_rollback_type = loom::EdgeType::IN;
        hyperVertex->m_cost = hyperVertex->m_in_cost;
    } else {
        hyperVertex->m_rollback_type = loom::EdgeType::OUT;
        hyperVertex->m_cost = hyperVertex->m_out_cost;
    }
}

/* 基于RBIndex快速回滚 */
void MinWRollback::fastRollback(const unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>>& RBIndex, std::vector<Vertex::Ptr>& rbList) {
    // cout << "RBIndex size: " << RBIndex.size() << endl;
    for (auto& rbMap : RBIndex) {
        auto& rbKey = rbMap.first;
        auto& rbSet = rbMap.second;
        // // rbKey.substr(0, 5) == "Dytd-" || rbKey.substr(0, 2) == "S-" || rbKey.substr(0, 10) == "Cdelivery-"
        if (rbKey.substr(0, 10) == "Cdelivery-") {
            // 把rbset中除第一个元素的其它元素的回滚子事务加入rbList中
            auto it = std::next(rbSet.begin());
            std::for_each(it, rbSet.end(), [&](const auto& elem) {
                std::copy(elem->cascadeVertices.begin(), elem->cascadeVertices.end(), std::back_inserter(rbList));
            });
        } else {
            // 把rbSet中除第一个元素的其它元素加入rbList中
            auto it = std::next(rbSet.begin());
            std::copy(it, rbSet.end(), std::back_inserter(rbList));
        }

        // if (rbKey.substr(0, 2) == "S-") {
        //     // 把rbSet中除第一个元素的其它元素加入rbList中
        //     auto it = std::next(rbSet.begin());
        //     std::copy(it, rbSet.end(), std::back_inserter(rbList));
        // }

        // if (rbKey.substr(0, 5) == "Dytd-") {
        //     // 把rbSet中除第一个元素的其它元素加入rbList中
        //     auto it = std::next(rbSet.begin());
        //     std::copy(it, rbSet.end(), std::back_inserter(rbList));
        // }
    }
}

void MinWRollback::fastRollback(const unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>>& RBIndex, std::vector<Vertex::Ptr>& rbList, std::vector<Vertex::Ptr>& nestedList) {
    // cout << "RBIndex size: " << RBIndex.size() << endl;
    for (auto& rbMap : RBIndex) {
        auto& rbKey = rbMap.first;
        auto& rbSet = rbMap.second;
        // // rbKey.substr(0, 5) == "Dytd-" || rbKey.substr(0, 2) == "S-" || rbKey.substr(0, 10) == "Cdelivery-"
        if (rbKey.substr(0, 10) == "Cdelivery-") {
            // 把rbset中除第一个元素的其它元素的回滚子事务加入rbList中
            auto it = std::next(rbSet.begin());
            std::for_each(it, rbSet.end(), [&](const auto& elem) {
                std::copy(elem->cascadeVertices.begin(), elem->cascadeVertices.end(), std::back_inserter(rbList));
            });
        } else if (rbKey.substr(0, 2) == "S-") {
            // 把rbSet中除第一个元素的其它元素加入rbList中
            auto it = std::next(rbSet.begin());
            std::copy(it, rbSet.end(), std::back_inserter(rbList));
        } else {
            // 把rbSet中除第一个元素的其它元素加入rbList中
            auto it = std::next(rbSet.begin());
            std::copy(it, rbSet.end(), std::back_inserter(nestedList));
        }

        // if (rbKey.substr(0, 2) == "S-") {
        //     // 把rbSet中除第一个元素的其它元素加入rbList中
        //     auto it = std::next(rbSet.begin());
        //     std::copy(it, rbSet.end(), std::back_inserter(rbList));
        // }

        // if (rbKey.substr(0, 5) == "Dytd-") {
        //     // 把rbSet中除第一个元素的其它元素加入rbList中
        //     auto it = std::next(rbSet.begin());
        //     std::copy(it, rbSet.end(), std::back_inserter(rbList));
        // }
    }
}

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
        // cout << "id: " << tx->m_id << " cost: " << tx->m_self_cost << endl;
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

// 初始化静态成员
std::vector<HyperVertex::Ptr> MinWRollback::dummyHyperVertices;
std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> MinWRollback::dummyRWIndex;