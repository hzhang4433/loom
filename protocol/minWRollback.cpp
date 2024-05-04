#include <algorithm>
#include "minWRollback.h"

using namespace std;

/*  执行算法: 将transaction转化为hyperVertex
    详细算法流程: 
        1. 多线程并发执行所有事务，获得事务执行信息（读写集，事务结构，执行时间 => 随机生成）
        2. 判断执行完成的事务与其它执行完成事务间的rw依赖，构建rw依赖图
    /待测试.../
*/
void minWRollback::execute(const Transaction::Ptr& tx) {
    int txid = getId();
    HyperVertex::Ptr hyperVertex = make_shared<HyperVertex>(txid);
    Vertex::Ptr rootVertex = make_shared<Vertex>(hyperVertex, txid, to_string(txid));
    // 根据事务结构构建超节点
    string txid_str = to_string(txid);
    hyperVertex->buildVertexs(tx, hyperVertex, rootVertex, txid_str);
    // 记录超节点包含的所有节点
    hyperVertex->m_vertices = rootVertex->cascadeVertices;
    // 根据子节点依赖更新回滚代价和级联子事务
    hyperVertex->m_rootVertex = rootVertex;
    hyperVertex->recognizeCascades(rootVertex);
    // 构建超图
    buileGraph(rootVertex->cascadeVertices);
    // 更新图中现有节点
    m_vertices.insert(rootVertex->cascadeVertices.begin(), rootVertex->cascadeVertices.end());
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
 */
void minWRollback::buileGraph(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& vertices) {
    for (auto& newV: vertices) {
        for (auto& oldV: m_vertices) {
            // 存在rw依赖
            if (hasConflict(newV->readSet, oldV->writeSet)) {
                // newV新增一条出边
                newV->m_out_edges.insert(oldV);

                // 更新newV对应的hyperVertex的out_edges
                // 记录的啥 有待商榷
                auto& out_edges = newV->m_hyperVertex->m_out_edges[oldV->m_hyperVertex];
                if (!std::any_of(out_edges.begin(), out_edges.end(), [&](const Vertex::Ptr& out_edge) {
                    return isAncester(out_edge->m_id, newV->m_id);
                })) {
                    out_edges.insert(newV);
                }
                
                // 更新依赖数
                newV->m_degree++;
                // 尝试更新newV对应的hyperVertex的min_out
                int min_out = min(oldV->m_hyperVertex->m_min_out, oldV->m_hyperId);
                if (min_out < newV->m_hyperVertex->m_min_out) {
                    recursiveUpdate(newV->m_hyperVertex, min_out, minw::EdgeType::OUT);
                }

                // oldV新增一条入边
                oldV->m_in_edges.insert(newV);

                // 更新oldV对应的hyperVertex的in_edges
                auto& in_edges = oldV->m_hyperVertex->m_in_edges[newV->m_hyperVertex];
                if (!std::any_of(in_edges.begin(), in_edges.end(), [&](const Vertex::Ptr& in_edge) {
                    return isAncester(in_edge->m_id, oldV->m_id);
                })) {
                    in_edges.insert(oldV);
                }

                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_in
                int min_in = min(newV->m_hyperVertex->m_min_in, newV->m_hyperId);
                if (min_in < oldV->m_hyperVertex->m_min_in) {
                    recursiveUpdate(oldV->m_hyperVertex, min_in, minw::EdgeType::IN);
                }
            
            // 存在wr依赖
            } else if (hasConflict(newV->writeSet, oldV->readSet)) {
                // newV新增一条入边
                newV->m_in_edges.insert(oldV);

                // 更新newV对应的hyperVertex的in_edges
                auto& in_edges = newV->m_hyperVertex->m_in_edges[oldV->m_hyperVertex];
                if (!std::any_of(in_edges.begin(), in_edges.end(), [&](const Vertex::Ptr& in_edge) {
                    return isAncester(in_edge->m_id, newV->m_id);
                })) {
                    in_edges.insert(newV);
                }

                // 更新依赖数
                newV->m_degree++;
                // 尝试更新newV对应的hyperVertex的min_in
                int min_in = min(oldV->m_hyperVertex->m_min_in, oldV->m_hyperId);
                if (min_in < newV->m_hyperVertex->m_min_in) {
                    recursiveUpdate(newV->m_hyperVertex, min_in, minw::EdgeType::IN);
                }

                // oldV新增一条出边                
                oldV->m_out_edges.insert(newV);

                // 更新oldV对应的hyperVertex的out_edges
                auto& out_edges = oldV->m_hyperVertex->m_out_edges[newV->m_hyperVertex];
                if (!std::any_of(out_edges.begin(), out_edges.end(), [&](const Vertex::Ptr& out_edge) {
                    return isAncester(out_edge->m_id, oldV->m_id);
                })) {
                    out_edges.insert(oldV);
                }

                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_out
                int min_out = min(newV->m_hyperVertex->m_min_out, newV->m_hyperId);
                if (min_out < oldV->m_hyperVertex->m_min_out) {
                    recursiveUpdate(oldV->m_hyperVertex, min_out, minw::EdgeType::OUT);
                }
            }
        }
    }
}

bool minWRollback::hasConflict(tbb::concurrent_unordered_set<std::string>& set1, tbb::concurrent_unordered_set<std::string>& set2) {
    for (auto item : set2) {
        if (set1.find(item) != set1.end()) {
            return true;
        }
    }
    return false;
}

// 判断v1是否是v2的祖先
bool minWRollback::isAncester(const string& v1, const string& v2) {
    // 子孙节点id的前缀包含祖先节点id
    return v2.find(v1) == 0;
}

void minWRollback::recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, minw::EdgeType type) {
    // 或者后续不在这里记录强连通分量，后续根据开销进行调整。。。
    
    // 更新前需要把这个Hypervertex从原有m_min2HyperVertex中删除 (前提:他们都不是初始值)
    if (hyperVertex->m_min_in != INT_MAX && hyperVertex->m_min_out != INT_MAX) {
        m_min2HyperVertex[combine(hyperVertex->m_min_in, hyperVertex->m_min_out)].unsafe_erase(hyperVertex);
    }
    
    if (type == minw::EdgeType::OUT) {
        if (hyperVertex->m_min_in != UINT64_MAX) {
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
        if (hyperVertex->m_min_out != UINT64_MAX) {
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

long long minWRollback::combine(int a, int b) {
    return a * 1000001LL + b;
}

/* 
  确定性回滚: 从hyperGraph中找到最小的回滚子事务集合
*/
tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> minWRollback::rollback() {
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> result;
    // 遍历所有强连通分量
    for (auto scc : m_min2HyperVertex) {
        // 定义优先队列，以hyperVertex.m_cost为key从小到大排序
        priority_queue<HyperVertex::Ptr, vector<HyperVertex::Ptr>, cmp> pq;

        // 遍历强连通分量中每个超节点，计算scc超节点间边权
        calculateHyperVertexWeight(scc.second, pq);

        // 贪心获取最小回滚代价的节点集
        auto minWVs = GreedySelectVertex(scc.second, pq);
        result.insert(minWVs.cbegin(), minWVs.cend()) ;
    }
    return result;
}

/* 计算强连通分量中每个超节点的回滚代价
    1. 计算scc超节点间边权
    2. 计算每个超节点的最小回滚代价
    3. 将超节点放入优先队列
*/
void minWRollback::calculateHyperVertexWeight(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, priority_queue<HyperVertex::Ptr, vector<HyperVertex::Ptr>, cmp>& pq) {
    for (auto& hyperVertex : scc) {
        // 遍历超节点的出边
        for (auto out_edge : hyperVertex->m_out_edges) {
            // 计算超节点间边权, 获得超节点出边回滚代价
            hyperVertex->m_out_cost += calculateVertexWeight(hyperVertex, out_edge.first, minw::EdgeType::OUT);
        }
        // 遍历超节点的入边
        for (auto in_edge : hyperVertex->m_in_edges) {
            // 计算超节点间边权, 获得超节点入边回滚代价
            hyperVertex->m_in_cost += calculateVertexWeight(hyperVertex, in_edge.first, minw::EdgeType::IN);
        }
        // 计算超节点的最小回滚代价
        hyperVertex->m_cost = min(hyperVertex->m_in_cost, hyperVertex->m_out_cost);
        pq.push(hyperVertex);
    }
}

/* 计算超节点间边权
    1. 计算hv1的回滚代价
    2. 计算hv2的回滚代价
    3. 设置边权为min(hv1回滚代价，hv2回滚代价)
*/
double minWRollback::calculateVertexWeight(HyperVertex::Ptr hv1, HyperVertex::Ptr hv2, minw::EdgeType type) {
    double hv1_cost = 0, hv2_cost = 0;
    int rollbackDegree1 = 0, rollbackDegree2 = 0;
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> rollbackVertex1, rollbackVertex2;
    
    if (type == minw::EdgeType::OUT) {
        // 如果已经存在边权，则直接返回
        if (hv1->m_out_weights.find(hv2) != hv1->m_out_weights.end()) {
            return hv1->m_out_weights[hv2];
        }
        // 计算hv1的回滚代价
// hv1->m_out_edges中记录的子节点不应该存在冗余
        for (auto& v : hv1->m_out_edges[hv2]) {
            rollbackVertex1.insert(v->cascadeVertices.cbegin(), v->cascadeVertices.cend());
            hv1_cost += v->m_cost;
        }
        for (auto& v : rollbackVertex1) {
            rollbackDegree1 += v->m_degree;
        }
        hv1_cost /= rollbackDegree1;

        // 计算hv2的回滚代价
        for (auto& v : hv2->m_in_edges[hv1]) {
            rollbackVertex2.insert(v->cascadeVertices.cbegin(), v->cascadeVertices.cend());
            hv2_cost += v->m_cost;
        }
        for (auto& v : rollbackVertex2) {
            rollbackDegree2 += v->m_degree;
        }
        hv2_cost /= rollbackDegree2;

        // 设置边权
        double weight = min(hv1_cost, hv2_cost);
        hv1->m_out_weights[hv2] = weight;
        hv2->m_in_weights[hv1] = weight;
        return weight;
    } else {
        // 如果已经存在边权，则直接返回
        if (hv1->m_in_weights.find(hv2) != hv1->m_in_weights.end()) {
            return hv1->m_in_weights[hv2];
        }
        // 计算hv1的回滚代价
        for (auto& v : hv1->m_in_edges[hv2]) {
            rollbackVertex1.insert(v->cascadeVertices.cbegin(), v->cascadeVertices.cend());
            hv1_cost += v->m_cost;
        }
        for (auto& v : rollbackVertex1) {
            rollbackDegree1 += v->m_degree;
        }
        hv1_cost /= rollbackDegree1;

        // 计算hv2的回滚代价
        for (auto& v : hv2->m_out_edges[hv1]) {
            rollbackVertex2.insert(v->cascadeVertices.cbegin(), v->cascadeVertices.cend());
            hv2_cost += v->m_cost;
        }
        for (auto& v : rollbackVertex2) {
            rollbackDegree2 += v->m_degree;
        }
        hv2_cost /= rollbackDegree2;

        // 设置边权
        double weight = min(hv1_cost, hv2_cost);
        hv1->m_in_weights[hv2] = weight;
        hv2->m_out_weights[hv1] = weight;
        return weight;
    }
}

/* 回滚最小代价的超节点
    1. 选择回滚代价最小的超节点
    2. 更新scc中与它相邻的超节点的回滚代价
    3. 若存在一条边只有出度或入度，则递归更新
    4. 更新scc
*/
tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> minWRollback::GreedySelectVertex
            (tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, 
            priority_queue<HyperVertex::Ptr, vector<HyperVertex::Ptr>, cmp>& pq) {
    // 存储回滚节点集合
    tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> result;
    
    
    /* 1. 选择回滚代价最小的超节点
        
    */


    /* 2. 更新scc
        2.1 拿出优先队列队头超节点rb，回滚该笔事务对应的（子）事务
        2.2 遍历与该超节点直接依赖的超节点di并更新回滚代价
            2.1 若它不存在出边或入边，则从scc中删除
                2.2.1 递归更新相关超节点回滚代价
            2.2 若它存在出边或入边，则更新回滚代价
        2.3 从scc中删除该超节点rb
    */
    
    if (scc.size() > 1) {
        // 递归调用GreedySelectVertex获取回滚节点集合

    }
}