#include "minWRollback.h"

using namespace std;

/*  执行算法: 将transaction转化为hyperVertex
    详细算法流程: 
        1. 多线程并发执行所有事务，获得事务执行信息（读写集，事务结构，执行时间 => 随机生成）
        2. 判断执行完成的事务与其它执行完成事务间的rw依赖，构建rw依赖图
    /待测试.../
*/
void minWRollback::execute(Transaction::Ptr tx) {
    int txid = getId();
    HyperVertex::Ptr hyperVertex = make_shared<HyperVertex>(txid);
    Vertex::Ptr rootVertex = make_shared<Vertex>(this, txid, to_string(txid));
    // 根据事务结构构建超节点
    hyperVertex->buildVertexs(tx, rootVertex, to_string(txid));
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
        2. 若与节点存在wr依赖（入边）
 */
void minWRollback::buileGraph(tbb::concurrent_unordered_set<Vertex::Ptr>& vertices) {
    for (auto& newV: vertices) {
        for (auto& oldV: m_vertices) {
            // 存在rw依赖
            if (hasConflict(newV->readSet, oldV->writeSet)) {
                // newV新增一条出边
                newV->m_out_edges.insert(oldV);
                // 更新newV对应的hyperVertex的out_edges
                // 记录的啥 有待商榷
                newV->m_hyperVertex->m_out_edges[oldV->m_hyperVertex].insert(newV);
                // 更新依赖数
                newV->m_degree++;
                // 尝试更新newV对应的hyperVertex的min_out
                int min_out = min(oldV->m_hyperVertex->m_min_out, oldV->m_hyperId);
                if (min_out < newV->m_hyperVertex->m_min_out) {
                    recursiveUpdate(newV->m_hyperVertex, min_out, minw::RecursiveType::OUT);
                }

                // oldV新增一条入边
                oldV->m_in_edges.insert(newV);
                // 更新oldV对应的hyperVertex的in_edges
                oldV->m_hyperVertex->m_in_edges[newV->m_hyperVertex].insert(oldV);
                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_in
                int min_in = min(newV->m_hyperVertex->m_min_in, newV->m_hyperId);
                if (min_in < oldV->m_hyperVertex->m_min_in) {
                    recursiveUpdate(oldV->m_hyperVertex, min_in, minw::RecursiveType::IN);
                }
            
            // 存在wr依赖
            } else if (hasConflict(newV->writeSet, oldV->readSet)) {
                // newV新增一条入边
                newV->m_in_edges.insert(oldV);
                // 更新newV对应的hyperVertex的in_edges
                newV->m_hyperVertex->m_in_edges[oldV->m_hyperVertex].insert(newV);
                // 更新依赖数
                newV->m_degree++;
                // 尝试更新newV对应的hyperVertex的min_in
                int min_in = min(oldV->m_hyperVertex->m_min_in, oldV->m_hyperId);
                if (min_in < newV->m_hyperVertex->m_min_in) {
                    recursiveUpdate(newV->m_hyperVertex, min_in, minw::RecursiveType::IN);
                }

                // oldV新增一条出边                
                oldV->m_out_edges.insert(newV);
                // 更新oldV对应的hyperVertex的out_edges
                oldV->m_hyperVertex->m_out_edges[newV->m_hyperVertex].insert(oldV);
                // 更新依赖数
                oldV->m_degree++;
                // 尝试更新oldV对应的hyperVertex的min_out
                int min_out = min(newV->m_hyperVertex->m_min_out, newV->m_hyperId);
                if (min_out < oldV->m_hyperVertex->m_min_out) {
                    recursiveUpdate(oldV->m_hyperVertex, min_out, minw::RecursiveType::OUT);
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

void minWRollback::recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, minw::RecursiveType type) {
    if (type == minw::RecursiveType::OUT) {
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

/* 
  确定性回滚: 从hyperGraph中找到最小的回滚子事务集合
*/
void minWRollback::rollback() {

}