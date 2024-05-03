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
    Vertex::Ptr rootVertex = make_shared<Vertex>(this, to_string(txid));
    // 根据事务结构构建超节点
    hyperVertex->buildVertexs(tx, rootVertex, to_string(txid));
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
    详细算法流程:

 */
void minWRollback::buileGraph(tbb::concurrent_unordered_set<Vertex::Ptr>& vertices) {

}

/* 
  确定性回滚: 从hyperGraph中找到最小的回滚子事务集合
*/
void minWRollback::rollback() {

}