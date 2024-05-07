#pragma once

#include <memory>
#include <tbb/concurrent_unordered_map.h>
#include <map>
#include <set>
#include "Vertex.h"
#include "../workload/tpcc/Transaction.hpp"

using namespace std;

class HyperVertex : public std::enable_shared_from_this<HyperVertex>
{
    public:
        typedef std::shared_ptr<HyperVertex> Ptr;

        HyperVertex(int id);

        ~HyperVertex();

        double buildVertexs(const Transaction::Ptr& tx, HyperVertex::Ptr& hyperVertex, Vertex::Ptr& vertex, string& txid);

        void recognizeCascades(Vertex::Ptr vertex);

        // 递归打印超节点结构树
        void printVertexTree();

        struct HyperVertexHash {
            std::size_t operator()(const HyperVertex::Ptr& v) const {
                // 使用HyperVertex的地址作为哈希值
                return std::hash<HyperVertex*>()(v.get());
            }
        };

    // 公共变量
        int m_hyperId;      // 超节点ID
        int m_min_in;       // 超节点的最小入度ID
        int m_min_out;      // 超节点的最小出度ID
        double m_cost;      // 超节点的最小回滚代价
        double m_in_cost;   // 超节点的最小入度回滚代价 m_in_cost = in_cost1 + in_cost2 + ... + in_costn
        double m_out_cost;  // 超节点的最小出度回滚代价 m_out_cost = out_cost1 + out_cost2 + ... + out_costn
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_in_allRB;  // 记录超节点中所有入边的级联回滚子事务
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_out_allRB; // 记录超节点中所有出边的级联回滚子事务
        tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_in_rollback;  // 记录入边的级联回滚子事务
        tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_out_rollback; // 记录出边的级联回滚子事务
// 哪种好，带测试
        // 想排序用下面这个
        tbb::concurrent_unordered_map<HyperVertex::Ptr, set<map<Vertex::Ptr, Vertex::Ptr, Vertex::VertexCompare>, Vertex::MapCompare>, HyperVertexHash> m_out_edges;    // 记录超节点中所有出边, 格式：hyperVertex => {<v1, v2>, <v1, v2>, ...}
        tbb::concurrent_unordered_map<HyperVertex::Ptr, set<map<Vertex::Ptr, Vertex::Ptr ,Vertex::VertexCompare>, Vertex::MapCompare>, HyperVertexHash> m_in_edges;     // 记录超节点中所有入边
        // 想并发用下面这个
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<tbb::concurrent_unordered_map<Vertex::Ptr, Vertex::Ptr, Vertex::VertexHash>>, HyperVertexHash> m_out_edges;    // 记录超节点中所有出边, 格式：hyperVertex => {<v1, v2>, <v1, v2>, ...}
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<tbb::concurrent_unordered_map<Vertex::Ptr, Vertex::Ptr ,Vertex::VertexHash>>, HyperVertexHash> m_in_edges;     // 记录超节点中所有入边
        tbb::concurrent_unordered_map<HyperVertex::Ptr, double, HyperVertexHash> m_out_weights; //记录出边边权
        tbb::concurrent_unordered_map<HyperVertex::Ptr, double, HyperVertexHash> m_in_weights;  //记录入边边权
        minw::EdgeType m_rollback_type; // 边类型
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_vertices;  // 记录所有节点
        Vertex::Ptr m_rootVertex;                               // 根节点
        
};