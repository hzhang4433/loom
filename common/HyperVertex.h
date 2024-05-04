#pragma once

#include <memory>
#include <tbb/concurrent_unordered_map.h>
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


        // Vertex::Ptr getVertexById(const std::string& id) const;

        struct HyperVertexHash {
            std::size_t operator()(const HyperVertex::Ptr& v) const {
                // 使用HyperVertex的地址作为哈希值
                return std::hash<HyperVertex*>()(v.get());
            }
        };

    // 公共变量
        int m_hyperId;      // 超节点ID
        int m_min_in;     // 超节点的最小入度ID
        int m_min_out;    // 超节点的最小出度ID
        tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_out_edges;    // 记录超节点中所有出边
        tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_in_edges;     // 记录超节点中所有入边
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_vertices;  // 记录所有节点
        Vertex::Ptr m_rootVertex;                               // 根节点
        
};