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

        void buildVertexs(Transaction::Ptr tx, Vertex::Ptr vertex, string txid);

        // Vertex::Ptr getVertexById(const std::string& id) const;

    private:
        int hyperId;    // 超节点ID
        int min_in;     // 超节点的最小入度ID
        int min_out;    // 超节点的最小出度ID
        tbb::concurrent_unordered_set<Vertex::Ptr> m_vertices;    // 记录所有节点
        
};