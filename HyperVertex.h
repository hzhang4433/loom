#pragma once

#include <memory>
#include <tbb/concurrent_unordered_map.h>
#include "Vertex.h"

class HyperVertex : public std::enable_shared_from_this<HyperVertex>
{
    public:
        typedef std::shared_ptr<HyperVertex> Ptr;

        HyperVertex();

        ~HyperVertex();

        void buildTree();

        Vertex::Ptr getVertexById(const std::string& id) const;

    private:
        tbb::concurrent_unordered_map<std::string, Vertex::Ptr> m_vertices;    // 记录所有节点
};