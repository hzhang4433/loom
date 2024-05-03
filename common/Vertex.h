#pragma once

#include <memory>
#include <string>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>
#include "common.h"

class HyperVertex;

class Vertex : public std::enable_shared_from_this<Vertex>
{
    public:
        typedef std::shared_ptr<Vertex> Ptr;

        struct VertexHash {
            std::size_t operator()(const Vertex::Ptr& v) const {
                // 使用Vertex的地址作为哈希值
                return std::hash<Vertex*>()(v.get());
            }
        };

        struct VertexEqual {
            bool operator()(const Vertex::Ptr& v1, const Vertex::Ptr& v2) const {
                return v1->m_id == v2->m_id;
            }
        };

        struct ChildVertex {
            Vertex::Ptr vertex;
            minw::DependencyType dependency;
        };

        Vertex(std::shared_ptr<HyperVertex> hyperVertex, int hyperId, std::string id, bool isNested = false);

        ~Vertex();

        // Vertex::Ptr getParent() const { return m_parent; }
        // void setParent(Vertex::Ptr parent) { m_parent = parent; }

        const tbb::concurrent_unordered_set<ChildVertex>& getChildren() const;
        void addChild(Vertex::Ptr child, minw::DependencyType dependency);

        minw::DependencyType getDependencyType() const;
        void setDependencyType(minw::DependencyType type);

        int mapToHyperId() const;

    // 公共变量
        std::shared_ptr<HyperVertex> m_hyperVertex;                                                 // 记录节点对应的超节点
        int m_hyperId;                                                                              // 记录节点对应的超节点id
        std::string m_id;                                                                           // 记录节点自身的id
        int m_min_in;                                                                               // 记录节点能被哪个最小id的节点到达
        int m_min_out;                                                                              // 记录节点能到达的最小id的节点
        double m_cost;                                                                              // 记录节点的执行代价 => 由执行时间正则化得到
        int m_degree;                                                                               // 记录节点的度
        tbb::concurrent_unordered_set<Vertex::Ptr> m_in_edges;        // 记录节点的入边, 格式：节点指针 => 可抵达最小id
        tbb::concurrent_unordered_set<Vertex::Ptr> m_out_edges;       // 记录节点的出边, 格式：节点指针 => 可到达最小id
        tbb::concurrent_unordered_set<Vertex::Ptr> cascadeVertices;                                 // 记录级联回滚节点
        tbb::concurrent_unordered_set<std::string> readSet;                                         // 记录读集
        tbb::concurrent_unordered_set<std::string> writeSet;                                        // 记录写集
        bool isNested;                                                                              // 标记节点是否是嵌套节点
        // Ptr m_parent;                                                                               // 记录父节点
        tbb::concurrent_unordered_set<ChildVertex> m_children;             // 记录子节点
        // minw::DependencyType m_dependencyType;                                                            // 记录父子节点间依赖类型
};