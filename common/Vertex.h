#pragma once

#include <memory>
#include <string>
#include <map>
#include <set>
#include <unordered_set>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>
#include "common.h"

using namespace std;

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

        struct VertexCompare {
            bool operator()(const Vertex::Ptr& a, const Vertex::Ptr& b) const {
                if (a->m_hyperVertex != b->m_hyperVertex) {
                    return a->m_hyperVertex < b->m_hyperVertex;
                }
                return a->m_id < b->m_id;
            }
        };

        struct VertexCmpCycle {
            bool operator()(const Vertex::Ptr& a, const Vertex::Ptr& b) const {
                if (a->m_cycle_num == b->m_cycle_num) {
                    return a->m_id < b->m_id;
                }
                return a->m_cycle_num < b->m_cycle_num;
            }
        };

        struct MapCompare {
            bool operator()(const std::map<Vertex::Ptr, Vertex::Ptr, Vertex::VertexCompare>& map1,
                            const std::map<Vertex::Ptr, Vertex::Ptr, Vertex::VertexCompare>& map2) const {
                // 比较两个 map 的第一个元素的key的id大小
                return map1.begin()->first->m_id < map2.begin()->first->m_id;
            }
        };

        struct ChildVertex {
            Vertex::Ptr vertex;
            Loom::DependencyType dependency;
        };

        struct ChildVertexHash {
            std::size_t operator()(const ChildVertex& v) const {
                // 在这里返回一个可以代表v的std::size_t值
                return std::hash<Vertex*>()(v.vertex.get());
            }
        };

        struct ChildVertexEqual {
            bool operator()(const ChildVertex& v1, const ChildVertex& v2) const {
                // 在这里返回一个表示v1和v2是否相等的bool值
                return v1.vertex->m_id == v2.vertex->m_id;
            }
        };

        struct ChildVertexCmp {
            bool operator()(const ChildVertex& v1, const ChildVertex& v2) const {
                // 从小到大排序
                return v1.vertex->m_id < v2.vertex->m_id;
            }
        };

        Vertex(shared_ptr<HyperVertex> hyperVertex, int hyperId, std::string id, int layer, bool isNested = false);

        ~Vertex();

        const unordered_set<ChildVertex, ChildVertexHash, ChildVertexEqual>& getChildren() const;
        // const set<ChildVertex, ChildVertexCmp>& getChildren() const;
        void addChild(Vertex::Ptr child, Loom::DependencyType dependency);

        Loom::DependencyType getDependencyType() const;
        void setDependencyType(Loom::DependencyType type);

        void printVertex();

        string DependencyTypeToString(Loom::DependencyType type);

        int mapToHyperId() const;

    // 公共变量
        int m_hyperId;                                                           // 记录节点对应的超节点id
        shared_ptr<HyperVertex> m_hyperVertex;                                   // 记录节点对应的超节点
        string m_id;                                                             // 记录节点自身的id
        int m_layer;                                                             // 记录节点所在层
        int m_cost;                                                              // 记录节点的执行代价 => 由执行时间正则化得到
        int m_self_cost;                                                         // 记录节点自身的执行代价
        int m_degree;                                                            // 记录节点的度
        int m_cycle_num;                                                         // 记录节点所在环路数
        unordered_set<Vertex::Ptr, VertexHash> m_in_edges;                       // 记录节点的入边, 格式：节点指针 => 可抵达最小id
        unordered_set<Vertex::Ptr, VertexHash> m_out_edges;                      // 记录节点的出边, 格式：节点指针 => 可到达最小id
        set<Vertex::Ptr, VertexCompare> cascadeVertices;                         // 记录级联回滚节点
        unordered_set<string> readSet;                                           // 记录读集
        unordered_set<string> writeSet;                                          // 记录写集
        unordered_set<string> allReadSet;                                        // 记录所有读集(包括子事务)
        unordered_set<string> allWriteSet;                                       // 记录所有写集(包括子事务)
        bool isNested;                                                           // 标记节点是否是嵌套节点
        unordered_set<ChildVertex, ChildVertexHash, ChildVertexEqual> m_children;// 记录子节点
        int scheduledTime;                                                       // 记录事务执行时刻
        bool hasStrong;                                                          // 记录是否有强依赖
        unordered_set<Vertex::Ptr, VertexHash> m_strongChildren;                 // 记录强依赖子节点
        Vertex::Ptr m_strongParent;                                              // 记录强依赖父节点

        
        // 时空图模块,用于事务重调度
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> dependencies_in; // 事务依赖关系
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> dependencies_out; // 事务依赖关系
};