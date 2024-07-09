#pragma once

#include <memory>
#include <tbb/concurrent_unordered_map.h>
#include "Vertex.h"
#include "workload/tpcc/Transaction.hpp"
#include "protocol/common.h"

using namespace std;

class HyperVertex : public std::enable_shared_from_this<HyperVertex>
{
    public:
        typedef std::shared_ptr<HyperVertex> Ptr;

        HyperVertex(int id, bool isNested);

        ~HyperVertex();

        int buildVertexs(const Transaction::Ptr& tx, HyperVertex::Ptr& hyperVertex, Vertex::Ptr& vertex, string& txid, std::unordered_map<string, protocol::RWSets<Vertex::Ptr>>& invertedIndex);

        void buildVertexs(const Transaction::Ptr& tx, Vertex::Ptr& vertex, std::unordered_map<string, protocol::RWSets<Vertex::Ptr>>& invertedIndex);

        void recognizeCascades(Vertex::Ptr vertex);

        // 递归打印超节点结构树
        void printVertexTree();

        struct HyperVertexHash {
            std::size_t operator()(const HyperVertex::Ptr& v) const {
                // 使用HyperVertex的地址作为哈希值
                return std::hash<HyperVertex*>()(v.get());
            }
        };

        struct compare {
            bool operator()(const HyperVertex::Ptr& a, const HyperVertex::Ptr& b) const {
                if (a->m_cost == b->m_cost) {
                    return a->m_hyperId < b->m_hyperId;  // 如果 m_cost 相同，那么 id 小的在前
                }
                return a->m_cost < b->m_cost;
            }
        };

    // 公共变量
        int m_hyperId;      // 超节点ID
        bool m_isNested;      // 标记节点是否是嵌套节点
        int m_min_in;       // 超节点的最小入度ID
        int m_min_out;      // 超节点的最小出度ID
        double m_cost;      // 超节点的最小回滚代价
        double m_in_cost;   // 超节点的最小入度回滚代价 m_in_cost = in_cost1 + in_cost2 + ... + in_costn
        double m_out_cost;  // 超节点的最小出度回滚代价 m_out_cost = out_cost1 + out_cost2 + ... + out_costn
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_in_allRB;  // 记录超节点中所有入边的级联回滚子事务, 格式：rbVertex => num
        tbb::concurrent_unordered_map<Vertex::Ptr, int, Vertex::VertexHash> m_out_allRB; // 记录超节点中所有出边的级联回滚子事务


        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_in_rollback;  // 记录入边的级联回滚子事务
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_out_rollback; // 记录出边的级联回滚子事务
// 哪种好，带测试
        // // 想排序用下面这个
        // 排序可解决节点多余回滚问题，但可能无法并发需要进一步尝试
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_map<Vertex::Ptr, set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexCompare>, HyperVertexHash> m_out_edges;    // 记录超节点中所有出边, 格式：hyperVertex => {<v1, {vi..vn}>, <v1, {vi..vn}>, ...}
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_map<Vertex::Ptr, set<Vertex::Ptr ,Vertex::VertexHash>, Vertex::VertexCompare>, HyperVertexHash> m_in_edges;     // 记录超节点中所有入边
        
        // 想并发用下面这个
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>, HyperVertexHash> m_out_edges;    // 记录超节点中所有出边, 格式：hyperVertex => {<v1, {vi..vn}>, <v1, {vi..vn}>, ...}
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr ,Vertex::VertexHash>, Vertex::VertexHash>, HyperVertexHash> m_in_edges;     // 记录超节点中所有入边
        
        // edit:6.16 -- 记录本超节点与其它超节点相关联的所有vertex —— 是否可以直接用m_out_rollback和m_in_rollback代替？
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_out_edges;    // 记录超节点中所有出边, 格式：hyperVertex => {vi,...,vn}
        // tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr ,Vertex::VertexHash>, HyperVertexHash> m_in_edges;     // 记录超节点中所有入边


        // edit:6.18 -- 替换下面更加高效的数据结构
        tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertexHash> m_out_hv;
        tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertexHash> m_in_hv;
        tbb::concurrent_vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_out_edges;    // 记录超节点中所有出边, 格式：hyperVertex => {vi,...,vn}
        tbb::concurrent_vector<tbb::concurrent_unordered_set<Vertex::Ptr ,Vertex::VertexHash>> m_in_edges;     // 记录超节点中所有入边
        tbb::concurrent_vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_in_rollback;  // 记录入边的级联回滚子事务
        tbb::concurrent_vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_out_rollback; // 记录出边的级联回滚子事务



        // 单线程版本
        unordered_set<Vertex::Ptr, Vertex::VertexHash> m_in_allRBS;  // 记录超节点中所有入边的级联回滚子事务, 格式：rbVertex => num
        unordered_map<HyperVertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_out_edgesS;
        unordered_map<HyperVertex::Ptr, unordered_set<Vertex::Ptr ,Vertex::VertexHash>, HyperVertexHash> m_in_edgesS;
        unordered_set<HyperVertex::Ptr, HyperVertexHash> m_out_hvS;
        unordered_set<HyperVertex::Ptr, HyperVertexHash> m_in_hvS;
        vector<unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_out_mapS;
        vector<unordered_set<Vertex::Ptr ,Vertex::VertexHash>> m_in_mapS;
        vector<unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_in_rollbackMS;  // 记录入边的级联回滚子事务
        vector<unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_out_rollbackMS; // 记录出边的级联回滚子事务
        unordered_map<HyperVertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertexHash> m_out_rollbackS; // 记录出边的级联回滚子事务


        tbb::concurrent_vector<double> m_out_weights; //记录出边边权
        tbb::concurrent_vector<double> m_in_weights;  //记录入边边权
        Loom::EdgeType m_rollback_type; // 边类型
        unordered_set<Vertex::Ptr, Vertex::VertexHash> m_vertices;  // 记录所有节点
        Vertex::Ptr m_rootVertex;                               // 根节点
        
};