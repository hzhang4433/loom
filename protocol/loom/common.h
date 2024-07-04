#pragma once

#include <set>
#include "common/HyperVertex.h"

namespace Loom {
    // 优先队列比较函数: 按照回滚代价从小到大排序
    struct cmp {
        bool operator()(const HyperVertex::Ptr& a, const HyperVertex::Ptr& b) const {
            if (a->m_cost == b->m_cost) {
                return a->m_hyperId < b->m_hyperId;  // 如果 m_cost 相同，那么 id 小的在前
            }
            return a->m_cost < b->m_cost;
        }
    };
    
    // 比较vertex id,从大到小排序
    struct idcmp {
        bool operator()(const Vertex::Ptr& a, const Vertex::Ptr& b) const {
            return a->m_id > b->m_id;
        }
    };

    // 重执行信息
    struct ReExecuteInfo {
        unordered_set<Vertex::Ptr, Vertex::VertexHash> m_rollbackTxs;
        vector<int> m_serialOrder;       // 回滚事务串行化顺序
    };

    // 自定义比较函数: 根据给定的顺序对Vertex进行排序
    struct customCompare {
        unordered_map<int, int> idToOrder;

        // 修改构造函数以接受unordered_map类型的参数
        customCompare(vector<int>& serialOrder) {
            for (int i = 0; i < serialOrder.size(); i++) {
                idToOrder[serialOrder[i]] = i;
            }
        }
    
        bool operator()(const Vertex::Ptr& a, const Vertex::Ptr& b) const {
            auto aIdx = idToOrder.at(a->m_hyperId);
            auto bIdx = idToOrder.at(b->m_hyperId);
            if (aIdx != bIdx) {
                return aIdx < bIdx;
            }
            // 如果数字部分相同，比较整个字符串
            return a->m_id > b->m_id; // 注意这里是按字典序比较
        }
    };

    // 输出回滚子事务
    template <typename T, typename Cmp>
    int printRollbackTxs(set<T, Cmp>& rollbackTxs) {
        cout << "====================Rollback Transactions====================" << endl;
        cout << "total size: " << rollbackTxs.size() << endl;
        int totalRollbackCost = 0;
        for (auto& tx : rollbackTxs) {
            totalRollbackCost += tx->m_self_cost;
            cout << "id: " << tx->m_id << " cost: " << tx->m_self_cost << endl;
        }
        cout << "rollback cost: " << totalRollbackCost << endl;
        cout << "=============================================================" << endl;
        return totalRollbackCost;
    }

    // 输出回滚子事务提交顺序
    template <typename T>
    void printTxsOrder(vector<T>& txsOrder) {
        cout << "Rollback Order: ";
        for (auto order : txsOrder) {
            cout << order << " ";
        }
        cout << endl;
    }
}