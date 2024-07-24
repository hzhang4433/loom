#pragma once

#include <set>
#include <glog/logging.h>
#include "common/HyperVertex.h"

namespace loom {
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

    // 自定义比较函数: 根据给定的顺序对Vertex进行排序
    struct customCompare {
        unordered_map<int, int> idToOrder;

        customCompare() = default;
        
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

    // 重执行信息
    struct ReExecuteInfo {
        vector<int> m_serialOrder;                                    // 回滚事务串行化顺序
        unordered_set<Vertex::Ptr, Vertex::VertexHash> m_rollbackTxs; // 回滚事务集合
        set<Vertex::Ptr, customCompare> m_orderedRollbackTxs;         // 排序后的回滚事务集合
    };

    // 输出回滚子事务
    template <typename T, typename Cmp>
    int printRollbackTxs(set<T, Cmp>& rollbackTxs) {
        cout << "================Ordered Rollback Transactions================" << endl;
        cout << "total size: " << rollbackTxs.size() << endl;
        int totalRollbackCost = 0;
        for (auto& tx : rollbackTxs) {
            totalRollbackCost += tx->m_self_cost;
            cout << "id: " << tx->m_id << " cost: " << tx->m_self_cost << " tx write set: ";
            for (auto& writeKey : tx->writeSet) {
                cout << writeKey << " ";
            }
            cout << endl;
        }
        cout << "rollback cost: " << totalRollbackCost << endl;
        cout << "=============================================================" << endl;
        return totalRollbackCost;
    }

    template <typename T, typename Hash>
    int printRollbackTxs(unordered_set<T, Hash>& rollbackTxs) {
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

    // 输出回滚子事务
    template <typename T>
    int printNormalRollbackTxs(vector<T>& rollbackTxs) {
        cout << "====================Normal Rollback Transactions====================" << endl;
        cout << "total size: " << rollbackTxs.size() << endl;
        int totalRollbackCost = 0;
        for (auto& tx : rollbackTxs) {
            cout << "======== Tx =========" << endl;
            totalRollbackCost += tx->m_cost;
            cout << "id: " << tx->m_id << " cost: " << tx->m_cost << " scheduledTime: " << tx->scheduledTime << endl;
            cout << "tx read set: ";
            for (auto& readKey : tx->allReadSet) {
                cout << readKey << " ";
            }
            cout << endl << "tx write set: ";
            for (auto& writeKey : tx->allWriteSet) {
                cout << writeKey << " ";
            }
            cout << endl;
        }
        cout << "rollback cost: " << totalRollbackCost << endl;
        cout << "=============================================================" << endl;
        return totalRollbackCost;
    }

    template <typename T>
    int printRollbackTxs(vector<T>& rollbackTxs) {
        DLOG(INFO) << "====================Rollback Transactions====================";
        DLOG(INFO) << "total size: " << rollbackTxs.size();
        int totalRollbackCost = 0;
        for (auto& tx : rollbackTxs) {
            DLOG(INFO) << "======== Tx =========";
            totalRollbackCost += tx->m_self_cost;
            DLOG(INFO) << "id: " << tx->m_id << " cost: " << tx->m_self_cost << " scheduledTime: " << tx->scheduledTime;
            string readKeys, writeKeys;
            for (auto& readKey : tx->readSet) {
                readKeys += readKey + " ";
            }
            DLOG(INFO) << "tx read set: " << readKeys;
            for (auto& writeKey : tx->writeSet) {
                writeKeys += writeKey + " ";
            }
            DLOG(INFO) << "tx write set: " << writeKeys;
        }
        DLOG(INFO) << "rollback cost: " << totalRollbackCost;
        DLOG(INFO) << "=============================================================";
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

    // 按照scheduledTime从小到大排序，如果scheduledTime相等，则按照id从小到大排序
    struct lessScheduledTime {
        bool operator()(const Vertex::Ptr& lhs, const Vertex::Ptr& rhs) const {
            if (lhs->scheduledTime != rhs->scheduledTime) {
                return lhs->scheduledTime < rhs->scheduledTime;
            }
            return lhs->m_id < rhs->m_id;
        }
    };

    // 按照scheduledTime从大到小排序，如果scheduledTime相等，则按照id从小到大排序
    struct greaterScheduledTime {
        bool operator()(const Vertex::Ptr& lhs, const Vertex::Ptr& rhs) const {
            if (lhs->scheduledTime != rhs->scheduledTime) {
                return lhs->scheduledTime > rhs->scheduledTime;
            }
            return lhs->m_id < rhs->m_id;
        }
    };
}