#include "DeterReExecute.h"
#include <assert.h>

using namespace std;
using namespace Util;

/* 构造函数 */
DeterReExecute::DeterReExecute(std::vector<Vertex::Ptr>& rbList, vector<vector<int>>& serialOrders, std::unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex) : m_rbList(rbList), m_serialOrders(serialOrders), m_conflictIndex(conflictIndex), m_normalList(dummyNormalList) {
    // 构建串行化序索引
    for (int i = 0; i < m_serialOrders.size(); i++) {
        for (auto txId : m_serialOrders[i]) {
            this->m_orderIndex[txId] = i;
        }
    }
    
    // 记录事务顺序
    int counter = 0;
    // std::unordered_set<Vertex::Ptr, Vertex::VertexHash> rbSet(m_rbList.begin(), m_rbList.end());
    for (auto& tx : m_rbList) {
        m_txOrder[tx] = counter++;
        // m_unConflictTxMap[tx->m_id] = rbSet;
    }

    // 重排序轮次定义为事务数的20%
    this->N = rbList.size() * 0.2;
    m_totalExecTime = 0;
} 


DeterReExecute::DeterReExecute(std::vector<HyperVertex::Ptr>& normalList, vector<vector<int>>& serialOrders, std::unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex, std::vector<Vertex::Ptr>& rbList) 
    : m_normalList(normalList), m_serialOrders(serialOrders), m_conflictIndex(conflictIndex), m_rbList(rbList) {
    // 构建串行化序索引
    for (int i = 0; i < serialOrders.size(); i++) {
        for (auto txId : serialOrders[i]) {
            this->m_orderIndex[txId] = i;
        }
    }
    // 记录事务顺序
    int counter = 0;
    for (auto& tx : m_rbList) {
        m_txOrder[tx] = counter++;
    }
    m_totalExecTime = 0;
}


/* 时空图模块 */

/* 构建初始时空图
    即不考虑嵌套事务结构的时空图 => 为了做对比
*/
void DeterReExecute::buildGraphOrigin() {
    auto counter = 0;
    // 按队列顺序，依次遍历事务
    for (int j = 0; j < m_rbList.size(); j++) {
        auto Tj = m_rbList[j];

        // 判断本事务与前序事务间冲突
        for (int i = j - 1; i > 0; i--) {
            auto Ti = m_rbList[i];
            // conflictTxs.find(Ti) != conflictTxs.end()
            if (loom::hasConflict(Tj->writeSet, Ti->readSet) || 
                loom::hasConflict(Tj->writeSet, Ti->writeSet) || 
                loom::hasConflict(Tj->readSet, Ti->writeSet)) {
                // 若Ti和Tj冲突
                counter++;
                // cout << "hasconflict on: " << i << ": " << Ti->m_id << " and " << j << ": " << Tj->m_id << endl;
                // 更新依赖关系和调度时间
                Tj->dependencies_in.insert(Ti);
                Ti->dependencies_out.insert(Tj);
                int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
            }
        }
    }

    cout << "counter in buildGraphOrigin: " << counter << endl;
}

/* old version
void DeterReExecute::buildGraphOrigin() {
    auto counter = 0;
    // 按队列顺序，依次遍历事务
    for (int j = 0; j < m_rbList.size(); j++) {
        auto Tj = m_rbList[j];
        auto& unflictTxs = m_unConflictTxMap[m_rbList[j]->m_id];

        // 判断本事务与前序事务间冲突
        for (int i = 0; i < m_rbList.size(); i++) {
            auto Ti = m_rbList[i];
            // conflictTxs.find(Ti) != conflictTxs.end()
            if (loom::hasConflict(Tj->writeSet, Ti->readSet) || 
                loom::hasConflict(Tj->writeSet, Ti->writeSet) || 
                loom::hasConflict(Tj->readSet, Ti->writeSet)) {
                // 若Ti和Tj冲突
                if (i < j) {
                    counter++;
                    // cout << "hasconflict on: " << i << ": " << Ti->m_id << " and " << j << ": " << Tj->m_id << endl;
                    // 更新依赖关系和调度时间
                    Tj->dependencies_in.insert(Ti);
                    Ti->dependencies_out.insert(Tj);
                    int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                    Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                }
            } else {
                // 添加到本事务的无冲突事务集合
                unflictTxs.insert(Ti);
            }
        }
    }

    cout << "counter in buildGraphOrigin: " << counter << endl;
}
*/


void DeterReExecute::buildGraphOriginByIndex() {
    auto counter = 0;
    // 按队列顺序，依次遍历事务
    for (int j = 0; j < m_rbList.size(); j++) {
        auto Tj = m_rbList[j];
        auto& unflictTxs = m_unConflictTxMap[m_rbList[j]->m_id];
        auto& conflictTxs = m_conflictIndex[Tj];
        
        if (Tj->m_cost > 500) {
            for (int i = 0; i < m_rbList.size(); i++) {
                auto Ti = m_rbList[i];
                if (loom::hasConflict(Tj->allWriteSet, Ti->allReadSet) || 
                    loom::hasConflict(Tj->allWriteSet, Ti->allWriteSet) || 
                    loom::hasConflict(Tj->allReadSet, Ti->allWriteSet)) {
                    // 若Ti和Tj冲突
                    unflictTxs.erase(Ti);
                    if (i < j) {
                        counter++;
                        // cout << "hasconflict on: " << i << ": " << Ti->m_id << " and " << j << ": " << Tj->m_id << endl;
                        // 更新依赖关系和调度时间
                        Tj->dependencies_in.insert(Ti);
                        Ti->dependencies_out.insert(Tj);
                        int newScheduledTime = Ti->scheduledTime + Ti->m_cost;
                        Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                    }
                }
            }
        } else {
            for (auto& Ti : conflictTxs) {
                unflictTxs.erase(Ti);
                if (m_txOrder.find(Ti) != m_txOrder.end() && m_txOrder[Ti] < j) {
                    counter++;
                    // 本事务记录前序事务
                    Tj->dependencies_in.insert(Ti);
                    
                    // 所有前序事务记录本事务
                    Ti->dependencies_out.insert(Tj);
                    
                    // 本事务调度时间为前序事务结束时间
                    int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                    Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                }
            }
        }
        // 移除自己
        unflictTxs.erase(Tj);
    }

    cout << "counter in buildGraphOriginByIndex: " << counter << endl;
}

/* 构建时空图
1. 为每个事务设置调度时间
2. 为每个事务添加依赖关系
3. 记录每个事务的非冲突事务集合
*/
void DeterReExecute::buildGraph() {
    // int counter = 0;
    // 按队列顺序，依次遍历事务
    for (int j = 0; j < m_rbList.size(); j++) {
        auto Tj = m_rbList[j];
        auto& unflictTxs = m_unConflictTxMap[m_rbList[j]->m_id];
        auto& conflictTxs = m_conflictIndex[Tj];

        // 判断本事务与前序事务间冲突
        for (int i = 0; i < m_rbList.size(); i++) {
            auto Ti = m_rbList[i];
            // 判断事务间冲突关系(同属于一个超节点的子事务无需判断冲突关系)
            if (Ti->m_hyperId != Tj->m_hyperId && conflictTxs.find(Ti) != conflictTxs.end()) {
                
                if (i < j) {
                    // counter++;
                    // 本事务记录前序事务
                    Tj->dependencies_in.insert(Ti);
                    
                    // 所有前序事务记录本事务
                    Ti->dependencies_out.insert(Tj);
                    
                    // 本事务调度时间为前序事务结束时间
                    int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                    Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                }
            } else {
                // 添加到本事务的无冲突事务集合
                unflictTxs.insert(Ti);
            }
        }

        /* 考虑嵌套结构：
            1. 若存在强依赖子节点，则将其添加至依赖关系
            2. 若存在强依赖父节点，则将其从unflictTxs中移除
        */
        if (Tj->hasStrong) {
            for (auto Ti : Tj->m_strongChildren) {
                // 父事务记录强依赖子事务
                Tj->dependencies_in.insert(Ti);
                // 子事务记录强依赖父事务
                Ti->dependencies_out.insert(Tj);
                // 父事务调度时间为前序事务结束时间
                int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                unflictTxs.erase(Ti);
            }
        } else if (Tj->m_strongParent != nullptr) {
            // 有必要删除吗？不管删除不删除，在判断idIdle时都无法通过的
            unflictTxs.erase(Tj->m_strongParent);
        }
    }

    // cout << "counter: " << counter << endl;
}

/* 通过索引构建时空图 */
void DeterReExecute::buildGraphByIndex() {
    int counter = 0;
    // 按队列顺序，依次遍历事务
    for (int j = 0; j < m_rbList.size(); j++) {
        auto Tj = m_rbList[j];
        auto& unflictTxs = m_unConflictTxMap[m_rbList[j]->m_id];
        auto& conflictTxs = m_conflictIndex[Tj];
        
        // 遍历本事务的所有冲突事务
        for (auto& Ti : conflictTxs) {
            counter++;
            unflictTxs.erase(Ti);
            if (m_txOrder.find(Ti) != m_txOrder.end() && m_txOrder[Ti] < j) {
                counter++;
                // 本事务记录前序事务
                Tj->dependencies_in.insert(Ti);
                
                // 所有前序事务记录本事务
                Ti->dependencies_out.insert(Tj);
                
                // 本事务调度时间为前序事务结束时间
                int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
            }
        }

        /* 考虑嵌套结构：若存在强依赖子节点，则将其添加至依赖关系 */
        if (Tj->hasStrong) {
            for (auto Ti : Tj->m_strongChildren) {
                // 父事务记录强依赖子事务
                Tj->dependencies_in.insert(Ti);
                // 子事务记录强依赖父事务
                Ti->dependencies_out.insert(Tj);
                // 父事务调度时间为前序事务结束时间
                int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                unflictTxs.erase(Ti);
            }
        }

        // 移除自己
        unflictTxs.erase(Tj);
    }

    cout << "counter in buildGraphByIndex: " << counter << endl;
}

/* 通过索引并发构建时空图 */
void DeterReExecute::buildGraphConcurrent(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures) {

    size_t totalTasks = m_rbList.size();
    size_t chunkSize = (totalTasks + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 1);
    cout << "totalTasks: " << totalTasks << " chunkSize: " << chunkSize << endl;

    for (size_t i = 0; i < totalTasks; i += chunkSize) {
        futures.emplace_back(Pool->commit([this, i, chunkSize, totalTasks] {
            size_t end = std::min(i + chunkSize, totalTasks);
            for (size_t j = i; j < end; ++j) {
                // 遍历队列事务
                auto Tj = m_rbList[j];
                auto& unflictTxs = m_unConflictTxMap[Tj->m_id];
                auto& conflictTxs = m_conflictIndex[Tj];
                
                // 遍历本事务的所有冲突事务
                for (auto& Ti : conflictTxs) {
                    unflictTxs.erase(Ti);
                    if (m_txOrder.find(Ti) != m_txOrder.end() && m_txOrder[Ti] < j) {
                        // 本事务记录前序事务
                        Tj->dependencies_in.insert(Ti);
                        
                        // 所有前序事务记录本事务
                        Ti->dependencies_out.insert(Tj);
                        
                        // 本事务调度时间为前序事务结束时间
                        int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                        Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                    }
                }

                /* 考虑嵌套结构：若存在强依赖子节点，则将其添加至依赖关系 */
                if (Tj->hasStrong) {
                    for (auto Ti : Tj->m_strongChildren) {
                        // 父事务记录强依赖子事务
                        Tj->dependencies_in.insert(Ti);
                        // 子事务记录强依赖父事务
                        Ti->dependencies_out.insert(Tj);
                        // 父事务调度时间为前序事务结束时间
                        int newScheduledTime = Ti->scheduledTime + Ti->m_self_cost;
                        Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                        unflictTxs.erase(Ti);
                    }
                }

                // 移除自己
                unflictTxs.erase(Tj);
                
            }
        }));
    }

    // 等待所有任务完成
    for (auto &future : futures) {
        future.get();
    }
}

/* 根据读写集构建时空图 */
void DeterReExecute::buildAndReScheduleFlat() {
    // int counter = 0;
    // 按队列顺序，依次遍历事务
    for (auto& Ti : m_rbList) {
        // auto& Ti = rb->m_rootVertex;
        // DLOG(INFO) << "Ti: " << Ti->m_id << " counter: " << counter;
        std::unordered_set<string> processedKeys;

        // write set first
        for (auto& wKey: Ti->writeSet) {
            auto& entry = m_tsGraph[wKey];
            if (!entry.deps_get.empty()) {
                auto& Tj = entry.deps_get.back();
                // Ti->scheduledTime = std::max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                auto newScheduledTime = Tj->scheduledTime + Tj->m_self_cost;
                if (newScheduledTime > Ti->scheduledTime) {
                    Ti->scheduledTime = newScheduledTime;
                    Ti->m_should_wait = Tj;
                }
            }
            if (!entry.deps_put.empty()) {
                auto& Tj = entry.deps_put.back();
                // Ti->scheduledTime = std::max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                auto newScheduledTime = Tj->scheduledTime + Tj->m_self_cost;
                if (newScheduledTime > Ti->scheduledTime) {
                    Ti->scheduledTime = newScheduledTime;
                    Ti->m_should_wait = Tj;
                }
            }
            entry.deps_put.push_back(Ti);
            entry.total_time = std::max(Ti->scheduledTime + Ti->m_self_cost, entry.total_time);
            processedKeys.insert(wKey);
            // counter++;
        }
        // then read set
        for (auto& rKey : Ti->readSet) {
            if (processedKeys.find(rKey) != processedKeys.end()) {
                continue;
            }
            auto& entry = m_tsGraph[rKey];
            if (!entry.deps_put.empty()) {
                auto& Tj = entry.deps_put.back();
                // Ti->scheduledTime = std::max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                auto newScheduledTime = Tj->scheduledTime + Tj->m_self_cost;
                if (newScheduledTime > Ti->scheduledTime) {
                    Ti->scheduledTime = newScheduledTime;
                    Ti->m_should_wait = Tj;
                }
            }
            entry.deps_get.push_back(Ti);
            entry.total_time = std::max(Ti->scheduledTime + Ti->m_self_cost, entry.total_time);
            // counter++;
        }

        // Ti->committed = std::make_shared<std::atomic<bool>>(false);
    }
    // cout << "counter in buildByWRSet: " << counter << endl;
}

/* 根据读写集构建时空图:考虑嵌套结构 */
void DeterReExecute::buildAndReSchedule() {
    // 按队列顺序，依次遍历事务
    for (auto& Ti : m_rbList) {
        std::unordered_set<string> processedKeys;

        // write set first
        for (auto& wKey: Ti->writeSet) {
            auto& entry = m_tsGraph[wKey];
            if (!entry.deps_get.empty()) {
                auto& Tj = entry.deps_get.back();
                // Ti->scheduledTime = std::max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                auto newScheduledTime = Tj->scheduledTime + Tj->m_self_cost;
                if (newScheduledTime > Ti->scheduledTime) {
                    Ti->scheduledTime = newScheduledTime;
                    Ti->m_should_wait = Tj;
                }
            }
            if (!entry.deps_put.empty()) {
                auto& Tj = entry.deps_put.back();
                // Ti->scheduledTime = std::max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                auto newScheduledTime = Tj->scheduledTime + Tj->m_self_cost;
                if (newScheduledTime > Ti->scheduledTime) {
                    Ti->scheduledTime = newScheduledTime;
                    Ti->m_should_wait = Tj;
                }
            }
            entry.total_time = std::max(Ti->scheduledTime + Ti->m_self_cost, entry.total_time);
            entry.deps_put.push_back(Ti);
            processedKeys.insert(wKey);
        }
        // then read set
        for (auto& rKey : Ti->readSet) {
            if (processedKeys.find(rKey) != processedKeys.end()) {
                continue;
            }
            auto& entry = m_tsGraph[rKey];
            if (!entry.deps_put.empty()) {
                auto& Tj = entry.deps_put.back();
                // Ti->scheduledTime = std::max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                auto newScheduledTime = Tj->scheduledTime + Tj->m_self_cost;
                if (newScheduledTime > Ti->scheduledTime) {
                    Ti->scheduledTime = newScheduledTime;
                    Ti->m_should_wait = Tj;
                }
            }
            entry.total_time = std::max(Ti->scheduledTime + Ti->m_self_cost, entry.total_time);
            entry.deps_get.push_back(Ti);
        }

        // 考虑嵌套结构：若存在强依赖子节点，则将其添加至依赖关系
        if (Ti->hasStrong) {
            for (auto Tj : Ti->m_strongChildren) {
                // 父事务调度时间为前序事务结束时间
                // Ti->scheduledTime = std::max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                auto newScheduledTime = Tj->scheduledTime + Tj->m_self_cost;
                if (newScheduledTime > Ti->scheduledTime) {
                    Ti->scheduledTime = newScheduledTime;
                    Ti->m_should_wait = Tj;
                }
            }
        }

        // Ti->committed = std::make_shared<std::atomic<bool>>(false);
    }
}


void DeterReExecute::buildByWRSetNested(vector<Vertex::Ptr>& txList) {
    tbb::parallel_for(tbb::blocked_range<size_t>(0, txList.size()),
                      [&](const tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i != range.end(); ++i) {
            auto& Ti = txList[i];
            assert(Ti != nullptr); // 添加断言

            // Write set first
            for (auto& wKey: Ti->writeSet) {
                auto& entry = m_tsGraph[wKey];
                {
                    std::lock_guard<std::mutex> lock(*entry.mtx);
                    if (!entry.deps_get.empty()) {
                        auto& Tj = entry.deps_get.back();
                        assert(Tj != nullptr); // 添加断言
                        Ti->scheduledTime = max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                    }
                    if (!entry.deps_put.empty()) {
                        auto& Tj = entry.deps_put.back();
                        assert(Tj != nullptr); // 添加断言
                        Ti->scheduledTime = max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                    }
                    entry.total_time = max(Ti->scheduledTime + Ti->m_self_cost, entry.total_time);
                    entry.deps_put.push_back(Ti);
                }
            }

            // Read set
            for (auto& rKey : Ti->readSet) {
                auto& entry = m_tsGraph[rKey];
                {
                    std::lock_guard<std::mutex> lock(*entry.mtx);
                    if (!entry.deps_put.empty() && entry.deps_put.back() != Ti) {
                        auto& Tj = entry.deps_put.back();
                        assert(Tj != nullptr); // 添加断言
                        Ti->scheduledTime = max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                    }
                    entry.total_time = max(Ti->scheduledTime + Ti->m_self_cost, entry.total_time);
                    entry.deps_get.push_back(Ti);
                }
            }

            // Nested structure
            if (Ti->hasStrong) {
                for (auto& Tj : Ti->m_strongChildren) {
                    assert(Tj != nullptr); // 添加断言
                    Ti->scheduledTime = max(Ti->scheduledTime, Tj->scheduledTime + Tj->m_self_cost);
                }
            }
        }
    });
}

/* 重调度事务 */
void DeterReExecute::rescheduleTransactions() {
    int lastTime = -1;
    for (int i = 0; i < N; i++) {
        // cout << "Round " << i << endl;
        // 1.取出一笔排在事务队列最靠前的事务T1，获取其读写集和执行时刻
        auto tx = m_rbList[i];
        // 1.1 若下一笔事务的执行时间和上一笔相同，则直接跳过该笔
        if ((lastTime != -1 && lastTime == tx->scheduledTime)) {
            if (N + 1 < m_rbList.size()) {
                N++;   
            }
            continue;
        }

        // 2.获取事务队列中读写集与其不冲突,且执行时刻在其之后的所有事务集Ts
        std::set<Vertex::Ptr, loom::lessScheduledTime> Ts;
        getCandidateTxSet(tx, Ts);
        // 若不存在满足条件的事务，则跳过
        if (Ts.empty()) {
            if (N + 1 < m_rbList.size()) {
                N++;
            }
            continue;
        }

        // 存不存在可直接退出的情况？

        lastTime = tx->scheduledTime;

        // 3.遍历Ts中的每笔事务Ti，判断阻塞Ti执行的所有在目标时间区域内的事务前是否有足够的空闲时间
        //  （即事务间空闲时间大于等于Ti的执行时间）可以使其前移至与Ts一样的执行时刻执行
        std::set<string> movedTxIds; // 记录当轮已经移动的事务
        for (auto Ti : Ts) {
            // 已经移动的事务不会被遍历(一轮中一笔事务只会被移动一次)
            if (movedTxIds.find(Ti->m_id) != movedTxIds.end()) {
                continue;
            }
            // cout << "Ti:" << Ti->id << " scheduleTime:" << Ti->scheduledTime << endl;

            // 3.0记录已移动事务, 本轮中只要遍历过就无需重新遍历
            movedTxIds.insert(Ti->m_id);

            // 3.1若Ti有足够的空闲时间前移至Tx时刻执行
            if (isIdle(Ti, tx->scheduledTime)) {
                // 3.1.0深拷贝Ti依赖边depedencies_out
                const std::set<Vertex::Ptr> originalDependencies(Ti->dependencies_out.begin(), Ti->dependencies_out.end());
                // 3.1.1移动Ti至目标位置
                reschedule(Ti, tx->scheduledTime);
                // 3.1.2贪心: 递归遍历与Ti有直接依赖的事务，尝试一起前移
                recursiveRescheduleTxs(Ti, tx, movedTxIds, originalDependencies);   
            }
            // 3.2若不可以，则继续遍历Ts
        }
        // 4.重复N轮
    }   
}

/* 获取候选重调度事务集 */
void DeterReExecute::getCandidateTxSet(const Vertex::Ptr& Tx, std::set<Vertex::Ptr, loom::lessScheduledTime>& Ts) {
    for (auto candidateTx : m_unConflictTxMap[Tx->m_id]) {
        // 事务执行时刻在tx之后
        if (candidateTx->scheduledTime > Tx->scheduledTime) {
            Ts.insert(candidateTx);
        }
    }
}

/* 重调度事务：修改事务间依赖关系
*/
void DeterReExecute::reschedule(Vertex::Ptr& Tx, int startTime) {
    // 1.修改事务执行时刻
    Tx->scheduledTime = startTime;
    
    // 2.若Tx没有依赖事务，则直接返回
    if (Tx->dependencies_in.empty()) {
        return;
    }

    // cout << "Reschedule Tx " << Tx->id << " to " << startTime << endl;

    // 3.更新事务依赖关系: 一些之前在Tx前面的事务现在可能被调度到Tx后面
    std::vector<Vertex::Ptr> toRemoveTxs;
    for (auto& conflictTx : Tx->dependencies_in) {
        // cout << "conflictTx: " << conflictTx->id << " scheduleTime: " << conflictTx->scheduledTime << endl;
        // 若冲突事务的执行时刻大于现在的Tx
        if (conflictTx->scheduledTime > startTime) {
            // 3.1记录要移除的conflictTx
            // Tx->dependencies_in.erase(conflictTx);
            toRemoveTxs.push_back(conflictTx);
            // 3.2从Tx的dependencies_out关系中添加conflictTx
            Tx->dependencies_out.insert(conflictTx);
            // 3.3从conflictTx的dependencies_out关系中移除Tx
            // conflictTx->dependencies_out.erase(Tx);
            conflictTx->dependencies_out.unsafe_erase(Tx);
            // 3.4从conflictTx的dependencies_in关系中添加Tx
            conflictTx->dependencies_in.insert(Tx);
        }
    }

    // 4.移除冲突事务
    for (auto& toRemoveTx : toRemoveTxs) {
        Tx->dependencies_in.unsafe_erase(toRemoveTx);
        // Tx->dependencies_in.erase(toRemoveTx);
    }
    
}

/* 递归重调度事务 */
void DeterReExecute::recursiveRescheduleTxs(const Vertex::Ptr& Ti, const Vertex::Ptr& Tx, std::set<string>& movedTxIds, const std::set<Vertex::Ptr>& originalDependencies) {
    // 0. 若Ti没有依赖事务，则直接返回
    if (originalDependencies.empty()) {
        return;
    }
    // 1. 遍历与Ti有直接依赖的事务
    for (auto Tj : originalDependencies) {
        // 1.1 若Tj已经被移动，则跳过
        if (movedTxIds.find(Tj->m_id) != movedTxIds.end()) {
            continue;
        }
        // cout << "Tj:" << Tj->id << " scheduleTime:" << Tj->scheduledTime << endl;
// 1.2 若Tj可以前移至Tx执行时刻  ==> 若Tj不是Ti的强依赖父节点？添加会提高性能吗？ —— 待测试性能影响 
        if (Tj != Ti->m_strongParent && isIdle(Tj, Tx->scheduledTime)) {
            //1.2.0深拷贝Tj依赖边depedencies_out
            std::set<Vertex::Ptr> m_originalDependencies(Tj->dependencies_out.begin(), Tj->dependencies_out.end());
            //1.2.1移动至Tx执行时刻执行
            reschedule(Tj, Tx->scheduledTime);
            //1.2.2记录已移动事务
            movedTxIds.insert(Tj->m_id);
            //1.2.3从Tj出发继续选取并尝试前移事务
            recursiveRescheduleTxs(Tj, Tx, movedTxIds, m_originalDependencies);
        } else if (Tj->scheduledTime == Ti->scheduledTime + Ti->m_self_cost) {
            //1.3.1 如果Tj执行时间就在Ti执行完成后的时刻，直接遍历下一个事务
            continue;
        // 1.4 若Tj可以前移至Tj执行完成后时刻
        } else if (isIdle(Tj, Ti->scheduledTime + Ti->m_self_cost)) {
            //1.4.0深拷贝Tj依赖边depedencies_out
            std::set<Vertex::Ptr> m_originalDependencies(Tj->dependencies_out.begin(), Tj->dependencies_out.end());
            //1.4.1移动至Ti执行完成后时刻执行
            reschedule(Tj, Ti->scheduledTime + Ti->m_self_cost);
            //1.4.2记录已移动事务
            movedTxIds.insert(Tj->m_id);
            //1.4.3从Tj出发继续选取并尝试前移事务
            recursiveRescheduleTxs(Tj, Tx, movedTxIds, m_originalDependencies);
        }
        
        // 1.6 若Tj不可以移动，则继续遍历Ti的依赖事务
    }
}

/* 清空时空图 */
void DeterReExecute::clearGraph() {
    for (auto& tx : m_rbList) {
        tx->dependencies_in.clear();
        tx->dependencies_out.clear();
        tx->scheduledTime = 0;
    }
}


/* 时间计算模块 */ 

/* 判断事务是否能够在startTime时刻执行 */
bool DeterReExecute::isIdle(const Vertex::Ptr& tx, int startTime) {
    // 1.计算事务Tx的执行结束时间,获取执行区间[startTime, endTime]
    int endTime = startTime + tx->m_self_cost;
    // 2.遍历Tx的冲突事务，判断目标时间区间是否空闲
    if (tx->isNested) {
        auto txOrderIndex = m_orderIndex[tx->m_hyperId];
        // 若移动的是嵌套子事务，对依赖的嵌套子事务，首先判断其所属嵌套事务ID是否属于同一个m_orderIndex
        for (auto& conflictTx : tx->dependencies_in) {
            // 若冲突事务属于同一个m_orderIndex
            if (conflictTx->isNested && txOrderIndex == m_orderIndex[conflictTx->m_hyperId]) {
                return false;
            }
            // 若冲突事务的执行时间在Tx之后 或 事务在startTime前执行完成
            if (!(conflictTx->scheduledTime >= endTime || conflictTx->scheduledTime + conflictTx->m_self_cost <= startTime)) {
                return false;
            }
        }
    } else {
        // 若移动的是普通事务，直接判断有无空隙即可
        for (auto& conflictTx : tx->dependencies_in) {
            // !(事务冲突事务的执行时间在Tx之后 或 事务在startTime前执行完成) -> !(无冲突)
            if (!(conflictTx->scheduledTime >= endTime || conflictTx->scheduledTime + conflictTx->m_self_cost <= startTime)) {
                return false;
            }
        }
    }
    return true;

    // // 简洁写法，但是未必高效
    // for (auto& conflictTx : tx->dependencies_in) {
    //     // 不能重排序 || !(事务冲突事务的执行时间在Tx之后 或 事务在startTime前执行完成) -> !(无冲突)
    //     if (!canReorder(tx, conflictTx) || !(conflictTx->scheduledTime >= endTime || conflictTx->scheduledTime + conflictTx->m_self_cost <= startTime)) {
    //         return false;
    //     }
    // }
    // return true;
}

/* 计算事务总执行时间 */
int DeterReExecute::calculateTotalExecutionTime() {
    // cout << "====calculateTotalExecutionTime====" << endl;
    // 按scheduledTime，从小到大排序
    std::sort(m_rbList.begin(), m_rbList.end(), loom::lessScheduledTime());
    // 计算每笔事务的执行时间
    int endTime = 0;
    for (auto& txn : m_rbList) {
        int txnEndTime = calculateExecutionTime(txn) + txn->m_self_cost;
        endTime = std::max(endTime, txnEndTime);
    }
    m_totalExecTime = endTime;
    return m_totalExecTime;
}

/* 计算每笔事务最早执行时间 */
int DeterReExecute::calculateExecutionTime(Vertex::Ptr& Tx) {
    int executionTime = 0;
    for (auto& conflictTx : Tx->dependencies_in) {
        executionTime = std::max(executionTime, conflictTx->scheduledTime + conflictTx->m_self_cost);
    }
    Tx->scheduledTime = executionTime;
    return executionTime;
}

int DeterReExecute::calculateTotalNormalExecutionTime() {
    // // cout << "====calculateTotalExecutionTime====" << endl;
    // // 按scheduledTime，从小到大排序
    // std::sort(m_rbList.begin(), m_rbList.end(), loom::lessScheduledTime());
    // // 计算每笔事务的执行时间
    // int endTime = 0;
    // for (auto& txn : m_rbList) {
    //     int txnEndTime = calculateNormalExecutionTime(txn) + txn->m_cost;
    //     endTime = std::max(endTime, txnEndTime);
    // }
    // m_totalExecTime = endTime;
    // return m_totalExecTime;
    for (auto& entry : m_tsGraph) {
        m_totalExecTime = std::max(m_totalExecTime, entry.second.total_time);
    }
    return m_totalExecTime;
}

int DeterReExecute::calculateNormalExecutionTime(Vertex::Ptr& Tx) {
    int executionTime = 0;
    for (auto& conflictTx : Tx->dependencies_in) {
        executionTime = std::max(executionTime, conflictTx->scheduledTime + conflictTx->m_cost);
    }
    Tx->scheduledTime = executionTime;
    return executionTime;
}

// 判断两个事务是否可调序
bool DeterReExecute::canReorder(const Vertex::Ptr& Tx1, const Vertex::Ptr& Tx2) {
    // // 检查Tx1或Tx2是否都是嵌套事务
    // if (Tx1->isNested && Tx2->isNested) {
    //     // 若两个事务在同一个集合中，无法调序
    //     return m_orderIndex[Tx1->m_hyperId] != m_orderIndex[Tx2->m_hyperId];
    // }
    // return true;

    return !(Tx1->isNested && Tx2->isNested) || m_orderIndex[Tx1->m_hyperId] != m_orderIndex[Tx2->m_hyperId];
}

/* 计算事务串行执行时间 */
int DeterReExecute::calculateSerialTime(){
    int totalTime = 0;
    for (auto& txn : m_rbList) {
        totalTime += txn->m_self_cost;
    }
    return totalTime;
}

/* 定义一个处理两个方向遍历的函数 */
void DeterReExecute::updateDependenciesAndScheduleTime(Vertex::Ptr& Tj, const Vertex::Ptr& Ti, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>& unflictTxs, bool forward) {
    int start = forward ? 0 : m_rbList.size() - 1;
    int end = forward ? m_rbList.size() : -1;
    int step = forward ? 1 : -1;

    for (int k = start; forward ? k < end : k > end; k += step) {
        auto Tk = m_rbList[k];
        if (Tk->m_hyperId != Ti->m_hyperId) break;

        // 更新依赖关系和调度时间
        Tj->dependencies_in.insert(Tk);
        Tk->dependencies_out.insert(Tj);
        int newScheduledTime = Tk->scheduledTime + Tk->m_self_cost;
        Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
        // 尝试从无冲突事务集合中移除
        unflictTxs.erase(Tk);
    }
}

/* 确定性重执行模块 */
void DeterReExecute::reExcution(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures, Statistics& statistics) {
    
    for (auto& tx : m_rbList) {
        if (tx->m_should_wait) {
            dependencyGraph[tx->m_should_wait].push_back(tx);
        }
    }

    // 在依赖关系完整建立后，提交无依赖的任务
    for (auto& tx : m_rbList) {
        if (!tx->m_should_wait) {
            futures.emplace_back(Pool->commit([this, tx, &statistics] {
                this->executeTransaction(tx, statistics);
            }));
        }
    }

    for (auto &future : futures) {
        future.get();
    }
}

void DeterReExecute::reExcution(ThreadPool::Ptr& Pool, std::vector<std::future<void>>& futures, Statistics& statistics) {
    // build dependency graph
    for (auto& tx : m_rbList) {
        if (tx->m_should_wait) {
            dependencyGraph[tx->m_should_wait].push_back(tx);
        }
    }

    // 在依赖关系完整建立后，提交无依赖的任务
    std::atomic<int> taskCounter(0);
    std::vector<std::tuple<std::function<void()>>> taskList;
    for (auto& tx : m_rbList) {
        if (!tx->m_should_wait) {
            // futures.emplace_back(Pool->enqueue([this, tx, &statistics] {
            //     this->executeTransaction(tx, statistics);
            // }));
            // // batch version
            // taskList.emplace_back(std::make_tuple([this, tx, &statistics] {
            //     this->executeTransaction(tx, statistics);
            // }));
            taskCounter.fetch_add(1, std::memory_order_relaxed);
            taskList.emplace_back(std::make_tuple([this, tx, &statistics, &Pool, &taskCounter] {
                this->executeTransactionWithPool(tx, statistics, Pool, taskCounter);
            }));
        }
    }
    futures = Pool->enqueueBatch(taskList);
    // for (auto &future : futures) {
    //     future.get();
    // }
    while (taskCounter.load(std::memory_order_relaxed) > 0) {
        std::this_thread::yield();
    }
}

void DeterReExecute::executeTransaction(const Vertex::Ptr& tx, Statistics& statistics) {
    tx->Execute();
    // record the last commit time
    tx->m_hyperVertex->m_commit_time = chrono::steady_clock::now();
    statistics.JournalOverheads(tx->CountOverheads());
    auto it = dependencyGraph.find(tx);
    if (it != dependencyGraph.end()) {
        auto& dependents = it->second;
        // 递归地执行所有依赖此交易的后续交易
        for (auto& dependent : dependents) {
            executeTransaction(dependent, statistics);
        }
    }
}

void DeterReExecute::executeTransactionWithPool(const Vertex::Ptr& tx, Statistics& statistics, ThreadPool::Ptr& Pool, std::atomic<int>& taskCounter) {
    tx->Execute();
    // record the last commit time
    tx->m_hyperVertex->m_commit_time = chrono::steady_clock::now();
    statistics.JournalOverheads(tx->CountOverheads());
    auto it = dependencyGraph.find(tx);
    if (it != dependencyGraph.end()) {
        auto& dependents = it->second;
        // 对于每个依赖项，提交任务到线程池，并将它们的 future 添加到 futures 中
        for (auto& dependent : dependents) {
            taskCounter.fetch_add(1, std::memory_order_relaxed);
            Pool->enqueue([this, dependent, &statistics, &Pool, &taskCounter] {
                executeTransactionWithPool(dependent, statistics, Pool, taskCounter);
            });
        }
    }
    taskCounter.fetch_sub(1, std::memory_order_relaxed);
}

/* 获取事务列表 */
std::vector<Vertex::Ptr>& DeterReExecute::getRbList() {return m_rbList;}

/* 利用rbList构造normalList */
void DeterReExecute::setNormalList(const vector<Vertex::Ptr>& rbList, vector<Vertex::Ptr>& normalList) {
    /* 具体逻辑如下：
        将rbList中属于同一个超节点（即tx->m_hyperId相同）的所有事务组合成一个事务，这个事务拥有它们所有的读写集，执行时间为所有事务的执行时间总和
        将这些事务按照超节点在rbList中的出现顺序插入normalList
    */
    unordered_map<int, Vertex::Ptr> hyperId2Tx;
    unordered_map<int, bool> hyperId2Flag;
    for (auto& rb : rbList) {
        int hyperId = rb->m_hyperId;
        if (hyperId2Flag.find(hyperId) == hyperId2Flag.end()) {
            // 形成一个新的vertex对象
            auto tx = std::make_shared<Vertex>(std::move(*rb));
            hyperId2Flag[tx->m_hyperId] = true;
            hyperId2Tx[tx->m_hyperId] = tx;
            normalList.push_back(tx);
        } else {
            auto& tx = hyperId2Tx[hyperId];
            // 合并事务
            tx->m_self_cost += rb->m_self_cost;
            tx->readSet.insert(rb->readSet.begin(), rb->readSet.end());
            tx->writeSet.insert(rb->writeSet.begin(), rb->writeSet.end());
        }
    }
}

void DeterReExecute::setNormalList(const vector<Vertex::Ptr>& rbList, vector<HyperVertex::Ptr>& normalList) {
    unordered_map<int, bool> hyperId2Flag;
    for (auto& rb : rbList) {
        int& hyperId = rb->m_hyperId;
        auto& hvTx = rb->m_tx;
        if (hyperId2Flag.find(hyperId) == hyperId2Flag.end()) {
            // 形成一个新的vertex对象
            hvTx->m_rootVertex = rb;
            hyperId2Flag[hyperId] = true;
            normalList.push_back(hvTx);
        } else {
            // 合并事务
            hvTx->m_rootVertex->m_self_cost += rb->m_self_cost;
            hvTx->m_rootVertex->readSet.insert(rb->readSet.begin(), rb->readSet.end());
            hvTx->m_rootVertex->writeSet.insert(rb->writeSet.begin(), rb->writeSet.end());
        }
    }
}

std::vector<HyperVertex::Ptr> DeterReExecute::dummyNormalList;