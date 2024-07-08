#include "DeterReExecute.h"

using namespace std;



/* 时空图模块 */

/* 构建初始时空图
    即不考虑嵌套事务结构的时空图 => 为了做对比
*/
void DeterReExecute::buildGraphOrigin() {
    // 按队列顺序，依次遍历事务
    for (int j = 0; j < m_rbList.size(); j++) {
        m_rbList[j]->scheduledTime = 0;
        auto Tj = m_rbList[j];
        auto& unflictTxs = m_unConflictTxMap[m_rbList[j]->m_id];
        // 判断本事务与前序事务间冲突
        for (int i = 0; i < m_rbList.size(); i++) {
            auto Ti = m_rbList[i];
            if ((protocol::hasConflict(Ti->writeSet, Tj->readSet) || protocol::hasConflict(Ti->writeSet, Tj->writeSet) ||
                protocol::hasConflict(Ti->readSet, Tj->writeSet))) {
                if (i < j) {
                    // 输出txid和与它冲突事务的id
                    // cout << "Tx" << m_rbList[j]->id << " conflicts with Tx" << m_rbList[i]->id << endl;

                    // 本事务记录前序事务
                    Tj->dependencies_in.insert(Ti);
                    
                    // 所有前序事务记录本事务
                    Ti->dependencies_out.insert(Tj);
                    
                    // 本事务调度时间为前序事务结束时间
                    int newScheduledTime = Ti->scheduledTime + Ti->m_cost;
                    Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                }
            } else {
                // 添加到本事务的无冲突事务集合
                unflictTxs.insert(Ti);
            }
        }
    }
}

/* 构建时空图
1. 为每个事务设置调度时间
2. 为每个事务添加依赖关系
3. 记录每个事务的非冲突事务集合
*/
void DeterReExecute::buildGraph() {
    // 按队列顺序，依次遍历事务
    for (int j = 0; j < m_rbList.size(); j++) {
        m_rbList[j]->scheduledTime = 0;
        auto Tj = m_rbList[j];
        auto& unflictTxs = m_unConflictTxMap[m_rbList[j]->m_id];
        // 判断本事务与前序事务间冲突
        for (int i = 0; i < m_rbList.size(); i++) {
            auto Ti = m_rbList[i];

            // 判断事务间冲突关系(同属于一个超节点的子事务无需判断冲突关系)
            if (Ti->m_hyperId != Tj->m_hyperId && (protocol::hasConflict(Ti->writeSet, Tj->readSet) ||
                protocol::hasConflict(Ti->writeSet, Tj->writeSet) || protocol::hasConflict(Ti->readSet, Tj->writeSet))) {
                if (i < j) {
                    // 输出txid和与它冲突事务的id
                    // cout << "Tx" << m_rbList[j]->id << " conflicts with Tx" << m_rbList[i]->id << endl;

                    // 本事务记录前序事务
                    Tj->dependencies_in.insert(Ti);
                    
                    // 所有前序事务记录本事务
                    Ti->dependencies_out.insert(Tj);
                    
                    // 本事务调度时间为前序事务结束时间
                    int newScheduledTime = Ti->scheduledTime + Ti->m_cost;
                    Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                }
            } else {
                // 添加到本事务的无冲突事务集合
                unflictTxs.insert(Ti);
            }
        }

        // 若存在强依赖子节点，则将其添加至依赖关系
        if (Tj->hasStrong) {
            for (auto Ti : Tj->m_strongChildren) {
                // 父事务记录强依赖子事务
                Tj->dependencies_in.insert(Ti);
                // 子事务记录强依赖父事务
                Ti->dependencies_out.insert(Tj);
                // 父事务调度时间为前序事务结束时间
                int newScheduledTime = Ti->scheduledTime + Ti->m_cost;
                Tj->scheduledTime = std::max(Tj->scheduledTime, newScheduledTime);
                unflictTxs.erase(Ti);
            }
        } else if (Tj->m_strongParent != nullptr) {
            // 有必要删除吗？不管删除不删除，在判断idIdle时都无法通过的
            unflictTxs.erase(Tj->m_strongParent);
        }
    }
}

/* 并发构建时空图 
*/
void DeterReExecute::buildGraphConcurrent(Util::UThreadPoolPtr& Pool) {

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
                continue;
            } else {
                break;
            }
        }

        // 2.获取事务队列中读写集与其不冲突,且执行时刻在其之后的所有事务集Ts
        std::set<Vertex::Ptr, Loom::lessScheduledTime> Ts;
        getCandidateTxSet(tx, Ts);
        // 若不存在满足条件的事务，则跳过
        if (Ts.empty()) {
            if (N + 1 < m_rbList.size()) {
                N++;
                continue;
            } else {
                break;
            }
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
void DeterReExecute::getCandidateTxSet(const Vertex::Ptr& Tx, std::set<Vertex::Ptr, Loom::lessScheduledTime>& Ts) {
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

/* 递归重调度事务
*/
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
        } else if (Tj->scheduledTime == Ti->scheduledTime + Ti->m_cost) {
            //1.3.1 如果Tj执行时间就在Ti执行完成后的时刻，直接遍历下一个事务
            continue;
        // 1.4 若Tj可以前移至Tj执行完成后时刻
        } else if (isIdle(Tj, Ti->scheduledTime + Ti->m_cost)) {
            //1.4.0深拷贝Tj依赖边depedencies_out
            std::set<Vertex::Ptr> m_originalDependencies(Tj->dependencies_out.begin(), Tj->dependencies_out.end());
            //1.4.1移动至Ti执行完成后时刻执行
            reschedule(Tj, Ti->scheduledTime + Ti->m_cost);
            //1.4.2记录已移动事务
            movedTxIds.insert(Tj->m_id);
            //1.4.3从Tj出发继续选取并尝试前移事务
            recursiveRescheduleTxs(Tj, Tx, movedTxIds, m_originalDependencies);
        }
        
        // 1.6 若Tj不可以移动，则继续遍历Ti的依赖事务
    }
}


/* 时间计算模块 */ 

/* 判断事务是否能够在startTime时刻执行
*/
bool DeterReExecute::isIdle(const Vertex::Ptr& tx, int startTime) {
    // 1.计算事务Tx的执行结束时间,获取执行区间[startTime, endTime]
    int endTime = startTime + tx->m_cost;
    // 2.遍历Tx的冲突事务，判断目标时间区间是否空闲
    for (auto& conflictTx : tx->dependencies_in) {
        // !(事务冲突事务的执行时间在Tx之后 或 事务在startTime前执行完成) -> !(无冲突)
        if (!(conflictTx->scheduledTime >= endTime || conflictTx->scheduledTime + conflictTx->m_cost <= startTime)) {
            return false;
        }
    }
    return true;
}

/* 计算事务总执行时间
*/
int DeterReExecute::calculateTotalExecutionTime() {
    // cout << "====calculateTotalExecutionTime====" << endl;
    int endTime = 0;
    for (auto& txn : m_rbList) {
        int txnEndTime = calculateExecutionTime(txn) + txn->m_cost;
        endTime = std::max(endTime, txnEndTime);
    }
    m_totalExecTime = endTime;
    return m_totalExecTime;
}

/* 计算每笔事务最早执行时间
*/
int DeterReExecute::calculateExecutionTime(Vertex::Ptr& Tx) {
    int executionTime = 0;
    for (auto& conflictTx : Tx->dependencies_in) {
        executionTime = std::max(executionTime, conflictTx->scheduledTime + conflictTx->m_cost);
    }
    Tx->scheduledTime = executionTime;
    return executionTime;
}


// 判断两个事务是否可调序
bool DeterReExecute::canReorder(const Vertex::Ptr& Tx1, const Vertex::Ptr& Tx2) {
    // // 检查Tx1或Tx2是否不在m_orderIndex中 => 无需检查, 因为非嵌套事务无需判断肯定canReorder
    // if (m_orderIndex.find(Tx1->m_hyperId) == m_orderIndex.end() || m_orderIndex.find(Tx2->m_hyperId) == m_orderIndex.end()) {
    //     return true; // Tx1或Tx2不在m_orderIndex中，可以调序
    // }

    // 两个事务在同一个集合中，无法调序
    return m_orderIndex[Tx1->m_hyperId] != m_orderIndex[Tx2->m_hyperId];
}

std::vector<Vertex::Ptr>& DeterReExecute::getRbList() { // 获取事务列表
    return m_rbList;
}