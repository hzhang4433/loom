#include "DeterReExecute.h"

using namespace std;



// 判断两个事务是否可调序
bool DeterReExecute::canReorder(const Vertex::Ptr& Tx1, const Vertex::Ptr& Tx2) {
    // 两个事务在同一个集合中，无法调序
    return m_orderIndex[Tx1->m_hyperId] != m_orderIndex[Tx2->m_hyperId];
}

std::vector<Vertex::Ptr>& DeterReExecute::getRbList() { // 获取事务列表
    return m_rbList;
}