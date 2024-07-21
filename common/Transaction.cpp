#include <Loom/common/Transaction.h>
#include <Loom/protocol/common.h>
#include <glog/logging.h>

Transaction::Transaction(
    HyperVertex::Ptr tx
): 
    m_tx(tx)
{}

Transaction::Transaction(
    const Transaction& other
): 
    m_tx(other.m_tx)
{} 

void Transaction::InstallSetStorageHandler(SetStorage &&handler) {
    setHandler = handler;
}

void Transaction::InstallGetStorageHandler(GetStorage &&handler) {
    getHandler = handler;
}

void Transaction::Execute() {
    DLOG(INFO) << "execute transaction: " << m_tx->m_hyperId << std::endl;
    if (getHandler) {
        getHandler(m_tx->m_rootVertex->readSet);
    }
    if (setHandler) {
        setHandler(m_tx->m_rootVertex->writeSet, "value");
    }
    auto tx = m_tx->m_rootVertex->m_cost;
    loom::Exec(tx);
}