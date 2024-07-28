#include <loom/common/Transaction.h>
#include <loom/protocol/common.h>
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
    DLOG(INFO) << "tx: " << m_tx << " execute transaction: " << m_tx->m_hyperId << std::endl;
    if (getHandler) {
        getHandler(m_tx->m_rootVertex->allReadSet);
    }
    if (setHandler) {
        setHandler(m_tx->m_rootVertex->allWriteSet, "value");
    }
    auto& tx = m_tx->m_rootVertex->m_cost;
    loom::Exec(tx);
}