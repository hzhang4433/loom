#pragma once

#include "HyperVertex.h"

class Transaction : public std::enable_shared_from_this<Transaction>
{
    public:
        typedef std::shared_ptr<Transaction> Ptr;
        Transaction(HyperVertex::Ptr tx, size_t txTime) : m_tx(tx), m_txTime(txTime) {}
        Transaction() = default;
        ~Transaction() = default;
        void Execute();
        void SetStorageHandler();
        void GetStorageHandler();
        const HyperVertex::Ptr GetTx() const {return m_tx;}
    
    private:
        HyperVertex::Ptr m_tx;
        size_t m_txTime;
};