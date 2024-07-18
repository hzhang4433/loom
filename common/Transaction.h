#pragma once

#include "HyperVertex.h"

namespace loom {

using SetStorage = std::function<void(const std::unordered_set<string>& writeSet, const std::string& value)>;
using GetStorage = std::function<void(const std::unordered_set<string>& readSet)>;

class Transaction : public std::enable_shared_from_this<Transaction>
{
    public:
        typedef std::shared_ptr<Transaction> Ptr;
        Transaction(HyperVertex::Ptr tx);
        Transaction(const Transaction& other); // copy constructor
        void InstallSetStorageHandler(SetStorage &&handler);
        void InstallGetStorageHandler(GetStorage &&handler);
        void Execute();
        const HyperVertex::Ptr GetTx() const {return m_tx;}
    private:
        HyperVertex::Ptr m_tx;

        // Handler functions for get and set operations
        SetStorage setHandler;
        GetStorage getHandler;
};
}
