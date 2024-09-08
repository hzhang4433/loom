#pragma once

// #include "HyperVertex.h"
#include <string>
#include <unordered_set>
#include <functional>
#include <memory>


namespace loom {

class HyperVertex;

using SetStorage = std::function<void(const std::unordered_set<std::string>& writeSet, const std::string& value)>;
using GetStorage = std::function<void(const std::unordered_set<std::string>& readSet)>;

class Transaction : public std::enable_shared_from_this<Transaction>
{
    public:
        typedef std::shared_ptr<Transaction> Ptr;
        Transaction(std::shared_ptr<HyperVertex> tx);
        Transaction(const Transaction& other); // copy constructor
        void InstallSetStorageHandler(SetStorage &&handler);
        void InstallGetStorageHandler(GetStorage &&handler);
        virtual void Execute();
        virtual size_t CountOverheads() const;
        const std::shared_ptr<HyperVertex> GetTx() const {return m_tx;}
        std::shared_ptr<HyperVertex> m_tx;

    protected:
        // Handler functions for get and set operations
        SetStorage setHandler;
        GetStorage getHandler;
};

}
