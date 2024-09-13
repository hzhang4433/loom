#pragma once

#include <string>

namespace TPCC {
    
enum TransactionType {
    NEW_ORDER,
    PAYMENT,
    ORDER_STATUS,
    DELIVERY,
    STOCK_LEVEL
};

enum ConsumptionType {
    HIGH = 80,
    MEDIUM = 40,
    LOW = 20,
};

static std::string transactionTypeToString(TransactionType type) {
    switch (type) {
        case TransactionType::NEW_ORDER:
            return "NEW_ORDER";
        case TransactionType::PAYMENT:
            return "PAYMENT";
        case TransactionType::ORDER_STATUS:
            return "ORDER_STATUS";
        case TransactionType::DELIVERY:
            return "DELIVERY";
        case TransactionType::STOCK_LEVEL:
            return "STOCK_LEVEL";
        default:
            return "UNKNOWN";
    }
}

extern size_t N_WAREHOUSES;
static const size_t N_DISTRICTS = 10;
static const size_t N_CUSTOMERS = 3000;
static const size_t N_CARRIERS = 10;

}