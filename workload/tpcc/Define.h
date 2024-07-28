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
    HIGH = 100,
    MEDIUM = 60,
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

static const int N_WAREHOUSES = 1;
static const int N_DISTRICTS = 10;
static const int N_CUSTOMERS = 3000;
static const int N_CARRIERS = 10;

}