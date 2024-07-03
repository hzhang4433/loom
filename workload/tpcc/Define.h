#pragma once


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

static const int N_WAREHOUSES = 5;
static const int N_DISTRICTS = 10;
static const int N_CUSTOMERS = 3000;
static const int N_CARRIERS = 10;

}