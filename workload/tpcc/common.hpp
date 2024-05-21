#pragma once

#include "common/common.h"

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

    constexpr int n_warehouses = 1;
    constexpr int n_districts = 10;
    constexpr int n_customers = 3000;
    constexpr int n_carriers = 10;

}