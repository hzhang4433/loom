#include <gtest/gtest.h>
#include "workload/tpcc/Workload.hpp"
#include "protocol/minW/minWRollback.h"

using namespace std;

// 测试NewOrderTransaction事务结构
TEST(TpccTest, NewOrderTransaction) {
    Transaction::Ptr txGenerator = std::make_shared<Transaction>();
    txGenerator = std::make_shared<NewOrderTransaction>();
    for (int i = 0; i < 10; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
        
        // 创建minWRollback实例，测试execute函数vertexs构建部分
        // minWRollback minw;
        // minw.execute(tx);
    }
}

TEST(TpccTest, PaymentTransaction) {
   
}

TEST(TpccTest, OrderStatusTransaction) {
   
}

TEST(TpccTest, DeliveryTransaction) {
   
}

TEST(TpccTest, StockLevelTransaction) {
   
}

/* 测试Workload类
    1. 测试静态变量
*/
TEST(TpccTest, WorkloadTEST) {
    Workload workload;
    Transaction::Ptr tx = workload.NextTransaction();
}