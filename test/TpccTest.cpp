#include <gtest/gtest.h>
#include <chrono>
#include "workload/tpcc/Workload.hpp"
#include "protocol/minW/MinWRollback.h"

using namespace std;

// 测试NewOrderTransaction事务结构
TEST(TpccTest, NewOrderTransaction) {
    Transaction::Ptr txGenerator = std::make_shared<Transaction>();
    txGenerator = std::make_shared<NewOrderTransaction>();
    for (int i = 0; i < 10; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
        
        // 创建MinWRollback实例，测试execute函数vertexs构建部分
        // MinWRollback minw;
        // auto start = chrono::high_resolution_clock::now();
        // minw.execute(tx);
        // auto end = chrono::high_resolution_clock::now();
        // cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }
}

TEST(TpccTest, PaymentTransaction) {
    Transaction::Ptr txGenerator;
    MinWRollback minw;
    txGenerator = std::make_shared<PaymentTransaction>();
    chrono::high_resolution_clock::time_point start, end;

    for (int i = 0; i < 10; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
        
        // 创建MinWRollback实例，测试execute函数vertexs构建部分
        start = chrono::high_resolution_clock::now();
        minw.execute(tx);
        end = chrono::high_resolution_clock::now();
        cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }
}

TEST(TpccTest, OrderStatusTransaction) {
    Transaction::Ptr txGenerator;
    MinWRollback minw;
    chrono::high_resolution_clock::time_point start, end;

    // 先生成一批newOrder事务
    txGenerator = std::make_shared<NewOrderTransaction>();
    for (int i = 0; i < 43; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
    }

    txGenerator = std::make_shared<OrderStatusTransaction>();
    for (int i = 0; i < 3; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
        
        // 创建MinWRollback实例，测试execute函数vertexs构建部分
        start = chrono::high_resolution_clock::now();
        minw.execute(tx);
        end = chrono::high_resolution_clock::now();
        cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }
}

TEST(TpccTest, DeliveryTransaction) {
    Transaction::Ptr txGenerator;
    MinWRollback minw;
    chrono::high_resolution_clock::time_point start, end;

    // 先生成一批newOrder事务
    txGenerator = std::make_shared<NewOrderTransaction>();
    for (int i = 0; i < 43; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
    }

    txGenerator = std::make_shared<DeliveryTransaction>();
    for (int i = 0; i < 3; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
        
        // 创建MinWRollback实例，测试execute函数vertexs构建部分
        start = chrono::high_resolution_clock::now();
        minw.execute(tx);
        end = chrono::high_resolution_clock::now();
        cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }
}

TEST(TpccTest, StockLevelTransaction) {
    Transaction::Ptr txGenerator;
    MinWRollback minw;
    chrono::high_resolution_clock::time_point start, end;

    // 先生成一批newOrder事务
    txGenerator = std::make_shared<NewOrderTransaction>();
    for (int i = 0; i < 43; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
    }

    txGenerator = std::make_shared<StockLevelTransaction>();
    for (int i = 0; i < 3; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
        if (tx == nullptr) {
            cout << "tx is nullptr" << endl;
            continue;
        }
        // 创建MinWRollback实例，测试execute函数vertexs构建部分
        start = chrono::high_resolution_clock::now();
        minw.execute(tx, true);
        end = chrono::high_resolution_clock::now();
        cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }
}

/* 测试Workload类
    1. 测试静态变量
*/
TEST(TpccTest, WorkloadTEST) {
    Workload workload;
    MinWRollback minw;
    Transaction::Ptr tx = std::make_shared<NewOrderTransaction>();
    chrono::high_resolution_clock::time_point start, end;
    
    // 先生成一笔newOrder事务
    tx->makeTransaction();
    for (int i = 0; i < 6; i++) {
        tx = workload.NextTransaction();
        if (tx == nullptr) {
            cout << "tx is nullptr" << endl;
            continue;
        }
        // 创建MinWRollback实例，测试execute函数vertexs构建部分
        start = chrono::high_resolution_clock::now();
        minw.execute(tx);
        end = chrono::high_resolution_clock::now();
        cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }
    
}