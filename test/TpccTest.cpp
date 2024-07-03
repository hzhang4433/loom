#include <gtest/gtest.h>
#include <chrono>
#include "workload/tpcc/Workload.hpp"
#include "protocol/minW/MinWRollback.h"

using namespace std;

// 测试NewOrderTransaction事务结构
TEST(TpccTest, NewOrderTransaction) {
    Random random;
    // Transaction::Ptr txGenerator = std::make_shared<NewOrderTransaction>(random);
    NewOrderTransaction::Ptr txGenerator = std::make_shared<NewOrderTransaction>(random);
    for (int i = 0; i < 200; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
        
        // 创建MinWRollback实例，测试execute函数vertexs构建部分
        // MinWRollback minw;
        // auto start = chrono::high_resolution_clock::now();
        // minw.execute(tx);
        // auto end = chrono::high_resolution_clock::now();
        // cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }
    // 打印订单行访问频率
    txGenerator->printOrderLineInfo();
}

TEST(TpccTest, PaymentTransaction) {
    Transaction::Ptr txGenerator;
    MinWRollback minw;
    Random random;
    txGenerator = std::make_shared<PaymentTransaction>(random);
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
    Random random;

    // 先生成一批newOrder事务
    txGenerator = std::make_shared<NewOrderTransaction>(random);
    for (int i = 0; i < 43; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
    }

    txGenerator = std::make_shared<OrderStatusTransaction>(random);
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
    Random random;
    chrono::high_resolution_clock::time_point start, end;

    // 先生成一批newOrder事务
    txGenerator = std::make_shared<NewOrderTransaction>(random);
    for (int i = 0; i < 43; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
    }

    txGenerator = std::make_shared<DeliveryTransaction>(random);
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
    Random random;
    chrono::high_resolution_clock::time_point start, end;

    // 先生成一批newOrder事务
    txGenerator = std::make_shared<NewOrderTransaction>(random);
    for (int i = 0; i < 43; i++) {
        Transaction::Ptr tx = txGenerator->makeTransaction();
    }

    txGenerator = std::make_shared<StockLevelTransaction>(random);
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
    Transaction::Ptr tx;
    chrono::high_resolution_clock::time_point start, end;

    cout << "Seed: " << workload.get_seed() << endl;

    for (int i = 0; i < 10; i++) {
        tx = workload.NextTransaction();
        if (tx == nullptr) {
            cout << "tx is nullptr" << endl;
            continue;
        }
        // // 创建MinWRollback实例，测试execute函数vertexs构建部分
        // start = chrono::high_resolution_clock::now();
        // minw.execute(tx);
        // end = chrono::high_resolution_clock::now();
        // cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }

    // 打印订单行访问频率
    tx->printOrderLineInfo();
}


TEST(TpccTest, RandomTEST) {
    Workload workload;
    workload.set_seed(123456);
    Random random(workload.get_seed());
    
    for (int i = 0; i < 20; i++) {
        cout << random.uniform_dist(1, 255) << " ";
    }
    cout << endl;

    auto newOrder1 = std::make_shared<NewOrderTransaction>(random);
    for (int i = 0; i < 20; i++) {
        cout << newOrder1->random.uniform_dist(1, 255) << " ";
    }
    cout << endl;

    auto newOrder2 = std::make_shared<NewOrderTransaction>(random);
    for (int i = 0; i < 10; i++) {
        cout << newOrder2->random.uniform_dist(1, 255) << " ";
    }
    cout << endl;

}

TEST(TpccTest, MultiWarehouseTEST) {
    Workload workload;
    MinWRollback minw;
    Transaction::Ptr tx;
    chrono::high_resolution_clock::time_point start, end;

    cout << "Seed: " << workload.get_seed() << endl;

    for (int i = 0; i < 500; i++) {
        tx = workload.NextTransaction();
        if (tx == nullptr) {
            cout << "tx is nullptr" << endl;
            continue;
        }
    }
}