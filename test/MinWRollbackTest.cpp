#include <gtest/gtest.h>
#include <chrono>

#include "protocol/minW/MinWRollback.h"
#include "workload/tpcc/Workload.hpp"

using namespace std;

void simulateRow(Transaction::Ptr tx, string txid) {
    tx->addReadRow(txid + "4");
    tx->addReadRow(txid + "5");
    tx->addReadRow(txid + "6");
    tx->addUpdateRow(txid + "4");
    tx->addUpdateRow(txid + "5");
    tx->addUpdateRow(txid + "6");
}

// 自定义tx1
Transaction::Ptr makeTx1() {
    Transaction::Ptr tx1;
    simulateRow(tx1, "1");

    Transaction::Ptr tx11;
    simulateRow(tx11, "11");
    tx1->addChild(tx11, minw::DependencyType::STRONG);
    Transaction::Ptr tx111;
    simulateRow(tx111, "111");
    tx11->addChild(tx111, minw::DependencyType::WEAK);
    Transaction::Ptr tx112;
    simulateRow(tx112, "112");
    tx11->addChild(tx112, minw::DependencyType::STRONG);

    Transaction::Ptr tx12;
    simulateRow(tx12, "12");
    tx1->addChild(tx12, minw::DependencyType::WEAK);
    Transaction::Ptr tx121;
    simulateRow(tx121, "121");
    tx12->addChild(tx121, minw::DependencyType::STRONG);
    
    // 需要修改读写集
    Transaction::Ptr tx122;
    simulateRow(tx122, "122");
    tx122->addReadRow("4");
    tx12->addChild(tx122, minw::DependencyType::WEAK);
    
    // 需要修改读写集
    Transaction::Ptr tx1221;
    simulateRow(tx1221, "1221");
    tx1221->addReadRow("5");
    tx122->addChild(tx1221, minw::DependencyType::STRONG);

    Transaction::Ptr tx12211;
    simulateRow(tx12211, "12211");
    tx1221->addChild(tx12211, minw::DependencyType::WEAK);

    // 需要修改读写集
    Transaction::Ptr tx12212;
    simulateRow(tx12212, "12212");
    tx12212->addUpdateRow("6");
    tx1221->addChild(tx12212, minw::DependencyType::STRONG);

    // 需要修改读写集
    Transaction::Ptr tx122121;
    simulateRow(tx122121, "122121");
    tx122121->addUpdateRow("7");
    tx12212->addChild(tx122121, minw::DependencyType::WEAK);
    
    // 需要修改读写集
    Transaction::Ptr tx1221211;
    simulateRow(tx1221211, "1221211");
    tx1221211->addReadRow("8");
    tx122121->addChild(tx1221211, minw::DependencyType::STRONG);

    return tx1;
}

// 自定义tx2
Transaction::Ptr makeTx2() {
    Transaction::Ptr tx2;
    simulateRow(tx2, "2");

    Transaction::Ptr tx21;
    simulateRow(tx21, "21");
    tx21->addUpdateRow("4");
    tx2->addChild(tx21, minw::DependencyType::STRONG);
    Transaction::Ptr tx211;
    simulateRow(tx211, "211");
    tx211->addUpdateRow("5");
    tx21->addChild(tx211, minw::DependencyType::WEAK);
    Transaction::Ptr tx212;
    simulateRow(tx212, "212");
    tx21->addChild(tx212, minw::DependencyType::STRONG);
    Transaction::Ptr tx2121;
    simulateRow(tx2121, "2121");
    tx2121->addReadRow("6");
    tx212->addChild(tx2121, minw::DependencyType::WEAK);

    


    Transaction::Ptr tx22;
    simulateRow(tx22, "22");
    tx22->addReadRow("9");
    tx2->addChild(tx22, minw::DependencyType::WEAK);
    
    Transaction::Ptr tx221;
    simulateRow(tx221, "221");
    tx221->addReadRow("44");
    tx22->addChild(tx221, minw::DependencyType::STRONG);
    
    Transaction::Ptr tx2211;
    simulateRow(tx2211, "2211");
    tx221->addChild(tx2211, minw::DependencyType::STRONG);
    Transaction::Ptr tx22111;
    simulateRow(tx22111, "22111");
    tx2211->addChild(tx22111, minw::DependencyType::WEAK);
    Transaction::Ptr tx22112;
    simulateRow(tx22112, "22112");
    tx2211->addChild(tx22112, minw::DependencyType::STRONG);

    Transaction::Ptr tx222;
    simulateRow(tx222, "222");
    tx222->addReadRow("10");
    tx22->addChild(tx222, minw::DependencyType::WEAK);

    return tx2;
}

// 自定义tx3
Transaction::Ptr makeTx3() {
    Transaction::Ptr tx3;
    simulateRow(tx3, "3");

    Transaction::Ptr tx31;
    simulateRow(tx31, "31");
    tx31->addUpdateRow("9");
    tx3->addChild(tx31, minw::DependencyType::STRONG);
    Transaction::Ptr tx311;
    simulateRow(tx311, "311");
    tx311->addUpdateRow("10");
    tx31->addChild(tx311, minw::DependencyType::STRONG);
    
    Transaction::Ptr tx312;
    simulateRow(tx312, "312");
    tx312->addReadRow("45");
    tx31->addChild(tx312, minw::DependencyType::WEAK);


    Transaction::Ptr tx32;
    simulateRow(tx32, "32");
    tx3->addChild(tx32, minw::DependencyType::WEAK);
    
    Transaction::Ptr tx321;
    simulateRow(tx321, "321");
    tx32->addChild(tx321, minw::DependencyType::STRONG);
    Transaction::Ptr tx3211;
    simulateRow(tx3211, "3211");
    tx321->addChild(tx3211, minw::DependencyType::WEAK);
    Transaction::Ptr tx32111;
    simulateRow(tx32111, "32111");
    tx32111->addReadRow("7");
    tx3211->addChild(tx32111, minw::DependencyType::STRONG);
    Transaction::Ptr tx32112;
    simulateRow(tx32112, "32112");
    tx32112->addUpdateRow("8");
    tx32112->addReadRow("55");
    tx3211->addChild(tx32112, minw::DependencyType::STRONG);


    Transaction::Ptr tx322;
    simulateRow(tx322, "322");
    tx322->addReadRow("54");
    tx32->addChild(tx322, minw::DependencyType::WEAK);

    return tx3;
}

TEST(MinWRollbackTest, TestExecute) {
    // 自定义构建Transaction
    int txNum = 5;
    vector<Transaction::Ptr> txs;
    // tx1
    Transaction::Ptr tx1 = makeTx1();
    Transaction::Ptr tx2 = makeTx2();
    Transaction::Ptr tx3 = makeTx3();

    // tx4
    Transaction::Ptr tx4;
    simulateRow(tx4, "4");
    tx4->addReadRow("22114");

    // tx5
    Transaction::Ptr tx5;
    simulateRow(tx5, "5");
    tx5->addReadRow("3214");
    tx5->addReadRow("32114");

    txs.push_back(tx1);
    txs.push_back(tx2);
    txs.push_back(tx3);
    txs.push_back(tx4);
    txs.push_back(tx5);

    chrono::high_resolution_clock::time_point start, end;
    // 创建MinWRollback实例，测试execute函数vertexs构建部分
    MinWRollback minw;
    for (int i = 0; i < txNum; i++) {
        // start = std::chrono::high_resolution_clock::now();
        minw.execute(txs[i]);
        // end = std::chrono::high_resolution_clock::now();

        // // print hyperGraph
        // minw.printHyperGraph();

        // print graph build time
        // cout << "tx" << i + 1 << " execute + build time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }

    start = std::chrono::high_resolution_clock::now();
    minw.rollback(0);
    end = std::chrono::high_resolution_clock::now();
    cout << "rollback time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    minw.printRollbackTxs();
    // minw.printHyperGraph();
}

TEST(MinWRollbackTest, TestPerformance) {
    Workload workload;
    MinWRollback minw;
    Random random(time(0));
    Transaction::Ptr tx;
    chrono::high_resolution_clock::time_point start, end;

    for (int i = 0; i < 10; i++) {
        tx = workload.NextTransaction();
        if (tx == nullptr) {
            cout << "=== tx is nullptr ===" << endl;
            continue;
        }

        // start = chrono::high_resolution_clock::now();
        // if (random.uniform_dist(1, 10) <= 5) {
        //     minw.execute(tx, false);
        // } else {
        //     minw.execute(tx);
        // }
        minw.execute(tx);
        // end = chrono::high_resolution_clock::now();
        // cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }
    
    start = std::chrono::high_resolution_clock::now();
    minw.buildGraph();
    end = std::chrono::high_resolution_clock::now();
    cout << "build time: " << chrono::duration_cast<chrono::milliseconds>(end - start).count() << "ms" << endl;


    start = std::chrono::high_resolution_clock::now();
    minw.rollback(0);
    end = std::chrono::high_resolution_clock::now();
    cout << "rollback time: " << chrono::duration_cast<chrono::milliseconds>(end - start).count() << "ms" << endl;

    minw.printRollbackTxs();
}

TEST(MinWRollbackTest, TestCombine) {
    int in = 14;
    int out = 2;
    MinWRollback minw;
    cout << minw.combine(in, out) << endl;
}

TEST(MinWRollbackTest, TestSCC) {
    Workload workload;
    MinWRollback minw;
    Random random;
    Transaction::Ptr tx = std::make_shared<NewOrderTransaction>(random);
    chrono::high_resolution_clock::time_point start, end;

    // 先生成一笔newOrder事务
    // auto newOrder = tx->makeTransaction();
    // minw.execute(newOrder);
    for (int i = 1; i < 20; i++) {
        tx = workload.NextTransaction();
        if (tx == nullptr) {
            cout << "=== tx is nullptr ===" << endl;
            continue;
        }
        minw.execute(tx);
    }

    start = std::chrono::high_resolution_clock::now();
    minw.buildGraph();
    end = std::chrono::high_resolution_clock::now();
    cout << "build time: " << chrono::duration_cast<chrono::milliseconds>(end - start).count() << "ms" << endl;

    for (auto hyperVertexs : minw.m_min2HyperVertex) {
        cout << "hyperVertexs: " << hyperVertexs.first << ", size = " << hyperVertexs.second.size() << endl;
        for (auto hv : hyperVertexs.second) {
            cout << hv->m_hyperId << " ";
        }
        cout << endl;
        if (hyperVertexs.second.size() <= 1) {
            continue;
        }

        vector<tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> sccs;
        if (!minw.recognizeSCC(hyperVertexs.second, sccs)) {
            cout << "这个不行" << endl;
            continue;
        }

        for (auto scc : sccs) {
            cout << "===== 输出这个scc中的每一个元素的详细信息 =====" << endl;
            
            cout << "size = " << scc.size() << ", all vertexs:"; 
            for (auto hv : scc) {
                cout << hv->m_hyperId << " ";
            }
            cout << endl;

            for (auto hv : scc) {
                cout << "hyperId: " << hv->m_hyperId << endl;
                cout << "outEdges: ";
                for (auto edge : hv->m_out_edges) {
                    cout << edge.first->m_hyperId << " ";
                }
                cout << endl << "inEdges: ";
                for (auto edge: hv->m_in_edges) {
                    cout << edge.first->m_hyperId << " ";
                }
                cout << endl;
            }
            cout << endl;
        }
    }

}

TEST(MinWRollbackTest, TestLoopPerformance) {
    Workload workload;
    chrono::high_resolution_clock::time_point start, end;
    Transaction::Ptr tx;

    uint64_t seed = workload.get_seed();
    cout << "seed = " << seed << endl;

    for (int i = 0; i < 2; i++) {        
        MinWRollback minw;

        // workload.set_seed(uint64_t(140719991741917));
        workload.set_seed(seed);

        for (int i = 0; i < 50; i++) {
            tx = workload.NextTransaction();
            if (tx == nullptr) {
                cout << "=== tx is nullptr ===" << endl;
                continue;
            }
            minw.execute(tx);
            // 输出minw中的m_hyperVertices
            for (auto& hv : minw.m_hyperVertices) {
                cout << "hv: " << hv->m_hyperId << " " 
                     << "hyperVertex->m_rootVertex " << hv->m_rootVertex->m_id << " "
                     << "hyperVertex->cost " << hv->m_rootVertex->m_cost << endl;
            }
        }
        
        start = std::chrono::high_resolution_clock::now();
        minw.buildGraph();
        end = std::chrono::high_resolution_clock::now();
        cout << "build time: " << chrono::duration_cast<chrono::milliseconds>(end - start).count() << "ms" << endl;

        start = std::chrono::high_resolution_clock::now();
        minw.rollback(0);
        end = std::chrono::high_resolution_clock::now();
        cout << "rollback time: " << chrono::duration_cast<chrono::milliseconds>(end - start).count() << "ms" << endl;

        // minw.printHyperGraph();

        minw.printRollbackTxs();
    }
    
}

TEST(MinWRollbackTest, TestOptCompare) {
    Workload workload;
    chrono::high_resolution_clock::time_point start, end;
    Transaction::Ptr tx;

    uint64_t seed = workload.get_seed();
    // uint64_t seed = uint64_t(140711476888349);
    // 140716750994125: 50-8ms
    // 140707595429853: 5-6ms
    cout << "seed = " << seed << endl;

    for (int i = 0; i < 2; i++) {        
        MinWRollback minw;
        workload.set_seed(seed);

        for (int i = 0; i < 50; i++) {
            tx = workload.NextTransaction();
            if (tx == nullptr) {
                cout << "=== tx is nullptr ===" << endl;
                continue;
            }
            minw.execute(tx);
        }
        cout << "transaction generate done" << endl;
        
        start = std::chrono::high_resolution_clock::now();
        minw.buildGraph();
        end = std::chrono::high_resolution_clock::now();
        cout << "build time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;

        cout << "mode: " << i << " rollback" << endl;
        start = std::chrono::high_resolution_clock::now();
        minw.rollback(i);
        end = std::chrono::high_resolution_clock::now();
        cout << "rollback time: " << (double)chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000 << "ms" << endl;

        minw.printRollbackTxs();
    }
}

TEST(MinWRollbackTest, TestBuildGraph) {
    Workload workload;
    chrono::high_resolution_clock::time_point start, end;
    Transaction::Ptr tx;
    int counter = 0;

    for (int j = 0; j < 10; j++) {
        MinWRollback minw1, minw2;
        // 用当前时间设置随机种子
        workload.set_seed(uint64_t(chrono::high_resolution_clock::now().time_since_epoch().count()));
        uint64_t seed = workload.get_seed();
        // uint64_t seed = uint64_t(140708231432333);
        cout << "seed = " << seed << endl;

        for (int i = 0; i < 100; i++) {
            tx = workload.NextTransaction();
            if (tx == nullptr) {
                cout << "=== tx is nullptr ===" << endl;
                continue;
            }
            minw1.execute(tx);
            minw2.execute(tx);
        }
        cout << "transaction generate done" << endl;
        
        start = std::chrono::high_resolution_clock::now();
        minw1.buildGraph();
        end = std::chrono::high_resolution_clock::now();
        auto build1 = chrono::duration_cast<chrono::milliseconds>(end - start).count();
        cout << "build1 time: " << build1 << "ms" << endl;

        start = std::chrono::high_resolution_clock::now();
        minw2.buildGraph2();
        end = std::chrono::high_resolution_clock::now();
        auto build2 = chrono::duration_cast<chrono::milliseconds>(end - start).count();
        cout << "build2 time: " << build2 << "ms" << endl;
        cout << "gap: " << build1 - build2 << "ms" << endl;
        if (build1 < build2) {
            counter++;
        }
    }
    
    cout << "counter: " << counter << endl;
}