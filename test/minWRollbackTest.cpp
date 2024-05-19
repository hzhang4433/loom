#include <gtest/gtest.h>
#include <chrono>
#include "protocol/minW/minWRollback.h"

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
    Transaction::Ptr tx1 = make_shared<Transaction>();
    simulateRow(tx1, "1");

    Transaction::Ptr tx11 = make_shared<Transaction>();
    simulateRow(tx11, "11");
    tx1->addChild(tx11, minw::DependencyType::STRONG);
    Transaction::Ptr tx111 = make_shared<Transaction>();
    simulateRow(tx111, "111");
    tx11->addChild(tx111, minw::DependencyType::WEAK);
    Transaction::Ptr tx112 = make_shared<Transaction>();
    simulateRow(tx112, "112");
    tx11->addChild(tx112, minw::DependencyType::STRONG);

    Transaction::Ptr tx12 = make_shared<Transaction>();
    simulateRow(tx12, "12");
    tx1->addChild(tx12, minw::DependencyType::WEAK);
    Transaction::Ptr tx121 = make_shared<Transaction>();
    simulateRow(tx121, "121");
    tx12->addChild(tx121, minw::DependencyType::STRONG);
    
    // 需要修改读写集
    Transaction::Ptr tx122 = make_shared<Transaction>();
    simulateRow(tx122, "122");
    tx122->addReadRow("4");
    tx12->addChild(tx122, minw::DependencyType::WEAK);
    
    // 需要修改读写集
    Transaction::Ptr tx1221 = make_shared<Transaction>();
    simulateRow(tx1221, "1221");
    tx1221->addReadRow("5");
    tx122->addChild(tx1221, minw::DependencyType::STRONG);

    Transaction::Ptr tx12211 = make_shared<Transaction>();
    simulateRow(tx12211, "12211");
    tx1221->addChild(tx12211, minw::DependencyType::WEAK);

    // 需要修改读写集
    Transaction::Ptr tx12212 = make_shared<Transaction>();
    simulateRow(tx12212, "12212");
    tx12212->addUpdateRow("6");
    tx1221->addChild(tx12212, minw::DependencyType::STRONG);

    // 需要修改读写集
    Transaction::Ptr tx122121 = make_shared<Transaction>();
    simulateRow(tx122121, "122121");
    tx122121->addUpdateRow("7");
    tx12212->addChild(tx122121, minw::DependencyType::WEAK);
    
    // 需要修改读写集
    Transaction::Ptr tx1221211 = make_shared<Transaction>();
    simulateRow(tx1221211, "1221211");
    tx1221211->addReadRow("8");
    tx122121->addChild(tx1221211, minw::DependencyType::STRONG);

    return tx1;
}

// 自定义tx2
Transaction::Ptr makeTx2() {
    Transaction::Ptr tx2 = make_shared<Transaction>();
    simulateRow(tx2, "2");

    Transaction::Ptr tx21 = make_shared<Transaction>();
    simulateRow(tx21, "21");
    tx21->addUpdateRow("4");
    tx2->addChild(tx21, minw::DependencyType::STRONG);
    Transaction::Ptr tx211 = make_shared<Transaction>();
    simulateRow(tx211, "211");
    tx211->addUpdateRow("5");
    tx21->addChild(tx211, minw::DependencyType::WEAK);
    Transaction::Ptr tx212 = make_shared<Transaction>();
    simulateRow(tx212, "212");
    tx21->addChild(tx212, minw::DependencyType::STRONG);
    Transaction::Ptr tx2121 = make_shared<Transaction>();
    simulateRow(tx2121, "2121");
    tx2121->addReadRow("6");
    tx212->addChild(tx2121, minw::DependencyType::WEAK);

    


    Transaction::Ptr tx22 = make_shared<Transaction>();
    simulateRow(tx22, "22");
    tx22->addReadRow("9");
    tx2->addChild(tx22, minw::DependencyType::WEAK);
    
    Transaction::Ptr tx221 = make_shared<Transaction>();
    simulateRow(tx221, "221");
    tx221->addReadRow("44");
    tx22->addChild(tx221, minw::DependencyType::STRONG);
    
    Transaction::Ptr tx2211 = make_shared<Transaction>();
    simulateRow(tx2211, "2211");
    tx221->addChild(tx2211, minw::DependencyType::STRONG);
    Transaction::Ptr tx22111 = make_shared<Transaction>();
    simulateRow(tx22111, "22111");
    tx2211->addChild(tx22111, minw::DependencyType::WEAK);
    Transaction::Ptr tx22112 = make_shared<Transaction>();
    simulateRow(tx22112, "22112");
    tx2211->addChild(tx22112, minw::DependencyType::STRONG);

    Transaction::Ptr tx222 = make_shared<Transaction>();
    simulateRow(tx222, "222");
    tx222->addReadRow("10");
    tx22->addChild(tx222, minw::DependencyType::WEAK);

    return tx2;
}

// 自定义tx3
Transaction::Ptr makeTx3() {
    Transaction::Ptr tx3 = make_shared<Transaction>();
    simulateRow(tx3, "3");

    Transaction::Ptr tx31 = make_shared<Transaction>();
    simulateRow(tx31, "31");
    tx31->addUpdateRow("9");
    tx3->addChild(tx31, minw::DependencyType::STRONG);
    Transaction::Ptr tx311 = make_shared<Transaction>();
    simulateRow(tx311, "311");
    tx311->addUpdateRow("10");
    tx31->addChild(tx311, minw::DependencyType::STRONG);
    
    Transaction::Ptr tx312 = make_shared<Transaction>();
    simulateRow(tx312, "312");
    tx312->addReadRow("45");
    tx31->addChild(tx312, minw::DependencyType::WEAK);


    Transaction::Ptr tx32 = make_shared<Transaction>();
    simulateRow(tx32, "32");
    tx3->addChild(tx32, minw::DependencyType::WEAK);
    
    Transaction::Ptr tx321 = make_shared<Transaction>();
    simulateRow(tx321, "321");
    tx32->addChild(tx321, minw::DependencyType::STRONG);
    Transaction::Ptr tx3211 = make_shared<Transaction>();
    simulateRow(tx3211, "3211");
    tx321->addChild(tx3211, minw::DependencyType::WEAK);
    Transaction::Ptr tx32111 = make_shared<Transaction>();
    simulateRow(tx32111, "32111");
    tx32111->addReadRow("7");
    tx3211->addChild(tx32111, minw::DependencyType::STRONG);
    Transaction::Ptr tx32112 = make_shared<Transaction>();
    simulateRow(tx32112, "32112");
    tx32112->addUpdateRow("8");
    tx32112->addReadRow("55");
    tx3211->addChild(tx32112, minw::DependencyType::STRONG);


    Transaction::Ptr tx322 = make_shared<Transaction>();
    simulateRow(tx322, "322");
    tx322->addReadRow("54");
    tx32->addChild(tx322, minw::DependencyType::WEAK);

    return tx3;
}

TEST(minWRollbackTest, TestExecute) {
    // 自定义构建Transaction
    int txNum = 5;
    vector<Transaction::Ptr> txs;
    // tx1
    Transaction::Ptr tx1 = makeTx1();
    Transaction::Ptr tx2 = makeTx2();
    Transaction::Ptr tx3 = makeTx3();

    // tx4
    Transaction::Ptr tx4 = make_shared<Transaction>();
    simulateRow(tx4, "4");
    tx4->addReadRow("22114");

    // tx5
    Transaction::Ptr tx5 = make_shared<Transaction>();
    simulateRow(tx5, "5");
    tx5->addReadRow("3214");
    tx5->addReadRow("32114");

    txs.push_back(tx1);
    txs.push_back(tx2);
    txs.push_back(tx3);
    txs.push_back(tx4);
    txs.push_back(tx5);

    chrono::high_resolution_clock::time_point start, end;
    // 创建minWRollback实例，测试execute函数vertexs构建部分
    minWRollback minw;
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
    minw.rollback();
    end = std::chrono::high_resolution_clock::now();
    cout << "rollback time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    minw.printRollbackTxs();
    // minw.printHyperGraph();
}