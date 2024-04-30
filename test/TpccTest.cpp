#include <gtest/gtest.h>
#include "../workload/tpcc/Workload.hpp"

using namespace std;

TEST(TpccTest, NewOrderTransaction) {
    Workload workload;
    Transaction::Ptr tx = workload.NextTransaction();
    // EXPECT_EQ(tx->getTransactionType(), TransactionType::NEW_ORDER);
}