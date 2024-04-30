#include "minWRollback.h"

using namespace std;

/*  执行算法: 将transaction转化为hyperVertex
    详细算法流程: 
        1. 多线程并发执行所有事务，获得事务执行信息（读写集，事务结构，执行时间 => 随机生成）
        2. 判断执行完成的事务与其它执行完成事务间的rw依赖，构建rw依赖图
*/
void minWRollback::execute() {
    
}

/*  构图算法（考虑多线程扩展性）: 将hyperVertex转化为hyperGraph
    详细算法流程:

 */
void minWRollback::buileGraph() {

}

/* 
  确定性回滚: 从hyperGraph中找到最小的回滚子事务集合
*/
void minWRollback::rollback() {

}