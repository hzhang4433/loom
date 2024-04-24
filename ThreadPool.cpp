#include "ThreadPool.h"

// 构造函数，创建指定数量的线程并让它们等待任务
ThreadPool::ThreadPool(size_t threadNum) : stop(false) {
    // 创建指定数量的线程
    for(size_t i = 0; i < threadNum; ++i)
        // 将新创建的线程添加到线程池中
        workers.emplace_back(
            [this] {
                for(;;) { // 每个线程会无限循环这个函数，直到线程被停止
                    // 用于存储要从任务队列中取出的任务
                    std::function<void()> task;

                    {
                        // 锁住任务队列
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        // 如果任务队列为空，就等待
                        this->condition.wait(lock, [this]{ return this->stop || !this->tasks.empty(); });
                        // 如果收到停止信号，并且任务队列为空，就退出循环
                        if(this->stop && this->tasks.empty())
                            return;
                        // 从任务队列中取出一个任务
                        task = std::move(this->tasks.front());
                        // 删除已经取出的任务
                        this->tasks.pop();
                    }
                    // 执行任务
                    task();
                }
            }
        );
}

// 析构函数，停止所有线程并等待它们完成任务
ThreadPool::~ThreadPool() {
    {
        // 锁住任务队列
        std::unique_lock<std::mutex> lock(queue_mutex);
        // 发出停止信号
        stop = true;
    }
    // 唤醒所有等待的线程
    condition.notify_all();
    // 等待线程完成
    for(std::thread &worker: workers)
        worker.join();
}

// 添加任务到任务队列
std::future<void> ThreadPool::enqueue(std::function<void()> task) {
    // 创建一个packaged_task，它包装了你的任务
    auto packaged_task = std::make_shared<std::packaged_task<void()>>(std::move(task));
    // 获取与packaged_task关联的future
    auto future = packaged_task->get_future();
    {
        // 锁住任务队列
        std::unique_lock<std::mutex> lock(queue_mutex);
        // 如果收到停止信号，就抛出异常
        if(stop) throw std::runtime_error("enqueue on stopped ThreadPool");
        // 将任务添加到任务队列
        tasks.push([packaged_task](){ (*packaged_task)();});
    }
    // 唤醒一个等待的线程
    condition.notify_one();
    return future;
}