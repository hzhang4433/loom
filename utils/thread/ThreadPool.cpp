#include "ThreadPool.h"
#include <pthread.h>
#include <sstream>
#include <iostream>
#include <glog/logging.h>

void ThreadPool::PinRoundRobin(std::thread& thread, unsigned rotate_id) {
    auto core_id = rotate_id % std::thread::hardware_concurrency(); // 获取核心ID
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(core_id, &cpu_set);
    auto rc = pthread_setaffinity_np(
        thread.native_handle(),   // 获取线程的本地句柄
        sizeof(cpu_set_t), &cpu_set
    );
    if (rc != 0) {
        std::stringstream ss;
        ss << "cannot pin thread-" << thread.get_id()
           << " to core " << core_id;
        throw std::runtime_error(ss.str());
    }
}

void ThreadPool::PinRoundRobin(pthread_t& thread, unsigned rotate_id) {
    auto core_id = rotate_id % std::thread::hardware_concurrency(); // 获取核心ID
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(core_id, &cpu_set);
    auto rc = pthread_setaffinity_np(
        thread,   // 获取线程的本地句柄
        sizeof(cpu_set_t), &cpu_set
    );
    if (rc != 0) {
        std::stringstream ss;
        ss << "cannot pin thread to core" << core_id;
        throw std::runtime_error(ss.str());
    }
}

void ThreadPool::PinRoundRobin(std::jthread& thread, unsigned rotate_id) {
    auto core_id    = rotate_id % std::thread::hardware_concurrency();
    cpu_set_t   cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET (core_id, &cpu_set);
    auto rc = pthread_setaffinity_np(
        thread.native_handle(),
        sizeof(cpu_set_t), &cpu_set
    );
    if (rc != 0) {
        std::stringstream ss;
        ss << "cannot pin thread-" << thread.get_id()
           << " to core "          << core_id;       
        throw std::runtime_error(ss.str());
    }
}

/* 构造函数，创建指定数量的线程并让它们等待任务 - orig */
ThreadPool::ThreadPool(size_t threadNum) : stop(false), threadDurations(threadNum), threadNum(threadNum), taskCounts(threadNum, 0) {
    // 创建指定数量的线程
    for(size_t i = 0; i < threadNum; ++i) {
        // 将新创建的线程添加到线程池中
        workers.emplace_back(
            [this, i] {
                for(;;) { // 每个线程会无限循环这个函数，直到线程被停止
                    // 用于存储要从任务队列中取出的任务
                    std::function<void()> task;
                    // Task task;
                    {
                        // 锁住任务队列
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        // 如果任务队列为空，就等待
                        // this->condition.wait(lock, [this]{ return this->stop || !this->tasks.empty(); });
                        condition.wait(lock, [this] { return stop || !highPriorityTasks.empty() || !lowPriorityTasks.empty(); });
                        // 如果收到停止信号，并且任务队列为空，就退出循环
                        // if(this->stop && this->tasks.empty()) return;
                        if(this->stop && highPriorityTasks.empty() && lowPriorityTasks.empty()) return;
                        // 从任务队列中取出一个任务
                        // task = std::move(this->tasks.front());
                        // task = std::move(this->tasks.top());
                        if (!highPriorityTasks.empty()) {
                            // LOG(INFO) << "Task priority: HIGH";
                            task = highPriorityTasks.front();
                            highPriorityTasks.pop();
                        } else {
                            // LOG(INFO) << "Task priority: LOW";
                            task = lowPriorityTasks.front();
                            lowPriorityTasks.pop();
                        }
                        // 删除已经取出的任务
                        // this->tasks.pop();
                    }
                    // 后面可以用来统计线程利用率？
                    // auto start = std::chrono::high_resolution_clock::now();
                    // execute task
                    // LOG(INFO) << "Task priority: " << loom::taskPriorityToString(task.priority);
                    task();
                    // task.func();
                    // auto end = std::chrono::high_resolution_clock::now();
                    // threadDurations[i] += std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // taskCounts[i] += 1; // 更新任务计数
                }
            }
        );
        // 绑定线程到核心
        PinRoundRobin(workers[i], i);
    }
}

// 析构函数，停止所有线程并等待它们完成任务
ThreadPool::~ThreadPool() {
    shutdown();
}

/* 添加任务到任务队列
std::future<void> ThreadPool::enqueue(std::function<void()> task, loom::TaskPriority priority) {
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
        tasks.push({priority, [packaged_task](){ (*packaged_task)();}});
    }
    // 唤醒一个等待的线程
    condition.notify_one();
    return future;
}

std::future<void> ThreadPool::enqueue(std::function<void()> task) {
    // // 创建一个packaged_task，它包装了你的任务
    // auto packaged_task = std::make_shared<std::packaged_task<void()>>(std::move(task));
    // // 获取与packaged_task关联的future
    // auto future = packaged_task->get_future();
    // {
    //     // 锁住任务队列
    //     std::unique_lock<std::mutex> lock(queue_mutex);
    //     // 如果收到停止信号，就抛出异常
    //     if(stop) throw std::runtime_error("enqueue on stopped ThreadPool");
    //     // 将任务添加到任务队列
    //     tasks.push([packaged_task](){ (*packaged_task)();});
    // }
    // // 唤醒一个等待的线程
    // condition.notify_one();
    // return future;
}
*/

/// @brief shutdown the thread pool
void ThreadPool::shutdown() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for(std::thread &worker: workers) {
        if(worker.joinable()) { worker.join(); }
    }
}

const std::vector<std::chrono::microseconds>& ThreadPool::getThreadDurations() const {
    return threadDurations;
}

const int ThreadPool::getThreadNum() const {
    return threadNum;
}

const std::vector<size_t>& ThreadPool::getTaskCounts() const {
    return taskCounts;
}