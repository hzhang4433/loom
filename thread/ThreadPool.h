#pragma once

#include <memory>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>

class ThreadPool : public std::enable_shared_from_this<ThreadPool>
{
    public:
        typedef std::shared_ptr <ThreadPool> Ptr;

        ThreadPool(size_t threadNum);
        
        ~ThreadPool();

        std::future<void> enqueue(std::function<void()> task);

        std::vector<std::future<void>> BG_futures; // 并发构建时空图的future

    private:
        std::vector<std::thread> workers;
        std::queue<std::function<void()>> tasks;

        std::mutex queue_mutex;
        std::condition_variable condition;
        bool stop;
};