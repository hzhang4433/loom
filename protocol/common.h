#pragma once

#include <tbb/tbb.h>
#include <atomic>

namespace protocol {

    // 判断两个set是否有交集
    template <typename T, typename Hash>
    bool hasIntersection(const tbb::concurrent_unordered_set<T, Hash>& set1, const tbb::concurrent_unordered_set<T, Hash>& set2) {
        // for (const auto& item : set1) {
        //     if (set2.find(item) != set2.end()) {
        //         return true;
        //     }
        // }
        // return false;

        std::atomic<bool> found(false);

        tbb::parallel_for(tbb::blocked_range<size_t>(0, set2.size()), [&](const tbb::blocked_range<size_t>& r) {
            for (size_t i = r.begin(); i != r.end() && !found; ++i) {
                auto it = set2.begin();
                std::advance(it, i);
                if (set1.find(*it) != set1.end()) {
                    found = true;
                }
            }
        });

        return found;
    }

    // 判断两个set是否有交集-无hash版（读写集是否冲突）
    template <typename T>
    bool hasConflict(tbb::concurrent_unordered_set<T>& set1, tbb::concurrent_unordered_set<T>& set2) {
        for (auto item : set2) {
            if (set1.find(item) != set1.end()) {
                return true;
            }
        }
        return false;
        
        /* 并行化冲突检测，带测试，对比效率 
            std::atomic<bool> found(false);

            tbb::parallel_for(tbb::blocked_range<size_t>(0, set2.size()), [&](const tbb::blocked_range<size_t>& r) {
                for (size_t i = r.begin(); i != r.end() && !found; ++i) {
                    auto it = set2.begin();
                    std::advance(it, i);
                    if (set1.find(*it) != set1.end()) {
                        found = true;
                    }
                }
            });

            return found;
        */ 
    }

    // 判断set1是否包含set2
    template <typename T, typename Hash>
    bool hasContain(const tbb::concurrent_unordered_set<T, Hash>& set1, const tbb::concurrent_unordered_set<T, Hash>& set2) {
        if (set1.size() < set2.size()) {
            return false;
        }

        for (const auto& item : set2) {
            if (set1.find(item) == set1.end()) {
                return false;
            }
        }
        return true;
    }

    // 判断set1是否包含map2的key
    template <typename T, typename Hash, typename U>
    bool hasContain(const tbb::concurrent_unordered_set<T, Hash>& set1, const tbb::concurrent_unordered_map<T, U, Hash>& map2) {
        if (set1.size() < map2.size()) {
            return false;
        }

        for (const auto& pair : map2) {
            if (set1.find(pair.first) == set1.end()) {
                return false;
            }
        }
        return true;
    }

    // 比较两个set是否相等
    template <typename T, typename Hash>
    bool areEqual(const tbb::concurrent_unordered_set<T, Hash>& set1,
                const tbb::concurrent_unordered_set<T, Hash>& set2) {
        if (set1.size() != set2.size()) {
            return false;
        }

        tbb::atomic<bool> result = true;

        tbb::parallel_for_each(set1.begin(), set1.end(),
            [&](const T& vertex) {
                if (set2.find(vertex) == set2.end()) {
                    result = false;
                }
            }
        );

        return result;
    }

    // 计算两个集合的差集
    template <typename T, typename Hash>
    tbb::concurrent_unordered_set<T, Hash> diff(const tbb::concurrent_unordered_set<T, Hash>& cascadeVertices, const tbb::concurrent_unordered_set<T, Hash>& rollbackVertex) {
        tbb::concurrent_unordered_set<T, Hash> diff;
        
        tbb::parallel_for(tbb::blocked_range<size_t>(0, cascadeVertices.size()), [&](const tbb::blocked_range<size_t>& r) {
            for (size_t i = r.begin(); i != r.end(); ++i) {
                auto it = cascadeVertices.begin();
                std::advance(it, i);
                if (rollbackVertex.find(*it) == rollbackVertex.end()) {
                    diff.insert(*it);
                }
            }
        });
        return diff;
    }
}