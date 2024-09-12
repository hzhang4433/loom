#include <loom/protocol/common.h>
#include <loom/protocol/aria/Aria.h>
#include <loom/protocol/harmony/Harmony.h>
#include <loom/protocol/moss/Moss.h>
#include <loom/protocol/loom/Loom.h>
#include <loom/protocol/serial/Serial.h>
#include <loom/workload/tpcc/Workload.hpp>
#include <loom/utils/UMacros.hpp>
#include <loom/utils/Generator/UTxGenerator.h>
#include <ranges>
#include <iostream>
#include <fmt/core.h>

// expanding macros to assignments and use them later
#define ASSGIN_ARGS_HELPER(X, ...) __VA_OPT__(auto NAME(__VA_ARGS__) = (X);)
#define FILLIN_ARGS_HELPER(X, ...) __VA_OPT__(, NAME(__VA_ARGS__))
#define ASSGIN_ARGS(...)           FOR_EACH(ASSGIN_ARGS_HELPER, __VA_ARGS__, _)
#define FILLIN_ARGS(X, ...)        NAME(__VA_ARGS__, _) FOR_EACH(FILLIN_ARGS_HELPER, __VA_ARGS__, _)

// throw error
#define THROW(...)   throw std::runtime_error(std::string{fmt::format(__VA_ARGS__)})

// declare some helper macros for argument parser (by default, we name the token iterator by 'iter')
#define INT     to<size_t>  (*++iter)
#define DOUBLE  to<double>  (*++iter)
#define BOOL    to<bool>    (*++iter)

using namespace loom;
using namespace std::chrono_literals;
using namespace std::chrono;

static auto split(std::basic_string_view<char> s) {
    auto iter = s | std::ranges::views::split(':')
    | std::ranges::views::transform([](auto&& str) { return std::string_view(&*str.begin(), std::ranges::distance(str)); });
    auto toks = std::vector<std::string>();
    for (auto x: iter) {
        toks.push_back(std::string{x});
    }
    return toks;
}

template<typename T>
static T to(std::basic_string_view<char> s) {
    std::stringstream sstream(std::string{s});
    T result; sstream >> result;
    return result;
}

template<>
bool to<bool>(std::basic_string_view<char> s) {
    if (s == "TRUE")    { return true; }
    if (s == "FALSE")   { return false; }
    THROW("cannot recognize ({}) as boolean should be either TRUE or FALSE", s);
}

template<>
milliseconds to<milliseconds>(std::basic_string_view<char> s) {
    std::stringstream is(std::string{s});
	static const std::unordered_map<std::string, milliseconds> suffix {
        {"ms", 1ms}, {"s", 1s}, {"m", 1min}, {"h", 1h}};
    unsigned n {};
    std::string unit;
    if (is >> n >> unit) {
        try {
            return duration_cast<milliseconds>(n * suffix.at(unit));
        } catch (const std::out_of_range&) {
            std::cerr << "ERROR: Invalid unit specified\n";
        }
    } else {
        std::cerr << "ERROR: Could not convert to numeric value\n";
    }
	return milliseconds{};
}

inline std::vector<Block::Ptr> ParseWorkload(const char* arg) {
    auto args = split(arg);
    auto name = *args.begin();
    auto iter = args.begin();
    TPCC::N_WAREHOUSES = INT;
    loom::BLOCK_SIZE = INT;
    auto num_blocks = INT;
    auto is_nest = BOOL;
    // Generate a workload
    TxGenerator txGenerator(loom::BLOCK_SIZE * num_blocks);
    LOG(INFO) << "Generating workload with " << num_blocks << " blocks of size " << loom::BLOCK_SIZE << " and " << TPCC::N_WAREHOUSES << " warehouses";
    return txGenerator.generateWorkload(is_nest);
}

inline std::unique_ptr<Protocol> ParseProtocol(const char* arg, vector<Block::Ptr>& workload, Statistics& statistics) {
    auto args = split(arg);
    auto name = *args.begin();
    auto dist = (size_t) (std::distance(args.begin(), args.end()) - 1);
    auto iter = args.begin();
    // map each option to an argparser
    #define OPT(X, Y...) if (name == #X) { \
        auto n = (size_t) COUNT(Y);        \
        ASSGIN_ARGS(Y);                    \
        if (dist != n) THROW("protocol {} has {} args -- ({}), but we found {} args", #X, n, #Y, dist); \
        return static_cast<std::unique_ptr<Protocol>>(std::make_unique<X>(workload, statistics, FILLIN_ARGS(Y)));  \
    };
    OPT(Serial,      INT, INT)
    OPT(Aria,        INT, INT, BOOL)
    OPT(Harmony,     INT, INT, BOOL)
    OPT(Moss,        INT, INT)
    OPT(Loom,        INT, INT, BOOL, BOOL)
    #undef OPT
    // fallback to an error
    THROW("unknown protocol option ({})", std::string{name});
}

// remove helper macros
#undef INT
#undef BOOL
#undef DOUBLE
#undef EVMTYPE
#undef FILLIN_ARGS
#undef FILLIN_ARGS_HELPER
#undef ASSGIN_ARGS
#undef ASSGIN_ARGS_HELPER
#undef THROW
