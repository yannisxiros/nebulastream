#include <benchmark/benchmark.h>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <random>

namespace NES {
    struct StringEntry {
        uint32_t size;
        std::byte prefix[4];
        int8_t* ptr;
        std::byte extrabuf[4];
    };
    static constexpr size_t inlineBufSize = sizeof(StringEntry)-sizeof(uint32_t); 

    struct VariableSizedAccess {
        uint32_t index;
        uint32_t offset;
        uint64_t size;
    };
}

static void BM_FlinkWrite(benchmark::State& state) {
    std::string testString = "This is a long string that will not be inlined into the buffer.";
    std::vector<int8_t> tupleBuffer(1024);
    int8_t* fieldAddress = tupleBuffer.data();

    // Mock record buffer 
    uint32_t memSize = 0;
    uint32_t runningSize = 16;
    uint32_t capacity = 10000;

    for (auto _ : state) {
        if (runningSize + memSize + testString.size() <= capacity) {
            std::memcpy(fieldAddress + runningSize, testString.data(), testString.size());
            *reinterpret_cast<uint64_t*>(fieldAddress + offsetof(NES::VariableSizedAccess, size)) = testString.size();
            *reinterpret_cast<uint32_t*>(fieldAddress + offsetof(NES::VariableSizedAccess, index)) = -1U;
            *reinterpret_cast<uint32_t*>(fieldAddress + offsetof(NES::VariableSizedAccess, offset)) = memSize + runningSize;
            runningSize += testString.size();
        }
    }
}
BENCHMARK(BM_FlinkWrite);

static void BM_GermanVarsizedWrite(benchmark::State& state) {
    std::string testString = "This is a long string that will not be inlined into the buffer.";
    std::vector<int8_t> tupleBuffer(1024);
    int8_t* fieldAddress = tupleBuffer.data();

    for (auto _ : state) {
        auto refToIndex = reinterpret_cast<NES::StringEntry*>(fieldAddress);
        refToIndex->size = testString.size();
        
        std::memcpy(refToIndex->prefix, testString.data(), NES::inlineBufSize);

        if (testString.size() > NES::inlineBufSize) {
            // Mock out of line allocation/write
            benchmark::DoNotOptimize(testString.size());
            // refToIndex->ptr = ...
        }
    }
}
BENCHMARK(BM_GermanVarsizedWrite);

BENCHMARK_MAIN();
