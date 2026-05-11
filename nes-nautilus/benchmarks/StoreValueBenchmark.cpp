/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <benchmark/benchmark.h>
#include <memory>
#include <string>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/InlineTupleBufferRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Runtime/StringEntry.hpp>

#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>

namespace NES {

static void BM_WriteRecord_Flink(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    RecordBuffer recordBuffer{std::addressof(tupleBuffer)};

    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::FLINK));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::ROW_LAYOUT);

    std::string testStr(state.range(0), 'A');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal inputString = VariableSizedData(strPtr, strSize);

    Record record;
    record.write("test_string", inputString);

    nautilus::val<uint64_t> recordOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();

    for (auto _ : state) {
        tupleBufferRef->writeRecord(recordOffset, recordBuffer, record, providerPtr);

        if (tupleBuffer.getMemSize() + state.range(0) + sizeof(VariableSizedAccess) > tupleBuffer.getBufferSize()) {
            state.PauseTiming();
            tupleBuffer = bufferManager->getBufferBlocking();
            recordBuffer = RecordBuffer{std::addressof(tupleBuffer)};
            recordOffset = 0;
            state.ResumeTiming();
        }
    }
}

static void BM_WriteRecord_GermanVarsized(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    RecordBuffer recordBuffer{std::addressof(tupleBuffer)};

    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::GERMAN_VARSIZED));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::ROW_LAYOUT);

    std::string testStr(state.range(0), 'B');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal inputString = VariableSizedData(strPtr, strSize);

    Record record;
    record.write("test_string", inputString);

    nautilus::val<uint64_t> recordOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();

    for (auto _ : state) {
        tupleBufferRef->writeRecord(recordOffset, recordBuffer, record, providerPtr);

        if (tupleBuffer.getMemSize() + state.range(0) + sizeof(StringEntry) > tupleBuffer.getBufferSize()) {
            state.PauseTiming();
            tupleBuffer = bufferManager->getBufferBlocking();
            recordBuffer = RecordBuffer{std::addressof(tupleBuffer)};
            recordOffset = 0;
            state.ResumeTiming();
        }
    }
}

static void BM_ReadRecord_Flink(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    RecordBuffer recordBuffer{std::addressof(tupleBuffer)};

    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::FLINK));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::ROW_LAYOUT);

    std::string testStr(state.range(0), 'A');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal inputString = VariableSizedData(strPtr, strSize);

    Record writeRec;
    writeRec.write("test_string", inputString);
    nautilus::val<uint64_t> recordOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();

    // Pre-populate buffer with a single record
    tupleBufferRef->writeRecord(recordOffset, recordBuffer, writeRec, providerPtr);

    std::vector<Record::RecordFieldIdentifier> projections = {{"test_string"}};
    nautilus::val<uint64_t> recordIndexToRead = 0;

    for (auto _ : state) {
        Record readRec = tupleBufferRef->readRecord(projections, recordBuffer, recordIndexToRead);
        auto varVal = readRec.read("test_string");
        benchmark::DoNotOptimize(varVal);
    }
}

static void BM_ReadRecord_GermanVarsized(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    RecordBuffer recordBuffer{std::addressof(tupleBuffer)};

    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::GERMAN_VARSIZED));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::ROW_LAYOUT);

    std::string testStr(state.range(0), 'B');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal inputString = VariableSizedData(strPtr, strSize);

    Record writeRec;
    writeRec.write("test_string", inputString);
    nautilus::val<uint64_t> recordOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();

    // Pre-populate buffer with a single record
    tupleBufferRef->writeRecord(recordOffset, recordBuffer, writeRec, providerPtr);

    std::vector<Record::RecordFieldIdentifier> projections = {{"test_string"}};
    nautilus::val<uint64_t> recordIndexToRead = 0;

    for (auto _ : state) {
        Record readRec = tupleBufferRef->readRecord(projections, recordBuffer, recordIndexToRead);
        auto varVal = readRec.read("test_string");
        benchmark::DoNotOptimize(varVal);
    }
}

static void BM_IdentityPipeline_Flink(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto sourceBuffer = bufferManager->getBufferBlocking();
    auto resultBuffer = bufferManager->getBufferBlocking();
    RecordBuffer sourceRecordBuffer{std::addressof(sourceBuffer)};
    RecordBuffer resultRecordBuffer{std::addressof(resultBuffer)};
    
    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::FLINK));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::STRINGS_INLINE);
    
    std::string testStr(state.range(0), 'A');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal externalString = VariableSizedData(strPtr, strSize);
    
    Record externalRecord;
    externalRecord.write("test_string", externalString);
    
    nautilus::val<uint64_t> dummyOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();
    std::vector<Record::RecordFieldIdentifier> projections = {{"test_string"}};

    for (auto _ : state) {
        // 1. Source Write (External to SourceBuffer)
        nautilus::val<uint64_t> sourceOffset = sourceBuffer.getMemSize();
        tupleBufferRef->writeRecord(dummyOffset, sourceRecordBuffer, externalRecord, providerPtr);
        
        // 2. Query Read (SourceBuffer to Query)
        Record queryRecord = tupleBufferRef->readRecord(projections, sourceRecordBuffer, sourceOffset);
        
        // 3. Query Write (Query to ResultBuffer)
        nautilus::val<uint64_t> resultOffset = resultBuffer.getMemSize();
        tupleBufferRef->writeRecord(dummyOffset, resultRecordBuffer, queryRecord, providerPtr);
        
        // 4. Sink Read (ResultBuffer to External)
        Record sinkRecord = tupleBufferRef->readRecord(projections, resultRecordBuffer, resultOffset);
        auto varVal = sinkRecord.read("test_string");
        benchmark::DoNotOptimize(varVal);
        
        // Manage State
        if (sourceBuffer.getMemSize() + state.range(0) + sizeof(VariableSizedAccess) > sourceBuffer.getBufferSize() ||
            resultBuffer.getMemSize() + state.range(0) + sizeof(VariableSizedAccess) > resultBuffer.getBufferSize()) {
            state.PauseTiming();
            sourceBuffer = bufferManager->getBufferBlocking();
            sourceRecordBuffer = RecordBuffer{std::addressof(sourceBuffer)};
            resultBuffer = bufferManager->getBufferBlocking();
            resultRecordBuffer = RecordBuffer{std::addressof(resultBuffer)};
            state.ResumeTiming();
        }
    }
}

static void BM_IdentityPipeline_GermanVarsized(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto sourceBuffer = bufferManager->getBufferBlocking();
    auto resultBuffer = bufferManager->getBufferBlocking();
    RecordBuffer sourceRecordBuffer{std::addressof(sourceBuffer)};
    RecordBuffer resultRecordBuffer{std::addressof(resultBuffer)};
    
    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::GERMAN_VARSIZED));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::ROW_LAYOUT);
    
    std::string testStr(state.range(0), 'B');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal externalString = VariableSizedData(strPtr, strSize);
    
    Record externalRecord;
    externalRecord.write("test_string", externalString);
    
    nautilus::val<uint64_t> dummyOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();
    std::vector<Record::RecordFieldIdentifier> projections = {{"test_string"}};

    for (auto _ : state) {
        // 1. Source Write (External to SourceBuffer)
        nautilus::val<uint64_t> sourceOffset = sourceBuffer.getMemSize();
        tupleBufferRef->writeRecord(dummyOffset, sourceRecordBuffer, externalRecord, providerPtr);
        
        // 2. Query Read (SourceBuffer to Query)
        Record queryRecord = tupleBufferRef->readRecord(projections, sourceRecordBuffer, sourceOffset);
        
        // 3. Query Write (Query to ResultBuffer)
        nautilus::val<uint64_t> resultOffset = resultBuffer.getMemSize();
        tupleBufferRef->writeRecord(dummyOffset, resultRecordBuffer, queryRecord, providerPtr);
        
        // 4. Sink Read (ResultBuffer to External)
        Record sinkRecord = tupleBufferRef->readRecord(projections, resultRecordBuffer, resultOffset);
        auto varVal = sinkRecord.read("test_string");
        benchmark::DoNotOptimize(varVal);
        
        // Manage State
        if (sourceBuffer.getMemSize() + state.range(0) + sizeof(StringEntry) > sourceBuffer.getBufferSize() ||
            resultBuffer.getMemSize() + state.range(0) + sizeof(StringEntry) > resultBuffer.getBufferSize()) {
            state.PauseTiming();
            sourceBuffer = bufferManager->getBufferBlocking();
            sourceRecordBuffer = RecordBuffer{std::addressof(sourceBuffer)};
            resultBuffer = bufferManager->getBufferBlocking();
            resultRecordBuffer = RecordBuffer{std::addressof(resultBuffer)};
            state.ResumeTiming();
        }
    }
}

static void BM_WriteRecord_Varsized(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    RecordBuffer recordBuffer{std::addressof(tupleBuffer)};
    
    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::ROW_LAYOUT);
    
    std::string testStr(state.range(0), 'C');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal inputString = VariableSizedData(strPtr, strSize);
    
    Record record;
    record.write("test_string", inputString);
    
    nautilus::val<uint64_t> recordOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();

    for (auto _ : state) {
        tupleBufferRef->writeRecord(recordOffset, recordBuffer, record, providerPtr);
        
        if (tupleBuffer.getMemSize() + state.range(0) + sizeof(VariableSizedAccess) > tupleBuffer.getBufferSize()) {
            state.PauseTiming();
            tupleBuffer = bufferManager->getBufferBlocking();
            recordBuffer = RecordBuffer{std::addressof(tupleBuffer)};
            recordOffset = 0;
            state.ResumeTiming();
        }
    }
}

static void BM_ReadRecord_Varsized(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    RecordBuffer recordBuffer{std::addressof(tupleBuffer)};
    
    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::ROW_LAYOUT);
    
    std::string testStr(state.range(0), 'C');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal inputString = VariableSizedData(strPtr, strSize);
    
    Record writeRec;
    writeRec.write("test_string", inputString);
    nautilus::val<uint64_t> recordOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();
    
    tupleBufferRef->writeRecord(recordOffset, recordBuffer, writeRec, providerPtr);

    std::vector<Record::RecordFieldIdentifier> projections = {{"test_string"}};
    nautilus::val<uint64_t> recordIndexToRead = 0;

    for (auto _ : state) {
        Record readRec = tupleBufferRef->readRecord(projections, recordBuffer, recordIndexToRead);
        auto varVal = readRec.read("test_string");
        benchmark::DoNotOptimize(varVal);
    }
}

static void BM_IdentityPipeline_Varsized(benchmark::State& state) {
    auto bufferManager = BufferManager::create(4096, 1000);
    auto sourceBuffer = bufferManager->getBufferBlocking();
    auto resultBuffer = bufferManager->getBufferBlocking();
    RecordBuffer sourceRecordBuffer{std::addressof(sourceBuffer)};
    RecordBuffer resultRecordBuffer{std::addressof(resultBuffer)};
    
    auto schema = Schema{}.addField("test_string", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(4096, schema, MemoryLayoutType::ROW_LAYOUT);
    
    std::string testStr(state.range(0), 'C');
    nautilus::val<int8_t*> strPtr{reinterpret_cast<int8_t*>(testStr.data())};
    nautilus::val<uint64_t> strSize{testStr.size()};
    VarVal externalString = VariableSizedData(strPtr, strSize);
    
    Record externalRecord;
    externalRecord.write("test_string", externalString);
    
    nautilus::val<uint64_t> dummyOffset = 0;
    nautilus::val<AbstractBufferProvider*> providerPtr = bufferManager.get();
    std::vector<Record::RecordFieldIdentifier> projections = {{"test_string"}};

    for (auto _ : state) {
        nautilus::val<uint64_t> sourceOffset = sourceBuffer.getMemSize();
        tupleBufferRef->writeRecord(dummyOffset, sourceRecordBuffer, externalRecord, providerPtr);
        
        Record queryRecord = tupleBufferRef->readRecord(projections, sourceRecordBuffer, sourceOffset);
        
        nautilus::val<uint64_t> resultOffset = resultBuffer.getMemSize();
        tupleBufferRef->writeRecord(dummyOffset, resultRecordBuffer, queryRecord, providerPtr);
        
        Record sinkRecord = tupleBufferRef->readRecord(projections, resultRecordBuffer, resultOffset);
        auto varVal = sinkRecord.read("test_string");
        benchmark::DoNotOptimize(varVal);
        
        if (sourceBuffer.getMemSize() + state.range(0) + sizeof(VariableSizedAccess) > sourceBuffer.getBufferSize() ||
            resultBuffer.getMemSize() + state.range(0) + sizeof(VariableSizedAccess) > resultBuffer.getBufferSize()) {
            state.PauseTiming();
            sourceBuffer = bufferManager->getBufferBlocking();
            sourceRecordBuffer = RecordBuffer{std::addressof(sourceBuffer)};
            resultBuffer = bufferManager->getBufferBlocking();
            resultRecordBuffer = RecordBuffer{std::addressof(resultBuffer)};
            state.ResumeTiming();
        }
    }
}

BENCHMARK(BM_WriteRecord_Flink)->RangeMultiplier(4)->Range(4, 256);
BENCHMARK(BM_WriteRecord_GermanVarsized)->RangeMultiplier(4)->Range(4, 256);
BENCHMARK(BM_ReadRecord_Flink)->RangeMultiplier(4)->Range(4, 256);
BENCHMARK(BM_ReadRecord_GermanVarsized)->RangeMultiplier(4)->Range(4, 256);
BENCHMARK(BM_IdentityPipeline_Flink)->RangeMultiplier(4)->Range(4, 256);
BENCHMARK(BM_IdentityPipeline_GermanVarsized)->RangeMultiplier(4)->Range(4, 256);

BENCHMARK(BM_WriteRecord_Varsized)->RangeMultiplier(4)->Range(4, 256);
BENCHMARK(BM_ReadRecord_Varsized)->RangeMultiplier(4)->Range(4, 256);
BENCHMARK(BM_IdentityPipeline_Varsized)->RangeMultiplier(4)->Range(4, 256);

} // namespace NES

BENCHMARK_MAIN();
