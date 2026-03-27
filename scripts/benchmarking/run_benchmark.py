#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Python script that runs the below systest files for different worker configurations
"""

import argparse
import ast
import subprocess
import json
import os
import csv
import shutil
import itertools
import socket
import re

from utils import *
from systest_utils import check_generate_systest


#### Benchmark Configurations
build_dir = os.path.join(".", get_build_dir())
working_dir = os.path.join(build_dir, "working_dir")
csv_file_path = "results_nebulastream.csv"
benchmark_json_file = os.path.abspath(os.path.join(working_dir, "BenchmarkResults.json"))
systest_executable = os.path.join(build_dir, "nes-systests/systest/systest")
test_data_dir = "nes-systests/testdata"
NUM_RUNS_PER_EXPERIMENT = 1

#### Worker Configurations
allExecutionModes = ["COMPILER"]  # ["COMPILER", "INTERPRETER"]
allNumberOfWorkerThreads = ['1']# '4']  #['1', '4', '8', '16', '24'] #['4', '16']
allJoinStrategies = ["HASH_JOIN"]
allStringTypes = ["VARSIZED", "GERMAN_VARSIZED" , "FLINK"]
# allStringTypes = [ "GERMAN_VARSIZED"]
allPageSizes = [8192]
allBufferConfigs = [
    (8192, 1500000),    # 8KB buffers: ~11.4GB
    (65536, 187500),    # 64KB buffers: ~11.4GB
    (131072, 93750),    # 128KB buffers: ~11.4GB
    # (262144, 46875),    # 256KB buffers: ~11.4GB
]

fieldnames =  [
    'query name', 'stringType', 'time', 'tuplesPerSecond', 'bytesPerSecond' ,'avg_used_buffers', 'max_used_buffers',
    'numberOfWorkerThreads', 'buffersInGlobalBufferManager',
    'bufferSizeInBytes'
]

#### Queries
queries = {
    # "allshort": "nes-systests/benchmark/micro.test:01",
    # "mixup": "nes-systests/benchmark/micro.test:02",
    # "alllong": "nes-systests/benchmark/micro.test:03",
    # "various": "nes-systests/benchmark/micro.test:04",
    "midway": "nes-systests/benchmark/micro.test:05",
    # "ZOO1": "nes-systests/benchmark/Zookeeper.test:01",
    # "ZOO2": "nes-systests/benchmark/Zookeeper.test:02",
    # "ZOO3": "nes-systests/benchmark/Zookeeper.test:03",
    # "AOL1": "nes-systests/benchmark/AOL.test:01",
    # "AOL2": "nes-systests/benchmark/AOL.test:02",
    # "AOL3": "nes-systests/benchmark/AOL.test:03",
    # "NM8": "nes-systests/benchmark/Nexmark_multiple_GB_of_Bids.test:05",
    # "YSB": "nes-systests/benchmark/YahooStreamingBenchmark.test:02",
    # "YSB10k": "nes-systests/benchmark/YahooStreamingBenchmark_more_data.test:02"
}

def initialize_csv_file():
    """Initialize the CSV file with headers."""
    print("Initializing CSV file...")
    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        print("CSV file initialized with headers.")

def parse_buffer_usage_from_listener(console_output):
    # Regular expression to parse each log line from BufferManagerListener
    log_pattern = re.compile(
       r'BufferManager is currently using (\d+) out of (\d+) buffers, unpooled: (\d+)'
    )

    # List to store the extracted data
    used_buffers_list = []
    for line in console_output.split('\n'):
        # Use regex to find matches in the log line
        match = log_pattern.match(line)
        if match:
            used_buffers = int(match.group(1))
            used_buffers_list.append(used_buffers)

    # Calculate average and max of the used buffers
    if len(used_buffers_list) == 0:
        return -1, -1
        
    avg_used_buffers = sum(used_buffers_list) / len(used_buffers_list)
    max_used_buffers = max(used_buffers_list)
    
    return avg_used_buffers, max_used_buffers

def run_benchmark(config, stringType, query, queryIdx, workerConfigIdx, no_combinations, no_queries):
    # Create the working directory
    create_folder_and_remove_if_exists(working_dir)
    try:
        # Extract configurations from the config dictionary
        numberOfWorkerThreads = config['numberOfWorkerThreads']
        executionMode = config['executionMode']
        buffersInGlobalBufferManager = config['buffersInGlobalBufferManager']
        bufferSizeInBytes = config['bufferSizeInBytes']
        pageSize = config['pageSize']

        # Running the query with a particular worker configuration@
        
        worker_config = (f"--worker.query_engine.number_of_worker_threads={numberOfWorkerThreads} "
                 f"--worker.default_query_execution.execution_mode={executionMode} "
                 f"--worker.default_query_execution.page_size={pageSize} "
                 f"--worker.default_query_execution.operator_buffer_size={bufferSizeInBytes} "
                 f"--worker.number_of_buffers_in_global_buffer_manager={buffersInGlobalBufferManager} ")
        
        base_query_path, systest_num = queries[query].split(":", 1)
        base_filename = os.path.basename(base_query_path)
        name_part = re.match(r"([^\.]+)", base_filename).group(1)
        this_query = f"nes-systests/benchmark/strings/{name_part}/{name_part}_{stringType}.test:{systest_num}"

        benchmark_command = f"{systest_executable} -b -t {os.path.abspath(this_query)} --data {test_data_dir} --workingDir={working_dir} -- {worker_config}"

        print(
            f"Running {query} [{queryIdx}/{no_queries}] for worker configuration [{workerConfigIdx}/{no_combinations}]...")
        stdout = run_command(benchmark_command)

        # Parse and save benchmark results
        with open(benchmark_json_file, 'r') as file:
            content = file.read()
            benchmark_results = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Failed to parse benchmark output as JSON from {benchmark_json_file}")
        print(f"Error details: {e}")
        benchmark_results = []
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        benchmark_results = []
        exit(1)

    with open(csv_file_path, mode='a', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        avg_used_buffers, max_used_buffers = parse_buffer_usage_from_listener(stdout)

        for result in benchmark_results:
            merged = {**result, **config}
            merged['query name'] = query
            merged['avg_used_buffers'] = avg_used_buffers
            merged['max_used_buffers'] = max_used_buffers
            # Keep only fields that appear in `fieldnames`
            row = {k: merged.get(k, '') for k in fieldnames}
            writer.writerow(row)
    # with open(csv_file_path, mode='a', newline='') as csv_file:
    #     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    #     average_throughput = parse_average_throughput_from_throughput_listener(stdout)
    #
    #     for result in benchmark_results:
    #         result['query name'] = query
    #         result['tuplesPerSecond_listener'] = average_throughput
    #         writer.writerow({**result, **config})
    #     print(f"Results for config {config} written to CSV.")

def parse_buffer_config(config_strings):
    """Parse a list of buffer config strings into a list of tuples."""
    result = []
    for s in config_strings:
        try:
            parsed = ast.literal_eval(s.strip())
            if isinstance(parsed, tuple) and len(parsed) == 2:
                result.append(parsed)
            else:
                raise ValueError(f"Expected a tuple of 2 elements, got {parsed}")
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Invalid tuple format: {s}. Expected format like '(1234, 100)'") from e
    return result

if __name__ == "__main__":
    # Initialize argument parser
    parser = argparse.ArgumentParser(description="Run NebulaStream queries.")
    parser.add_argument("--all", action="store_true", help="Run all queries.")
    parser.add_argument("-o", "--output-dir", default=".", help="Output directory for results.")
    parser.add_argument("-q", "--queries", nargs="+", help="List of queries to run.")
    parser.add_argument("-w", "--worker-threads", nargs="+", help="Number of worker threads to run the queries.")
    parser.add_argument("-b", "--buffer-config", nargs="+", help="List of buffer configurations as tuples and buffer size is first, e.g., '(1234, 100) (128, 40)'.")
    parser.add_argument("-s", "--string-type", nargs="+", help="List of string types to run the queries.")
    parser.add_argument("-g", "--generate", action="store_true", help="Only generate systests (without Discard sink) and exit.")
    parser.add_argument("--skip-build", action="store_true", help="Skip the build process and use existing binaries.")
    parser.add_argument("-n", "--num-runs", type=int, default=1, help="Number of runs per experiment configuration.")
    args = parser.parse_args()

    NUM_RUNS_PER_EXPERIMENT = args.num_runs

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    csv_file_path = os.path.join(args.output_dir, "results_nebulastream.csv")

    # Determine which queries to runW
    queries_to_run = queries

    if not args.all and args.queries:
        # Filter queries based on the provided list
        queries_to_run = {k: v for k, v in queries.items() if k in args.queries}

    # Determine which string types to run (was slice caches)
    string_types_to_run = allStringTypes
    if args.string_type:
        string_types_to_run = [s for s in allStringTypes if s in args.string_type]

    # Determine the number of worker threads to run with
    number_of_worker_threads_to_run = allNumberOfWorkerThreads
    if args.worker_threads:
        number_of_worker_threads_to_run = [str(no_worker_threads) for no_worker_threads in args.worker_threads]

    # Parse buffer configurations
    if args.buffer_config:
        allBufferConfigs = parse_buffer_config(args.buffer_config)

    if args.generate:
        print("Generating systests without benchmark updates...")
        check_generate_systest(string_types_to_run, queries_to_run, "nes-systests/benchmark", benchmark_mode=False)
        print("Done.")
        exit(0)

    check_generate_systest(string_types_to_run, queries_to_run, "nes-systests/benchmark", benchmark_mode=True)

    # Print results
    print(",".join(queries_to_run.keys()))
    print(",".join(string_types_to_run))
    print(",".join(number_of_worker_threads_to_run))
    print(",".join(map(str, allBufferConfigs)))

    # Checking if the script has been executed from the repository root
    check_repository_root()

    if not args.skip_build and socket.gethostname() != "hollow":
        # Create folder
        create_folder_and_remove_if_exists(build_dir)
        cmake_flags = ("-G Ninja "
                       "-DCMAKE_BUILD_TYPE=Release "
                       f"-DCMAKE_TOOLCHAIN_FILE={get_vcpkg_dir()} "
                       "-DUSE_LIBCXX_IF_AVAILABLE:BOOL=OFF "
                       "-DENABLE_LARGE_TESTS=1 "
                       "-DNES_LOG_LEVEL:STRING=LEVEL_NONE "
                       "-DNES_BUILD_NATIVE:BOOL=ON")
        # Build NebulaStream
        compile_nebulastream(cmake_flags, build_dir)

    # Init csv files
    initialize_csv_file()

    # Iterate over all cross-product combinations for each query
    no_combinations = (
            len(allExecutionModes) *
            len(number_of_worker_threads_to_run) *
            len(allJoinStrategies) *
            len(string_types_to_run) *
            len(allPageSizes) *
            len(allBufferConfigs)
    )
    no_queries = len(queries_to_run)
    for queryIdx, query in enumerate(queries_to_run):
        workerConfigIdx = 0

        combinations = itertools.product(allExecutionModes, number_of_worker_threads_to_run,
                                         allBufferConfigs, allJoinStrategies,
                                         string_types_to_run,
                                         allPageSizes)
        for [executionMode, numberOfWorkerThreads, (bufferSizeInBytes, buffersInGlobalBufferManager), joinStrategy,
             stringType, pageSize] in combinations:
            workerConfigIdx += 1

            config = {
                'executionMode': executionMode,
                'numberOfWorkerThreads': numberOfWorkerThreads,
                'buffersInGlobalBufferManager': buffersInGlobalBufferManager,
                'joinStrategy': joinStrategy,
                'bufferSizeInBytes': bufferSizeInBytes,
                'pageSize': pageSize,
                'stringType': stringType
            }

            for i in range(NUM_RUNS_PER_EXPERIMENT):
                run_benchmark(config, stringType, query, queryIdx + 1, workerConfigIdx, no_combinations, no_queries)

    abs_csv_path = os.path.abspath(csv_file_path)
    print(f"CSV Measurement file can be found in {abs_csv_path}")
