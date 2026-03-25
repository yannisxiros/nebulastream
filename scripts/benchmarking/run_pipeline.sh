#!/bin/bash

# Exit on error
set -e
# Base output directory
BASE_OUTPUT_DIR="benchmark_results"

# Parse arguments
SKIP_BUILD_FLAG=""
NUM_RUNS=1

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD_FLAG="--skip-build"
            shift
            ;;
        -n|--num-runs)
            NUM_RUNS="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Setup virtual environment
VENV_DIR="scripts/benchmarking/.venv"
echo "Creating virtual environment in $VENV_DIR..."
python3 -m venv "$VENV_DIR"

# Activate venv and install dependencies
source "$VENV_DIR/bin/activate"
echo "Ensuring dependencies are installed (pandas, matplotlib)..."
pip install --quiet pandas matplotlib

echo "Starting benchmark run... Output directory: $BASE_OUTPUT_DIR"

# Run the benchmark
python3 scripts/benchmarking/run_benchmark.py --output-dir "$BASE_OUTPUT_DIR" --all $SKIP_BUILD_FLAG --num-runs "$NUM_RUNS"

# Generate the plots
#python3 scripts/benchmarking/plot.py --input-csv "${BASE_OUTPUT_DIR}/results_nebulastream.csv" --output-dir "$BASE_OUTPUT_DIR"
#
## Cleanup: Deactivate and remove the virtual environment
#deactivate
echo "Cleaning up virtual environment..."
rm -rf "$VENV_DIR"

echo "Benchmark run completed. Results are in $BASE_OUTPUT_DIR"
