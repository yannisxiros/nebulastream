#!/bin/bash

# Exit on error
set -e

# Number of iterations
ITERATIONS=4

# Base output directory
BASE_OUTPUT_DIR="benchmark_results"

# Parse arguments
SKIP_BUILD_FLAG=""
for arg in "$@"; do
    if [ "$arg" == "--skip-build" ]; then
        SKIP_BUILD_FLAG="--skip-build"
    fi
done

# Setup virtual environment
VENV_DIR="scripts/benchmarking/.venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
fi

# Activate venv and install dependencies
source "$VENV_DIR/bin/activate"
echo "Ensuring dependencies are installed (pandas, matplotlib)..."
pip install --quiet pandas matplotlib

for i in $(seq 1 $ITERATIONS); do
    RUN_DIR="${BASE_OUTPUT_DIR}/run_${i}"
    echo "Starting benchmark run $i... Output directory: $RUN_DIR"
    
    # Run the benchmark
    python3 scripts/benchmarking/run_benchmark.py --output-dir "$RUN_DIR" --all $SKIP_BUILD_FLAG
    
    # Generate the plots
    python3 scripts/benchmarking/plot.py --input-csv "${RUN_DIR}/results_nebulastream.csv" --output-dir "$RUN_DIR"
    
    echo "----------------Completed run $i.-----------------"
done

# Cleanup: Deactivate and remove the virtual environment
deactivate
echo "Cleaning up virtual environment..."
rm -rf "$VENV_DIR"

echo "All $ITERATIONS benchmark runs completed. Results are in $BASE_OUTPUT_DIR"
