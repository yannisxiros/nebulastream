import csv
import random
import string
import time
import sys
import argparse
import os

# ==========================================
# CONFIGURATION KNOBS
# ==========================================

STRING_TYPES = ["VARSIZED", "GERMAN_VARSIZED", "FLINK"]

# 1. Target total file size in Gigabytes
TARGET_SIZE_GB = 1.0
FILE = "long-20"

# 2. Fixed size record configuration (in bytes)
# The script will pad the schema with 8-byte integer columns 
# to approximately reach this size before adding string fields.
FIXED_RECORD_BYTES = 0

# 3. String Fields Configuration
# Add as many dictionaries here as you need for your string fields.
STRING_FIELDS = [
    {
        "name": "short_str_field",
        "min_length": 5,
        "max_length": 20,
        "distinct_values": 5000
    }
]

# 4. Output configuration
BATCH_SIZE = 100_000  # Number of rows to hold in memory before writing to disk

# ==========================================

def generate_random_string(min_len, max_len):
    """Generates a random string of length between min_len and max_len."""
    length = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_zipf_weights(n, a):
    """Generates weights for a Zipfian distribution with n elements and parameter a."""
    weights = [1.0 / (i**a) for i in range(1, n + 1)]
    return weights

def main():
    global TARGET_SIZE_GB, FILE, STRING_FIELDS, FIXED_RECORD_BYTES

    parser = argparse.ArgumentParser(description="Generate string data for NebulaStream benchmarks.")
    parser.add_argument("--size_gb", type=float, help="Target total file size in GB")
    parser.add_argument("--num_rows", type=int, help="Target total number of rows")
    parser.add_argument("--file", type=str, help="Output file name (without extension)")
    parser.add_argument("--string_len", type=int, help="Fixed length for a single string field")
    parser.add_argument("--distinct_values", type=int, default=1, help="Number of distinct string values")
    parser.add_argument("--skewed_lengths", action="store_true", help="Generate skewed length strings")
    parser.add_argument("--skew_ratio", type=float, default=0.95, help="Ratio of short strings (default: 0.95 for 95/5)")
    parser.add_argument("--fixed_record_bytes", type=int, help="Fixed record size in bytes (used for dummy columns)")
    args = parser.parse_args()

    TARGET_ROWS = None
    if args.size_gb is not None:
        TARGET_SIZE_GB = args.size_gb
    if args.num_rows is not None:
        TARGET_ROWS = args.num_rows
    if args.file is not None:
        FILE = args.file
    if args.fixed_record_bytes is not None:
        FIXED_RECORD_BYTES = args.fixed_record_bytes
    
    if args.skewed_lengths:
        short_ratio = args.skew_ratio
        long_ratio = 1.0 - short_ratio
        STRING_FIELDS = [
            {
                "name": "skewed_str_field",
                "distinct_values": args.distinct_values,
                "length_distribution": [
                    (0.5, 4, 4),
                    (0.5, 64, 64)
                ],
                "skewed_weights": [short_ratio, long_ratio]
            }
        ]
    elif args.string_len is not None:
        STRING_FIELDS = [
            {
                "name": "str_field",
                "min_length": args.string_len,
                "max_length": args.string_len,
                "distinct_values": args.distinct_values
            }
        ]

    file_path = f"nes-systests/testdata/{FILE}.csv"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    print("=== Data Generation Configuration ===")
    print(f"Target size:   {TARGET_SIZE_GB} GB")
    print(f"Fixed record:  ~{FIXED_RECORD_BYTES} bytes")
    print(f"Output file:   {FILE}")
    
    num_dummy_cols = max(0, FIXED_RECORD_BYTES // 8)
    print(f"Action: Generating {num_dummy_cols} dummy integer columns (8 bytes each) to fulfill fixed size.")

    print("\nPre-generating distinct string pools (this might take a moment)...")
    string_pools_info = []
    for field in STRING_FIELDS:
        if "length_distribution" in field:
            pool = []
            for percentage, min_len, max_len in field["length_distribution"]:
                count = int(field["distinct_values"] * percentage)
                pool.extend([generate_random_string(min_len, max_len) for _ in range(count)])
            # Fill remaining to match distinct_values if any due to rounding
            while len(pool) < field["distinct_values"]:
                _, min_len, max_len = field["length_distribution"][0]
                pool.append(generate_random_string(min_len, max_len))
            dist_label = f"Custom length distribution: {field['length_distribution']}"
        else:
            pool = [generate_random_string(field["min_length"], field["max_length"]) 
                    for _ in range(field["distinct_values"])]
            dist_label = f"length [{field['min_length']}-{field['max_length']}]"
        
        dist = field.get("distribution", "uniform").lower()
        if dist == "zipf":
            a = field.get("zipf_a", 1.5)
            weights = generate_zipf_weights(field["distinct_values"], a)
            access_dist = f"Zipf (a={a})"
        elif "skewed_weights" in field:
            ratios = field["skewed_weights"]
            weights = []
            for (percentage, min_len, max_len), ratio in zip(field["length_distribution"], ratios):
                count = int(field["distinct_values"] * percentage)
                weights.extend([ratio / count] * count)
            # Pad if needed
            while len(weights) < field["distinct_values"]:
                weights.append(weights[-1])
            access_dist = f"Skewed {ratios}"
        else:
            weights = None
            access_dist = "Uniform"

        print(f" - '{field['name']}': {field['distinct_values']} values, {dist_label}, Access Dist: {access_dist}.")
            
        string_pools_info.append({"pool": pool, "weights": weights})

    target_bytes = TARGET_SIZE_GB * 1024 * 1024 * 1024
    bytes_written = 0
    rows_written = 0
    
    start_time = time.time()

    print("\nStarting generation...")
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        while (TARGET_ROWS is None and bytes_written < target_bytes) or (TARGET_ROWS is not None and rows_written < TARGET_ROWS):
            dummy_cols = [[random.randint(0, 1000) for _ in range(BATCH_SIZE)] for _ in range(num_dummy_cols)]
            
            string_cols = []
            for info in string_pools_info:
                if info["weights"]:
                    string_cols.append(random.choices(info["pool"], weights=info["weights"], k=BATCH_SIZE))
                else:
                    string_cols.append(random.choices(info["pool"], k=BATCH_SIZE))
            
            batch = []
            for i in range(BATCH_SIZE):
                row = [dummy_cols[j][i] for j in range(num_dummy_cols)]
                for j in range(len(string_pools_info)):
                    row.append(string_cols[j][i])
                batch.append(row)
            
            writer.writerows(batch)
            rows_written += BATCH_SIZE
            bytes_written = f.tell()
            
            elapsed = time.time() - start_time
            speed = (bytes_written / 1024 / 1024) / elapsed if elapsed > 0 else 0
            sys.stdout.write(f"\rProgress: {bytes_written / 1024 / 1024 / 1024:.2f} GB / {TARGET_SIZE_GB:.2f} GB | Speed: {speed:.2f} MB/s")
            sys.stdout.flush()

    total_time = time.time() - start_time
    print(f"\n\n=== Generation Complete ===")
    print(f"Total rows written: {rows_written:,}")
    print(f"Total file size:    {bytes_written / 1024 / 1024 / 1024:.2f} GB")
    print(f"Time taken:         {total_time:.2f} seconds")

    print("\n=== NebulaStream Systest Schema ===")
    schema_fields = [f"int64_{i} UINT64" for i in range(num_dummy_cols)]
    schema_fields.extend([f"{field['name']} VARSIZED" for field in STRING_FIELDS])
    schema_str = ", ".join(schema_fields)

    for string_type in STRING_TYPES:
        current_schema_str = schema_str.replace("VARSIZED", string_type)
        stream_name = f"stream_{FILE}"
        print(f"# For {string_type}:")
        print(f"CREATE LOGICAL SOURCE {stream_name}({current_schema_str});")
        print(f"CREATE PHYSICAL SOURCE FOR {stream_name} TYPE File;")
        print(f"ATTACH FILE {FILE}.csv\n")

if __name__ == '__main__':
    main()