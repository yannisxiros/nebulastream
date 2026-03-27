import csv
import random
import string
import time
import sys

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
    },
    # {
    #     "name": "fixed_len_str",
    #     "min_length": 32,
    #     "max_length": 32,
    #     "distinct_values": 5000,
    #     "distribution": "zipf",
    #     "zipf_a": 1.5
    # },
    # {
    #     "name": "long_str_field",
    #     "min_length": 100,
    #     "max_length": 500,
    #     "distinct_values": 1000
    # }
]

# 4. Output configuration
FILE_PATH = f"nes-systests/testdata/{FILE}.csv"
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
    print("=== Data Generation Configuration ===")
    print(f"Target size:   {TARGET_SIZE_GB} GB")
    print(f"Fixed record:  ~{FIXED_RECORD_BYTES} bytes")
    print(f"Output file:   {FILE}")
    
    num_dummy_cols = max(0, FIXED_RECORD_BYTES // 8)
    print(f"Action: Generating {num_dummy_cols} dummy integer columns (8 bytes each) to fulfill fixed size.")

    print("\nPre-generating distinct string pools (this might take a moment)...")
    string_pools_info = []
    for field in STRING_FIELDS:
        pool = [generate_random_string(field["min_length"], field["max_length"]) 
                for _ in range(field["distinct_values"])]
        
        dist = field.get("distribution", "uniform").lower()
        a = field.get("zipf_a", 1.5)
        weights = generate_zipf_weights(field["distinct_values"], a) if dist == "zipf" else None
        dist_label = f"Zipf distribution, a={a}" if dist == "zipf" else "Uniform distribution"
        print(f" - '{field['name']}': {field['distinct_values']} values, length [{field['min_length']}-{field['max_length']}] ({dist_label}).")
            
        string_pools_info.append({"pool": pool, "weights": weights})

    header = [f"int64_{i}" for i in range(num_dummy_cols)]
    header.extend([field["name"] for field in STRING_FIELDS])

    target_bytes = TARGET_SIZE_GB * 1024 * 1024 * 1024
    bytes_written = 0
    rows_written = 0
    
    start_time = time.time()

    print("\nStarting generation...")
    with open(FILE_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # We omit writing the CSV header because NebulaStream's CSV parser 
        # will try to parse it as data and fail on the UINT64 columns.
        # writer.writerow(header)
        
        bytes_written = f.tell()

        while bytes_written < target_bytes:
            # Batch generating columns is faster in Python than row-by-row
            dummy_cols = [[random.randint(0, 1000) for _ in range(BATCH_SIZE)] for _ in range(num_dummy_cols)]
            
            string_cols = []
            for info in string_pools_info:
                if info["weights"]:
                    string_cols.append(random.choices(info["pool"], weights=info["weights"], k=BATCH_SIZE))
                else:
                    string_cols.append(random.choices(info["pool"], k=BATCH_SIZE))
            
            # Construct rows
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
    print("You can copy/paste this into your .test file:")

    for string_type in STRING_TYPES:
        current_schema_str = schema_str.replace("VARSIZED", string_type)
        stream_name = f"stream_{FILE}"
        print(f"# For {string_type}:")
        print(f"CREATE LOGICAL SOURCE {stream_name}({current_schema_str});")
        print(f"CREATE PHYSICAL SOURCE FOR {stream_name} TYPE File;")
        print(f"ATTACH FILE {FILE}.csv\n")

if __name__ == '__main__':
    main()
