import os
import re

def update_systest_for_benchmark(content, string_type, benchmark_mode):
    """Deletes expected output and changes sink to Discard if benchmark_mode is True."""
    # Replace VARSIZED
    content = content.replace("VARSIZED", string_type)
    
    if benchmark_mode:
        # Replace INTO ...() with INTO DISCARD()
        content = re.sub(r"INTO\s+[a-zA-Z0-9_\(\)]+",
                         "INTO DISCARD()", 
                         content, flags=re.IGNORECASE)


        # Delete the line immediately following each ----
        lines = content.split('\n')
        updated_lines = []
        skip_next = False
        for line in lines:
            if skip_next:
                skip_next = False
                continue
            updated_lines.append(line)
            if line.strip() == "----":
                skip_next = True
        return '\n'.join(updated_lines)
    
    return content

def check_generate_systest(allStringTypes, queries, queries_dir, benchmark_mode=True):
    # Ensure "strings" directory exists
    strings_dir = os.path.join(queries_dir, "strings")
    if not os.path.exists(strings_dir):
        os.makedirs(strings_dir)

    # Iterate over each query with :0x at the end
    for _, query_path in queries.items():
        base_path = query_path.split(":", 1)[0]
        print(base_path)
        base_filename = os.path.basename(base_path )
        match = re.match(r"([^\.]+)", base_filename)
        name_part = match.group(1) if match else base_filename
        query_dir = os.path.join(strings_dir, name_part)
        if not os.path.exists(query_dir):
            os.makedirs(query_dir)

        # For each string type, ensure query_{string}.test exists
        for string_type in allStringTypes:
            test_file_name = f"{name_part}_{string_type}.test"
            # Open the original file, replace VARSIZED with string_type, and write to test_file_path
            test_file_path = os.path.join(query_dir, test_file_name)
            
            with open(base_path, 'r') as src_file:
                content = src_file.read()
            
            # Update content for benchmarking
            updated_content = update_systest_for_benchmark(content, string_type, benchmark_mode)
            
            with open(test_file_path, 'w') as dst_file:
                dst_file.write(updated_content)
            print(f"Created {test_file_path}")
