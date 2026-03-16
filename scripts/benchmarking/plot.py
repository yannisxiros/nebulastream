import pandas as pd
import matplotlib.pyplot as plt
import io
import argparse
import os

def main():
    # 1. Setup the data
    parser = argparse.ArgumentParser(description="Plot benchmark results.")
    parser.add_argument("-i", "--input-csv", default="results_nebulastream.csv", help="Input CSV file.")
    parser.add_argument("-o", "--output-dir", default=".", help="Output directory for PNGs.")
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    df = pd.read_csv(args.input_csv)

    # 2. Configure variables
    target_metric = 'time'
    grouping_var = 'stringType'
    query_col = 'query name'

    # Columns to check for configuration changes
    config_cols = ['numberOfWorkerThreads', 'executionMode', 'joinStrategy', 'bufferSizeInBytes', 'buffersInGlobalBufferManager', 'pageSize']

    # 3. Process each query
    for query in df[query_col].unique():
        query_df = df[df[query_col] == query].copy()
        
        # Identify variables that change (e.g., worker threads)
        varying = [c for c in config_cols if query_df[c].nunique() > 1]
        
        # Create the X-axis label
        if not varying:
            query_df['label'] = "Standard"
        else:
            # Create a label like "Threads: 4"
            query_df['label'] = query_df[varying].astype(str).agg(', '.join, axis=1)
        
        # Pivot so stringTypes are columns (side-by-side bars)
        plot_data = query_df.pivot(index='label', columns=grouping_var, values=target_metric)
        
        # 4. Generate the plot
        num_string_types = len(plot_data.columns)
        cmap = plt.get_cmap('tab10')
        colors = [cmap(i) for i in range(num_string_types)]
        
        ax = plot_data.plot(kind='bar', figsize=(10, 6), width=0.75, color=colors)
        
        plt.title(f"Execution Time Comparison - Query: {query}", fontsize=14, fontweight='bold')
        plt.ylabel("Time (seconds)", fontsize=12)
        plt.xlabel(f"Configuration ({', '.join(varying)})", fontsize=12)
        plt.xticks(rotation=0)
        plt.grid(axis='y', linestyle=':', alpha=0.7)
        plt.legend(title="String Type", loc='best')
        
        # Annotate bars with precise time values (4 decimal places)
        for p in ax.patches:
            val = p.get_height()
            if val > 0:
                ax.annotate(f'{val:.4f}s', 
                            (p.get_x() + p.get_width() / 2., val),
                            ha='center', va='bottom', xytext=(0, 4), 
                            textcoords='offset points', fontsize=9)

        plt.tight_layout()
        output_path = os.path.join(args.output_dir, f"time_plot_{query}.png")
        plt.savefig(output_path)
        plt.close()

    print(f"Time-based bar plots saved as PNG files in {args.output_dir}.")

if __name__ == "__main__":
    main()
