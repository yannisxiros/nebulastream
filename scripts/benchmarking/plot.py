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

    # Convert bufferSizeInBytes to KB for better readability in plots
    if 'bufferSizeInBytes' in df.columns:
        df['bufferSizeInKB'] = (df['bufferSizeInBytes'] / 1024).astype(int)

    # 2. Configure variables
    grouping_var = 'stringType'
    query_col = 'query name'

    # Columns to check for configuration changes
    config_cols = ['numberOfWorkerThreads', 'executionMode', 'joinStrategy', 'bufferSizeInKB', 'pageSize']

    # Define metrics to plot: (column_name, y_label, file_prefix, title_prefix)
    metrics = [
        ('tuplesPerSecond', 'Throughput (tuples/second)', 'throughput', 'Throughput Comparison'),
        ('time', 'Execution Time (ms)', 'time', 'Execution Time Comparison')
    ]

    # 3. Process each query
    for query in df[query_col].unique():
        query_base_df = df[df[query_col] == query].copy()
        
        # Identify variables that change (e.g., worker threads)
        varying = [c for c in config_cols if query_base_df[c].nunique() > 1]
        
        # Sort by varying columns to ensure numerical variables like bufferSizeInKB are ordered correctly
        if varying:
            query_base_df = query_base_df.sort_values(by=varying)

        # Create the X-axis label
        if not varying:
            query_base_df['label'] = "Standard"
        else:
            # Create a label like "Threads: 4"
            query_base_df['label'] = query_base_df[varying].astype(str).agg(', '.join, axis=1)
            # Make label categorical to preserve the sorted order during pivoting
            query_base_df['label'] = pd.Categorical(query_base_df['label'], categories=query_base_df['label'].unique(), ordered=True)
        
        for target_metric, y_label, file_prefix, title_prefix in metrics:
            if target_metric not in query_base_df.columns:
                continue

            # Aggregate multiple runs by calculating the mean across the full configuration
            group_cols = config_cols + [grouping_var, 'label']
            query_df = query_base_df.groupby(group_cols, observed=True)[target_metric].mean().reset_index()

            # Pivot so stringTypes are columns (side-by-side bars)
            plot_data = query_df.pivot(index='label', columns=grouping_var, values=target_metric)
            
            # 4. Generate the plot
            color_map = {
                'VARSIZED': 'green',
                'FLINK': 'blue',
                'GERMAN_VARSIZED': 'orange',
                'GERMAN_VARSIZED opt': 'red'
            }
            colors = [color_map.get(col, 'gray') for col in plot_data.columns]
            
            ax = plot_data.plot(kind='bar', figsize=(10, 6), width=0.75, color=colors)
            
            plt.title(f"{title_prefix} - Query: {query}", fontsize=14, fontweight='bold')
            plt.ylabel(y_label, fontsize=12)
            plt.xlabel(f"Configuration ({', '.join(varying)})", fontsize=12)
            plt.xticks(rotation=0)
            plt.grid(axis='y', linestyle=':', alpha=0.7)
            plt.legend(title="String Type", loc='best')
            
            # Annotate bars with precise values
            for p in ax.patches:
                val = p.get_height()
                if val > 0:
                    label = f'{val:,.0f}' if target_metric == 'tuplesPerSecond' else f'{val:,.2f}'
                    ax.annotate(label, 
                                (p.get_x() + p.get_width() / 2., val),
                                ha='center', va='bottom', xytext=(0, 4), 
                                textcoords='offset points', fontsize=9)

            plt.tight_layout()
            output_path = os.path.join(args.output_dir, f"{file_prefix}_plot_{query}.png")
            plt.savefig(output_path)
            plt.close()

    print(f"Benchmark plots saved as PNG files in {args.output_dir}.")

if __name__ == "__main__":
    main()
