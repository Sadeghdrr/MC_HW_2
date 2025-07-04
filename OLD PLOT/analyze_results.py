import os
import pandas as pd # Re-added pandas
import re
import matplotlib.pyplot as plt
import shutil # For rmtree

def parse_filename(filename):
    """
    Parses the filename to extract parameters.
    Example: Results_MCC_030402_401106039_150000_1_120000.txt
    Extracts: dataset_size=150000, some_param=1, table_size=120000 (example interpretation)
    """
    # Corrected regex:
    # - Ensure no unintended escape sequences
    # - The original regex r_(\\d+)_(\\d+)_(\\d+)\\.txt$' seems to have an extra backslash before the first underscore
    # - Assuming the pattern is like "Results_MCC_..._NUMBER_NUMBER_NUMBER.txt"
    # - The initial part "Results_MCC_030402_401106039_" is constant, so we can be more specific
    #   or keep it general if other prefixes exist.
    # For now, sticking to the general pattern based on the error, let's ensure the raw string is clean.
    # The error "unexpected character after line continuation character" often means a stray backslash
    # at the end of a line or an unescaped special character that's being misinterpreted.
    # The provided regex in the prompt was: r_(\\d+)_(\\d+)_(\\d+)\\.txt$'
    # A cleaner version for matching three groups of digits at the end, preceded by underscores:
    match = re.search(r"_(\d+)_(\d+)_(\d+)\.txt$", filename)
    if match:
        return {
            "dataset_size": int(match.group(1)),
            "threads_or_buffer": int(match.group(2)), # Assuming this is threads or a buffer related param
            "table_size": int(match.group(3))
        }
    return {}

def read_result_file(filepath):
    """Reads ExecutionTime and NumberOfHandledCollision from a result file."""
    data = {}
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if "ExecutionTime:" in line:
                    data["ExecutionTime_ms"] = int(line.split(":")[1].strip().replace("ms", ""))
                elif "NumberOfHandledCollision:" in line:
                    data["NumberOfHandledCollision"] = int(line.split(":")[1].strip())
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return data

def main():
    results_dir = "results"
    plots_dir = "plots"
    all_data = []

    # Ensure plots directory exists and is empty
    if os.path.exists(plots_dir):
        shutil.rmtree(plots_dir) # Remove existing plots dir to avoid clutter
    os.makedirs(plots_dir, exist_ok=True)

    if not os.path.isdir(results_dir):
        print(f"Directory not found: {results_dir}")
        return

    for filename in os.listdir(results_dir):
        if filename.startswith("Results_MCC_") and filename.endswith(".txt"):
            filepath = os.path.join(results_dir, filename)
            
            file_params = parse_filename(filename)
            file_content_data = read_result_file(filepath)
            
            if file_content_data: # Ensure we got data from the file
                # Combine filename parameters and file content data
                combined_data = {**file_params, **file_content_data, "filename": filename}
                all_data.append(combined_data)

    if not all_data:
        print("No data processed. Check filenames or file contents.")
        return

    df = pd.DataFrame(all_data)

    # Convert columns to numeric if they are not already, errors to NaN then drop
    numeric_cols = ["dataset_size", "threads_or_buffer", "table_size", "ExecutionTime_ms", "NumberOfHandledCollision"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=numeric_cols, inplace=True) # Drop rows where conversion failed
    for col in ["dataset_size", "threads_or_buffer", "table_size", "NumberOfHandledCollision"]:
        df[col] = df[col].astype(int)

    print("Collected and Processed Data:")
    print(df.to_string())

    # --- Generate Collision Plots ---
    unique_dataset_sizes = sorted(df["dataset_size"].unique())

    for ds_size in unique_dataset_sizes:
        plt.figure(figsize=(12, 7))
        subset_df = df[df["dataset_size"] == ds_size].sort_values(by=["threads_or_buffer", "table_size"])
        
        if subset_df.empty:
            print(f"No data for dataset_size {ds_size} to plot collisions.")
            continue

        # Pivot for grouped bar chart
        pivot_collision = subset_df.pivot_table(index="threads_or_buffer", 
                                                columns="table_size", 
                                                values="NumberOfHandledCollision")
        
        if pivot_collision.empty:
            print(f"Could not pivot data for dataset_size {ds_size} for collision plot.")
            continue
            
        pivot_collision.plot(kind='bar', ax=plt.gca())
        
        # Add text labels on top of each bar for collision plots
        for p in plt.gca().patches:
            plt.gca().annotate(f"{int(p.get_height())}", # Value to display
                               (p.get_x() + p.get_width() / 2., p.get_height()), # Position (x, y)
                               ha='center', va='center', # Alignment
                               xytext=(0, 5), # Text offset
                               textcoords='offset points')

        plt.title(f"Collisions vs. Threads for Dataset Size: {ds_size}")
        plt.xlabel("Number of Threads")
        plt.ylabel("Number of Handled Collisions")
        plt.xticks(rotation=45)
        plt.legend(title="Table Size")
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f"collisions_dataset_{ds_size}.png"))
        plt.close()
        print(f"Generated: {os.path.join(plots_dir, f'collisions_dataset_{ds_size}.png')}")

    # --- Generate Execution Time Plots (Bar Chart with Gradient Colors) ---
    import matplotlib.cm as cm
    import numpy as np
    
    unique_combinations = df[["dataset_size", "table_size"]].drop_duplicates().values

    for ds_size, ts_size in unique_combinations:
        plt.figure(figsize=(10, 6))
        subset_df = df[(df["dataset_size"] == ds_size) & (df["table_size"] == ts_size)].sort_values(by="threads_or_buffer")
        
        if subset_df.empty:
            print(f"No data for dataset_size {ds_size} and table_size {ts_size} to plot execution time.")
            continue

        # Create gradient colors based on execution time values
        norm = plt.Normalize(vmin=subset_df["ExecutionTime_ms"].min(), vmax=subset_df["ExecutionTime_ms"].max())
        colors = cm.viridis(norm(subset_df["ExecutionTime_ms"]))
        
        # Create bar chart with gradient colors
        bars = plt.bar(range(len(subset_df)), subset_df["ExecutionTime_ms"], color=colors)
        
        # Add text labels on top of each bar
        for i, (bar, row) in enumerate(zip(bars, subset_df.itertuples())):
            plt.text(bar.get_x() + bar.get_width() / 2., bar.get_height() + bar.get_height() * 0.01,
                     f"{row.ExecutionTime_ms}", 
                     ha='center', va='bottom', fontweight='bold')

        plt.title(f"Execution Time vs. Threads\nDataset: {ds_size}, Table Size: {ts_size}")
        plt.xlabel("Number of Threads")
        plt.ylabel("Execution Time (ms)")
        plt.xticks(range(len(subset_df)), subset_df["threads_or_buffer"].values)
        plt.grid(True, axis='y', alpha=0.3)
        
        # Add colorbar to show the gradient scale
        sm = plt.cm.ScalarMappable(cmap=cm.viridis, norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=plt.gca())
        cbar.set_label('Execution Time (ms)', rotation=270, labelpad=15)
        
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f"exectime_dataset_{ds_size}_tsize_{ts_size}.png"))
        plt.close()
        print(f"Generated: {os.path.join(plots_dir, f'exectime_dataset_{ds_size}_tsize_{ts_size}.png')}")

    # --- Generate Grouped Execution Time Plots (similar to collision plots) ---
    for ds_size in unique_dataset_sizes:
        plt.figure(figsize=(12, 7))
        subset_df = df[df["dataset_size"] == ds_size].sort_values(by=["threads_or_buffer", "table_size"])
        
        if subset_df.empty:
            print(f"No data for dataset_size {ds_size} to plot grouped execution times.")
            continue

        # Pivot for grouped bar chart
        pivot_exectime = subset_df.pivot_table(index="threads_or_buffer", 
                                               columns="table_size", 
                                               values="ExecutionTime_ms")
        
        if pivot_exectime.empty:
            print(f"Could not pivot data for dataset_size {ds_size} for grouped execution time plot.")
            continue
            
        pivot_exectime.plot(kind='bar', ax=plt.gca())
        
        # Add text labels on top of each bar
        for p in plt.gca().patches:
            # Check for NaN height which can happen if data is missing for a group
            if not pd.isna(p.get_height()): 
                plt.gca().annotate(f"{p.get_height():.0f}", # Value to display (formatted as integer)
                                   (p.get_x() + p.get_width() / 2., p.get_height()), # Position (x, y)
                                   ha='center', va='center', # Alignment
                                   xytext=(0, 5), # Text offset
                                   textcoords='offset points')

        plt.title(f"Execution Time vs. Threads (Grouped by Table Size)\nDataset Size: {ds_size}")
        plt.xlabel("Number of Threads")
        plt.ylabel("Execution Time (ms)")
        plt.xticks(rotation=45)
        plt.legend(title="Table Size")
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f"exectime_grouped_dataset_{ds_size}.png"))
        plt.close()
        print(f"Generated: {os.path.join(plots_dir, f'exectime_grouped_dataset_{ds_size}.png')}")

if __name__ == "__main__":
    main()
