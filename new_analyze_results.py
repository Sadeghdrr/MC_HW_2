
# filepath: e:\\UNI\\Term 6\\MultiCore\\Tamrin\\HW2\\new_analyze_results.py
import os
import pandas as pd
import re
import matplotlib.pyplot as plt
import shutil
import matplotlib.cm as cm
import numpy as np

def parse_size(size_str):
    """Converts size strings like '150K' to integers."""
    size_str = size_str.upper()
    if 'K' in size_str:
        return int(size_str.replace('K', '')) * 1000
    if 'M' in size_str:
        return int(size_str.replace('M', '')) * 1000000
    return int(size_str)

def parse_filename(filename):
    """
    Parses the filename to extract parameters.
    Example: Results_HW2_MCC_030402_401106039_150K_8_150K_insert_insert_delete_insert.txt
    """
    match = re.search(r"_([^_]+)_(\d+)_([^_]+)_[^_]+\\.txt$", filename)
    if match:
        return {
            "dataset_size": parse_size(match.group(1)),
            "threads_or_buffer": int(match.group(2)),
            "table_size": parse_size(match.group(3))
        }
    return {}

def read_result_file(filepath):
    """Reads ExecutionTime and NumberOfHandledCollision from a result file."""
    data = {}
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            # Sum up all execution times and collisions if multiple operations are in one file
            exec_times = re.findall(r"ExecutionTime: (\\d+) ms", content)
            collisions = re.findall(r"NumberOfHandledCollision: (\\d+)", content)
            
            if exec_times:
                data["ExecutionTime_ms"] = sum(int(t) for t in exec_times)
            if collisions:
                data["NumberOfHandledCollision"] = sum(int(c) for c in collisions)
                
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return data

def main():
    results_dir = "results"
    plots_dir = "new_plots"
    all_data = []

    if os.path.exists(plots_dir):
        shutil.rmtree(plots_dir)
    os.makedirs(plots_dir, exist_ok=True)

    if not os.path.isdir(results_dir):
        print(f"Directory not found: {results_dir}")
        return

    for filename in os.listdir(results_dir):
        if filename.startswith("Results_HW2_MCC_") and filename.endswith(".txt"):
            filepath = os.path.join(results_dir, filename)
            
            file_params = parse_filename(filename)
            file_content_data = read_result_file(filepath)
            
            if file_params and file_content_data:
                combined_data = {**file_params, **file_content_data, "filename": filename}
                all_data.append(combined_data)

    if not all_data:
        print("No data processed. Check filenames or file contents.")
        return

    df = pd.DataFrame(all_data)

    numeric_cols = ["dataset_size", "threads_or_buffer", "table_size", "ExecutionTime_ms", "NumberOfHandledCollision"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=numeric_cols, inplace=True)
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
            continue

        pivot_collision = subset_df.pivot_table(index="threads_or_buffer", 
                                                columns="table_size", 
                                                values="NumberOfHandledCollision")
        
        if pivot_collision.empty:
            continue
            
        pivot_collision.plot(kind='bar', ax=plt.gca())
        
        for p in plt.gca().patches:
            plt.gca().annotate(f"{int(p.get_height())}", 
                               (p.get_x() + p.get_width() / 2., p.get_height()),
                               ha='center', va='center',
                               xytext=(0, 5),
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
    unique_combinations = df[["dataset_size", "table_size"]].drop_duplicates().values

    for ds_size, ts_size in unique_combinations:
        plt.figure(figsize=(10, 6))
        subset_df = df[(df["dataset_size"] == ds_size) & (df["table_size"] == ts_size)].sort_values(by="threads_or_buffer")
        
        if subset_df.empty:
            continue

        norm = plt.Normalize(vmin=subset_df["ExecutionTime_ms"].min(), vmax=subset_df["ExecutionTime_ms"].max())
        colors = cm.viridis(norm(subset_df["ExecutionTime_ms"]))
        
        bars = plt.bar(range(len(subset_df)), subset_df["ExecutionTime_ms"], color=colors)
        
        for i, (bar, row) in enumerate(zip(bars, subset_df.itertuples())):
            plt.text(bar.get_x() + bar.get_width() / 2., bar.get_height() + bar.get_height() * 0.01,
                     f"{row.ExecutionTime_ms}", 
                     ha='center', va='bottom', fontweight='bold')

        plt.title(f"Execution Time vs. Threads\\nDataset: {ds_size}, Table Size: {ts_size}")
        plt.xlabel("Number of Threads")
        plt.ylabel("Execution Time (ms)")
        plt.xticks(range(len(subset_df)), subset_df["threads_or_buffer"].values)
        plt.grid(True, axis='y', alpha=0.3)
        
        sm = plt.cm.ScalarMappable(cmap=cm.viridis, norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=plt.gca())
        cbar.set_label('Execution Time (ms)', rotation=270, labelpad=15)
        
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f"exectime_dataset_{ds_size}_tsize_{ts_size}.png"))
        plt.close()
        print(f"Generated: {os.path.join(plots_dir, f'exectime_dataset_{ds_size}_tsize_{ts_size}.png')}")

    # --- Generate Grouped Execution Time Plots ---
    for ds_size in unique_dataset_sizes:
        plt.figure(figsize=(12, 7))
        subset_df = df[df["dataset_size"] == ds_size].sort_values(by=["threads_or_buffer", "table_size"])
        
        if subset_df.empty:
            continue

        pivot_exectime = subset_df.pivot_table(index="threads_or_buffer", 
                                               columns="table_size", 
                                               values="ExecutionTime_ms")
        
        if pivot_exectime.empty:
            continue
            
        pivot_exectime.plot(kind='bar', ax=plt.gca())
        
        for p in plt.gca().patches:
            if not pd.isna(p.get_height()): 
                plt.gca().annotate(f"{p.get_height():.0f}",
                                   (p.get_x() + p.get_width() / 2., p.get_height()),
                                   ha='center', va='center',
                                   xytext=(0, 5),
                                   textcoords='offset points')

        plt.title(f"Execution Time vs. Threads (Grouped by Table Size)\\nDataset Size: {ds_size}")
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
