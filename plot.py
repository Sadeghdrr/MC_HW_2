#!/usr/bin/env python3
import os
import re
import glob
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

# --- CONFIGURATION ------------------------------------------------------
RESULTS_DIR = "results"
PLOTS_DIR   = "plots"
os.makedirs(PLOTS_DIR, exist_ok=True)

# Filename pattern
FNAME_RE = re.compile(
    r"Results_HW2_MCC_030402_[^_]+_(?P<data>\d+)K_(?P<threads>\d+)_(?P<tsize>\d+)K_.*\.txt$"
)

# --- PARSE ALL RESULTS INTO MEMORY --------------------------------------
# data[(ds,ts)][threads] -> { "time": ms, "coll": count }
data = defaultdict(lambda: defaultdict(lambda: {"time": 0.0, "coll": 0.0}))

for path in glob.glob(os.path.join(RESULTS_DIR, "*.txt")):
    fn = os.path.basename(path)
    m = FNAME_RE.match(fn)
    if not m:
        continue

    ds = int(m.group("data"))      # e.g. 150
    ts = int(m.group("tsize"))     # e.g. 90
    th = int(m.group("threads"))   # e.g. 512

    total_time = 0.0
    total_coll = 0.0
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("ExecutionTime:"):
                parts = line.split()
                total_time += float(parts[-2])
            elif line.startswith("NumberOfHandledCollision:"):
                parts = line.split()
                total_coll += float(parts[-1])

    data[(ds, ts)][th]["time"] = total_time
    data[(ds, ts)][th]["coll"] = total_coll

# --- HELPER TO DRAW A SINGLE MEASURE PLOT -------------------------------
def draw_bar_plot(
    threads, values, title, ylabel, outpath
):
    """
    threads: list of thread-counts (ints)
    values:  list of corresponding measure values (floats)
    """
    # Normalize & colormap
    cmap = plt.cm.viridis
    norm = plt.Normalize(vmin=0, vmax=max(values))
    colors = cmap(norm(values))

    fig, ax = plt.subplots(figsize=(8,4))
    bars = ax.bar(
        [str(t) for t in threads],
        values,
        color=colors,
        edgecolor="black"
    )

    # Annotate
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width()/2,
            val + max(values)*0.01,
            f"{val:.0f}",
            ha="center", va="bottom", fontsize=8
        )

    ax.set_xlabel("Number of Threads")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticklabels([str(t) for t in threads], rotation=45)

    # colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, pad=0.02)
    cbar.set_label(ylabel)

    plt.tight_layout()
    fig.savefig(outpath, dpi=150)
    plt.close(fig)

# --- 1) Individual plots (as before, but with gradient) -----------------
for measure, ylabel, prefix in [
    ("time", "Total Execution Time (ms)",    "exectime"),
    ("coll", "Total # Handled Collisions",    "collision")
]:
    for (ds, ts), th_dict in sorted(data.items()):
        threads = sorted(th_dict.keys())
        vals    = [th_dict[t][measure] for t in threads]
        title   = f"{ylabel} vs Threads\nDataset: {ds}K, Table Size: {ts}K"
        fname   = f"{prefix}_{ds}K_{ts}K.png"
        outpath = os.path.join(PLOTS_DIR, fname)
        draw_bar_plot(threads, vals, title, ylabel, outpath)

# --- 2) Compacted collision plots by data_size --------------------------
# group by data_size
by_ds = defaultdict(dict)
for (ds, ts), th_dict in data.items():
    by_ds[ds][ts] = th_dict

for ds, ts_dict in sorted(by_ds.items()):
    # gather all unique thread-counts (should be same for each tsize)
    threads = sorted(
        set().union(*[set(d.keys()) for d in ts_dict.values()])
    )
    tsizes = sorted(ts_dict.keys())  # e.g. [60, 90, 120]

    # build matrix of collisions: rows=tsize, cols=threads
    coll_matrix = []
    for ts in tsizes:
        row = [ts_dict[ts].get(t, 0.0)["coll"] if False else ts_dict[ts][t]["coll"]
               for t in threads]
        coll_matrix.append(row)
    coll_matrix = np.array(coll_matrix)  # shape (n_tsize, n_thread)

    # plot grouped bars
    n_ts = len(tsizes)
    x = np.arange(len(threads))
    width = 0.8 / n_ts

    fig, ax = plt.subplots(figsize=(8,4))
    cmap = plt.cm.viridis
    norm = plt.Normalize(vmin=0, vmax=coll_matrix.max())

    for i, ts in enumerate(tsizes):
        vals = coll_matrix[i]
        colors = cmap(norm(vals))
        ax.bar(
            x + (i - n_ts/2)*width + width/2,
            vals,
            width=width,
            color=colors,
            edgecolor="black",
            label=f"{ts}K"
        )

    # annotate bars
    for i in range(n_ts):
        for j, val in enumerate(coll_matrix[i]):
            xpos = x[j] + (i - n_ts/2)*width + width/2
            ax.text(
                xpos, val + coll_matrix.max()*0.01,
                f"{val:.0f}",
                ha="center", va="bottom", fontsize=7
            )

    ax.set_xticks(x)
    ax.set_xticklabels([str(t) for t in threads], rotation=45)
    ax.set_xlabel("Number of Threads")
    ax.set_ylabel("Total # Handled Collisions")
    ax.set_title(f"Collisions vs Threads\nDataset: {ds}K (all T-sizes)")
    ax.legend(title="Table Size (K)", bbox_to_anchor=(1.02,1), loc="upper left")

    # colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, pad=0.02)
    cbar.set_label("Total # Handled Collisions")

    plt.tight_layout()
    outpath = os.path.join(PLOTS_DIR, f"collision_group_{ds}K.png")
    fig.savefig(outpath, dpi=150)
    plt.close(fig)

print(f"Done! Plots saved under ./{PLOTS_DIR}/")
