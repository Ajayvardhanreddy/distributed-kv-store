"""
Benchmark: Throughput & Latency

Measures ops/sec and latency percentiles for the StorageEngine and
ConsistentHashRing directly (no network, no Docker — raw performance).

Usage:
    python benchmarks/benchmark_throughput.py

Output:
    benchmarks/results/throughput.png
    benchmarks/results/latency_distribution.png
    Prints summary table to stdout
"""
import asyncio
import os
import sys
import tempfile
import time
import statistics

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.storage.engine import StorageEngine
from app.cluster.consistent_hash import ConsistentHashRing


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
NUM_OPS = 10_000        # operations per benchmark run
KEY_PREFIX = "bench"
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def ensure_results_dir():
    os.makedirs(RESULTS_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Throughput benchmark
# ---------------------------------------------------------------------------

async def bench_storage_throughput() -> dict:
    """Measure raw put/get/delete ops per second on StorageEngine."""
    results = {}

    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "bench.wal"))
        await engine.initialize()

        # --- PUT throughput ---
        start = time.perf_counter()
        for i in range(NUM_OPS):
            await engine.put(f"{KEY_PREFIX}:{i}", f"value-{i}")
        elapsed = time.perf_counter() - start
        results["put_ops_sec"] = int(NUM_OPS / elapsed)
        results["put_total_sec"] = round(elapsed, 3)

        # --- GET throughput ---
        start = time.perf_counter()
        for i in range(NUM_OPS):
            await engine.get(f"{KEY_PREFIX}:{i}")
        elapsed = time.perf_counter() - start
        results["get_ops_sec"] = int(NUM_OPS / elapsed)
        results["get_total_sec"] = round(elapsed, 3)

        # --- DELETE throughput ---
        start = time.perf_counter()
        for i in range(NUM_OPS):
            await engine.delete(f"{KEY_PREFIX}:{i}")
        elapsed = time.perf_counter() - start
        results["delete_ops_sec"] = int(NUM_OPS / elapsed)
        results["delete_total_sec"] = round(elapsed, 3)

        await engine.close()

    return results


def bench_ring_lookup_throughput() -> dict:
    """Measure hash ring lookup speed."""
    ring = ConsistentHashRing(num_vnodes=150)
    for i in range(3):
        ring.add_node(f"node-{i}")

    keys = [f"lookup:{i}" for i in range(NUM_OPS)]

    start = time.perf_counter()
    for key in keys:
        ring.get_node(key)
    elapsed = time.perf_counter() - start

    return {
        "ring_lookup_ops_sec": int(NUM_OPS / elapsed),
        "ring_lookup_total_sec": round(elapsed, 3),
    }


# ---------------------------------------------------------------------------
# Latency benchmark
# ---------------------------------------------------------------------------

async def bench_latency() -> dict:
    """Measure per-operation latency for put and get."""
    put_latencies = []
    get_latencies = []

    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "latency.wal"))
        await engine.initialize()

        for i in range(NUM_OPS):
            start = time.perf_counter()
            await engine.put(f"lat:{i}", f"v{i}")
            put_latencies.append((time.perf_counter() - start) * 1_000_000)  # μs

        for i in range(NUM_OPS):
            start = time.perf_counter()
            await engine.get(f"lat:{i}")
            get_latencies.append((time.perf_counter() - start) * 1_000_000)

        await engine.close()

    def percentiles(data):
        data.sort()
        return {
            "p50": round(data[len(data) // 2], 1),
            "p95": round(data[int(len(data) * 0.95)], 1),
            "p99": round(data[int(len(data) * 0.99)], 1),
            "mean": round(statistics.mean(data), 1),
        }

    return {
        "put_latency_us": percentiles(put_latencies),
        "get_latency_us": percentiles(get_latencies),
    }


# ---------------------------------------------------------------------------
# Plot generation
# ---------------------------------------------------------------------------

def plot_throughput(results: dict):
    """Bar chart: ops/sec by operation type."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("  [skip] matplotlib not installed — run: pip install matplotlib")
        return

    ops = ["PUT", "GET", "DELETE", "Ring Lookup"]
    values = [
        results["put_ops_sec"],
        results["get_ops_sec"],
        results["delete_ops_sec"],
        results["ring_lookup_ops_sec"],
    ]
    colors = ["#4CAF50", "#2196F3", "#F44336", "#FF9800"]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(ops, values, color=colors, edgecolor="white", linewidth=1.5)
    ax.set_ylabel("Operations / second", fontsize=13)
    ax.set_title("Storage Engine & Ring Lookup Throughput", fontsize=15, fontweight="bold")
    ax.set_ylim(0, max(values) * 1.2)

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(values) * 0.02,
                f"{val:,}", ha="center", va="bottom", fontsize=12, fontweight="bold")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3)

    path = os.path.join(RESULTS_DIR, "throughput.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def plot_latency(latency: dict):
    """Grouped bar chart: latency percentiles for PUT vs GET."""
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("  [skip] matplotlib not installed")
        return

    labels = ["p50", "p95", "p99"]
    put_vals = [latency["put_latency_us"][k] for k in labels]
    get_vals = [latency["get_latency_us"][k] for k in labels]

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(9, 6))
    bars1 = ax.bar(x - width / 2, put_vals, width, label="PUT", color="#4CAF50", edgecolor="white")
    bars2 = ax.bar(x + width / 2, get_vals, width, label="GET", color="#2196F3", edgecolor="white")

    ax.set_ylabel("Latency (μs)", fontsize=13)
    ax.set_title("Per-Operation Latency Percentiles", fontsize=15, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=12)
    ax.legend(fontsize=12)

    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, h + max(put_vals + get_vals) * 0.02,
                    f"{h:.0f}μs", ha="center", va="bottom", fontsize=10)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3)

    path = os.path.join(RESULTS_DIR, "latency_distribution.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    ensure_results_dir()

    print(f"\n{'='*60}")
    print(f"  Distributed KV Store — Performance Benchmark")
    print(f"  Operations per test: {NUM_OPS:,}")
    print(f"{'='*60}\n")

    # Throughput
    print("[1/3] Running throughput benchmark...")
    storage_results = await bench_storage_throughput()
    ring_results = bench_ring_lookup_throughput()
    all_throughput = {**storage_results, **ring_results}

    print(f"  PUT:          {all_throughput['put_ops_sec']:>10,} ops/sec")
    print(f"  GET:          {all_throughput['get_ops_sec']:>10,} ops/sec")
    print(f"  DELETE:       {all_throughput['delete_ops_sec']:>10,} ops/sec")
    print(f"  Ring Lookup:  {all_throughput['ring_lookup_ops_sec']:>10,} ops/sec")

    # Latency
    print("\n[2/3] Running latency benchmark...")
    latency = await bench_latency()
    for op in ["put", "get"]:
        p = latency[f"{op}_latency_us"]
        print(f"  {op.upper():6s}  p50={p['p50']:>7.1f}μs  p95={p['p95']:>7.1f}μs  p99={p['p99']:>7.1f}μs")

    # Plots
    print("\n[3/3] Generating graphs...")
    plot_throughput(all_throughput)
    plot_latency(latency)

    print(f"\n{'='*60}")
    print("  Done! Graphs saved to benchmarks/results/")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
