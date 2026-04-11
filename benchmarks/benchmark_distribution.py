"""
Benchmark: Key Distribution & Rebalance Impact

Measures:
  1. How evenly keys are distributed across nodes (with virtual nodes)
  2. What % of keys move when a 4th node is added (vs modulo hashing)

Usage:
    python benchmarks/benchmark_distribution.py

Output:
    benchmarks/results/key_distribution.png
    benchmarks/results/rebalance_impact.png
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.cluster.consistent_hash import ConsistentHashRing

NUM_KEYS = 100_000
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def ensure_results_dir():
    os.makedirs(RESULTS_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Key distribution analysis
# ---------------------------------------------------------------------------

def measure_distribution(num_nodes: int = 3, num_vnodes: int = 150) -> dict:
    """Count how many of NUM_KEYS each node owns."""
    ring = ConsistentHashRing(num_vnodes=num_vnodes)
    for i in range(num_nodes):
        ring.add_node(f"node-{i}")

    counts = {f"node-{i}": 0 for i in range(num_nodes)}
    for i in range(NUM_KEYS):
        owner = ring.get_node(f"key:{i}")
        counts[owner] += 1

    ideal = NUM_KEYS / num_nodes
    variance_pct = {}
    for node, count in counts.items():
        variance_pct[node] = round(abs(count - ideal) / ideal * 100, 2)

    return {
        "counts": counts,
        "ideal": int(ideal),
        "max_variance_pct": max(variance_pct.values()),
        "variance_per_node": variance_pct,
    }


# ---------------------------------------------------------------------------
# Rebalance impact: consistent hashing vs modulo
# ---------------------------------------------------------------------------

def measure_rebalance() -> dict:
    """Compare key movement when adding a 4th node: consistent hash vs modulo."""

    # --- Consistent hashing ---
    ring3 = ConsistentHashRing(num_vnodes=150)
    for i in range(3):
        ring3.add_node(f"node-{i}")

    ring4 = ConsistentHashRing(num_vnodes=150)
    for i in range(4):
        ring4.add_node(f"node-{i}")

    moved_ch = 0
    for i in range(NUM_KEYS):
        key = f"key:{i}"
        if ring3.get_node(key) != ring4.get_node(key):
            moved_ch += 1

    # --- Modulo hashing ---
    import hashlib
    def modulo_hash(key, n):
        h = int(hashlib.sha256(key.encode()).hexdigest()[:16], 16)
        return f"node-{h % n}"

    moved_mod = 0
    for i in range(NUM_KEYS):
        key = f"key:{i}"
        if modulo_hash(key, 3) != modulo_hash(key, 4):
            moved_mod += 1

    return {
        "consistent_hash": {
            "keys_moved": moved_ch,
            "pct": round(moved_ch / NUM_KEYS * 100, 1),
            "theoretical_min_pct": round(100 / 4, 1),  # 1/N_new
        },
        "modulo_hash": {
            "keys_moved": moved_mod,
            "pct": round(moved_mod / NUM_KEYS * 100, 1),
        },
        "improvement_factor": round(moved_mod / max(moved_ch, 1), 1),
    }


# ---------------------------------------------------------------------------
# Plot generation
# ---------------------------------------------------------------------------

def plot_distribution(dist: dict):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("  [skip] matplotlib not installed")
        return

    nodes = list(dist["counts"].keys())
    counts = list(dist["counts"].values())
    ideal = dist["ideal"]

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ["#4CAF50", "#2196F3", "#FF9800"]
    bars = ax.bar(nodes, counts, color=colors[:len(nodes)], edgecolor="white", linewidth=1.5)
    ax.axhline(y=ideal, color="#F44336", linestyle="--", linewidth=2, label=f"Ideal ({ideal:,})")

    for bar, count in zip(bars, counts):
        diff = count - ideal
        sign = "+" if diff >= 0 else ""
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 200,
                f"{count:,}\n({sign}{diff:,})", ha="center", va="bottom", fontsize=11)

    ax.set_ylabel("Keys Owned", fontsize=13)
    ax.set_title(f"Key Distribution Across 3 Nodes ({NUM_KEYS:,} keys, 150 vnodes)",
                 fontsize=14, fontweight="bold")
    ax.legend(fontsize=12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3)

    path = os.path.join(RESULTS_DIR, "key_distribution.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def plot_rebalance(rebal: dict):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("  [skip] matplotlib not installed")
        return

    labels = ["Consistent Hash", "Modulo Hash"]
    pcts = [rebal["consistent_hash"]["pct"], rebal["modulo_hash"]["pct"]]
    colors = ["#4CAF50", "#F44336"]

    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.bar(labels, pcts, color=colors, edgecolor="white", linewidth=1.5, width=0.5)

    ax.axhline(y=rebal["consistent_hash"]["theoretical_min_pct"],
               color="#FF9800", linestyle="--", linewidth=2,
               label=f"Theoretical min ({rebal['consistent_hash']['theoretical_min_pct']}%)")

    for bar, pct in zip(bars, pcts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{pct}%", ha="center", va="bottom", fontsize=14, fontweight="bold")

    ax.set_ylabel("Keys Moved (%)", fontsize=13)
    ax.set_title("Key Movement When Adding 4th Node (3→4)",
                 fontsize=14, fontweight="bold")
    ax.set_ylim(0, 100)
    ax.legend(fontsize=12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3)

    path = os.path.join(RESULTS_DIR, "rebalance_impact.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ensure_results_dir()

    print(f"\n{'='*60}")
    print(f"  Key Distribution & Rebalance Benchmark")
    print(f"  Keys: {NUM_KEYS:,}")
    print(f"{'='*60}\n")

    # Distribution
    print("[1/2] Measuring key distribution...")
    dist = measure_distribution()
    for node, count in dist["counts"].items():
        var = dist["variance_per_node"][node]
        print(f"  {node}: {count:>7,} keys  (±{var}% from ideal)")
    print(f"  Max variance: {dist['max_variance_pct']}%")

    # Rebalance
    print("\n[2/2] Measuring rebalance impact (3→4 nodes)...")
    rebal = measure_rebalance()
    print(f"  Consistent hash: {rebal['consistent_hash']['pct']}% keys moved "
          f"(theoretical min: {rebal['consistent_hash']['theoretical_min_pct']}%)")
    print(f"  Modulo hash:     {rebal['modulo_hash']['pct']}% keys moved")
    print(f"  Improvement:     {rebal['improvement_factor']}x fewer keys moved")

    # Plots
    print("\nGenerating graphs...")
    plot_distribution(dist)
    plot_rebalance(rebal)

    print(f"\n{'='*60}")
    print("  Done!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
