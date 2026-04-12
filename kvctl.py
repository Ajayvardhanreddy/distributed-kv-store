#!/usr/bin/env python3
"""
kvctl — CLI client for the distributed KV store.

Usage:
    kvctl get <key>                  Read a key
    kvctl put <key> <value>          Write a key
    kvctl delete <key>               Delete a key
    kvctl health                     Cluster health status
    kvctl stats                      Node statistics
    kvctl bench [--ops N]            Quick throughput benchmark

Examples:
    kvctl put user:1 alice
    kvctl get user:1
    kvctl health
    kvctl bench --ops 1000
"""
import argparse
import json
import sys
import time
from urllib.parse import urljoin

try:
    import httpx
except ImportError:
    print("Error: httpx is required. Install with: pip install httpx")
    sys.exit(1)

DEFAULT_URL = "http://localhost:8000"


def cmd_get(args):
    """GET /kv/{key}"""
    url = urljoin(args.url + "/", f"kv/{args.key}")
    r = httpx.get(url, timeout=5.0)
    if r.status_code == 200:
        data = r.json()
        print(f"{data['key']} = {data['value']}  (version={data.get('version', '?')})")
    elif r.status_code == 404:
        print(f"(nil) — key '{args.key}' not found")
        sys.exit(1)
    else:
        print(f"Error {r.status_code}: {r.text}")
        sys.exit(1)


def cmd_put(args):
    """PUT /kv/{key}"""
    url = urljoin(args.url + "/", f"kv/{args.key}")
    payload = {"key": args.key, "value": args.value}
    r = httpx.put(url, json=payload, timeout=5.0)
    if r.status_code == 200:
        data = r.json()
        print(f"OK — {data['key']} = {data['value']}  (version={data.get('version', '?')})")
    else:
        print(f"Error {r.status_code}: {r.text}")
        sys.exit(1)


def cmd_delete(args):
    """DELETE /kv/{key}"""
    url = urljoin(args.url + "/", f"kv/{args.key}")
    r = httpx.delete(url, timeout=5.0)
    if r.status_code == 200:
        print(f"Deleted '{args.key}'")
    elif r.status_code == 404:
        print(f"Key '{args.key}' not found")
    else:
        print(f"Error {r.status_code}: {r.text}")
        sys.exit(1)


def cmd_health(args):
    """GET /cluster/health"""
    url = urljoin(args.url + "/", "cluster/health")
    r = httpx.get(url, timeout=5.0)
    data = r.json()

    print(f"{'Node':<12} {'Status':<14} {'Keys':>6}")
    print("─" * 34)
    for node_id, info in sorted(data.get("cluster", {}).items()):
        status = info.get("status", "unknown")
        keys = info.get("local_keys", "?")
        icon = "🟢" if status == "healthy" else "🔴"
        print(f"{icon} {node_id:<10} {status:<14} {keys:>6}")


def cmd_stats(args):
    """GET /stats"""
    url = urljoin(args.url + "/", "stats")
    r = httpx.get(url, timeout=5.0)
    data = r.json()
    print(json.dumps(data, indent=2))


def cmd_bench(args):
    """Quick throughput benchmark against a running cluster."""
    n = args.ops
    base = args.url

    # --- PUT benchmark ---
    start = time.perf_counter()
    for i in range(n):
        r = httpx.put(
            f"{base}/kv/bench:{i}",
            json={"key": f"bench:{i}", "value": f"val-{i}"},
            timeout=5.0,
        )
        if r.status_code != 200:
            print(f"PUT failed at i={i}: {r.status_code}")
            sys.exit(1)
    put_elapsed = time.perf_counter() - start
    put_ops = n / put_elapsed

    # --- GET benchmark ---
    start = time.perf_counter()
    for i in range(n):
        r = httpx.get(f"{base}/kv/bench:{i}", timeout=5.0)
    get_elapsed = time.perf_counter() - start
    get_ops = n / get_elapsed

    # --- Cleanup ---
    for i in range(n):
        httpx.delete(f"{base}/kv/bench:{i}", timeout=5.0)

    print(f"Benchmark ({n} ops)")
    print(f"  PUT: {put_ops:,.0f} ops/sec  ({put_elapsed:.2f}s)")
    print(f"  GET: {get_ops:,.0f} ops/sec  ({get_elapsed:.2f}s)")


def main():
    parser = argparse.ArgumentParser(
        prog="kvctl",
        description="CLI client for the distributed KV store",
    )
    parser.add_argument(
        "--url", default=DEFAULT_URL,
        help=f"Base URL of any cluster node (default: {DEFAULT_URL})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # get
    p_get = sub.add_parser("get", help="Read a key")
    p_get.add_argument("key")

    # put
    p_put = sub.add_parser("put", help="Write a key")
    p_put.add_argument("key")
    p_put.add_argument("value")

    # delete
    p_del = sub.add_parser("delete", help="Delete a key")
    p_del.add_argument("key")

    # health
    sub.add_parser("health", help="Cluster health status")

    # stats
    sub.add_parser("stats", help="Node statistics")

    # bench
    p_bench = sub.add_parser("bench", help="Quick throughput benchmark")
    p_bench.add_argument("--ops", type=int, default=100, help="Number of operations")

    args = parser.parse_args()

    dispatch = {
        "get": cmd_get,
        "put": cmd_put,
        "delete": cmd_delete,
        "health": cmd_health,
        "stats": cmd_stats,
        "bench": cmd_bench,
    }

    try:
        dispatch[args.command](args)
    except httpx.ConnectError:
        print(f"Error: Could not connect to {args.url}")
        print("Is the cluster running? Try: docker-compose up --build")
        sys.exit(1)
    except httpx.TimeoutException:
        print(f"Error: Request to {args.url} timed out")
        sys.exit(1)


if __name__ == "__main__":
    main()
