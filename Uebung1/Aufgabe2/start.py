# Add necessary imports
import json
import numpy as np
import time
import sys
import argparse
import statistics
import subprocess
import os
from multiprocessing import Process

from Zunder import Zunder


# Define a function to run a node
def run_node(node_id, num_nodes, initial_p, termination_handover, node_addresses):
    node = Zunder(node_id, num_nodes, initial_p, termination_handover, node_addresses)
    print(f"Starting node {node_id}...")
    node.start()

    # Collect statistics from node 0
    stats = {
        "num_nodes": num_nodes,
        "token_rounds": node.token_rounds,
        "fired_count": node.overall_fired_count,
        "round_times": node.round_times,
    }

    # Calculate round time statistics
    if node.round_times:
        stats["min_round_time"] = min(node.round_times)
        stats["avg_round_time"] = statistics.mean(node.round_times)
        stats["max_round_time"] = max(node.round_times)
    else:
        stats["min_round_time"] = 0
        stats["avg_round_time"] = 0
        stats["max_round_time"] = 0

    return stats


def main():
    parser = argparse.ArgumentParser(description="Token Ring with Fireworks")

    parser.add_argument("--node_id", type=float, help="Node ID")
    parser.add_argument(
        "--initial_p", type=float, default=0.5, help="Initial probability of firing"
    )
    parser.add_argument(
        "--consecutive_quiet_rounds",
        type=int,
        default=5,
        help="Number of consecutive quiet rounds before termination",
    )
    parser.add_argument(
        "--my_ip",
        type=str,
        default="",
        help="IP address of this machine on the network",
    )
    parser.add_argument(
        "--node_addresses",
        type=str,
        help="Comma-separated list of IP addresses for all nodes in the ring",
    )

    args = parser.parse_args()

    node_addresses = args.node_addresses.split(",")
    num_nodes = len(node_addresses)
    print("id", args.node_id)
    print("num_nodes", num_nodes)
    print("initial_p", args.initial_p)
    print("consecutive_quiet_rounds", args.consecutive_quiet_rounds)
    print("node_addresses", node_addresses)

    result = run_node(
        int(args.node_id),
        num_nodes,
        args.initial_p,
        args.consecutive_quiet_rounds,
        node_addresses,
    )

    # Print summary
    print("\n=== Experiment Summary ===")
    print(
        "Nodes | Token Rounds | Multicasts | Min Round (ms) | Avg Round (ms) | Max Round (ms)"
    )
    print("-" * 90)

    print(
        f"{result['num_nodes']:5d} | {result['token_rounds']:12d} | {result['fired_count']:9d} | "
        f"{result['min_round_time']*1000:13.2f} | {result['avg_round_time']*1000:13.2f} | "
        f"{result['max_round_time']*1000:13.2f}"
    )


if __name__ == "__main__":
    main()
