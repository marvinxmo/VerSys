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
def run_node(node_id, num_nodes, initial_p, termination_handover):
    node = Zunder(node_id, num_nodes, initial_p, termination_handover)
    node.start()


def run_circle(num_nodes, initial_p=0.5, consecutive_quiet_handover=3):
    """Run an experiment with the given number of nodes"""
    print(f"\n=== Running experiment with {num_nodes} nodes ===")

    # Start the child processes for nodes 1 to n-1
    processes = []
    for i in range(1, num_nodes):

        # Use the run_node function in a subprocess
        p = Process(
            target=run_node, args=(i, num_nodes, initial_p, consecutive_quiet_handover)
        )
        p.start()
        processes.append(p)
        print(f"Started node {i} in subprocess (PID: {p.pid})")
        time.sleep(0.5)  # Give each process a little time to start

    # Create node 0 (the controller) in the main process
    node_0 = Zunder(0, num_nodes, initial_p, consecutive_quiet_handover)

    try:
        # Start node 0
        print("Starting node 0 in main process")
        node_0.start()

        # Terminate all child processes
        for i, proc in enumerate(processes):
            print(f"Terminating process for node {i+1}")
            proc.terminate()
            proc.join()

        # Collect statistics from node 0
        stats = {
            "num_nodes": num_nodes,
            "token_rounds": node_0.token_rounds,
            "fired_count": node_0.overall_fired_count,
            "round_times": node_0.round_times,
        }

        # Calculate round time statistics
        if node_0.round_times:
            stats["min_round_time"] = min(node_0.round_times)
            stats["avg_round_time"] = statistics.mean(node_0.round_times)
            stats["max_round_time"] = max(node_0.round_times)
        else:
            stats["min_round_time"] = 0
            stats["avg_round_time"] = 0
            stats["max_round_time"] = 0

        # print(stats)

        return stats

    except KeyboardInterrupt:
        print("Experiment interrupted by user")
        # Terminate all child processes
        for proc in processes:
            proc.terminate()
        return None


def main():
    parser = argparse.ArgumentParser(description="Token Ring with Fireworks")
    parser.add_argument(
        "--total_nodes", type=int, default=8, help="Total number of nodes in the ring"
    )
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
        "--experimental_mode",
        type=bool,
        default=False,
        help="Run experiments with increasing number of nodes",
    )

    args = parser.parse_args()

    if args.experimental_mode:
        # Run experiments with increasing number of nodes
        results = []
        node_counts = [2, 4, 6, 128]

        try:
            for n in node_counts:
                print(f"\nRunning experiment with {n} nodes...")

                stats = run_circle(n, args.initial_p, args.consecutive_quiet_rounds)

                if stats:
                    results.append(stats)
                    print(f"\nExperiment with {n} nodes completed:")
                    print(f"Total token rounds: {stats['token_rounds']}")
                    print(f"Total multicasts: {stats['fired_count']}")
                    print(
                        f"Round times (min/avg/max): {stats['min_round_time']:.6f}s / "
                        f"{stats['avg_round_time']:.6f}s / {stats['max_round_time']:.6f}s"
                    )
                else:
                    print(f"Experiment with {n} nodes failed")
                    break

            # Save results to a file
            with open("token_ring_results.json", "w") as f:
                json.dump(results, f, indent=2)

            print(
                "\nAll experiments completed. Results saved to token_ring_results.json"
            )

            # Print summary
            print("\n=== Experiment Summary ===")
            print(
                "Nodes | Token Rounds | Multicasts | Min Round (ms) | Avg Round (ms) | Max Round (ms)"
            )
            print("-" * 90)
            for result in results:
                print(
                    f"{result['num_nodes']:5d} | {result['token_rounds']:12d} | {result['fired_count']:9d} | "
                    f"{result['min_round_time']*1000:13.2f} | {result['avg_round_time']*1000:13.2f} | "
                    f"{result['max_round_time']*1000:13.2f}"
                )

        except KeyboardInterrupt:
            print("\nExperiments interrupted by user")
    else:
        # Run a defined number of nodes in a circle
        try:
            result = run_circle(
                args.total_nodes, args.initial_p, args.consecutive_quiet_rounds
            )

            if result:
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
            else:
                print(f"Experiment with {args.total_nodes} nodes failed")
        except KeyboardInterrupt:
            print(f"Node {args.node_id} interrupted by user")


if __name__ == "__main__":
    main()
