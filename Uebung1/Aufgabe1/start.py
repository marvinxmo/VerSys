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
import questionary as q

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
        time.sleep(0.05)  # Give each process a little time to start

    time.sleep(15)
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


def get_inputs():
    """Get inputs from the user"""

    def validate_total_nodes(val):
        try:
            # Try to parse as int
            int(val)
            return True
        except ValueError:
            try:
                # Try to parse as a list of ints
                arr = json.loads(val)
                if isinstance(arr, list) and all(isinstance(x, int) for x in arr):
                    return True
                return "Please enter an integer or a list of integers (e.g., [2,4,6])"
            except Exception:
                return "Please enter an integer or a list of integers (e.g., [2,4,6])"

    total_nodes = q.text(
        "Enter the total number of nodes (int or int[]): ",
        validate=validate_total_nodes,
    ).ask()

    initial_p = q.text(
        "Enter the initial probability of firing (float): ",
        validate=lambda x: (
            (
                x.replace(".", "", 1).isdigit()
                and x.count(".") < 2
                and float(x) >= 0
                and float(x) <= 1
            )
            or "Please enter a valid float."
        ),
    ).ask()

    consecutive_quiet_handover = q.text(
        "Enter the number of consecutive quiet handover before termination (int): ",
        validate=lambda x: (x.isdigit() or "Please enter a valid integer."),
    ).ask()

    # Convert inputs to appropriate types

    # Parse total_nodes as a list of integers, even if a single int is given
    if "[" in total_nodes:
        total_nodes = json.loads(total_nodes)
    else:
        total_nodes = [int(total_nodes)]

    initial_p = float(initial_p)
    consecutive_quiet_handover = int(consecutive_quiet_handover)

    return total_nodes, initial_p, consecutive_quiet_handover


def main():

    print("Welcome to the Token Ring Experiment!")

    # Get inputs from the user
    total_nodes, initial_p, consecutive_quiet_handover = get_inputs()

    results = []

    try:
        for n in total_nodes:
            if n < 2:
                print("Number of nodes must be at least 2.")
                sys.exit(1)

            print(f"\nRunning experiment with {n} nodes...")

            stats = run_circle(n, initial_p, consecutive_quiet_handover)

            if stats:
                results.append(stats)
                print(f"\nExperiment with {n} nodes completed:")
                print(f"Total token rounds: {stats['token_rounds']}")
                print(f"Total rockets fired: {stats['fired_count']}")
                print(
                    f"Round times (min/avg/max): {stats['min_round_time']:.6f}s / "
                    f"{stats['avg_round_time']:.6f}s / {stats['max_round_time']:.6f}s"
                )
            else:
                print(f"Experiment with {n} nodes failed")
                break

            # Save results to a file
            # with open("token_ring_results.json", "w") as f:
            #     json.dump(results, f, indent=2)

            # print(
            #     "\nAll experiments completed. Results saved to token_ring_results.json"
            # )

            # Print summary
        print("\n=== Summary ===")
        print(
            f"{'Nodes':>7} | {'Token Rounds':>13} | {'Rockets fired':>14} | {'Min Round (ms)':>15} | {'Avg Round (ms)':>15} | {'Max Round (ms)':>15}"
        )
        print(
            "------- | ------------- | -------------- | --------------- | --------------- | ---------------"
        )
        for result in results:
            print(
                f"{result['num_nodes']:7d} | "
                f"{result['token_rounds']:13d} | "
                f"{result['fired_count']:14d} | "
                f"{result['min_round_time']*1000:15.2f} | "
                f"{result['avg_round_time']*1000:15.2f} | "
                f"{result['max_round_time']*1000:15.2f}"
            )

    except KeyboardInterrupt:
        print("\nExperiments interrupted by user")


if __name__ == "__main__":
    main()
