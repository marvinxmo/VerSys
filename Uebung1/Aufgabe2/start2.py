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

from ZunderNode2 import ZunderNode


def get_inputs():
    """Get inputs from the user"""

    port = q.text(
        "Enter the port: ",
        validate=lambda x: (
            (x.isdigit() and 1024 <= int(x) <= 65535)
            or "Please enter a valid int between 1024 and 65535."
        ),
    ).ask()

    port = int(port)

    return port


def main():

    print("Welcome to the Token Ring Experiment!")

    # Get inputs from the user
    port = get_inputs()

    results = []

    try:

        node = ZunderNode(port)
        node.start()

        if node.node_id == 0:
            print("You are Host. Wait for others to join or press enter to start ring.")
            input("Press enter to start the ring...")

            # Collect statistics from node 0
            n = len(node.lobby.node_addresses)
            stats = {
                "num_nodes": n,
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

            if stats:
                results.append(stats)
                print(f"\nExperiment with {n} nodes completed:")
                print(f"Total token rounds: {stats['token_rounds']}")
                print(f"Total rockets fired: {stats['fired_count']}")
                print(
                    f"Round times (min/avg/max): {stats['min_round_time']:.6f}s / "
                    f"{stats['avg_round_time']:.6f}s / {stats['max_round_time']:.6f}s"
                )

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
        print("\n Interrupted by user")


if __name__ == "__main__":
    main()
