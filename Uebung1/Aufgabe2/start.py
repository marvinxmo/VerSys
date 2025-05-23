# Add necessary imports
import json
import numpy as np
import time
import sys
import argparse
import statistics
import subprocess
import os
import threading
import questionary as q
import socket  # For get_local_ip in main

from Zunder import Zunder, get_local_ip  # Import get_local_ip if it's in Zunder.py


def run_node_thread(
    port, is_host, host_ip=None, initial_p=0.5, termination_handover=3, expected_nodes=1
):
    """Runs a Zunder node in a separate thread."""
    try:
        node = Zunder(
            port=port,
            is_host=is_host,
            host_ip=host_ip,
            initial_p=initial_p,
            max_consecutive_handover=termination_handover,
            expected_nodes=expected_nodes,
        )
        node.run()  # This will block until the node's simulation ends or lobby fails
    except Exception as e:
        print(
            f"Error running node (host: {is_host}, IP: {host_ip if host_ip else 'N/A'}): {e}"
        )


def main():
    print("Welcome to the Networked Zunder Token Ring!")

    lobby_port_str = q.text(
        "Enter the LOBBY port number (e.g., 50000): ",
        validate=lambda x: (x.isdigit() and 1024 <= int(x) <= 65535)
        or "Please enter a valid port number (1024-65535).",
    ).ask()
    if not lobby_port_str:
        print("No port entered. Exiting.")
        sys.exit(1)
    lobby_port = int(lobby_port_str)

    my_ip = get_local_ip()
    print(f"Your local IP address appears to be: {my_ip}")
    # print(f"Attempting to check if a host is already running on port {lobby_port}...")

    is_host_already = False
    try:
        # Try to connect to see if a host is listening
        # This is a quick check, not a foolproof method for all network configurations
        # but good enough for typical local networks.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)  # Short timeout
            # We try to connect to localhost first, then ask user if they are host
            # This is because another process on *this* machine could be the host.
            try:
                s.connect(("127.0.0.1", lobby_port))
                is_host_already = True
                print(
                    f"A Zunder host seems to be running on this machine ({my_ip}:{lobby_port})."
                )
            except (ConnectionRefusedError, socket.timeout):
                # If localhost fails, it doesn't mean no host exists on the network.
                # The user will clarify their role.
                pass
    except Exception as e:
        print(
            f"Could not check for existing host due to: {e}. Assuming no host on this machine."
        )

    if is_host_already:
        role = q.select(
            "A Zunder instance might be running on this machine. What do you want to do?",
            choices=[
                {
                    "name": f"Join as a client to the host on this machine ({my_ip})",
                    "value": "client_local",
                },
                {
                    "name": "Join as a client to a host on a DIFFERENT machine",
                    "value": "client_remote",
                },
                {
                    "name": "Attempt to start a new host anyway (might fail if port is truly in use)",
                    "value": "host_anyway",
                },
                {"name": "Exit", "value": "exit"},
            ],
        ).ask()
    else:
        role = q.select(
            "Are you starting a new Zunder network (Host) or joining an existing one (Client)?",
            choices=[
                {"name": "Start as Host", "value": "host"},
                {
                    "name": "Join as Client",
                    "value": "client_remote",
                },  # Default to remote client if no local host detected
                {"name": "Exit", "value": "exit"},
            ],
        ).ask()

    if not role or role == "exit":
        print("Exiting.")
        sys.exit(0)

    if role == "host" or role == "host_anyway":
        print("\n--- Host Configuration ---")
        initial_p_str = q.text(
            "Enter the initial probability of firing (0.0 to 1.0): ",
            default="0.5",
            validate=lambda x: (
                (
                    x.replace(".", "", 1).isdigit()
                    and x.count(".") < 2
                    and 0.0 <= float(x) <= 1.0
                )
                or "Please enter a valid float between 0.0 and 1.0."
            ),
        ).ask()
        initial_p = float(initial_p_str) if initial_p_str else 0.5

        term_handover_str = q.text(
            "Enter maximum consecutive misfires (e.g., 3): ",
            default="5",
            validate=lambda x: (x.isdigit() and int(x) > 0)
            or "Please enter a positive integer.",
        ).ask()
        term_handover = int(term_handover_str) if term_handover_str else 5

        expected_nodes_str = q.text(
            "Enter total number of nodes expected (including this host, min 1): ",
            default="2",
            validate=lambda x: (x.isdigit() and int(x) >= 2)
            or "Please enter an integer >= 2.",
        ).ask()
        expected_nodes = int(expected_nodes_str) if expected_nodes_str else 2

        print(
            f"Starting Host on port {lobby_port} with p={initial_p}, max_consecutive_misfires={term_handover}, n={expected_nodes}"
        )
        print("For Joiners:")
        print(f"    IP: {my_ip}")
        print(f"    Port: {lobby_port}")

        # Run host in the main thread or a new thread if you want main to do other things
        run_node_thread(
            port=lobby_port,
            is_host=True,
            initial_p=initial_p,
            termination_handover=term_handover,
            expected_nodes=expected_nodes,
        )

    elif role == "client_local" or role == "client_remote":
        host_ip_to_join = get_local_ip()
        if role == "client_remote":
            host_ip_to_join = q.text(
                "Enter the Host's IP address: ",
                validate=lambda x: (
                    len(x.split(".")) == 4
                    and all(p.isdigit() and 0 <= int(p) <= 255 for p in x.split("."))
                )
                or "Enter a valid IPv4 address.",
            ).ask()
            if not host_ip_to_join:
                print("No host IP entered. Exiting.")
                sys.exit(1)

        print(f"Attempting to join Host at {host_ip_to_join}:{lobby_port} as a Client.")
        # Run client in the main thread or a new thread
        run_node_thread(
            port=lobby_port,
            is_host=False,
            host_ip=host_ip_to_join,
            # Client does not set p, term_handover, expected_nodes; it gets them from host
        )

    # The run_node_thread will block, so the script effectively waits here
    # until the node (host or client) finishes its execution.
    print("Zunder program finished or was interrupted.")


if __name__ == "__main__":
    main()
