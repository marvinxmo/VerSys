#!/usr/bin/env python3
import socket
import time
import threading
import random
import sys
import argparse
import subprocess
import statistics
import struct
from typing import List, Tuple

# Configuration constants
MULTICAST_GROUP = "224.3.29.71"
MULTICAST_PORT = 10000
BASE_PORT = 20000
TOKEN = b"STREICHHOLZ"
TERMINATE = b"TERMINATE"


class Zunder:
    def __init__(
        self,
        node_id: int,
        total_nodes: int,
        initial_p: float,
        termination_handover: int = 5,
    ):

        self.node_id = node_id
        self.total_nodes = total_nodes
        self.p = initial_p
        self.termination_handover = termination_handover
        self.overall_fired_count = 0

        if self.node_id == 0:
            # Statistics tracking
            self.token_rounds = 0
            self.fired_count = 0
            self.round_times = []
            self.last_token_time = 0

        # Set up sockets (recv_socket, send_socket, multicast_socket, multicast_recv_socket)
        self._setup_sockets()

        # Track if this node is currently active
        self.active = True

        print(f"Node {self.node_id} initialized with p={self.p}")

    def _setup_sockets(self):
        # Socket for receiving token from previous node
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv_socket.bind(("127.0.0.1", BASE_PORT + self.node_id))
        self.recv_socket.settimeout(30)  # 60 seconds timeout

        # Socket for sending token to next node
        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Socket for multicast (broadcast) messages
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_, 1)

        # Socket for receiving multicast messages
        self.multicast_recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.multicast_recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_recv_socket.bind(("", MULTICAST_PORT))

        # Join multicast group
        group = socket.inet_aton(MULTICAST_GROUP)
        mreq = struct.pack("4sL", group, socket.INADDR_ANY)
        self.multicast_recv_socket.setsockopt(
            socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq
        )
        self.multicast_recv_socket.settimeout(
            0.01
        )  # Short timeout for non-blocking checking

    def start(self):
        """Start the node: listen for messages and handle token passing"""
        # Start listening for multicast messages in a separate thread
        self.multicast_thread = threading.Thread(target=self._listen_for_multicast)
        self.multicast_thread.daemon = True
        self.multicast_thread.start()

        # If this is node 0, initiate the token passing
        if self.node_id == 0:
            print(f"= Node {self.node_id} initiating token passing =")
            time.sleep(2)  # Give other nodes time to start
            self.last_token_time = time.time()

            msg = TOKEN + b":0"
            self.forward(msg)

        # Set timeout to allow checking active status periodically
        self.recv_socket.settimeout(1)

        # Main loop to receive token
        while self.active:
            try:
                data, addr = self.recv_socket.recvfrom(1024)
                self._handle_message(data)
            except socket.timeout:
                # This is just a timeout to check active status, not an error
                continue
            except Exception as e:
                print(f"Node {self.node_id} encountered error: {e}")
                self.active = False

        # Cleanup
        print(f"Node {self.node_id} is shutting down...")
        self.recv_socket.close()
        self.send_socket.close()
        self.multicast_socket.close()
        self.multicast_recv_socket.close()
        print(f"Node {self.node_id} terminated")

    def _handle_message(self, data: bytes):
        """Handle received messages"""

        if data.startswith(TOKEN):

            # Received token
            current_time = time.time()
            consecutive_quite_handover = 0
            consecutive_quite_handover = int(data.decode().split(":")[-1])

            if self.node_id == 0:
                # Node 0 is responsible for collecting statistics
                # Statistics tracking

                self.round_times.append(current_time - self.last_token_time)
                self.last_token_time = current_time
                self.token_rounds += 1

            # Decide whether to fire a rocket
            should_fire = random.random() < self.p

            if should_fire:
                self.fire()
                consecutive_quite_handover = 0
                time.sleep(0.5)  # Give time for other Nodes to watch the firework
            else:
                consecutive_quite_handover += 1
                print(f"Node {self.node_id} did not fire this round p={self.p:.6f}")

            # Check termination condition
            if self.termination_handover <= consecutive_quite_handover:
                print(
                    f"Node {self.node_id} initiating termination after {str(consecutive_quite_handover)} quiet rounds"
                )
                self.multicast(TERMINATE)
                time.sleep(0.5)  # Give time for multicast to propagate
                self.active = False
                return

            # Reduce probability for next round
            self.p = self.p / 2

            # Forward token to next node
            msg = TOKEN + b":" + str(consecutive_quite_handover).encode()
            self.forward(msg)

        elif data == TERMINATE:
            print(f"Node {self.node_id} received termination signal")
            self.active = False

    def forward(self, message):
        """Forward the token to the next node in the ring"""
        next_id = (self.node_id + 1) % self.total_nodes
        next_address = ("127.0.0.1", BASE_PORT + next_id)

        try:
            self.send_socket.sendto(message, next_address)
        except Exception as e:
            print(f"Node {self.node_id} failed to forward token: {e}")

    def fire(self):
        """Fire a rocket (send a multicast to all nodes)"""
        print(f"Node {self.node_id} firing rocket! (p={self.p:.6f})")
        message = f"ROCKET_FROM_{self.node_id}".encode()
        self.multicast(message)

    def multicast(self, message):
        """Send a multicast message to all nodes"""
        try:
            self.multicast_socket.sendto(message, (MULTICAST_GROUP, MULTICAST_PORT))
        except Exception as e:
            print(f"Node {self.node_id} failed to send multicast: {e}")

    def _listen_for_multicast(self):
        """Listen for multicast messages in a separate thread"""
        while self.active:
            try:
                data, addr = self.multicast_recv_socket.recvfrom(1024)
                # Update last activity time when we receive any multicast message
                self.last_activity_time = time.time()

                if data.startswith(b"ROCKET_FROM_"):
                    self.overall_fired_count += 1
                    sender = int(data.decode().split("_")[-1])
                    if sender != self.node_id:
                        print(f"Node {self.node_id} saw rocket from Node {sender}! 🎆")
                elif data == TERMINATE:
                    print(f"Node {self.node_id} received termination via multicast")
                    self.active = False
                    break
            except socket.timeout:
                # This is expected due to the non-blocking socket
                pass
            except Exception as e:
                print(f"Node {self.node_id} multicast error: {e}")

            if not self.active:
                break
            time.sleep(0.01)
