import socket
import time
import threading
import random
import sys
import argparse
import subprocess
import statistics
import struct
import json
from typing import List, Tuple, Dict, Optional

# Configuration constants
MULTICAST_GROUP = "224.3.29.71"
DM_PORT = 20000

TOKEN = b"STREICHHOLZ"
DISCOVERY_REQUEST = b"DISCOVERY_REQUEST"
DISCOVERY_RESPONSE = b"DISCOVERY_RESPONSE"
LOBBY_UPDATE = b"LOBBY_UPDATE"
JOIN_REJECTED = b"JOIN_REJECTED"

TERMINATE = b"TERMINATE"


class Lobby:
    def __init__(self, initial_p: float, termination_handover: int):
        self.initial_p = initial_p
        self.termination_handover = termination_handover
        self.node_addresses = {}  # Dict[node_id, ip_address]
        self.host_id = None
        self.ring_started = False

    def add_node(self, node_id: int, ip_address: str):
        self.node_addresses[node_id] = ip_address
        if self.host_id is None:
            self.host_id = node_id

    def remove_node(self, node_id: int):
        if node_id in self.node_addresses:
            del self.node_addresses[node_id]
            # If the host left, elect a new one
            if node_id == self.host_id and self.node_addresses:
                self.host_id = min(self.node_addresses.keys())

    def get_predecessor(self, node_id: int) -> int | None:
        if not self.node_addresses:
            return None

        node_ids = sorted(self.node_addresses.keys())
        if node_id not in node_ids:
            return None

        idx = node_ids.index(node_id)
        return node_ids[idx - 1] if idx > 0 else node_ids[-1]

    def to_json(self) -> str:
        return json.dumps(
            {
                "initial_p": self.initial_p,
                "termination_handover": self.termination_handover,
                "node_addresses": self.node_addresses,
                "host_id": self.host_id,
                "ring_started": self.ring_started,
            }
        )

    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        lobby = cls(data["initial_p"], data["termination_handover"])
        lobby.node_addresses = {int(k): v for k, v in data["node_addresses"].items()}
        lobby.host_id = data["host_id"]
        lobby.ring_started = data["ring_started"]
        return lobby


class ZunderNode:
    def __init__(
        self,
        channel: int,
    ):

        # In case Node becomes Host, the p and termination_handover of the node will be used to create the lobby
        self.p: float = 0.5  # Probability of firing a rocket
        self.termination_handover: int = 5  # Number of quiet rounds before termination

        # node_id will be assigned during registration process
        self.node_id: int

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # Doesn't need to be reachable
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
        finally:
            s.close()

        print(f"Local IP: {local_ip}")

        self.my_ip = local_ip
        self.channel: int = channel

        # Set up sockets (recv_socket, send_socket, multicast_socket, multicast_recv_socket)
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_socket.bind(("", channel))

        self.multicast_recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.multicast_recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_recv_socket.bind(("", channel))

        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv_socket.bind((self.my_ip, DM_PORT))
        self.recv_socket.settimeout(1)  # 1 second timeout for receiving messages

        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Track if this node is currently active
        self.active = True

        # Statistics tracking
        self.token_rounds = 0
        self.fired_count = 0
        self.round_times = []

        # Lobby management
        self.lobby_mode = True
        self.lobby: Lobby
        self.is_host = False
        self.successor_id: int

        print(f"Node initialized with IP {self.my_ip}")

    def send_discovery_request(self):
        """Send a discovery request to the multicast group"""

        self.multicast(DISCOVERY_REQUEST)
        print(f"Node announced itself via multicast")
        self.multicast_socket.settimeout(10)  # Wait for 10 seconds for a response

        try:
            data, addr = self.multicast_socket.recvfrom(1024)

            if data.startswith(LOBBY_UPDATE):
                # Parse response and join the lobby
                lobby_json = data[len(LOBBY_UPDATE) + 1 :]
                self.lobby = Lobby.from_json(lobby_json.decode())

                # Check my assigned node_id by reverse lookup of node_addresses dict
                self.node_id = [
                    key
                    for key in self.lobby.node_addresses.items()
                    if key[1] == self.my_ip
                ][0][0]

                print(f"Node was assigned ID {self.node_id}, host={self.lobby.host_id}")

            elif data.startswith(JOIN_REJECTED):
                reason = data[len(JOIN_REJECTED) + 1 :].decode()
                print(f"Join request was rejected: {reason}")
                self.active = False
                return

            else:
                # Unexpected message, claim host
                print(f"Unexpected message received: {data}")
                self.active = False

        except socket.timeout:

            # No answer, claim host role with node_id 0
            print(f"No response received in 10s, claiming host role with ID 0")
            self.node_id = 0

            # Create a new lobby as host
            self.lobby = Lobby(self.p, self.termination_handover)
            self.lobby.add_node(self.node_id, self.my_ip)
            self.is_host = True
            self.predecessor_id = self.node_id  # Self-reference for single node
            print(
                f"Node created new lobby as host (ID {self.node_id}) and settings p={self.p}, termination={self.termination_handover}"
            )

    def listen_for_multicast(self):
        """Listen for multicast messages in a separate thread"""
        while self.active:
            try:
                data, addr = self.multicast_recv_socket.recvfrom(1024)

                if data.startswith(DISCOVERY_REQUEST):

                    # Handle discovery request if we're the host
                    if self.is_host:

                        if self.lobby_mode:

                            # Assign next available node_id (max existing + 1)
                            new_node_id = max(self.lobby.node_addresses.keys()) + 1
                            self.lobby.node_addresses[new_node_id] = addr[0]
                            print(
                                "Added new Node to lobby: {new_node_id} with IP {addr[0]}"
                            )

                            message = (
                                LOBBY_UPDATE + b":" + self.lobby.to_json().encode()
                            )

                            try:
                                self.multicast(message)
                            except Exception as e:
                                print(
                                    f"Node {self.node_id} failed to send multicast: {e}"
                                )

                        if not self.lobby_mode:
                            message = (
                                JOIN_REJECTED
                                + b":"
                                + "Ring Simulation already started".encode()
                            )

                            try:
                                self.send_socket.sendto(message, (addr[0], DM_PORT))
                            except Exception as e:
                                print(
                                    f"Node {self.node_id} failed to send message: {e}"
                                )

                elif data.startswith(LOBBY_UPDATE):
                    # Update our lobby information
                    if not self.is_host:  # Only non-hosts receive updates
                        lobby_json = data[len(LOBBY_UPDATE) + 1 :]
                        self.lobby = Lobby.from_json(lobby_json.decode())
                        print(f"Received lobby update")
                        self.handle_lobby_update()

                elif data.startswith(b"ROCKET_FROM_"):
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

    def handle_lobby_update(self):
        # Check my assigned node_id by reverse lookup of node_addresses dict
        self.node_id = [
            key for key in self.lobby.node_addresses.items() if key[1] == self.my_ip
        ][0][0]

        self.p = self.lobby.initial_p
        self.termination_handover = self.lobby.termination_handover

        next_id = (self.node_id + 1) % len(self.lobby.node_addresses)
        self.successor_id = next_id

        print(
            f"Lobby updated: Node ID {self.node_id}, p={self.p}, termination={self.termination_handover}"
        )
        print(f"Participating nodes: {self.lobby.node_addresses}")
        print(f"Successor ID: {self.successor_id}")

    def handle_private_message(self, data: bytes):
        """Handle received messages"""

        if self.lobby_mode:
            # Handle lobby-related messages
            if data.startswith(JOIN_REJECTED):
                reason = data[len(JOIN_REJECTED) + 1 :].decode()
                print(f"Node {self.node_id}'s join request was rejected: {reason}")
                self.active = False
            return

        # Ring mode message handling
        if data.startswith(TOKEN):

            # Received token
            current_time = time.time()
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

        # Termination via multicast
        # elif data == TERMINATE:
        #     print(f"Node {self.node_id} received termination signal")
        #     self.active = False

    def start(self):
        """Start the node: register, join/create lobby, and participate in the ring"""
        # Register with the network
        self.send_discovery_request()

        if not self.active:
            return

        # Start listening for multicast messages in a separate thread
        self.multicast_thread = threading.Thread(target=self.listen_for_multicast)
        self.multicast_thread.daemon = True
        self.multicast_thread.start()

        if self.is_host:
            self.start_ring_thread = threading.Thread(target=self.start_ring)
            self.start_ring_thread.daemon = True

        # Set timeout to allow checking active status periodically
        self.recv_socket.settimeout(1)

        # Main loop to receive messages
        while self.active:
            try:
                data, addr = self.recv_socket.recvfrom(1024)
                self.handle_private_message(data)
            except socket.timeout:
                # This is just a timeout to check active status
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

    def start_ring(self):
        """Start the token ring process (host only)"""
        if not self.is_host:
            print(f"Node {self.node_id} is not the host and cannot start the ring")
            return

        print(f"Host node {self.node_id} starting the token ring")
        self.lobby.ring_started = True
        self.lobby_mode = False

        # If this is node 0, initiate the token passing
        if self.node_id == 0:
            print(f"= Node {self.node_id} initiating token passing =")
            time.sleep(2)  # Give other nodes time to transition to ring mode
            self.last_token_time = time.time()
            msg = TOKEN + b":0"
            self.forward(msg)

    def fire(self):
        """Fire a rocket (send a multicast to all nodes)"""
        print(f"Node {self.node_id} firing rocket! (p={self.p:.6f})")
        message = f"ROCKET_FROM_{self.node_id}".encode()
        self.multicast(message)
        self.overall_fired_count += 1
        if self.node_id == 0:
            self.fired_count += 1

    def forward(self, message):
        # Forward token to next node
        try:
            next_ip = self.lobby.node_addresses[self.successor_id]
            self.send_socket.sendto(message, (next_ip, DM_PORT))
        except Exception as e:
            print(f"Node {self.node_id} failed to forward token: {e}")

    def multicast(self, message):
        """Send a multicast message to all nodes"""
        try:
            self.multicast_socket.sendto(message, (MULTICAST_GROUP, self.channel))
        except Exception as e:
            print(f"Node {self.node_id} failed to send multicast: {e}")
