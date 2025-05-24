#!/usr/bin/env python3
import socket
import time
import threading
import random
import sys
import statistics
import json  # Added for peer list serialization
from typing import Optional  # Add Optional

# Configuration constants
# MULTICAST_GROUP and MULTICAST_PORT are no longer used for direct IP multicast
BASE_GAME_PORT = 20000  # Base for finding a free UDP game port
TOKEN = b"STREICHHOLZ"
TERMINATE = b"TERMINATE"


def get_local_ip():
    """Helper function to get the local IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't even have to be reachable
        s.connect(("10.255.255.255", 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = "127.0.0.1"  # Fallback
    finally:
        s.close()
    return IP


class Zunder:
    def __init__(
        self,
        port: int,  # Lobby port
        is_host: bool,
        host_ip: Optional[str] = None,  # For clients
        initial_p: float = 0.5,  # For host's configuration
        max_consecutive_handover: int = 5,  # For host's configuration
        expected_nodes: int = 2,  # For host's configuration
    ):
        self.node_id = -1  # Assigned by host, or 0 if this instance is the host
        self.lobby_port = port
        self.is_host = is_host
        self.host_ip = host_ip

        # Configuration values (host sets these, client receives them)
        self.initial_p_config = initial_p
        self.max_consecutive_misfires_config = max_consecutive_handover
        self.expected_nodes_config = expected_nodes

        # Live simulation parameters
        self.p = 0.0
        self.max_consecutive_misfires = 0

        self.peers = {}  # Stores node_id: (ip, game_port) for all nodes
        self.client_tcp_connections = {}  # For host: client_node_id: tcp_conn for lobby

        # Each node finds a free UDP port for game messages (token, rocket, etc.)
        self.game_port = self._find_free_udp_port(BASE_GAME_PORT, BASE_GAME_PORT + 1000)
        if self.game_port is None:
            raise IOError("Could not find a free UDP port for game messages.")

        self.active = False  # True when simulation is running
        self.simulation_started = (
            False  # True after lobby setup is complete and start signal received/sent
        )

        self.overall_fired_count = 0  # Count of all rockets seen by this node

        # Statistics for Node 0 (host)
        if self.is_host:  # Will be confirmed when node_id becomes 0
            self.token_rounds = 0
            self.fired_by_this_node_count = (
                0  # Rockets fired by this node (if it's node 0)
            )
            self.round_times = []
            self.last_token_time = 0

        self.lobby_lock = threading.Lock()  # To protect shared data during client joins

    def _find_free_udp_port(self, start_port, end_port):
        for port_num in range(start_port, end_port + 1):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.bind(("0.0.0.0", port_num))
                s.close()
                return port_num
            except OSError:
                continue  # Port is likely in use
        return None  # No free port found

    def _setup_game_socket(self):
        """Sets up the UDP socket for game messages."""
        self.game_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.game_socket.bind(("0.0.0.0", self.game_port))
        self.game_socket.settimeout(1.0)  # Non-blocking receive for the main loop
        # print(f"Node {self.node_id} UDP game socket listening on port {self.game_port}")

    def run(self):
        """Main entry point. Starts either the host lobby or joins a client lobby."""
        if self.is_host:
            self._host_lobby()
        else:
            self._join_lobby()

        # If simulation_started is False, it means lobby failed or was aborted.
        if not self.simulation_started and self.active:
            print(
                f"Node {self.node_id}: Simulation start was aborted or failed. Shutting down."
            )
            self.active = (
                False  # Ensure cleanup if run finishes without starting simulation
            )

        # Cleanup any remaining resources if active is false and sockets exist
        if not self.active:
            if hasattr(self, "game_socket"):
                self.game_socket.close()
            if hasattr(self, "control_socket_listener") and self.is_host:
                self.control_socket_listener.close()
            for (
                conn
            ) in self.client_tcp_connections.values():  # Host closes client TCP sockets
                conn.close()
            if (
                hasattr(self, "control_socket_client") and not self.is_host
            ):  # Client closes its TCP socket
                self.control_socket_client.close()

    def _host_lobby(self):
        self.node_id = 0
        self.p = self.initial_p_config
        self.max_consecutive_misfires = self.max_consecutive_misfires_config

        my_ip = get_local_ip()
        with self.lobby_lock:
            self.peers[self.node_id] = (my_ip, self.game_port)
            self.total_nodes = 1  # Host is the first node

        # print(
        #     f"Host (Node {self.node_id}) started. Lobby on TCP port {self.lobby_port}. Game UDP on {self.game_port}."
        # )

        # setup the game socket for this node to block the UDP port
        self._setup_game_socket()
        print(
            f"\n Waiting for {self.expected_nodes_config - 1} more node(s) to join..."
        )

        self.control_socket_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket_listener.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
        )
        self.control_socket_listener.bind(("0.0.0.0", self.lobby_port))
        self.control_socket_listener.listen(self.expected_nodes_config)

        client_handling_threads = []
        try:
            while self.total_nodes < self.expected_nodes_config:
                try:
                    # Set a timeout on accept to allow checking for KeyboardInterrupt or other signals
                    self.control_socket_listener.settimeout(1.0)
                    conn, addr = self.control_socket_listener.accept()
                    conn.settimeout(5.0)  # Timeout for individual client operations

                    # dont print in case of checks for lobby existence from my machine
                    if addr[0] != "127.0.0.1":
                        print(f"Host: New connection from {addr[0]}:{addr[1]}")

                    # Handle each client in a new thread to allow concurrent joins
                    # and keep the accept loop responsive.
                    thread = threading.Thread(
                        target=self._handle_client_join, args=(conn, addr)
                    )
                    client_handling_threads.append(thread)
                    thread.start()
                except socket.timeout:
                    # Allows checking self.active or other conditions periodically if needed
                    if (
                        not self.active and self.simulation_started
                    ):  # If terminated during lobby
                        break
                    continue
                except Exception as e:
                    print(f"Host: Error accepting connection: {e}")
                    break  # Exit lobby on major error

            for thread in client_handling_threads:  # Wait for all join handlers
                thread.join()

            if self.total_nodes == self.expected_nodes_config:
                print("Host: All expected nodes have joined. \n")
                # Prepare and send the start signal to all connected clients
                start_message_payload = f"START_SIMULATION:{self.p}:{self.max_consecutive_misfires}:{json.dumps(self.peers)}"
                start_message = start_message_payload.encode()

                print(f"{40*'='}")

                for client_id, client_conn in self.client_tcp_connections.items():
                    try:
                        print(f"Host: Sending START_SIMULATION to Node {client_id}")
                        client_conn.sendall(start_message)
                    except Exception as e:
                        print(
                            f"Host: Failed to send START_SIMULATION to Node {client_id}: {e}"
                        )
                    finally:
                        client_conn.close()  # Close TCP control connection

                print(
                    f"  Config: p={self.p}, max_consecutive_misfires={self.max_consecutive_misfires}, total_nodes={self.total_nodes}"
                )
                print(f"  Peers: {self.peers}")  # Can be verbose
                print(f"{40*'='} \n ")

                self.simulation_started = True
                self.start_simulation_logic()  # Host starts its simulation part
            else:
                print(
                    f"Host: Lobby ended. Expected {self.expected_nodes_config} nodes, but only {self.total_nodes} joined. Shutting down."
                )
                self.active = False  # Prevent simulation start

        except KeyboardInterrupt:
            print("Host: Lobby interrupted by user.")
            self.active = False
        finally:
            self.control_socket_listener.close()
            # Ensure all client connections are closed if not already
            for conn_to_close in self.client_tcp_connections.values():
                try:
                    conn_to_close.close()
                except:
                    pass

    def _handle_client_join(self, conn: socket.socket, addr: tuple):
        try:
            data = conn.recv(1024)
            if not data:
                # print(f"Host: No data from {addr[0]}. Closing connection.")
                conn.close()
                return

            message = data.decode()
            if message.startswith("JOIN:"):
                client_game_port = int(message.split(":")[1])

                with self.lobby_lock:
                    if self.total_nodes >= self.expected_nodes_config:
                        print(f"Host: Max nodes reached. Rejecting {addr[0]}.")
                        conn.sendall(b"REJECT:Lobby full")
                        conn.close()
                        return

                    new_node_id = self.total_nodes  # Assign next available ID
                    self.peers[new_node_id] = (addr[0], client_game_port)
                    self.client_tcp_connections[new_node_id] = conn
                    self.total_nodes += 1

                print(
                    f"Host: Node {new_node_id} ({addr[0]}:{client_game_port}) joined. Total nodes: {self.total_nodes}"
                )
                print(
                    f"Waiting for {self.expected_nodes_config - self.total_nodes} more node(s)..."
                )

                # Acknowledge join; client will wait for START_SIMULATION with full data
                ack_msg = f"ACK_JOIN:{new_node_id}".encode()
                conn.sendall(ack_msg)
                # Do not close conn here; it's needed for START_SIMULATION
            else:
                print(f"Host: Invalid message from {addr[0]}: {message}")
                conn.close()

        except socket.timeout:
            print(f"Host: Timeout communicating with {addr[0]}. Closing connection.")
            conn.close()
        except Exception as e:
            print(f"Host: Error handling client join from {addr[0]}: {e}")
            if conn:
                conn.close()
            # If a client fails, we might need to decrement total_nodes if it was counted
            # For simplicity, current logic assumes successful join or error before counting.

    def _join_lobby(self):
        self.control_socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print(
                f"Client: Connecting to host {self.host_ip} on TCP port {self.lobby_port}..."
            )
            self.control_socket_client.connect((self.host_ip, self.lobby_port))

            join_message = f"JOIN:{self.game_port}".encode()
            self.control_socket_client.sendall(join_message)
            print(f"Client: Sent JOIN request to lobby. Waiting for ACK.")

            # Wait for ACK_JOIN
            ack_data = self.control_socket_client.recv(1024)
            if not ack_data:
                print("Client: Host disconnected or sent no ACK. Exiting.")
                self.control_socket_client.close()
                return

            ack_message = ack_data.decode()
            if ack_message.startswith("ACK_JOIN:"):
                self.node_id = int(ack_message.split(":")[1])
                print(
                    f"Client: Joined lobby successfully. Assigned Node ID: {self.node_id}. Waiting for START_SIMULATION."
                )
                # Setup the game socket here to block the UDP port for this node
                self._setup_game_socket()
            elif ack_message.startswith("REJECT:"):
                print(
                    f"Client: Join rejected by host: {ack_message.split(':',1)[1]}. Exiting."
                )
                self.control_socket_client.close()
                return
            else:
                print(f"Client: Unexpected ACK from host: {ack_message}. Exiting.")
                self.control_socket_client.close()
                return

            # Wait for START_SIMULATION (this might take a while)
            # Set a longer timeout for this blocking receive
            self.control_socket_client.settimeout(300.0)  # 5 minutes for host to start
            start_data = self.control_socket_client.recv(
                4096
            )  # Expecting a larger message
            if not start_data:
                print(
                    "Client: Host disconnected before sending START_SIMULATION. Exiting."
                )
                self.control_socket_client.close()
                return

            start_message = start_data.decode()
            if start_message.startswith("START_SIMULATION:"):
                parts = start_message.split(":", 3)
                self.p = float(parts[1])
                self.max_consecutive_misfires = int(parts[2])
                peers_json_str = parts[3]

                loaded_peers = json.loads(peers_json_str)
                self.peers = {int(k): tuple(v) for k, v in loaded_peers.items()}
                self.total_nodes = len(
                    self.peers
                )  # Update total_nodes based on received peer list

                print(f"\n {40*'='}")
                print(f"Client (Node {self.node_id}): Received START_SIMULATION.")
                print(
                    f"  Config: p={self.p}, max_consecutive_misfires={self.max_consecutive_misfires}, total_nodes={self.total_nodes}"
                )
                print(f"  Peers: {self.peers}")  # Can be verbose
                print(f"{40*'='} \n ")

                self.simulation_started = True
                self.start_simulation_logic()
            else:
                print(
                    f"Client: Expected START_SIMULATION, but got: {start_message}. Exiting."
                )

        except socket.timeout:
            print(
                f"Client: Timeout waiting for ACK or START_SIMULATION from host. Exiting."
            )
        except ConnectionRefusedError:
            print(
                f"Client: Connection to host {self.host_ip}:{self.lobby_port} refused. Is the host running?"
            )
        except Exception as e:
            print(f"Client: Error in lobby: {e}")
        finally:
            if hasattr(self, "control_socket_client"):
                self.control_socket_client.close()

    def start_simulation_logic(self):
        """The core simulation logic, run after lobby setup."""
        self.active = True  # Mark as active for the simulation loops

        # Setup UDP socket earlier in __join_lobby or __host_lobby in order to block the UDP Port for others
        # self._setup_game_socket()

        if self.node_id == 0:  # Host-specific initializations
            self.token_rounds = 0
            self.fired_by_this_node_count = 0
            self.round_times = []
            self.last_token_time = time.time()  # Initialize for first round timing
            print(f"Node {self.node_id} (Host) initiating token passing...")
            time.sleep(2)  # Brief pause for clients to fully initialize their listeners
            initial_token_message = (
                TOKEN + b":0"
            )  # consecutive_quiet_handover starts at 0
            self.forward(initial_token_message)

        # Main simulation loop: listen for UDP game messages
        while self.active:
            try:
                data, addr = self.game_socket.recvfrom(1024)

                if data.startswith(TOKEN):
                    # Check if the token is from the expected previous node
                    is_from_expected_sender = False
                    sender_ip, sender_port = addr
                    expected_prev_node_id = (
                        self.node_id - 1 + self.total_nodes
                    ) % self.total_nodes

                    if expected_prev_node_id in self.peers:
                        expected_ip, expected_game_port = self.peers[
                            expected_prev_node_id
                        ]
                        if (
                            expected_ip == sender_ip
                            and expected_game_port == sender_port
                        ):
                            is_from_expected_sender = True

                    if is_from_expected_sender:
                        print(f"{10*'-'}")
                        print(f"Received TOKEN from Node {expected_prev_node_id}.")
                        self._handle_token_message(data)
                    else:
                        # This can happen if a late/duplicate token arrives.
                        # print(f"Node {self.node_id}: Received TOKEN from unexpected sender {addr} (expected from peer {expected_prev_node_id}). Ignoring.")
                        pass  # Silently ignore unexpected tokens to reduce noise

                elif data.startswith(b"ROCKET_FROM_"):
                    self.overall_fired_count += 1
                    try:
                        sender_node_id_str = data.decode().split("_")[-1]
                        sender_node_id = int(sender_node_id_str)
                        if (
                            sender_node_id != self.node_id
                        ):  # Don't report seeing your own rocket
                            print(
                                f"Node {self.node_id} saw rocket from Node {sender_node_id}! 🎆"
                            )
                    except ValueError:
                        print(
                            f"Node {self.node_id}: Received malformed ROCKET_FROM message."
                        )

                elif data == TERMINATE:
                    print(
                        f"\nNode {self.node_id} received TERMINATE signal from {addr[0]}. Shutting down."
                    )
                    self.active = False  # This will cause the loop to exit

            except socket.timeout:
                # Timeout is normal, allows checking self.active flag
                continue
            except Exception as e:
                if self.active:  # Avoid error messages if already shutting down
                    print(f"Node {self.node_id}: Error in game loop: {e}")
                self.active = False  # Terminate on other errors

        # Cleanup after simulation loop
        # print(f"Node {self.node_id} simulation loop ended. Shutting down...")

        if hasattr(self, "game_socket"):
            self.game_socket.close()

        # Node 0 (Host) prints statistics
        if self.node_id == 0:
            print("\n--- Node 0 (Host) Statistics ---")
            if hasattr(self, "token_rounds"):  # Check if stats were initialized
                print(f"Total token rounds completed: {self.token_rounds}")
                print(f"Rockets fired by Node 0: {self.fired_by_this_node_count}")
                print(f"Total rockets seen by Node 0: {self.overall_fired_count}")
                if self.round_times:
                    print(f"Min round time: {min(self.round_times):.4f}s")
                    print(f"Avg round time: {statistics.mean(self.round_times):.4f}s")
                    print(f"Max round time: {max(self.round_times):.4f}s")
                else:
                    print("No round times recorded (or no rounds completed).")
            else:
                print("Statistics not available for Node 0.")
        else:
            print(
                f"\nNode {self.node_id} saw a total of {self.overall_fired_count} rockets."
            )

        print(f"\nNode {self.node_id} terminated.")

    def _handle_token_message(self, data: bytes):
        """Handles a received TOKEN message."""
        if not self.active:
            return

        current_time = time.time()
        try:
            # Token format is TOKEN:consecutive_quiet_handover_count
            consecutive_quiet_handover = int(data.decode().split(":")[-1])
        except (ValueError, IndexError):
            print(
                f"Node {self.node_id}: Received malformed token data: {data}. Ignoring."
            )
            return

        if self.node_id == 0:  # Node 0 (Host) specific stats update
            if (
                hasattr(self, "last_token_time") and self.last_token_time > 0
            ):  # Avoid issues if first token
                self.round_times.append(current_time - self.last_token_time)
            self.last_token_time = current_time
            self.token_rounds += 1

        should_fire = random.random() < self.p
        if should_fire:
            self.fire_rocket()  # This will call unicast_to_all
            consecutive_quiet_handover = 0  # Reset counter
            if self.node_id == 0:
                self.fired_by_this_node_count += 1
            time.sleep(0.5)  # Simulate time for firework display / message propagation
        else:
            consecutive_quiet_handover += 1
            print(
                f"Node {self.node_id} did not fire. p={self.p:.6f}, quiet_rounds={consecutive_quiet_handover}/{self.max_consecutive_misfires}"
            )

        # Check termination condition (based on this node's configured max_consecutive_misfires)
        if self.max_consecutive_misfires <= consecutive_quiet_handover:
            print(
                f"\n Node {self.node_id} initiating termination: reached limit of consecutive misfires {self.max_consecutive_misfires}."
            )
            self.unicast_to_all(TERMINATE)  # Send TERMINATE to all other nodes
            time.sleep(0.5)  # Allow time for TERMINATE to propagate
            self.active = False  # This node stops
            return  # Do not forward token or reduce p

        # Reduce probability for the next round (for this node)
        self.p /= 2

        # Forward the token to the next node
        token_to_forward = TOKEN + b":" + str(consecutive_quiet_handover).encode()
        self.forward(token_to_forward)

    def forward(self, message: bytes):
        """Forwards a message (typically the token) to the next node in the ring."""
        if not self.active:
            return

        next_node_id = (self.node_id + 1) % self.total_nodes
        if next_node_id in self.peers:
            next_ip, next_game_port = self.peers[next_node_id]
            try:
                self.game_socket.sendto(message, (next_ip, next_game_port))
                print(
                    f"Node {self.node_id} forwarded message to Node {next_node_id} ({next_ip}:{next_game_port})."
                )
                print(f"{10*'-'}")
            except Exception as e:
                print(
                    f"Node {self.node_id}: Failed to forward to Node {next_node_id} ({next_ip}:{next_game_port}): {e}"
                )
        else:
            print(
                f"Node {self.node_id}: Cannot forward, next Node {next_node_id} not found in peers."
            )
            self.active = False  # Critical error, stop this node

    def fire_rocket(self):
        """This node fires a rocket: sends a ROCKET_FROM message to all other nodes."""
        if not self.active:
            return
        print(f"Node {self.node_id} FIRING ROCKET! (p={self.p:.6f})")
        rocket_message = f"ROCKET_FROM_{self.node_id}".encode()
        self.unicast_to_all(rocket_message)

    def unicast_to_all(self, message_bytes: bytes):
        """Sends a UDP message to all other known peers."""
        if not self.active:
            return
        # print(f"Node {self.node_id} unicasting message: {message_bytes.decode() if len(message_bytes)<50 else message_bytes[:50]}")
        for peer_id, (ip_addr, g_port) in self.peers.items():
            if peer_id != self.node_id:  # Don't send to self
                try:
                    self.game_socket.sendto(message_bytes, (ip_addr, g_port))
                except Exception as e:
                    print(
                        f"Node {self.node_id}: Failed to unicast to Node {peer_id} ({ip_addr}:{g_port}): {e}"
                    )
