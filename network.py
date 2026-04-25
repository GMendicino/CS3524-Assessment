# network.py - Perfect network: routes all messages with consistent small delay
#
# This is the base network implementation. It acts as a central message router
# that all nodes and clients connect to via TCP. Messages are forwarded based
# on the "dst" field in each message:
#
#   - Specific ID (e.g. "node-2") - forwarded to that endpoint
#   - "all_nodes" - forwarded to all registered nodes (except sender)
#   - "leader" - forwarded to all registered nodes (each decides
#     whether to handle it based on its role)
#
# This is the PERFECT network: all messages are delivered with a small,
# consistent delay. No drops, no reordering, no partitions.
#
# Network variants (network_delayed.py, network_lossy.py, etc.) inherit from
# this class and override only the _deliver() method to introduce failures.
#
# Do not modify this file

import socket
import threading
import time

from message import send_message, recv_message, MSG_REGISTER, MSG_REGISTER_ACK
from config import NETWORK_HOST, NETWORK_PORT, NETWORK_BASE_DELAY, NODE_IDS


class Network:
    """Central message router for the distributed system."""

    def __init__(self):
        self.connections = {} # id -> socket
        self.connection_types = {} # id -> "node" or "client"
        self.lock = threading.Lock()
        self.running = True

    def start(self):
        """Start the network server."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((NETWORK_HOST, NETWORK_PORT))
        server_socket.listen(20)
        server_socket.settimeout(1.0)

        print(f"[Network] {self.__class__.__name__} listening on "
              f"{NETWORK_HOST}:{NETWORK_PORT}")
        print(f"[Network] Expecting {len(NODE_IDS)} nodes")

        try:
            while self.running:
                try:
                    client_socket, addr = server_socket.accept()
                    thread = threading.Thread(
                        target=self._handle_connection,
                        args=(client_socket, addr),
                        daemon=True,
                    )
                    thread.start()
                except socket.timeout:
                    continue
        except KeyboardInterrupt:
            print("\n[Network] Shutting down...")
        finally:
            self.running = False
            server_socket.close()

    def _handle_connection(self, sock, addr):
        """Handle a single connection (node or client)."""
        sender_id = None
        try:
            # First message must be REGISTER
            msg = recv_message(sock)
            if not msg or msg.get("type") != MSG_REGISTER:
                print(f"[Network] Bad registration from {addr}, closing")
                sock.close()
                return

            sender_id = msg["src"]
            sender_type = msg["sender_type"]

            with self.lock:
                self.connections[sender_id] = sock
                self.connection_types[sender_id] = sender_type

            print(f"[Network] Registered {sender_type} '{sender_id}' from {addr}")

            # Send registration acknowledgment
            send_message(sock, {
                "type": MSG_REGISTER_ACK,
                "src": "network",
                "dst": sender_id,
            })

            # Main message routing loop
            while self.running:
                msg = recv_message(sock)
                if msg is None:
                    break
                self._route_message(msg, sender_id)

        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            with self.lock:
                if sender_id and sender_id in self.connections:
                    del self.connections[sender_id]
                if sender_id and sender_id in self.connection_types:
                    del self.connection_types[sender_id]
            if sender_id:
                print(f"[Network] '{sender_id}' disconnected")

    def _route_message(self, msg, sender_id):
        """
        Route a message to its destination(s).

        Routing modes:
          - "all_nodes" - broadcast to all registered nodes except sender
          - "leader" - broadcast to all registered nodes (including sender,
            since the sender may not know it is the leader)
          - specific ID - unicast to that endpoint
        """
        dst = msg.get("dst", "")

        if dst == "all_nodes":
            self._broadcast_to_nodes(msg, exclude=sender_id)
        elif dst == "leader":
            self._broadcast_to_nodes(msg, exclude=None)
        else:
            self._send_to(msg, dst)

    def _broadcast_to_nodes(self, msg, exclude=None):
        """Send message to all registered nodes."""
        with self.lock:
            targets = [
                (nid, sock)
                for nid, sock in self.connections.items()
                if self.connection_types.get(nid) == "node" and nid != exclude
            ]
        for target_id, target_sock in targets:
            # Deliver in a separate thread so one slow delivery doesn't
            # block others
            threading.Thread(
                target=self._deliver,
                args=(msg, target_id, target_sock),
                daemon=True,
            ).start()

    def _send_to(self, msg, dst_id):
        """Send message to a specific destination."""
        with self.lock:
            sock = self.connections.get(dst_id)
        if sock:
            threading.Thread(
                target=self._deliver,
                args=(msg, dst_id, sock),
                daemon=True,
            ).start()

    def _deliver(self, msg, dst_id, dst_sock):
        """
        Deliver a message after applying network behaviour.

        In the perfect network, this adds a small consistent delay.
        Subclasses override this method to add drops, delays, etc.
        """
        time.sleep(NETWORK_BASE_DELAY)
        try:
            send_message(dst_sock, msg)
        except (BrokenPipeError, OSError):
            pass


if __name__ == "__main__":
    network = Network()
    network.start()
