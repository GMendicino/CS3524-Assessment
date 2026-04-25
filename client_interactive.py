# client_interactive.py - Interactive client for debugging
#
# A simple command-line client for manually testing the distributed KV store.
# Connect to the network, then issue PUT/GET/DELETE commands to verify that
# your Raft implementation is working.
#
# Usage:
# python client_interactive.py (auto-generated client ID)
# python client_interactive.py my-client (custom client ID)
#
# Commands:
# PUT <key> <value> - Store a key-value pair
# GET <key> - Retrieve a value by key
# DELETE <key> - Delete a key
# HELP - Show available commands
# QUIT - Exit
#
# Do not modify this file

import socket
import sys
import time
import uuid

from message import (
    send_message, recv_message, make_register, make_client_request,
    MSG_CLIENT_RESPONSE, MSG_REGISTER_ACK,
)
from config import NETWORK_HOST, NETWORK_PORT, CLIENT_TIMEOUT


class InteractiveClient:
    """Interactive client for manually testing the cluster."""

    def __init__(self, client_id=None):
        self.client_id = client_id or f"client-{uuid.uuid4().hex[:6]}"
        self.sock = None

    def connect(self):
        """Connect to the network and register."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((NETWORK_HOST, NETWORK_PORT))
        self.sock.settimeout(CLIENT_TIMEOUT)

        # Register with network
        send_message(self.sock, make_register(self.client_id, "client"))
        ack = recv_message(self.sock)
        if ack and ack.get("type") == MSG_REGISTER_ACK:
            print(f"Connected as '{self.client_id}'")
            return True
        else:
            print("Registration failed")
            return False

    def send_request(self, operation, key, value=None):
        """Send a request and wait for the first matching response."""
        request_id = str(uuid.uuid4())
        msg = make_client_request(
            self.client_id, request_id, operation, key, value
        )

        start_time = time.time()
        send_message(self.sock, msg)

        # Wait for a response matching our request_id
        try:
            while True:
                response = recv_message(self.sock)
                if response is None:
                    print("Connection lost")
                    return None
                if (response.get("type") == MSG_CLIENT_RESPONSE and
                        response.get("request_id") == request_id):
                    elapsed = time.time() - start_time
                    return response, elapsed
                # Ignore responses to other requests (e.g. from other
                # nodes that also tried to respond)
        except socket.timeout:
            print(f"Request timed out after {CLIENT_TIMEOUT}s")
            return None

    def run(self):
        """Main interactive loop."""
        if not self.connect():
            return

        print("\nDistributed KV Store - Interactive Client")
        print("Type HELP for commands\n")

        while True:
            try:
                line = input(f"[{self.client_id}] > ").strip()
            except (EOFError, KeyboardInterrupt):
                break

            if not line:
                continue

            parts = line.split()
            cmd = parts[0].upper()

            if cmd in ("QUIT", "EXIT"):
                break
            elif cmd == "HELP":
                print("PUT <key> <value> - Store a key-value pair")
                print("GET <key> - Retrieve a value")
                print("DELETE <key> - Delete a key")
                print("QUIT - Exit")
            elif cmd == "PUT" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                result = self.send_request("PUT", key, value)
                if result:
                    response, elapsed = result
                    if response["success"]:
                        print(f"OK ({elapsed * 1000:.0f}ms)")
                    else:
                        error = response.get("error", "unknown error")
                        print(f"FAILED: {error}")
                        if response.get("leader_hint"):
                            print(f"(Leader hint: {response['leader_hint']})")
            elif cmd == "GET" and len(parts) >= 2:
                key = parts[1]
                result = self.send_request("GET", key)
                if result:
                    response, elapsed = result
                    if response["success"]:
                        print(f"{key} = {response['value']} ({elapsed * 1000:.0f}ms)")
                    else:
                        print(f"NOT FOUND ({elapsed * 1000:.0f}ms)")
            elif cmd == "DELETE" and len(parts) >= 2:
                key = parts[1]
                result = self.send_request("DELETE", key)
                if result:
                    response, elapsed = result
                    if response["success"]:
                        print(f"DELETED ({elapsed * 1000:.0f}ms)")
                    else:
                        error = response.get("error", "unknown error")
                        print(f"FAILED: {error}")
            elif cmd == "PUT":
                print("Usage: PUT <key> <value>")
            elif cmd == "GET":
                print("Usage: GET <key>")
            elif cmd == "DELETE":
                print("Usage: DELETE <key>")
            else:
                print(f"Unknown command '{cmd}'. Type HELP for usage.")

        print("Exiting")
        if self.sock:
            self.sock.close()


if __name__ == "__main__":
    client_id = sys.argv[1] if len(sys.argv) > 1 else None
    client = InteractiveClient(client_id)
    client.run()
