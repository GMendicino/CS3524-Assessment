# message.py - Message protocol for the distributed system
#
# This file defines the message types, serialisation protocol, and helper
# functions for constructing messages.
#
# The protocol uses length prefixed JSON over TCP, understanding the protocol
# probably only matters if you plan to change it in your extension
# Every message is sent as:
#   [4 bytes: big-endian uint32 length] [N bytes: UTF-8 JSON]
#
# Every message has at minimum these fields:
#   - "type" - one of the MSG_* constants below
#   - "src" - sender ID (e.g. "node-0", "client-abc123")
#   - "dst" - destination ID, "all_nodes", or "leader"

import json
import struct

# === Message Type Constants ===

# Registration (node/client <-> network)
MSG_REGISTER = "REGISTER"
MSG_REGISTER_ACK = "REGISTER_ACK"

# Client operations (client <-> node)
MSG_CLIENT_REQUEST = "CLIENT_REQUEST"
MSG_CLIENT_RESPONSE = "CLIENT_RESPONSE"

# Raft RPCs (node <-> node)
MSG_APPEND_ENTRIES = "APPEND_ENTRIES"
MSG_APPEND_ENTRIES_RESPONSE = "APPEND_ENTRIES_RESPONSE"
MSG_REQUEST_VOTE = "REQUEST_VOTE"
MSG_REQUEST_VOTE_RESPONSE = "REQUEST_VOTE_RESPONSE"
MSG_INSTALL_SNAPSHOT = "INSTALL_SNAPSHOT"
MSG_INSTALL_SNAPSHOT_RESPONSE = "INSTALL_SNAPSHOT_RESPONSE"


# === Wire Protocol ===

def send_message(sock, msg_dict):
    """Send a length prefixed JSON message over a TCP socket.

    Protocol: 4-byte big-endian length prefix followed by UTF-8 JSON bytes.

    Args:
        sock: TCP socket to send on.
        msg_dict: Dictionary to serialise and send.
    """
    json_bytes = json.dumps(msg_dict).encode("utf-8")
    header = struct.pack("!I", len(json_bytes))
    sock.sendall(header + json_bytes)


def recv_message(sock):
    """
    Receive a length prefixed JSON message from a TCP socket.

    Returns:
        dict: The deserialised message, or None if connection closed.
    """
    header = _recv_exact(sock, 4)
    if not header:
        return None
    length = struct.unpack("!I", header)[0]
    json_bytes = _recv_exact(sock, length)
    if not json_bytes:
        return None
    return json.loads(json_bytes.decode("utf-8"))


def _recv_exact(sock, num_bytes):
    """
    Receive exactly num_bytes from the socket.

    Returns:
        bytes: The received data, or None if connection closed before
               all bytes were received.
    """
    data = b""
    while len(data) < num_bytes:
        chunk = sock.recv(num_bytes - len(data))
        if not chunk:
            return None
        data += chunk
    return data


# === Provided Message Constructors ===

def make_register(sender_id, sender_type):
    """
    Create a REGISTER message.

    Sent by a node or client when it first connects to the network.

    Args:
        sender_id: e.g. "node-0" or "client-1"
        sender_type: "node" or "client"
    """
    return {
        "type": MSG_REGISTER,
        "src": sender_id,
        "dst": "network",
        "sender_type": sender_type,
    }


def make_client_request(client_id, request_id, operation, key, value=None):
    """
    Create a CLIENT_REQUEST message.

    Sent by a client to the cluster. The network delivers this to all nodes;
    only the leader should process it.

    Args:
        client_id: The client's identifier.
        request_id: Unique ID for this request (for deduplication).
        operation: "PUT", "GET", or "DELETE".
        key: The key to operate on.
        value: The value (for PUT only).
    """
    return {
        "type": MSG_CLIENT_REQUEST,
        "src": client_id,
        "dst": "leader",
        "request_id": request_id,
        "operation": operation,
        "key": key,
        "value": value,
    }


def make_client_response(node_id, client_id, request_id, success,
                         value=None, leader_hint=None, error=None):
    """
    Create a CLIENT_RESPONSE message.

    Sent by a node back to the client.

    Args:
        node_id: The responding node's ID.
        client_id: The destination client's ID.
        request_id: Echoed from the original request (for matching).
        success: True if the operation succeeded.
        value: The value returned by a GET operation.
        leader_hint: If this node is not the leader, hint at who is.
        error: Error message string if the operation failed.
    """
    return {
        "type": MSG_CLIENT_RESPONSE,
        "src": node_id,
        "dst": client_id,
        "request_id": request_id,
        "success": success,
        "value": value,
        "leader_hint": leader_hint,
        "error": error,
    }


def make_request_vote(node_id, dst, term, last_log_index, last_log_term):
    """
    Create a REQUEST_VOTE message.

    Args:
        node_id: The candidate's ID.
        dst: The destination node's ID or "all_nodes".
        term: The candidate's current term.
        last_log_index: Index of candidate's last log entry.
        last_log_term: Term of candidate's last log entry.
    """
    return {
        "type": MSG_REQUEST_VOTE,
        "src": node_id,
        "dst": dst,
        "term": term,
        "last_log_index": last_log_index,
        "last_log_term": last_log_term,
    }


def make_request_vote_response(node_id, dst, term, vote_granted):
    """
    Create a REQUEST_VOTE_RESPONSE message.

    Args:
        node_id: The responding node's ID.
        dst: The candidate's ID.
        term: Responding node's current term.
        vote_granted: True if the node grants its vote to the candidate.
    """
    return {
        "type": MSG_REQUEST_VOTE_RESPONSE,
        "src": node_id,
        "dst": dst,
        "term": term,
        "vote_granted": vote_granted,
    }


def make_append_entries_response(node_id, dst, term, success):
    """
    Create an APPEND_ENTRIES_RESPONSE message.

    Args:
        node_id: The responding node's ID.
        dst: The leader's ID.
        term: Responding node's current term.
        success: True if follower contained entry matching prev_log_index and prev_log_term.
    """
    return {
        "type": MSG_APPEND_ENTRIES_RESPONSE,
        "src": node_id,
        "dst": dst,
        "term": term,
        "success": success,
    }


def make_append_entries(node_id, dst, term, prev_log_index, prev_log_term, entries, leader_commit):
    """
    Create an APPEND_ENTRIES message.

    Args:
        node_id: The leader's ID.
        dst: The follower's ID.
        term: The leader's current term.
        prev_log_index: Index of log entry immediately preceding new ones.
        prev_log_term: Term of prev_log_index entry.
        entries: Log entries to store (empty for heartbeat).
        leader_commit: Leader's commit_index.
    """
    return {
        "type": MSG_APPEND_ENTRIES,
        "src": node_id,
        "dst": dst,
        "term": term,
        "prev_log_index": prev_log_index,
        "prev_log_term": prev_log_term,
        "entries": entries,
        "leader_commit": leader_commit,
    }


def make_install_snapshot(node_id, dst, term, last_included_index, last_included_term, data):
    """
    Create an INSTALL_SNAPSHOT message.

    Args:
        node_id: The leader's ID.
        dst: The follower's ID.
        term: The leader's current term.
        last_included_index: The snapshot replaces all entries up through this index.
        last_included_term: Term of last_included_index.
        data: Raw bytes of the snapshot (state machine state, etc.).
    """
    return {
        "type": MSG_INSTALL_SNAPSHOT,
        "src": node_id,
        "dst": dst,
        "term": term,
        "last_included_index": last_included_index,
        "last_included_term": last_included_term,
        "data": data,
    }


def make_install_snapshot_response(node_id, dst, term, last_included_index, success):
    """
    Create an INSTALL_SNAPSHOT_RESPONSE message.

    Args:
        node_id: The responding node's ID.
        dst: The leader's ID.
        term: Responding node's current term.
        last_included_index: The index of the snapshot being acknowledged.
        success: True if the snapshot was successfully installed.
    """
    return {
        "type": MSG_INSTALL_SNAPSHOT_RESPONSE,
        "src": node_id,
        "dst": dst,
        "term": term,
        "last_included_index": last_included_index,
        "success": success,
    }
