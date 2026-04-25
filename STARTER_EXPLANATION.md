# How the Existing Code Works

This document explains the base infrastructure you are building on top of. Read this before you start implementing.

## System Architecture

The system uses a star topology. The Network process sits at the centre. All 5 nodes and all clients connect to the Network via individual connections. Every message passes through the Network, which routes it based on the `dst` field:

- `dst = "node-2"` - delivered to that specific node or client
- `dst = "all_nodes"` - broadcast to all registered nodes except the sender
- `dst = "leader"` - broadcast to all registered nodes (your node decides whether to act on it based on its role)

Nodes and clients never communicate directly. You send a message with `_send()` and the Network delivers it to the destination.

## Messages

Every message is a Python dictionary with at least three fields: `type`, `src`, and `dst`. You send and receive messages using `send_message(sock, msg)` and `recv_message(sock)` from `message.py`.

### Message types

Registration (handled for you in `start()`):
- `REGISTER` / `REGISTER_ACK`

Client operations:
- `CLIENT_REQUEST` - sent by clients with `dst="leader"`, so all nodes receive it
- `CLIENT_RESPONSE` - sent by your node back to a specific client

Raft:
- `REQUEST_VOTE` / `REQUEST_VOTE_RESPONSE`
- `APPEND_ENTRIES` / `APPEND_ENTRIES_RESPONSE`

### Provided constructors

Three constructors are already written in `message.py`:

- `make_register(sender_id, sender_type)` - used by `start()`, you won't need to modify this
- `make_client_request(client_id, request_id, operation, key, value)` - used by clients, not by nodes, so agian you do not need to use or modify it unless you are writing new tests
- `make_client_response(node_id, client_id, request_id, success, value, leader_hint, error)` - you will use this to respond to clients

You will need to write constructors for new message types, it is probably best to follow the existing pattern.

## The Node Skeleton (`node.py`)

### What is provided

The skeleton handles all networking and thread management for sending/recieving messages as well as timings for detecting timeouts of heartbeats. You do not need to modify:

- `start()` - connects to the network, registers, starts background threads
- `_receive_loop()` - continuously receives messages and passes them to `_dispatch()`
- `_dispatch()` - routes messages by type to your `handle_*` methods
- `_send(msg)` - sends a message through the network
- `_timer_loop()` - drives election timeouts and heartbeat scheduling (see Locking below as it is very important that you do not break the locking mechanisms)

### What you implement

All TODO methods are for you to fill in. They fall into four groups:

Leader Election: `start_election()`, `handle_request_vote()`, `handle_request_vote_response()`

Log Replication: `send_heartbeats()`, `handle_append_entries()`, `handle_append_entries_response()`

Client Requests: `handle_client_request()`

State Machine: `apply_committed()`

Persistence (Part 3): `save_state()`, `load_state()`, `take_snapshot()`, `load_snapshot()`

## Locking

There is a single lock for each node, held in `self.lock`. Understanding when it is held is critical for ensuring that this system does not block itself.

### Timer-driven methods (lock IS held)

The `_timer_loop` acquires `self.lock` using a context manager before calling:
- `start_election()` - called when the election timeout fires (candidates/followers)
- `send_heartbeats()` - called every `HEARTBEAT_INTERVAL` (leaders)

These methods run with the lock already held. Do not acquire `self.lock` again inside them or you will deadlock. This is the case throughout your assignment, if the method calling another method already holds the lock, you do not need to aquire it again as you will be taking it from the parent call.

### Message handlers (lock is NOT held)

The `_dispatch` method calls your handlers directly from the receive thread without holding the lock. This means `handle_request_vote()`, `handle_request_vote_response()`, `handle_append_entries()`, `handle_append_entries_response()`, and `handle_client_request()` all run without the lock.

These handlers will read and modify shared state (`current_term`, `voted_for`, `role`, `log`, `commit_index`, etc.), and the timer loop is also reading and modifying this same state on a separate thread. Without the lock, both threads could modify state simultaneously and corrupt it. You must acquire `self.lock` in each handler before accessing or modifying any state in the node:

```python
def handle_request_vote(self, msg):
    with self.lock:
        # your implementation
```

The same applies to any other method you write that is called from a handler and touches shared state, if the lock is not already held by the caller, you need to acquire it.

### Sending messages

`_send(msg)` does not require the lock. It only writes to the socket, which is independent of the Raft state. You can call it from anywhere, whether you hold the lock or not.

## State Variables

All state is initialised in `__init__`. You will read and modify these fields in your implementation.

### Raft persistent state

- `current_term` - the current term number (starts at 0)
- `voted_for` - which node this node voted for in the current term (or `None`)
- `log` - list of log entries, each is `{"index": int, "term": int, "command": dict}`

### Raft volatile state

- `commit_index` - index of the highest log entry known to be committed
- `last_applied` - index of the highest log entry applied to the state machine
- `role` - one of `FOLLOWER`, `CANDIDATE`, or `LEADER`
- `leader_id` - the node ID of the current leader (or `None`)

### Leader only state

- `next_index` - dict mapping each follower's node ID to the next log index to send them
- `match_index` - dict mapping each follower's node ID to the highest log index known to be replicated on them
- `votes_received` - set of node IDs that have voted for this node in the current election

### Election timing

- `last_heartbeat_time` - timestamp of the last heartbeat received (followers) or sent (leaders)
- `election_timeout` - randomised timeout for this node (regenerated after each election)

### Application state

- `kv_store` - the key-value store dictionary. This is the state machine that Raft replicates. 

## Helper Methods

These are provided and ready to use:

- `_get_last_log_index()` - index of the last log entry (0 if empty)
- `_get_last_log_term()` - term of the last log entry (0 if empty)
- `_get_log_term(index)` - term of the entry at the given index (0 if not found)
- `_get_log_entry(index)` - the full entry dict at the given index (or `None`)
- `_get_log_slice(from_index)` - all entries from `from_index` onward (inclusive)
- `_random_election_timeout()` - random float between `ELECTION_TIMEOUT_MIN` and `ELECTION_TIMEOUT_MAX`
- `_step_down(new_term)` - sets `role=FOLLOWER`, updates `current_term`, clears `voted_for` and `leader_id`

## Configuration (`config.py`)

Key constants your implementation will use, you are only likely to need to change these if your extension in task 5 requires it:

- `CLUSTER_SIZE = 5`, `NODE_IDS = ["node-0", ..., "node-4"]`
- `HEARTBEAT_INTERVAL = 0.5` - leaders send heartbeats every 500ms
- `ELECTION_TIMEOUT_MIN = 1.5`, `ELECTION_TIMEOUT_MAX = 3.0` - election timeout range (seconds)
- `SNAPSHOT_THRESHOLD = 100` - take a snapshot after this many log entries
- `DATA_DIR = "data"` - directory for persistent state files
