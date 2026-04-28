import socket
import threading
import time
import random
import json
import os
import sys

from message import (
    send_message, recv_message, make_register, make_client_response,
    make_request_vote, make_request_vote_response,
    make_append_entries, make_append_entries_response,
    make_install_snapshot, make_install_snapshot_response,
    MSG_REGISTER_ACK, MSG_APPEND_ENTRIES, MSG_APPEND_ENTRIES_RESPONSE,
    MSG_REQUEST_VOTE, MSG_REQUEST_VOTE_RESPONSE,
    MSG_CLIENT_REQUEST, MSG_INSTALL_SNAPSHOT, MSG_INSTALL_SNAPSHOT_RESPONSE,
)
from config import (
    NETWORK_HOST, NETWORK_PORT, CLUSTER_SIZE, NODE_IDS,
    HEARTBEAT_INTERVAL, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX,
    SNAPSHOT_THRESHOLD, DATA_DIR,
)


# === Raft Roles ===
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"


class RaftNode:
    """
    A single Raft node that connects to the network.

    Architecture:
        - Connects to the central network via TCP
        - Receives messages via _receive_loop
        - Election timeouts and heartbeats driven by _timer_loop
        - You implement the Raft logic in the TODO methods below

    State (all initialised for you):
        Raft persistent state:
            current_term, voted_for, log

        Raft volatile state:
            commit_index, last_applied, role, leader_id

        Leader-only state:
            next_index, match_index, votes_received

        Application state:
            kv_store
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.sock = None
        self.lock = threading.Lock()

        # === Raft Persistent State ===
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.last_included_index = 0
        self.last_included_term = 0

        # === Raft Volatile State ===
        self.commit_index = 0
        self.last_applied = 0
        self.role = FOLLOWER
        self.leader_id = None

        # === Leader-Only State ===
        self.next_index = {}
        self.match_index = {}
        self.votes_received = set()

        # === Election Timing ===
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._random_election_timeout()

        # === Application State Machine ===
        self.kv_store = {}

        # === Client Deduplication Cache ===
        self.client_responses = {}

        # === Load Persisted State ===
        self.load_state()

    def _random_election_timeout(self):
        """Generate a random election timeout."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    # CONNECTION & MESSAGE HANDLING (do not modify)

    def start(self):
        """Connect to the network and start the node."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((NETWORK_HOST, NETWORK_PORT))
        self.sock.settimeout(0.1)

        # Register with the network
        send_message(self.sock, make_register(self.node_id, "node"))
        ack = recv_message(self.sock)
        if not ack or ack.get("type") != MSG_REGISTER_ACK:
            print(f"[{self.node_id}] Registration failed")
            return

        print(f"[{self.node_id}] Registered with network as {self.role}")

        # Start background threads
        threading.Thread(target=self._receive_loop, daemon=True).start()
        threading.Thread(target=self._timer_loop, daemon=True).start()

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.node_id}] Shutting down")
            self.sock.close()

    def _receive_loop(self):
        """Continuously receive and dispatch messages from the network."""
        while True:
            try:
                msg = recv_message(self.sock)
                if msg is None:
                    print(f"[{self.node_id}] Connection to network lost")
                    break
                self._dispatch(msg)
            except socket.timeout:
                continue
            except (ConnectionResetError, BrokenPipeError, OSError):
                print(f"[{self.node_id}] Connection error")
                break

    def _dispatch(self, msg):
        """Route incoming messages to the appropriate handler."""
        msg_type = msg.get("type")

        if msg_type == MSG_APPEND_ENTRIES:
            self.handle_append_entries(msg)
        elif msg_type == MSG_APPEND_ENTRIES_RESPONSE:
            self.handle_append_entries_response(msg)
        elif msg_type == MSG_REQUEST_VOTE:
            self.handle_request_vote(msg)
        elif msg_type == MSG_REQUEST_VOTE_RESPONSE:
            self.handle_request_vote_response(msg)
        elif msg_type == MSG_INSTALL_SNAPSHOT:
            self.handle_install_snapshot(msg)
        elif msg_type == MSG_INSTALL_SNAPSHOT_RESPONSE:
            self.handle_install_snapshot_response(msg)
        elif msg_type == MSG_CLIENT_REQUEST:
            self.handle_client_request(msg)
        # Ignore unknown message types silently

    def _send(self, msg):
        """
        Send a message through the network.

        All messages go through the central network, which routes them
        based on the 'dst' field.
        """
        try:
            send_message(self.sock, msg)
        except (BrokenPipeError, OSError):
            pass

    # TIMER LOOP (do not modify)

    def _timer_loop(self):
        """
        Periodic timer that drives election timeouts and heartbeats.

        Runs every 100ms. The lock is held before calling start_election()
        or send_heartbeats().
        """
        while True:
            time.sleep(0.1)

            with self.lock:
                now = time.time()
                elapsed = now - self.last_heartbeat_time

                if self.role == LEADER:
                    if elapsed >= HEARTBEAT_INTERVAL:
                        self.send_heartbeats()
                        self.last_heartbeat_time = now
                else:
                    if elapsed >= self.election_timeout:
                        self.start_election()
                        self.last_heartbeat_time = now
                        self.election_timeout = self._random_election_timeout()

    # RAFT LEADER ELECTION

    def start_election(self):
        """
        Transition to candidate state and start a new election.
        
        As per Raft Section 5.2:
        - Increment current_term
        - Vote for self
        - Reset election timer
        - Send RequestVote RPCs to all other servers
        """
        self.current_term += 1
        self.role = CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.leader_id = None
        
        print(f"[{self.node_id}] Starting election for term {self.current_term}")

        msg = make_request_vote(
            self.node_id,
            "all_nodes",
            self.current_term,
            self._get_last_log_index(),
            self._get_last_log_term()
        )
        self._send(msg)

    def handle_request_vote(self, msg):
        """
        Handle a RequestVote RPC from a candidate.
        
        Follows Raft Section 5.1, 5.2, and 5.4.1:
        1. Reply false if term < current_term (Section 5.1).
        2. If voted_for is null or candidate_id, AND candidate's log is at 
           least as up-to-date as receiver's log, grant vote (Section 5.2, 5.4.1).
        """
        with self.lock:
            candidate_term = msg.get("term")
            candidate_id = msg.get("src")
            candidate_last_index = msg.get("last_log_index")
            candidate_last_term = msg.get("last_log_term")

            # Term discovery: if candidate's term is newer, step down to follower
            if candidate_term > self.current_term:
                self._step_down(candidate_term)
                self.save_state()

            vote_granted = False

            # Rule 1: Reply false if term < current_term
            if candidate_term < self.current_term:
                vote_granted = False
            # Rule 2: If voted_for is null or candidate_id, and log is up-to-date, grant vote
            elif self.voted_for in (None, candidate_id):
                my_last_index = self._get_last_log_index()
                my_last_term = self._get_last_log_term()
                
                # Log up-to-date check (Section 5.4.1)
                log_ok = (candidate_last_term > my_last_term) or \
                         (candidate_last_term == my_last_term and \
                          candidate_last_index >= my_last_index)
                
                if log_ok:
                    vote_granted = True
                    self.voted_for = candidate_id
                    # Reset election timeout when granting vote
                    self.last_heartbeat_time = time.time()
                    self.save_state()
                    print(f"[{self.node_id}] Term {self.current_term}: Voted for {candidate_id}")

            # Send response
            response = make_request_vote_response(
                self.node_id,
                candidate_id,
                self.current_term,
                vote_granted
            )
            self._send(response)

    def handle_request_vote_response(self, msg):
        """
        Handle a RequestVoteResponse from another node.
        
        As per Raft Section 5.2:
        - On majority, become leader and send heartbeats
        - If responder's term > current term, step down
        """
        with self.lock:
            term = msg.get("term")
            vote_granted = msg.get("vote_granted")
            voter_id = msg.get("src")

            # If response contains a newer term, step down
            if term > self.current_term:
                print(f"[{self.node_id}] Term {term} is higher than {self.current_term}, stepping down")
                self._step_down(term)
                self.save_state()
                return

            # Only process if we are still a candidate and it's for our term
            if self.role == CANDIDATE and term == self.current_term:
                if vote_granted:
                    self.votes_received.add(voter_id)
                    
                    # Check for majority
                    if len(self.votes_received) > CLUSTER_SIZE // 2:
                        print(f"[{self.node_id}] Won election for term {self.current_term} with {len(self.votes_received)} votes")
                        self.role = LEADER
                        self.leader_id = self.node_id
                        
                        # Initialize leader state (Section 5.3)
                        last_index = self._get_last_log_index()
                        for other_id in NODE_IDS:
                            if other_id != self.node_id:
                                self.next_index[other_id] = last_index + 1
                                self.match_index[other_id] = 0
                        
                        # Send heartbeats immediately to establish leadership
                        self.send_heartbeats()

    # RAFT LOG REPLICATION

    def send_heartbeats(self):
        """
        Send AppendEntries RPCs to all followers.
        
        As per Raft Section 5.3:
        - Include log entries starting from next_index[follower]
        - If log is empty or heartbeat, entries will be empty
        - (Log Compaction): If next_index <= last_included_index, send InstallSnapshot
        """
        for follower_id in NODE_IDS:
            if follower_id == self.node_id:
                continue
                
            if self.next_index[follower_id] <= self.last_included_index:
                # Follower needs a snapshot
                msg = make_install_snapshot(
                    self.node_id,
                    follower_id,
                    self.current_term,
                    self.last_included_index,
                    self.last_included_term,
                    {
                        "kv_store": self.kv_store,
                        "client_responses": self.client_responses
                    }
                )
                self._send(msg)
                continue

            prev_log_index = self.next_index[follower_id] - 1
            prev_log_term = self._get_log_term(prev_log_index)
            entries = self._get_log_slice(self.next_index[follower_id])
            
            msg = make_append_entries(
                self.node_id,
                follower_id,
                self.current_term,
                prev_log_index,
                prev_log_term,
                entries,
                self.commit_index
            )
            self._send(msg)

    def handle_append_entries(self, msg):
        """
        Handle an AppendEntries RPC from the leader.
        
        Follows Raft Section 5.1 and 5.3:
        1. Reply false if term < current_term (Section 5.1).
        2. If term >= current_term, reset election timeout and update leader_id.
        3. If term > current_term, step down.
        4. Reply false if log doesn't contain an entry at prev_log_index 
           whose term matches prev_log_term (Section 5.3).
        5. If an existing entry conflicts with a new one (same index but 
           different terms), delete the existing entry and all that follow it (Section 5.3).
        6. Append any new entries not already in the log.
        7. If leader_commit > commit_index, set commit_index = 
           min(leader_commit, index of last new entry).
        """
        with self.lock:
            term = msg.get("term")
            leader_id = msg.get("src")
            prev_log_index = msg.get("prev_log_index")
            prev_log_term = msg.get("prev_log_term")
            entries = msg.get("entries", [])
            leader_commit = msg.get("leader_commit")

            success = False

            # Rule 1: Reply false if term < current_term
            if term < self.current_term:
                success = False
            else:
                # Rule 2: Valid heartbeat/append - reset election timeout
                self.last_heartbeat_time = time.time()
                self.leader_id = leader_id

                # Rule 3: Term discovery
                if term > self.current_term:
                    self._step_down(term)
                    self.save_state()

                # Rule 4: Consistency check
                # Note: index 0 is always "consistent" (base case)
                log_ok = (prev_log_index == 0) or \
                         (prev_log_index <= self._get_last_log_index() and \
                          self._get_log_term(prev_log_index) == prev_log_term)

                if not log_ok:
                    success = False
                else:
                    success = True
                    
                    # Rule 5 & 6: Append new entries and resolve conflicts
                    if entries:
                        # Find where the conflict starts or where to append
                        for entry in entries:
                            index = entry["index"]
                            term = entry["term"]
                            
                            existing = self._get_log_entry(index)
                            if existing:
                                if existing["term"] != term:
                                    # Conflict: delete this and all follow
                                    self.log = [e for e in self.log if e["index"] < index]
                                    self.log.append(entry)
                            else:
                                # Not in log, just append
                                self.log.append(entry)
                        
                        self.save_state()

                    # Rule 7: Update commit_index
                    if leader_commit > self.commit_index:
                        last_new_index = entries[-1]["index"] if entries else prev_log_index
                        self.commit_index = min(leader_commit, last_new_index)
                        self.apply_committed()

            # Send response
            response = make_append_entries_response(
                self.node_id,
                leader_id,
                self.current_term,
                success
            )
            self._send(response)

    def handle_append_entries_response(self, msg):
        """
        Handle a response to an AppendEntries RPC.
        
        Follows Raft Section 5.3:
        - If successful, update next_index and match_index for follower
        - Check if there exists an N > commit_index such that a majority
          of match_index[i] >= N, and log[N].term == current_term
        """
        with self.lock:
            term = msg.get("term")
            success = msg.get("success")
            follower_id = msg.get("src")

            if term > self.current_term:
                self._step_down(term)
                self.save_state()
                return

            if self.role == LEADER and term == self.current_term:
                if success:
                    # Update indices based on our last sent message for this follower
                    # (Simplified: we assume success means they have up to our last log index)
                    # A more robust version would use the number of entries sent.
                    self.match_index[follower_id] = self._get_last_log_index()
                    self.next_index[follower_id] = self.match_index[follower_id] + 1
                    
                    # Try to advance commit_index
                    # Find highest N such that a majority have match_index >= N
                    # and log[N].term == current_term (Section 5.4.2)
                    last_index = self._get_last_log_index()
                    for n in range(last_index, self.commit_index, -1):
                        count = 1 # count ourselves
                        for other_id in NODE_IDS:
                            if other_id != self.node_id and self.match_index.get(other_id, 0) >= n:
                                count += 1
                        
                        if count > CLUSTER_SIZE // 2:
                            if self._get_log_term(n) == self.current_term:
                                self.commit_index = n
                                self.apply_committed()
                            break
                else:
                    # Part 3: Handle log backtracking
                    # Retry by decrementing next_index and resending AppendEntries
                    follower_id = msg.get("src")
                    self.next_index[follower_id] = max(
                        1,
                        self.next_index.get(follower_id, 1) - 1
                    )

                    # Retry replication immediately
                    self.send_heartbeats()

    # CLIENT REQUEST HANDLING




    def handle_client_request(self, msg):  
        """
        Handle a request from a client.
        
        Follows Raft Section 5.3:
        - If not leader, redirect to leader (Part 3 adds forwarding)
        - If GET, can be served directly from state machine (Part 4 adds linearizability)
        - If PUT/DELETE, append to log and wait for commitment
        """
        with self.lock:
            client_id = msg.get("src")
            request_id = msg.get("request_id")
            operation = msg.get("operation")
            key = msg.get("key")
            value = msg.get("value")

            # Part 3: Return cached response if this request was already handled
            if request_id in self.client_responses:
                self._send(self.client_responses[request_id])
                return

            if self.role != LEADER:
                # Part 2: Just report we are not the leader
                # Part 3: If leader is known, forward the request instead
                if self.leader_id:
                    msg["dst"] = self.leader_id
                    self._send(msg)
                    return
                else:
                    #parta3 if no leader is known ignore the request
                    return
            # Part 3: ensure we dont append duplicate requests already in the log
            for entry in self.log:
                if entry.get("request_id") == request_id:
                    # If already committed, resend cached response
                    if request_id in self.client_responses:
                        self._send(self.client_responses[request_id])
                    return

            if operation == "GET":
                # Part 2: Serve GET directly from kv_store
                # Part 4: Ensure leader is still valid before serving GET

                if self.role != LEADER:
                    if self.leader_id:
                        msg["dst"] = self.leader_id
                        self._send(msg)
                    else:
                        response = make_client_response(
                            self.node_id,
                            client_id,
                            request_id,
                            False,
                            value=None,
                            leader_hint=None,
                            error="Not leader"
                        )
                        self._send(response)
                    return

                # Part 4: Leader confirms majority by sending heartbeat
                self.send_heartbeats()

                val = self.kv_store.get(key)
                success = key in self.kv_store

                response = make_client_response(
                    self.node_id,
                    client_id,
                    request_id,
                    success,
                    value=val,
                    error=None if success else "Key not found"
                )

                self._send(response)               



            elif operation in ("PUT", "DELETE"):
                # Part 2: Append to log, do not apply yet
                new_index = self._get_last_log_index() + 1
                entry = {
                    "index": new_index,
                    "term": self.current_term,
                    "command": {
                        "operation": operation,
                        "key": key,
                        "value": value
                    },
                    # Part 3: Store client metadata for deduplication and response caching
                    "client_id": client_id,
                    "request_id": request_id
                }

                self.log.append(entry)
                self.save_state()

                # Update match_index for self
                self.match_index[self.node_id] = new_index
                self.next_index[self.node_id] = new_index + 1

                self.send_heartbeats()

                # We do NOT send a response here.
                # The response is sent in apply_committed() once the
                # entry is replicated and committed.

    def handle_install_snapshot(self, msg):
        """
        Handle an InstallSnapshot RPC from the leader.
        """
        with self.lock:
            term = msg.get("term")
            leader_id = msg.get("src")
            last_included_index = msg.get("last_included_index")
            last_included_term = msg.get("last_included_term")
            data = msg.get("data")

            if term < self.current_term:
                success = False
            else:
                self.last_heartbeat_time = time.time()
                self.leader_id = leader_id
                if term > self.current_term:
                    self._step_down(term)
                    self.save_state()

                # Rule 6: If existing log entry has same index and term as 
                # snapshot's last included entry, retain log entries following it
                # (Simplified: we just clear if it's ahead)
                if last_included_index > self.last_included_index:
                    self.load_snapshot(data)
                    self.last_included_index = last_included_index
                    self.last_included_term = last_included_term
                    
                    # Discard log entries up to last_included_index
                    self.log = [e for e in self.log if e["index"] > last_included_index]
                    
                    if last_included_index > self.commit_index:
                        self.commit_index = last_included_index
                    if last_included_index > self.last_applied:
                        self.last_applied = last_included_index
                    
                    self.save_state()
                    print(f"[{self.node_id}] Installed snapshot up to index {last_included_index}")
                
                success = True

            response = make_install_snapshot_response(
                self.node_id,
                leader_id,
                self.current_term,
                last_included_index,
                success
            )
            self._send(response)

    def handle_install_snapshot_response(self, msg):
        """
        Handle an InstallSnapshot response from a follower.
        """
        with self.lock:
            term = msg.get("term")
            success = msg.get("success")
            follower_id = msg.get("src")
            last_included_index = msg.get("last_included_index")

            if term > self.current_term:
                self._step_down(term)
                self.save_state()
                return

            if self.role == LEADER and term == self.current_term:
                if success:
                    # Update indices based on the snapshot
                    self.match_index[follower_id] = max(self.match_index.get(follower_id, 0), last_included_index)
                    self.next_index[follower_id] = self.match_index[follower_id] + 1
                    
                    # Try to advance commit_index (though snapshot itself doesn't advance it, 
                    # it might have been advanced by other means)

    # STATE MACHINE APPLICATION

    def apply_committed(self):
        """
        Apply committed log entries to the state machine.
        
        Follows Raft Section 5.3:
        - Apply each entry from last_applied + 1 up to commit_index
        - If we are leader, send response to the client
        - (Log Compaction): Trigger snapshot if log exceeds threshold
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self._get_log_entry(self.last_applied)
            if not entry:
                continue

            command = entry.get("command", {})
            op = command.get("operation")
            key = command.get("key")
            val = command.get("value")

            if op == "PUT":
                self.kv_store[key] = val
            elif op == "DELETE":
                self.kv_store.pop(key, None)

            # If we are the leader who originally received this, respond to client
            if self.role == LEADER:
                client_id = entry.get("client_id")
                request_id = entry.get("request_id")
                if client_id and request_id:
                    response = make_client_response(
                        self.node_id,
                        client_id,
                        request_id,
                        True,
                        value=val,
                        error=None
                    )
                    self.client_responses[request_id] = response
                    self._send(response)

        # Check if we should take a snapshot
        if len(self.log) >= SNAPSHOT_THRESHOLD:
            self.take_snapshot()

    # CHECKPOINTING / SNAPSHOTTING (Part 3)

    def take_snapshot(self):
        """
        Create a snapshot of the current state machine and discard old log entries.
        """
        # We can only snapshot what has been applied to the state machine
        if self.last_applied <= self.last_included_index:
            return

        # Find the entry at last_applied to get its term
        last_applied_entry = None
        for entry in self.log:
            if entry["index"] == self.last_applied:
                last_applied_entry = entry
                break
        
        if not last_applied_entry:
            return

        self.last_included_index = last_applied_entry["index"]
        self.last_included_term = last_applied_entry["term"]

        # Discard log entries up to last_included_index
        self.log = [e for e in self.log if e["index"] > self.last_included_index]

        self.save_state()
        print(f"[{self.node_id}] Took snapshot up to index {self.last_included_index}, log size now {len(self.log)}")

    def load_snapshot(self, snapshot_data):
        """
        Load the state machine state from a snapshot.
        """
        if snapshot_data:
            self.kv_store = snapshot_data.get("kv_store", {})
            self.client_responses = snapshot_data.get("client_responses", {})

    # STATE PERSISTENCE (Part 3)

    def save_state(self):
        """
        Persist Raft state to disk.
        """
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "kv_store": self.kv_store,
            "client_responses": self.client_responses
        }

        path = os.path.join(DATA_DIR, f"node_{self.node_id}.json")
        try:
            with open(path + ".tmp", "w") as f:
                json.dump(state, f)
            os.replace(path + ".tmp", path)
        except Exception as e:
            print(f"[{self.node_id}] Error saving state: {e}")

    def load_state(self):
        """
        Load persisted Raft state from disk.
        """
        path = os.path.join(DATA_DIR, f"node_{self.node_id}.json")
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    state = json.load(f)
                    self.current_term = state.get("current_term", 0)
                    self.voted_for = state.get("voted_for")
                    self.log = state.get("log", [])
                    self.last_included_index = state.get("last_included_index", 0)
                    self.last_included_term = state.get("last_included_term", 0)
                    self.kv_store = state.get("kv_store", {})
                    self.client_responses = state.get("client_responses", {})
                    
                    # Update volatile state to match snapshot
                    self.commit_index = max(self.commit_index, self.last_included_index)
                    self.last_applied = max(self.last_applied, self.last_included_index)
            except Exception as e:
                print(f"[{self.node_id}] Error loading state: {e}")

    # HELPER METHODS 

    def _get_last_log_index(self):
        """Return the index of the last log entry, or 0 if log is empty."""
        if self.log:
            return self.log[-1]["index"]
        return self.last_included_index

    def _get_last_log_term(self):
        """Return the term of the last log entry, or 0 if log is empty."""
        if self.log:
            return self.log[-1]["term"]
        return self.last_included_term

    def _get_log_term(self, index):
        """Return the term of the log entry at the given index, or 0."""
        if index == 0:
            return 0
        if index == self.last_included_index:
            return self.last_included_term
        for entry in self.log:
            if entry["index"] == index:
                return entry["term"]
        return 0

    def _get_log_entry(self, index):
        """Return the log entry at the given index, or None."""
        if index <= self.last_included_index:
            return None
        for entry in self.log:
            if entry["index"] == index:
                return entry
        return None

    def _get_log_slice(self, from_index):
        """Return all log entries from from_index onward (inclusive)."""
        return [e for e in self.log if e["index"] >= from_index]

    def _step_down(self, new_term):
        """Revert to follower state with a new term."""
        self.current_term = new_term
        self.role = FOLLOWER
        self.voted_for = None
        self.leader_id = None

# ENTRY POINT

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python node.py <node-id>")
        print(f"  Valid node IDs: {NODE_IDS}")
        sys.exit(1)

    node_id = sys.argv[1]
    if node_id not in NODE_IDS:
        print(f"Invalid node ID '{node_id}'. Must be one of: {NODE_IDS}")
        sys.exit(1)

    node = RaftNode(node_id)
    node.start()
