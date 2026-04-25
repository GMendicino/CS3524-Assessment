# CS3524 Assignment

## Overview

In this assignment you will implement a distributed key-value store powered by the Raft consensus algorithm. Your system will consist of a cluster of 5 nodes that work together to provide a reliable, consistent key-value storage service, even when the network is unreliable.

You are given:
- A network simulator that routes messages between nodes and clients
- Network variants that simulate real world failures (delays, drops, partitions)
- An interactive client for manual testing and debugging
- An automated test client that checks your implementation against the criteria below
- A node template (`node.py`) with the skeleton code you need to complete
- `STARTER_EXPLANATION.md` explains the basics of the starter code, but if you have additional questions or if anything is unclear, please ask on the discussion board

You will extend `node.py` to add:
- Raft leader election - nodes elect a single leader to coordinate operations
- Raft log replication - the leader replicates client operations to all followers
- A key-value state machine - committed operations are applied to a shared store
- Checkpointing - periodic snapshots to prevent unbounded log growth
- An extension of your choosing - research, implement, and test a feature which improves upon the final implementation. Feel free to ask in practicals if you are unsure if a chosen feature is suitable or not

## Architecture

The system uses a star topology (https://en.wikipedia.org/wiki/Star_network) with the Network process at the centre. All five nodes (Node 0 through Node 4) and all clients connect to the Network via individual TCP socket connections. The Network acts as a central message router. It receives messages from any connected endpoint and forwards them to the appropriate destination based on the `dst` (destination) field in each message. Nodes and clients never communicate directly with each other, every message passes through the Network.

This design lets us swap the network behaviour (perfect, delayed, lossy, partitioned) without changing your node code. The message protocol uses length prefixed JSON over TCP.

Message routing works as follows:
- If `dst` is a specific ID (e.g. `"node-2"`), the message is forwarded to that endpoint.
- If `dst` is `"all_nodes"`, the message is broadcast to all registered nodes except the sender.
- If `dst` is `"leader"`, the message is broadcast to all registered nodes (each node decides whether to handle it based on its current role).

## Files

Do not modify (they will be overwritten with the originals when I test your code):

- `config.py` - Configuration constants (ports, timeouts, cluster size).
- `network.py` - Perfect network (all messages delivered, small delay).
- `network_delayed.py` - Variable delays (50-500ms), messages may reorder.
- `network_lossy.py` - 10% message drop rate + variable delays.
- `network_partition.py` - Periodic network partitions (2 vs 3 nodes).
- `network_chaos.py` - Everything combined: drops + delays + partitions. 
- `client_interactive.py` - Interactive client for manual testing. 
- `client_test.py` - Automated test suite organised by level. 
- `run_cluster.py` - Helper to launch network + all nodes at once. 

You may modify:

- `node.py` - A basic node, currently it contains the base code required for sending/recieving messages and for detecting timeouts. The majority of your Raft node implementation will extend this file. 
- `message.py` - Message types, send/receive protocol. You will implement the additional Raft message constructors that you need.
- `test_extension.py` - Additional tests you create for your extension. You will create this file.

## Getting Started

### Step 1: Read the provided code

Start by reading the files you will modify:

1. `config.py` - All the timing constants your Raft implementation will use
2. `message.py` - The message types and wire protocol. `make_client_response()` is provided as an example of the constructor pattern. You will implement the remaining Raft message constructors
3. `node.py` - Read the entire file. The provided code handles networking, message dispatch, and timing. You should fill in the TODO methods

You should also read through the files you won't modify e.g. `network.py`, `client_interactive.py`, `client_test.py`, and `run_cluster.py`. This is so you understand how the system fits together and how your node will be tested. Understanding the network routing, client request flow, and test expectations will help you make better design decisions based upon a holistic view of the network simulation.

### Step 2: Run the cluster

Open a terminal and start the cluster with the perfect network:

```bash
python run_cluster.py
```

In another terminal, try the interactive client:

```bash
python client_interactive.py
```

Commands won't work yet (your node doesn't handle them), but you'll see the system start up and the nodes connect.

### Step 3: Implement node.py

Work through the tasks in this document, extending `node.py`. Stub methods exist for most functionality you will need to write. After each section, test your progress:

```bash
python client_test.py basic # Test basic functionality (Part 1)
python client_test.py core # Test core Raft (Part 2)
python client_test.py robust # Test robustness (Part 3)
python client_test.py advanced # Test advanced features (Part 4)
python client_test.py # Run ALL tests
```

You can also stress test your implementation with multiple concurrent clients:

```bash
python client_test.py stress # 5 clients, 50 ops each
python client_test.py stress 10 # 10 clients, 50 ops each
python client_test.py stress 10 200 # 10 clients, 200 ops each
```

The stress test connects the specified number of clients simultaneously and each performs a mix of PUT (60%), GET (30%), and DELETE (10%) operations. It reports success rates, throughput (ops/sec), latency percentiles (p50/p95/p99), and verifies a sample of keys for correctness.

### Step 4: Test with harder networks

Once your implementation works on the perfect network, try progressively harder networks:

```bash
python run_cluster.py network_delayed.py
python run_cluster.py network_lossy.py
python run_cluster.py network_partition.py
python run_cluster.py network_chaos.py
```

## Submission Requirements

You must submit four things:

### Code - In a .zip

- All files, but most importantly:
- `node.py` - Your Raft node implementation
- `message.py` - With your implemented message constructors

### Report

You must submit a written report covering your entire implementation. The report should include:

- An overview of your design decisions for each part
- How you implemented the key Raft mechanisms (leader election, log replication, commitment, etc.)
- You may use images in your report
- You should cite sources that you use, although using external sources is not required. Citations will not count towards page count

The report should demonstrate your understanding of the Raft protocol and the design trade-offs you made. A page limit of 15 pages (20 pages if you attempted task 5) is in place for this report, but remember that this is a limit not a target, concise reports are better than a overly wordy report at the page limit.

### Video Demonstration

You must submit a video demonstration that walks through your implementation task by task. The video should:

- Show your system running on the appropriate network configurations
- Demonstrate each part's functionality (e.g. leader election, log replication, recovery from partitions)
- Walk through code implementation to show how it works
- Show the automated tests passing for each level
- For Part 5, demonstrate your extension working and explain what it does
- Be no longer than 15 minutes (20 minutes if you attempt task 5)

The video does not need to be polished, a screen recording with narration is sufficient. The purpose is to show the marker that your code works and that you understand what it is doing.

### 

You must both submit a group workload form that evaluates what proportion of the work was done by each partner.

### How marks are awarded

Marks for each part are awarded holistically. You will be assessed on each part based on the combined understanding from your code, report, and video demonstration. All three sources contribute to the marker's judgement of your understanding and the quality of your work.

This means:
- Strong code with a weak report and no video will score lower than it otherwise could
- A well-explained report and clear video can help the marker appreciate subtle aspects of your implementation
- If something is not working perfectly, explaining why in your report and showing what does work in your video can still earn partial marks

## Tasks

### Part 1 - Basic Functionality (30 marks)

Implement basic leader election and a simple key-value store.

Methods to implement:

1. `start_election()` - Increment term, become candidate, vote for self, send RequestVote to all nodes
2. `handle_request_vote()` - Grant a vote if the candidate's term is at least as high as yours and you haven't already voted for someone else this term. You do not need to compare logs at this stage (the full log up-to-date check is added in Part 2)
3. `handle_request_vote_response()` - Count votes, become leader on majority
4. `send_heartbeats()` - Send empty AppendEntries to all followers to prevent new elections. For now, heartbeats do not need to carry log entries (that is added in Part 2)
5. `handle_append_entries()` - Reset election timeout when receiving a valid heartbeat. For now, you do not need to handle log entries or consistency checks (those are added in Part 2)
6. `handle_client_request()` - If leader, handle PUT/GET directly on `self.kv_store`. For now, operations do not go through the Raft log (that is added in Part 2). Non-leader handling and deduplication are added in Part 3

You will also need to implement the corresponding message constructors in `message.py` (e.g. `make_request_vote`, `make_append_entries`) following the pattern shown by `make_client_response`.

What should work: On the perfect network, a leader is elected and basic PUT/GET operations succeed.

Example - using the interactive client after completing Part 1:

```
[client-a1b2c3] > PUT mykey hello
OK (45ms)

[client-a1b2c3] > GET mykey
mykey = hello (38ms)

[client-a1b2c3] > PUT mykey world
OK (41ms)

[client-a1b2c3] > GET mykey
mykey = world (36ms)

[client-a1b2c3] > GET nonexistent
NOT FOUND (33ms)
```

What the automated tests check at this level:
- PUT a key then GET it back, the returned value must match
- PUT 5 different keys then GET each one, all values must be correct
- PUT the same key twice with different values then GET it, it must return the second value
- GET a key that was never stored, it must return `success=False`

### Part 2 - Core Raft (20 marks)

Make operations go through the Raft log for proper replication.

Extend your implementation:

7. `handle_request_vote()` - Add the log up-to-date check, only grant a vote if the candidate's log is at least as up-to-date as yours. Compare last log term first (higher term wins), then last log index (longer log wins if terms are equal)
8. `handle_client_request()` - PUT/DELETE: append the operation to the Raft log as a new entry instead of writing directly to kv_store. The entry will be applied later when it is committed. GET can still read directly from kv_store for now (linearisable reads are added in Part 4)
9. `send_heartbeats()` - Include log entries that each follower is missing. Use `next_index[follower_id]` to determine which entries to send to each follower
10. `handle_append_entries()` - Check that the previous log entry matches (same index and term) before accepting new entries. If the check fails, respond with `success=False`. If it passes, append the new entries to the log and update `commit_index` if the leader's commit index is higher
11. `handle_append_entries_response()` - On success, update `match_index` and `next_index` for that follower. Then check if any log entry has been replicated to a majority of nodes - if so, advance `commit_index` to that entry (only commit entries from the current term). For now, you can assume AppendEntries always succeeds on the perfect network (handling `success=False` with log backtracking is added in Part 3)
12. `apply_committed()` - Walk through all log entries between `last_applied` and `commit_index`, applying each command (PUT/DELETE) to `kv_store`. Send the client response after applying each entry

What should work: On the perfect network, all operations go through the Raft log, are replicated to a majority, and are committed before responding to clients.

Example - sequential operations and delete:

```
[client-a1b2c3] > PUT seq-0 alpha
OK (52ms)

[client-a1b2c3] > PUT seq-1 bravo
OK (48ms)

[client-a1b2c3] > PUT seq-2 charlie
OK (50ms)

[client-a1b2c3] > GET seq-1
seq-1 = bravo (39ms)

[client-a1b2c3] > DELETE seq-1
DELETED (47ms)

[client-a1b2c3] > GET seq-1
NOT FOUND (35ms)
```

What the automated tests check at this level:
- A leader is elected within 5 seconds and can process a PUT request
- 10 sequential PUT operations followed by 10 GETs - every value must match and ordering must be preserved
- PUT a key, DELETE it, then GET it - the GET must return `success=False`

### Part 3 - Robustness (20 marks)

Handle failures, duplicates, log compaction, and state persistence.

Extend your implementation:

13. Term discovery in all handlers - When any handler receives a message with a term higher than `current_term`, update your term to match and convert to follower (reset `voted_for`, stop being leader/candidate etc). This ensures that stale leaders step down when a new term begins, enabling re-election
14. `handle_append_entries_response()` - When a follower responds with `success=False`, decrement `next_index` for that follower by 1 and retry on the next heartbeat. This allows the leader to find the correct point where the follower's log matches (log backtracking)
15. `handle_client_request()` - If this node is not the leader but knows who the leader is, forward the request to the leader instead of dropping it. This gives the request a second chance to reach the leader on lossy networks where the direct broadcast may have been dropped. Also track each request's `request_id` and cache the response so that if a duplicate request arrives (e.g. the client retried after a timeout, or multiple followers forwarded the same request), you return the cached response instead of applying the operation twice
16. `save_state()` and `load_state()` - Persist `current_term`, `voted_for`, and `log` to disk as JSON after any change to these fields. Restore them on startup. See the Persistence Format section below for the exact file structure
17. `take_snapshot()` - Save the current `kv_store` state along with the index and term of the last applied entry. Discard all log entries up to and including that index. Call this periodically (e.g. when the log exceeds a configurable size)
18. `load_snapshot()` - On startup, if a snapshot exists in the saved state, restore `kv_store` from the snapshot and set `last_applied` and `commit_index` to the snapshot's last included index

What should work: On the delayed and lossy networks, the system recovers from message loss, handles duplicate client requests, compacts the log, and preserves its state across restarts.

What the automated tests check at this level:

Note: On lossy networks, the network may drop the response from the leader back to the client even though the operation was committed successfully inside Raft. This means some requests will appear to fail from the client's perspective despite being correctly applied. The tests account for this by using retries and by requiring a high (but not 100%) success rate. Correctness is always strictly enforced, i.e every value that is confirmed written must be retrievable and correct.

- Two clients both PUT the same key with different values, then both GET it - they must see the same final value (the last committed write). Requests are retried up to 3 times to handle dropped responses
- Three clients writing concurrently (5 keys each, 15 keys total) - at least 80% of keys must be written successfully. Every successfully written key must return the correct value when verified
- 20 PUT operations in sequence - at least 80% must succeed
- Sending the same request twice (same `request_id`) does not apply the operation twice. The second request returns the cached response from the first

### Part 4 - Advanced Features (10 marks)

Handle network partitions and provide strong consistency guarantees.

Extend your implementation:

19. Partition handling - When a network partition splits the cluster, the majority side (3+ nodes) must be able to elect a new leader and continue serving requests. When the partition heals, any stale leader on the minority side must step down because it will see a higher term from the new leader. If your term discovery from task 13 is correct, this should work without additional code
20. Linearisable reads - A GET served by a stale leader could return an outdated value. To prevent this, before serving a GET the leader must confirm it is still the active leader by sending a round of heartbeats and receiving responses from a majority (this is the ReadIndex protocol). Only serve the read after confirmation succeeds
21. Survive chaos - Test your implementation on `network_chaos.py` (which combines message drops, delays, and partitions simultaneously) and fix any issues that arise. There is no new code to write for this task if everything is already handled, it is a validation that your implementation from the previous tasks is robust

What should work: On the partition and chaos networks, the system remains correct: committed data is never lost, reads are consistent, and the cluster recovers from any network condition.

Example - interleaved reads and writes that must be linearisable:

```
[client-a1b2c3] > PUT counter version-0
OK (55ms)

[client-a1b2c3] > GET counter
counter = version-0 (62ms)

[client-a1b2c3] > PUT counter version-1
OK (58ms)

[client-a1b2c3] > GET counter
counter = version-1 (60ms)

[client-a1b2c3] > PUT counter version-2
OK (71ms)

[client-a1b2c3] > GET counter
counter = version-2 (65ms)
```

Each GET must always return the value from the immediately preceding PUT - never a stale value from an earlier write. This is the linearisability guarantee.

What the automated tests check at this level:

Note: On partition and chaos networks, there will be periods where no leader is reachable (during a partition, the minority side has no quorum). Requests sent during these periods will time out. Raft still guarantees that any data that was committed is never lost and all nodes converge to the same state once the partition heals. The tests account for unavailability by tolerating timeouts, but correctness is always strictly enforced, returning a wrong value is always a hard fail.

- 10 keys are PUT, then after a 5 second delay (during which partitions may occur), the successfully written keys are verified. At least 70% of PUTs must succeed, and every key that was confirmed written must still return the correct value
- 50 PUT operations under stress, at least 60% must succeed. Every key that was confirmed written must return the correct value when sampled
- 10 rounds of PUT then immediately GET on the same key, at least 70% of rounds must complete successfully. Timeouts are tolerated (the network may be unavailable), but returning a stale or incorrect value is a hard fail (consistency violation)

### Part 5 - Extension (20 marks)

Research, implement, and test an extension to your Raft system. This part is open ended. You choose a feature, justify why it matters, build it, write tests that prove it works, and discuss it in your report/video.

You must submit:
- Your implementation (in `node.py`, and optionally `message.py` if you need new message types)
- Your tests (in `test_extension.py`)
- A discussion of your extension in your report 
- An additional 5 pages and 5 minutes may be added to your report/video to cover your extension. I.E 20 pages & 20 minute video

#### Choosing an extension

Research and choose your own extension to the Raft protocol. Your extension should be something that improves the system in a meaningful way. Whether that is correctness, performance, usability, or resilience. Read the Raft paper, related literature, or documentation of real Raft implementations to find ideas. If you would like to discuss whether a specific idea is appropriate, please email me, discuss with me in the practicals or book office hours.

A well implemented smaller extension with thorough tests and a clear explanation will score higher than a half finished ambitious one. 

#### Writing your tests (test_extension.py)

You must write an automated test file called `test_extension.py` that verifies your extension works correctly. Your tests should follow the same pattern as the provided `client_test.py`: connect to the cluster as a client, send requests, and check that the responses are correct.

Your test file must:
- Be runnable with `python test_extension.py` (while the cluster is running)
- Print clear output showing what is being tested and whether each test passed or failed
- Exit with code 0 if all tests pass, code 1 if any test fails
- Include at least 3 distinct test cases

Your tests are expected to cover:
- The basic working/expected functionality of your extension
- At least one edge case or error condition
- Behaviour under a non perfect network (e.g. run the cluster with `network_lossy.py` and verify your extension still works)

The key principle is that your tests should be able to run against your cluster and verify that your extension actually works, not just that the code doesn't crash.

I am assessing extensions based upon:

- Research and justification - Does the report clearly explain what the extension does and why it matters in a distributed system? Is it demonstrated how the extension fits into the Raft protocol?
- Implementation quality - Is the extension functional? Does it integrate correctly with the existing Raft protocol without breaking leader election, log replication, or safety properties?
- Testing - Are there meaningful test cases covering different aspects of the extension? Do the tests cover edge cases and non perfect networks? Would the tests actually catch bugs if the extension were broken?
- Report and video - Does the student clearly explain their design decisions, demonstrate the extension working, and honestly assess its limitations?

## Persistence Format

Each node saves its Raft state to `data/<node-id>.json` (e.g. `data/node-0.json`). The `data/` directory is defined by `DATA_DIR` in `config.py`. Create it if it does not exist. Write the file atomically (write to a temporary file, then `os.replace()` into place) to prevent corruption if the node crashes mid write.

Before a snapshot has been taken:

```json
{
  "current_term": 3,
  "voted_for": "node-2",
  "log": [
    {"index": 1, "term": 1, "command": {"operation": "PUT", "key": "x", "value": "1"}},
    {"index": 2, "term": 1, "command": {"operation": "PUT", "key": "y", "value": "2"}},
    {"index": 3, "term": 3, "command": {"operation": "DELETE", "key": "x", "value": null}}
  ],
  "snapshot": null
}
```

After a snapshot has been taken, the `log` only contains entries after the snapshot point:

```json
{
  "current_term": 5,
  "voted_for": null,
  "log": [
    {"index": 101, "term": 5, "command": {"operation": "PUT", "key": "z", "value": "3"}}
  ],
  "snapshot": {
    "kv_store": {"x": "1", "y": "2"},
    "last_included_index": 100,
    "last_included_term": 4
  }
}
