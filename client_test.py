# client_test.py - Automated test client
#
# Runs test scenarios against the distributed KV store cluster and reports
# pass/fail results
#
# Usage:
# python client_test.py (Run ALL tests)
# python client_test.py basic (Run basic tests only (Part 1))
# python client_test.py core (Run core tests only (Part 2))
# python client_test.py robust (Run robustness tests only (Part 3))
# python client_test.py advanced (Run advanced tests only (Part 4))
#
# python client_test.py stress (5 clients, 50 ops each)
# python client_test.py stress 10 (10 clients, 50 ops each)
# python client_test.py stress 10 200 (10 clients, 200 ops each)
#
# Before running tests, start the cluster:
# python run_cluster.py (perfect network)
# python run_cluster.py network_lossy.py (lossy network)
# python run_cluster.py network_partition.py (partition network)
#
# Do not modify this file

import socket
import sys
import time
import threading
import random
import uuid

from message import (
    send_message, recv_message, make_register, make_client_request,
    MSG_CLIENT_RESPONSE, MSG_REGISTER_ACK,
)
from config import NETWORK_HOST, NETWORK_PORT, CLIENT_TIMEOUT


# === Test Client Helper ===

class TestClient:
    """A client for use in automated tests."""

    def __init__(self, client_id=None):
        self.client_id = client_id or f"test-{uuid.uuid4().hex[:6]}"
        self.sock = None

    def connect(self):
        """Connect to the network and register. Returns True on success."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((NETWORK_HOST, NETWORK_PORT))
            self.sock.settimeout(CLIENT_TIMEOUT)
            send_message(self.sock, make_register(self.client_id, "client"))
            ack = recv_message(self.sock)
            return ack is not None and ack.get("type") == MSG_REGISTER_ACK
        except (ConnectionRefusedError, OSError) as e:
            print(f"Could not connect: {e}")
            return False

    def request(self, operation, key, value=None, timeout=None):
        """
        Send a request and wait for a matching response.

        Returns the response dict, or None on timeout/failure.
        """
        request_id = str(uuid.uuid4())
        msg = make_client_request(
            self.client_id, request_id, operation, key, value
        )
        send_message(self.sock, msg)

        deadline = time.time() + (timeout or CLIENT_TIMEOUT)
        while time.time() < deadline:
            try:
                response = recv_message(self.sock)
                if response is None:
                    return None
                if (response.get("type") == MSG_CLIENT_RESPONSE and
                        response.get("request_id") == request_id):
                    return response
            except socket.timeout:
                break
        return None

    def request_with_retry(self, operation, key, value=None,
                           timeout=None, retries=3):
        """Send a request with retries. Returns the first successful
        response, or the last failed response after all retries."""
        last = None
        for _ in range(retries):
            r = self.request(operation, key, value, timeout=timeout)
            if r and r.get("success"):
                return r
            last = r
        return last

    def _wait_for_response(self, request_id, timeout=None):
        """Wait for a response matching the given request_id."""
        deadline = time.time() + (timeout or CLIENT_TIMEOUT)
        while time.time() < deadline:
            try:
                response = recv_message(self.sock)
                if response is None:
                    return None
                if (response.get("type") == MSG_CLIENT_RESPONSE and
                        response.get("request_id") == request_id):
                    return response
            except socket.timeout:
                break
        return None

    def close(self):
        """Close the connection."""
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None


# === Test Runner ===

class TestRunner:
    """Runs test scenarios and reports results."""

    def __init__(self):
        self.results = []

    def run_test(self, name, test_func):
        """Run a single test and record the result."""
        print(f"\n--- TEST: {name} ---")
        try:
            passed = test_func()
            status = "PASS" if passed else "FAIL"
        except Exception as e:
            status = "ERROR"
            print(f"Exception: {e}")
            import traceback
            traceback.print_exc()
            passed = False
        self.results.append((name, status))
        print(f">>> {status}")
        return passed

    def report(self):
        """Print the final test report."""
        print()
        print("=== TEST RESULTS ===")
        passed = sum(1 for _, s in self.results if s == "PASS")
        total = len(self.results)
        for name, status in self.results:
            if status == "PASS":
                marker = "PASS"
            elif status == "FAIL":
                marker = "FAIL"
            else:
                marker = "ERR "
            print(f"[{marker}] {name}")
        print()
        print(f"{passed}/{total} tests passed")


# === BASIC TESTS - Basic Functionality (Part 1) ===

def test_basic_put_get():
    """PUT a value, then GET it back."""
    c = TestClient()
    if not c.connect():
        print("Could not connect to cluster")
        return False

    # Wait for leader election
    print("Waiting for leader election...")
    time.sleep(5)

    print("PUT test-key = test-value")
    r = c.request("PUT", "test-key", "test-value")
    if not r or not r.get("success"):
        print(f"PUT failed: {r}")
        c.close()
        return False

    print("GET test-key")
    r = c.request("GET", "test-key")
    c.close()

    if not r or not r.get("success"):
        print(f"GET failed: {r}")
        return False
    if r.get("value") != "test-value":
        print(f"Expected 'test-value', got '{r.get('value')}'")
        return False

    print("Got correct value: test-value")
    return True


def test_multiple_keys():
    """PUT and GET multiple different keys."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    keys = {f"multi-key-{i}": f"value-{i}" for i in range(5)}

    for k, v in keys.items():
        print(f"PUT {k} = {v}")
        r = c.request("PUT", k, v)
        if not r or not r.get("success"):
            print(f"PUT {k} failed: {r}")
            c.close()
            return False

    for k, expected in keys.items():
        print(f"GET {k}")
        r = c.request("GET", k)
        if not r or not r.get("success"):
            print(f"GET {k} failed: {r}")
            c.close()
            return False
        if r.get("value") != expected:
            print(f"Expected '{expected}', got '{r.get('value')}'")
            c.close()
            return False

    c.close()
    print(f"All {len(keys)} keys correct")
    return True


def test_overwrite():
    """PUT same key twice, GET returns the latest value."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    print("PUT overwrite-key = first")
    r = c.request("PUT", "overwrite-key", "first")
    if not r or not r.get("success"):
        print(f"First PUT failed: {r}")
        c.close()
        return False

    print("PUT overwrite-key = second")
    r = c.request("PUT", "overwrite-key", "second")
    if not r or not r.get("success"):
        print(f"Second PUT failed: {r}")
        c.close()
        return False

    print("GET overwrite-key")
    r = c.request("GET", "overwrite-key")
    c.close()

    if not r or r.get("value") != "second":
        print(f"Expected 'second', got '{r}'")
        return False

    print("Got correct value: second")
    return True


def test_get_nonexistent():
    """GET a key that was never PUT."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    print("GET nonexistent-key-12345")
    r = c.request("GET", "nonexistent-key-12345")
    c.close()

    if r is None:
        print("No response (timed out)")
        return False
    if r.get("success"):
        print(f"Should not have succeeded, got value: {r.get('value')}")
        return False

    print("Correctly reported key not found")
    return True


# === CORE TESTS - Core Raft (Part 2) ===

def test_leader_election():
    """Verify that the cluster elects a leader and can serve requests."""
    c = TestClient()
    if not c.connect():
        return False

    print("Waiting for leader election...")
    time.sleep(5)

    # A successful PUT implies a leader exists and is coordinating
    print("PUT election-test = works")
    r = c.request("PUT", "election-test", "works")
    c.close()

    if not r or not r.get("success"):
        print(f"PUT failed (no leader elected?): {r}")
        return False

    print("Leader is operational")
    return True


def test_sequential_operations():
    """Verify multiple sequential operations succeed through Raft log."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    num_ops = 10
    print(f"Running {num_ops} sequential PUT operations...")
    for i in range(num_ops):
        r = c.request("PUT", f"seq-key-{i}", f"seq-value-{i}")
        if not r or not r.get("success"):
            print(f"PUT seq-key-{i} failed at operation {i}: {r}")
            c.close()
            return False

    print(f"Verifying {num_ops} GET operations...")
    for i in range(num_ops):
        r = c.request("GET", f"seq-key-{i}")
        if not r or not r.get("success"):
            print(f"GET seq-key-{i} failed: {r}")
            c.close()
            return False
        if r.get("value") != f"seq-value-{i}":
            print(f"seq-key-{i}: expected 'seq-value-{i}', "
                  f"got '{r.get('value')}'")
            c.close()
            return False

    c.close()
    print(f"All {num_ops} operations successful")
    return True


def test_delete_operation():
    """PUT a key, DELETE it, then verify GET returns not found."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    print("PUT delete-me = temporary")
    r = c.request("PUT", "delete-me", "temporary")
    if not r or not r.get("success"):
        print(f"PUT failed: {r}")
        c.close()
        return False

    print("DELETE delete-me")
    r = c.request("DELETE", "delete-me")
    if not r or not r.get("success"):
        print(f"DELETE failed: {r}")
        c.close()
        return False

    print("GET delete-me (should fail)")
    r = c.request("GET", "delete-me")
    c.close()

    if r is None:
        print("No response")
        return False
    if r.get("success"):
        print(f"Key should have been deleted, got: {r.get('value')}")
        return False

    print("Key correctly deleted")
    return True


# === ROBUST TESTS - Robustness (Part 3) ===

def test_multiple_clients():
    """Two clients doing sequential operations on the same keys."""
    c1 = TestClient("multi-c1")
    c2 = TestClient("multi-c2")
    if not c1.connect() or not c2.connect():
        c1.close()
        c2.close()
        return False
    time.sleep(5)

    # Client 1 writes (retry to tolerate dropped responses)
    print("Client 1: PUT shared-key = from-c1")
    r1 = c1.request_with_retry("PUT", "shared-key", "from-c1", timeout=10)
    if not r1 or not r1.get("success"):
        print(f"Client 1 PUT failed after retries: {r1}")
        c1.close()
        c2.close()
        return False

    # Client 2 overwrites (retry to tolerate dropped responses)
    print("Client 2: PUT shared-key = from-c2")
    r2 = c2.request_with_retry("PUT", "shared-key", "from-c2", timeout=10)
    if not r2 or not r2.get("success"):
        print(f"Client 2 PUT failed after retries: {r2}")
        c1.close()
        c2.close()
        return False

    # Both clients should see the same final value
    print("Client 1: GET shared-key")
    r1 = c1.request_with_retry("GET", "shared-key", timeout=10)
    print("Client 2: GET shared-key")
    r2 = c2.request_with_retry("GET", "shared-key", timeout=10)

    c1.close()
    c2.close()

    if not r1 or not r2:
        print("One or both GETs failed after retries")
        return False

    v1 = r1.get("value")
    v2 = r2.get("value")
    if v1 != v2:
        print(f"Consistency violation! c1 sees '{v1}', c2 sees '{v2}'")
        return False

    if v1 != "from-c2":
        print(f"Expected 'from-c2' (last write), got '{v1}'")
        return False

    print(f"Both clients see '{v1}' (consistent)")
    return True


def test_concurrent_writes():
    """Multiple clients writing different keys concurrently."""
    num_clients = 3
    lock = threading.Lock()
    successful_keys = {}  # key -> expected value
    errors = []

    def client_work(client_num):
        c = TestClient(f"conc-{client_num}")
        if not c.connect():
            with lock:
                errors.append(f"Client {client_num} failed to connect")
            return
        time.sleep(5)

        for i in range(5):
            key = f"conc-{client_num}-key-{i}"
            val = f"conc-{client_num}-val-{i}"
            r = c.request_with_retry("PUT", key, val, timeout=10)
            if r and r.get("success"):
                with lock:
                    successful_keys[key] = val
        c.close()

    # Launch clients in parallel
    print(f"Launching {num_clients} concurrent clients...")
    threads = []
    for i in range(num_clients):
        t = threading.Thread(target=client_work, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=60)

    if errors:
        for e in errors:
            print(e)
        return False

    total_keys = num_clients * 5
    print(f"{len(successful_keys)}/{total_keys} keys written successfully")

    # Need at least 80% of keys written
    if len(successful_keys) < total_keys * 0.8:
        print(f"Too few keys succeeded (need {int(total_keys * 0.8)})")
        return False

    # Verify the keys that were successfully written
    print(f"Verifying {len(successful_keys)} keys...")
    c = TestClient("conc-verify")
    if not c.connect():
        return False

    verified = 0
    for key, expected in successful_keys.items():
        r = c.request_with_retry("GET", key, timeout=10)
        if r and r.get("success") and r.get("value") == expected:
            verified += 1
        else:
            print(f"{key}: expected '{expected}', got '{r}'")

    c.close()
    print(f"Verified {verified}/{len(successful_keys)} keys")

    # All successfully written keys must verify correctly
    return verified == len(successful_keys)


def test_throughput():
    """Measure operations per second."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    num_ops = 20
    print(f"Running {num_ops} operations...")
    start = time.time()
    successes = 0
    for i in range(num_ops):
        r = c.request("PUT", f"perf-{i}", f"val-{i}")
        if r and r.get("success"):
            successes += 1

    elapsed = time.time() - start
    c.close()

    ops_per_sec = successes / elapsed if elapsed > 0 else 0
    print(f"{successes}/{num_ops} ops in {elapsed:.2f}s "
          f"= {ops_per_sec:.1f} ops/sec")

    # Pass if at least 80% of operations succeeded (responses may be
    # dropped on lossy networks even when the operation committed)
    return successes >= num_ops * 0.8


def test_deduplication():
    """Verify duplicate requests (same request_id) are not applied twice."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    # Step 1: PUT key with request_id "dedup-1"
    req_id_1 = "dedup-1"
    msg1 = make_client_request(c.client_id, req_id_1, "PUT", "dedup-key", "first")
    send_message(c.sock, msg1)
    r1 = c._wait_for_response(req_id_1)
    if not r1 or not r1.get("success"):
        print(f"First PUT failed: {r1}")
        c.close()
        return False
    print("First PUT succeeded (dedup-key = first)")

    # Step 2: PUT same key with a different value and request_id "dedup-2"
    req_id_2 = "dedup-2"
    msg2 = make_client_request(c.client_id, req_id_2, "PUT", "dedup-key", "second")
    send_message(c.sock, msg2)
    r2 = c._wait_for_response(req_id_2)
    if not r2 or not r2.get("success"):
        print(f"Second PUT failed: {r2}")
        c.close()
        return False
    print("Second PUT succeeded (dedup-key = second)")

    # Step 3: Resend the first request (same request_id "dedup-1")
    # If deduplication works, this should return the cached response
    # and NOT overwrite the key back to "first"
    send_message(c.sock, msg1)
    r3 = c._wait_for_response(req_id_1)
    if r3 is None:
        print("No response to duplicate request")
        c.close()
        return False
    print(f"Duplicate request returned: {r3}")

    # Step 4: GET the key - should still be "second"
    r4 = c.request("GET", "dedup-key")
    c.close()

    if not r4 or not r4.get("success"):
        print(f"GET failed: {r4}")
        return False

    if r4.get("value") == "second":
        print("Deduplication works: value is still 'second'")
        return True
    else:
        print(f"Deduplication failed: expected 'second', got '{r4.get('value')}'")
        return False


# === ADVANCED TESTS - Advanced Features (Part 4) ===

def test_data_persistence_after_delay():
    """Write data, wait through network delays, verify data persists."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    num_keys = 10

    # Write some data (track which PUTs succeeded)
    written = []
    print(f"Writing {num_keys} keys...")
    for i in range(num_keys):
        r = c.request("PUT", f"persist-{i}", f"data-{i}", timeout=10)
        if r and r.get("success"):
            written.append(i)
        else:
            print(f"PUT persist-{i} failed: {r}")

    print(f"{len(written)}/{num_keys} keys written successfully")

    if len(written) < num_keys * 0.7:
        print(f"Too few keys written (need {int(num_keys * 0.7)})")
        c.close()
        return False

    # Wait to let network conditions fluctuate
    print("Waiting 5 seconds...")
    time.sleep(5)

    # Verify the data that was successfully written
    print(f"Verifying {len(written)} keys...")
    verified = 0
    for i in written:
        r = c.request("GET", f"persist-{i}", timeout=10)
        if r and r.get("success") and r.get("value") == f"data-{i}":
            verified += 1
        else:
            print(f"persist-{i}: expected 'data-{i}', got '{r}'")

    c.close()
    print(f"Verified {verified}/{len(written)} keys")

    # All successfully written keys must still be readable
    return verified == len(written)


def test_stress():
    """High volume of operations to stress test the system."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    num_ops = 50
    print(f"Stress test: {num_ops} PUT operations...")
    successes = 0
    failures = 0
    start = time.time()

    for i in range(num_ops):
        r = c.request("PUT", f"stress-{i}", f"val-{i}", timeout=10)
        if r and r.get("success"):
            successes += 1
        else:
            failures += 1

    elapsed = time.time() - start
    print(f"{successes}/{num_ops} succeeded, {failures} failed "
          f"in {elapsed:.1f}s")

    # Verify a sample of the successful writes
    print("Verifying a sample of written keys...")
    verified = 0
    for i in range(0, num_ops, 5):  # Check every 5th key
        r = c.request("GET", f"stress-{i}", timeout=10)
        if r and r.get("success") and r.get("value") == f"val-{i}":
            verified += 1

    c.close()
    sample_size = len(range(0, num_ops, 5))
    print(f"Verified {verified}/{sample_size} sampled keys")

    # Pass if at least 60% of operations succeeded (on chaos networks,
    # drops and partitions mean many responses are lost even when the
    # operation committed on the Raft cluster)
    return successes >= num_ops * 0.6


def test_interleaved_read_write():
    """Interleave reads and writes to verify consistency."""
    c = TestClient()
    if not c.connect():
        return False
    time.sleep(5)

    num_rounds = 10
    passed_rounds = 0
    failed_rounds = 0

    print("Running interleaved read/write pattern...")
    for i in range(num_rounds):
        # Write
        r = c.request("PUT", "interleave-key", f"version-{i}", timeout=10)
        if not r or not r.get("success"):
            print(f"PUT version-{i} timed out (skipping round)")
            failed_rounds += 1
            continue

        # Immediately read back
        r = c.request("GET", "interleave-key", timeout=10)
        if not r or not r.get("success"):
            print(f"GET after version-{i} timed out (skipping round)")
            failed_rounds += 1
            continue

        value = r.get("value")
        # The read should return exactly version-{i} since we just wrote
        # it and this is a single-client test
        if value != f"version-{i}":
            print(f"Consistency violation: PUT version-{i}, "
                  f"GET returned '{value}'")
            c.close()
            return False

        passed_rounds += 1

    c.close()
    print(f"{passed_rounds}/{num_rounds} read-after-write checks passed, {failed_rounds} timed out")

    # Pass if at least 70% of rounds completed successfully. Timeouts
    # are tolerated (network may drop responses), but any round where
    # the GET returned the wrong value is a hard failure above.
    return passed_rounds >= num_rounds * 0.7


# === Stress Test Mode ===

def run_stress_test(num_clients, ops_per_client):
    """
    Configurable stress test with multiple concurrent clients.

    Each client connects independently and performs a mix of PUT, GET,
    and DELETE operations using its own key space. A verification pass
    at the end checks that committed data is consistent.

    Usage:
        python client_test.py stress (5 clients, 50 ops)
        python client_test.py stress 10 (10 clients, 50 ops)
        python client_test.py stress 10 200 (10 clients, 200 ops)
    """
    print()
    print(f"=== STRESS TEST: {num_clients} clients x {ops_per_client} ops = {num_clients * ops_per_client} total ===")
    print(f"Cluster: {NETWORK_HOST}:{NETWORK_PORT}")
    print(f"Operation mix: 60% PUT, 30% GET, 10% DELETE")
    print()

    # Shared tracking across all client threads
    lock = threading.Lock()
    stats = {
        "put_ok": 0, "put_fail": 0,
        "get_ok": 0, "get_fail": 0,
        "del_ok": 0, "del_fail": 0,
        "timeouts": 0,
        "latencies": [],
        "errors": [],
    }
    # Track the last value written per key for verification
    written_keys = {}

    def client_worker(client_num):
        cid = f"stress-{client_num}"
        c = TestClient(cid)
        if not c.connect():
            with lock:
                stats["errors"].append(f"Client {cid} failed to connect")
            return

        # Stagger start slightly to avoid thundering herd
        time.sleep(3 + random.uniform(0, 1))

        prefix = f"s{client_num}"
        local_keys = {}

        for i in range(ops_per_client):
            # Choose operation: 60% PUT, 30% GET, 10% DELETE
            roll = random.random()
            if roll < 0.60 or not local_keys:
                # PUT
                key = f"{prefix}-k{i}"
                val = f"{prefix}-v{i}"
                t0 = time.time()
                r = c.request("PUT", key, val, timeout=10)
                elapsed = time.time() - t0
                with lock:
                    stats["latencies"].append(elapsed)
                    if r and r.get("success"):
                        stats["put_ok"] += 1
                        local_keys[key] = val
                        written_keys[key] = val
                    elif r is None:
                        stats["timeouts"] += 1
                    else:
                        stats["put_fail"] += 1
            elif roll < 0.90:
                # GET a previously written key
                key = random.choice(list(local_keys.keys()))
                t0 = time.time()
                r = c.request("GET", key, timeout=10)
                elapsed = time.time() - t0
                with lock:
                    stats["latencies"].append(elapsed)
                    if r and r.get("success"):
                        stats["get_ok"] += 1
                    elif r is None:
                        stats["timeouts"] += 1
                    else:
                        stats["get_fail"] += 1
            else:
                # DELETE a previously written key
                key = random.choice(list(local_keys.keys()))
                t0 = time.time()
                r = c.request("DELETE", key, timeout=10)
                elapsed = time.time() - t0
                with lock:
                    stats["latencies"].append(elapsed)
                    if r and r.get("success"):
                        stats["del_ok"] += 1
                        del local_keys[key]
                        written_keys.pop(key, None)
                    elif r is None:
                        stats["timeouts"] += 1
                    else:
                        stats["del_fail"] += 1

        c.close()

    # Launch all client threads
    print(f"Launching {num_clients} clients...")
    wall_start = time.time()
    threads = []
    for i in range(num_clients):
        t = threading.Thread(target=client_worker, args=(i,))
        t.start()
        threads.append(t)

    # Wait for all to finish (generous timeout)
    timeout_per = max(30, ops_per_client * 2)
    for t in threads:
        t.join(timeout=timeout_per)

    wall_elapsed = time.time() - wall_start

    # Connection errors
    if stats["errors"]:
        for e in stats["errors"]:
            print(f"ERROR: {e}")
        print()

    # Verification pass: sample up to 20 keys
    verified_ok = 0
    verified_fail = 0
    sample_keys = list(written_keys.items())
    if len(sample_keys) > 20:
        sample_keys = random.sample(sample_keys, 20)

    if sample_keys:
        print(f"Verifying {len(sample_keys)} keys...")
        vc = TestClient("stress-verify")
        if vc.connect():
            for key, expected in sample_keys:
                r = vc.request("GET", key, timeout=10)
                if r and r.get("success") and r.get("value") == expected:
                    verified_ok += 1
                else:
                    verified_fail += 1
            vc.close()

    # Compute stats
    total_ok = stats["put_ok"] + stats["get_ok"] + stats["del_ok"]
    total_fail = stats["put_fail"] + stats["get_fail"] + stats["del_fail"]
    total_ops = total_ok + total_fail + stats["timeouts"]
    success_rate = (total_ok / total_ops * 100) if total_ops > 0 else 0
    throughput = total_ok / wall_elapsed if wall_elapsed > 0 else 0

    latencies = sorted(stats["latencies"])
    if latencies:
        avg_lat = sum(latencies) / len(latencies)
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        max_lat = latencies[-1]
    else:
        avg_lat = p50 = p95 = p99 = max_lat = 0

    # Report
    print()
    print("--- RESULTS ---")
    print()
    print("Operations")
    print(f"PUT:{stats['put_ok']} ok / {stats['put_fail']} failed")
    print(f"GET:{stats['get_ok']} ok / {stats['get_fail']} failed")
    print(f"DELETE:{stats['del_ok']} ok / {stats['del_fail']} failed")
    print(f"Timeouts:{stats['timeouts']}")
    print(f"Total:{total_ok} ok / {total_ops} attempted ({success_rate:.1f}% success)")
    print()
    print("Throughput")
    print(f"Wall time:{wall_elapsed:.1f}s")
    print(f"Ops/sec:{throughput:.1f}")
    print()
    print("Latency")
    print(f"Average:{avg_lat * 1000:.0f}ms")
    print(f"p50:{p50 * 1000:.0f}ms")
    print(f"p95:{p95 * 1000:.0f}ms")
    print(f"p99:{p99 * 1000:.0f}ms")
    print(f"Max:{max_lat * 1000:.0f}ms")
    print()
    if sample_keys:
        print("Verification")
        print(f"Sampled:{verified_ok} ok {verified_ok + verified_fail} checked")
    print()


# === Main ===

def main():
    # Handle stress test mode
    if len(sys.argv) > 1 and sys.argv[1].lower() == "stress":
        num_clients = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        ops_per_client = int(sys.argv[3]) if len(sys.argv) > 3 else 50
        run_stress_test(num_clients, ops_per_client)
        return

    runner = TestRunner()

    level = sys.argv[1].lower() if len(sys.argv) > 1 else "all"

    print(f"\nRunning {'all' if level == 'all' else level} tests")
    print(f"Cluster expected at {NETWORK_HOST}:{NETWORK_PORT}")
    print("Make sure run_cluster.py is running!\n")

    # Basic tests (Part 1)
    if level in ("basic", "all"):
        print("\n=== PART 1 TESTS - Basic Functionality ===")
        runner.run_test("Basic PUT/GET", test_basic_put_get)
        runner.run_test("Multiple Keys", test_multiple_keys)
        runner.run_test("Overwrite Value", test_overwrite)
        runner.run_test("GET Nonexistent Key", test_get_nonexistent)

    # Core tests (Part 2)
    if level in ("core", "all"):
        print("\n=== PART 2 TESTS - Core Raft ===")
        runner.run_test("Leader Election", test_leader_election)
        runner.run_test("Sequential Operations", test_sequential_operations)
        runner.run_test("Delete Operation", test_delete_operation)

    # Robustness tests (Part 3)
    if level in ("robust", "all"):
        print("\n=== PART 3 TESTS - Robustness ===")
        runner.run_test("Multiple Clients", test_multiple_clients)
        runner.run_test("Concurrent Writes", test_concurrent_writes)
        runner.run_test("Throughput", test_throughput)
        runner.run_test("Deduplication", test_deduplication)

    # Advanced tests (Part 4)
    if level in ("advanced", "all"):
        print("\n=== PART 4 TESTS - Advanced Features ===")
        runner.run_test("Data Persistence Under Delay", test_data_persistence_after_delay)
        runner.run_test("Stress Test", test_stress)
        runner.run_test("Interleaved Read/Write", test_interleaved_read_write)

    runner.report()


if __name__ == "__main__":
    main()
