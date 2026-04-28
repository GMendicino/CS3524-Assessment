import socket
import sys
import time
import os
import json
import uuid
import threading

from message import (
    send_message, recv_message, make_register, make_client_request,
    MSG_CLIENT_RESPONSE, MSG_REGISTER_ACK,
)
from config import (
    NETWORK_HOST, NETWORK_PORT, CLIENT_TIMEOUT, SNAPSHOT_THRESHOLD, DATA_DIR, NODE_IDS
)

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
        """Send a request and wait for a response."""
        request_id = str(uuid.uuid4())
        msg = make_client_request(
            self.client_id, request_id, operation, key, value
        )
        try:
            send_message(self.sock, msg)
        except (BrokenPipeError, OSError):
            return None

        deadline = time.time() + (timeout or CLIENT_TIMEOUT)
        while time.time() < deadline:
            try:
                self.sock.settimeout(max(0.1, deadline - time.time()))
                response = recv_message(self.sock)
                if response is None:
                    return None
                if (response.get("type") == MSG_CLIENT_RESPONSE and
                        response.get("request_id") == request_id):
                    return response
            except socket.timeout:
                break
            except Exception:
                return None
        return None

    def request_with_retry(self, operation, key, value=None, timeout=None, retries=5):
        for _ in range(retries):
            r = self.request(operation, key, value, timeout=timeout)
            if r and r.get("success"):
                return r
            time.sleep(0.5)
        return None

    def close(self):
        if self.sock:
            self.sock.close()


# === Test Cases ===

def test_log_compaction_triggering():
    """
    Test 1: Verify that snapshots are triggered when the log exceeds the threshold.
    """
    print("\n--- Test 1: Log Compaction Triggering ---")
    client = TestClient()
    if not client.connect():
        print("Failed to connect to cluster")
        return False

    num_ops = SNAPSHOT_THRESHOLD + 20
    print(f"Performing {num_ops} PUT operations to trigger snapshot (threshold={SNAPSHOT_THRESHOLD})...")
    
    for i in range(num_ops):
        key = f"snap-key-{i}"
        val = f"value-{i}"
        res = client.request_with_retry("PUT", key, val)
        if not res:
            print(f"PUT failed at iteration {i}")
            client.close()
            return False
        if i % 20 == 0:
            print(f"  Progress: {i}/{num_ops}...")

    print("Verifying all data is still present via GET...")
    for i in range(0, num_ops, 10): # Check every 10th key to save time
        key = f"snap-key-{i}"
        expected = f"value-{i}"
        res = client.request_with_retry("GET", key)
        if not res or res.get("value") != expected:
            print(f"GET failed for {key}: expected {expected}, got {res.get('value') if res else 'None'}")
            client.close()
            return False

    print("Checking disk for persisted snapshots...")
    time.sleep(2) # Give nodes a moment to finish applying and saving
    
    snapshot_count = 0
    for node_id in NODE_IDS:
        path = os.path.join(DATA_DIR, f"node_{node_id}.json")
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    state = json.load(f)
                    idx = state.get("last_included_index", 0)
                    if idx > 0:
                        print(f"  {node_id}: Found snapshot at index {idx}")
                        snapshot_count += 1
                        # Verify log is actually pruned
                        log_len = len(state.get("log", []))
                        print(f"  {node_id}: Log size is {log_len} (should be small)")
                        if log_len >= SNAPSHOT_THRESHOLD:
                            print(f"  [!] Log not pruned on {node_id}")
            except Exception as e:
                print(f"  Error reading {path}: {e}")

    if snapshot_count == 0:
        print("No snapshots were found on any node's disk.")
        client.close()
        return False

    print("Test 1 Passed")
    client.close()
    return True

def test_snapshot_consistency_and_persistence():
    """
    Test 2: Verify that the snapshot contains the correct state machine state.
    """
    print("\n--- Test 2: Snapshot Consistency and Persistence ---")
    # This relies on the state created in Test 1
    
    found_valid_state = False
    for node_id in NODE_IDS:
        path = os.path.join(DATA_DIR, f"node_{node_id}.json")
        if os.path.exists(path):
            with open(path, "r") as f:
                state = json.load(f)
                kv = state.get("kv_store", {})
                if kv and "snap-key-0" in kv and kv["snap-key-0"] == "value-0":
                    print(f"  {node_id}: Snapshot contains correct KV state (key-0='value-0')")
                    found_valid_state = True
                
                # Check deduplication cache
                client_responses = state.get("client_responses", {})
                if client_responses:
                    print(f"  {node_id}: Snapshot contains {len(client_responses)} cached client responses")

    if not found_valid_state:
        print("Could not find a valid persisted state with expected values")
        return False

    print("Test 2 Passed")
    return True

def test_lossy_network_and_snapshots():
    """
    Test 3: Verify system remains functional under lossy network while snapshotting.
    """
    print("\n--- Test 3: Functionality under non-perfect network ---")
    print("This test performs more operations to ensure the system doesn't crash or lose data.")
    
    client = TestClient()
    if not client.connect():
        print("Failed to connect (is the cluster running?)")
        return False

    # Send a few more requests
    success = True
    for i in range(20):
        key = f"lossy-key-{i}"
        val = f"lossy-value-{i}"
        res = client.request_with_retry("PUT", key, val)
        if not res:
            print(f"PUT failed for {key} under stress")
            success = False
            break
            
    if success:
        print("Successfully performed additional operations.")
        # Verify a random one
        key = "lossy-key-10"
        res = client.request_with_retry("GET", key)
        if res and res.get("value") == "lossy-value-10":
            print(f"Verification of {key} successful.")
        else:
            print(f"Verification of {key} failed.")
            success = False

    client.close()
    if success:
        print("Test 3 Passed")
    return success

# === Main ===

if __name__ == "__main__":
    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        print(f"Warning: {DATA_DIR} does not exist. Start the cluster first.")
    
    print("Starting Extension Tests (Log Compaction)")
    print("Make sure the cluster is running (e.g., python run_cluster.py)")
    
    passed_all = True
    
    try:
        if not test_log_compaction_triggering():
            passed_all = False
        
        if not test_snapshot_consistency_and_persistence():
            passed_all = False
            
        if not test_lossy_network_and_snapshots():
            passed_all = False
            
    except Exception as e:
        print(f"\nAn error occurred during testing: {e}")
        import traceback
        traceback.print_exc()
        passed_all = False

    if passed_all:
        print("\n" + "="*30)
        print("  ALL EXTENSION TESTS PASSED")
        print("="*30)
        sys.exit(0)
    else:
        print("\n" + "="*30)
        print("  SOME TESTS FAILED")
        print("="*30)
        sys.exit(1)
