# network_partition.py - Network that simulates network partitions
#
# Drop-in replacement for network.py. Periodically creates a network
# partition that splits the cluster into two groups. Messages between
# groups are silently dropped; messages within the same group are delivered
# with small random delays.
#
# The partition schedule is:
#   1. Network runs normally for PARTITION_HEAL_DURATION seconds
#   2. A partition is created for PARTITION_DURATION seconds
#   3. The partition heals and the cycle repeats
#
# The partition always splits into a minority (2 nodes) and a majority
# (3 nodes).
#
# Usage: python network_partition.py
#   (or: python run_cluster.py network_partition.py)
#
# Do not modify this file

import random
import time
import threading

from message import send_message
from network import Network
from config import (
    NETWORK_DELAY_MIN, NETWORK_DELAY_MAX,
    PARTITION_DURATION, PARTITION_HEAL_DURATION, NODE_IDS,
)


class PartitionNetwork(Network):
    """Network that periodically creates network partitions."""

    def __init__(self):
        super().__init__()
        # None means no partition is active.
        # When active: (group_a: set, group_b: set)
        self.partition_groups = None
        self.partition_lock = threading.Lock()

    def start(self):
        """Start the network and the partition scheduler."""
        partition_thread = threading.Thread(
            target=self._partition_scheduler, daemon=True
        )
        partition_thread.start()
        super().start()

    def _partition_scheduler(self):
        """Periodically create and heal partitions."""
        # Let the cluster stabilise before introducing partitions
        time.sleep(8.0)

        while self.running:
            # --- Heal phase ---
            with self.partition_lock:
                self.partition_groups = None
            print("[PartitionNetwork] === Network HEALED === "
                  "(all messages flowing)")
            time.sleep(PARTITION_HEAL_DURATION)

            # --- Partition phase ---
            # Split: first 2 nodes vs remaining 3 nodes
            partition_point = 2
            group_a = set(NODE_IDS[:partition_point])
            group_b = set(NODE_IDS[partition_point:])

            with self.partition_lock:
                self.partition_groups = (group_a, group_b)
            print(f"[PartitionNetwork] === PARTITION === "
                  f"Group A: {sorted(group_a)} | Group B: {sorted(group_b)}")
            time.sleep(PARTITION_DURATION)

    def _deliver(self, msg, dst_id, dst_sock):
        """Deliver message, but drop if it crosses a partition boundary."""
        src_id = msg.get("src", "")

        with self.partition_lock:
            if self.partition_groups is not None:
                group_a, group_b = self.partition_groups
                src_in_a = src_id in group_a
                dst_in_a = dst_id in group_a
                src_in_b = src_id in group_b
                dst_in_b = dst_id in group_b

                # Drop messages that cross the partition
                if (src_in_a and dst_in_b) or (src_in_b and dst_in_a):
                    return

        # Within same partition group (or no partition active):
        # deliver with a small random delay
        delay = random.uniform(NETWORK_DELAY_MIN, NETWORK_DELAY_MAX)
        time.sleep(delay)
        try:
            send_message(dst_sock, msg)
        except (BrokenPipeError, OSError):
            pass


if __name__ == "__main__":
    network = PartitionNetwork()
    network.start()
