# network_chaos.py - Chaotic network combining all failure modes
#
# Drop-in replacement for network.py. This is the most hostile network:
# it combines random delays, random message drops, and randomised network
# partitions.
#
# Compared to the individual variants:
#   - Drop rate is doubled (20% vs 10%)
#   - Delay range is wider (50ms - 1000ms)
#   - Partition splits are randomised (not always 2 vs 3)
#   - Partition/heal durations vary randomly
#
# Usage: python network_chaos.py
#   (or: python run_cluster.py network_chaos.py)
#
# Do not modify this file

import random
import time
import threading

from message import send_message
from network import Network
from config import (
    NETWORK_DELAY_MIN, NETWORK_DELAY_MAX, NETWORK_DROP_RATE,
    PARTITION_DURATION, PARTITION_HEAL_DURATION, NODE_IDS,
)


class ChaosNetwork(Network):
    """The most hostile network: delays + drops + partitions."""

    def __init__(self):
        super().__init__()
        self.partition_groups = None
        self.partition_lock = threading.Lock()
        self.chaos_drop_rate = NETWORK_DROP_RATE * 2  # Double the drop rate

    def start(self):
        """Start the network and the partition scheduler."""
        partition_thread = threading.Thread(
            target=self._partition_scheduler, daemon=True
        )
        partition_thread.start()
        super().start()

    def _partition_scheduler(self):
        """Create randomised partitions with varying durations."""
        time.sleep(10.0)  # Let cluster stabilise

        while self.running:
            # --- Heal phase (random duration) ---
            heal_time = random.uniform(
                PARTITION_HEAL_DURATION * 0.5,
                PARTITION_HEAL_DURATION * 1.5,
            )
            with self.partition_lock:
                self.partition_groups = None
            print("[ChaosNetwork] === Network HEALED ===")
            time.sleep(heal_time)

            # --- Partition phase (random split, random duration) ---
            shuffled = list(NODE_IDS)
            random.shuffle(shuffled)
            split = random.randint(1, len(shuffled) - 1)
            group_a = set(shuffled[:split])
            group_b = set(shuffled[split:])

            partition_time = random.uniform(
                PARTITION_DURATION * 0.5,
                PARTITION_DURATION * 1.5,
            )

            with self.partition_lock:
                self.partition_groups = (group_a, group_b)
            print(f"[ChaosNetwork] === PARTITION for {partition_time:.1f}s === "
                  f"A: {sorted(group_a)} | B: {sorted(group_b)}")
            time.sleep(partition_time)

    def _deliver(self, msg, dst_id, dst_sock):
        """Deliver with drops, delays, and partition awareness."""
        src_id = msg.get("src", "")

        # Check partition
        with self.partition_lock:
            if self.partition_groups is not None:
                group_a, group_b = self.partition_groups
                if ((src_id in group_a and dst_id in group_b) or
                        (src_id in group_b and dst_id in group_a)):
                    return  # Silently dropped by partition

        # Random message drop
        if random.random() < self.chaos_drop_rate:
            return

        # Random delay (wider range than normal)
        delay = random.uniform(NETWORK_DELAY_MIN, NETWORK_DELAY_MAX * 2)
        time.sleep(delay)

        try:
            send_message(dst_sock, msg)
        except (BrokenPipeError, OSError):
            pass


if __name__ == "__main__":
    network = ChaosNetwork()
    network.start()
