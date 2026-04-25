# network_lossy.py - Network that randomly drops messages
#
# Drop-in replacement for network.py. Each message has a NETWORK_DROP_RATE
# probability of being silently dropped. Surviving messages also experience
# random delays (like network_delayed.py).
#
# Usage: python network_lossy.py
#   (or: python run_cluster.py network_lossy.py)
#
# Do not modify this file

import random
import time

from message import send_message
from network import Network
from config import NETWORK_DELAY_MIN, NETWORK_DELAY_MAX, NETWORK_DROP_RATE


class LossyNetwork(Network):
    """Network that randomly drops messages and adds variable delays."""

    def _deliver(self, msg, dst_id, dst_sock):
        """Deliver with random delay, or drop the message entirely."""
        # Drop with configured probability
        if random.random() < NETWORK_DROP_RATE:
            return

        delay = random.uniform(NETWORK_DELAY_MIN, NETWORK_DELAY_MAX)
        time.sleep(delay)
        try:
            send_message(dst_sock, msg)
        except (BrokenPipeError, OSError):
            pass


if __name__ == "__main__":
    network = LossyNetwork()
    network.start()
