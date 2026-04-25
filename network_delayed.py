# network_delayed.py - Network with variable message delays
#
# Drop-in replacement for network.py. Messages are delivered with random
# delays between NETWORK_DELAY_MIN and NETWORK_DELAY_MAX (default 50-500ms).
# Because each message gets a different delay, messages may arrive out of
# order.
#
# Usage: python network_delayed.py
#   (or: python run_cluster.py network_delayed.py)
#
# Do not modify this file

import random
import time

from message import send_message
from network import Network
from config import NETWORK_DELAY_MIN, NETWORK_DELAY_MAX


class DelayedNetwork(Network):
    """Network that introduces random delays to each message."""

    def _deliver(self, msg, dst_id, dst_sock):
        """Deliver with a random delay between DELAY_MIN and DELAY_MAX."""
        delay = random.uniform(NETWORK_DELAY_MIN, NETWORK_DELAY_MAX)
        time.sleep(delay)
        try:
            send_message(dst_sock, msg)
        except (BrokenPipeError, OSError):
            pass


if __name__ == "__main__":
    network = DelayedNetwork()
    network.start()
