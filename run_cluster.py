# run_cluster.py - Launch the network and all nodes for testing
#
# This helper script starts the network process and all 5 node processes
# so you don't need to open 6 separate terminals.
#
# Usage:
# python run_cluster.py
# python run_cluster.py network_delayed.py
# python run_cluster.py network_lossy.py 
# python run_cluster.py network_partition.py
# python run_cluster.py network_chaos.py
#
# After starting:
#   - Use client_interactive.py in another terminal to test manually
#   - Use client_test.py in another terminal to run automated tests
#   - Press Ctrl+C to stop all processes
#
# Do not modify this file

import os
import signal
import subprocess
import sys
import time

from config import NODE_IDS


def main():
    network_script = sys.argv[1] if len(sys.argv) > 1 else "network.py"

    # Resolve path relative to this script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    network_path = os.path.join(script_dir, network_script)
    node_path = os.path.join(script_dir, "node.py")

    if not os.path.exists(network_path):
        print(f"Error: Network script '{network_script}' not found")
        sys.exit(1)

    processes = []

    # Start network
    print(f"Starting network: {network_script}")
    net_proc = subprocess.Popen(
        [sys.executable, network_path],
        cwd=script_dir,
    )
    processes.append(("network", net_proc))
    time.sleep(1)

    # Start nodes
    for node_id in NODE_IDS:
        print(f"Starting {node_id}")
        node_proc = subprocess.Popen(
            [sys.executable, node_path, node_id],
            cwd=script_dir,
        )
        processes.append((node_id, node_proc))
        time.sleep(0.3)

    print(f"\nCluster running: {len(NODE_IDS)} nodes on {network_script}")
    print("Press Ctrl+C to stop all processes\n")

    def shutdown(sig=None, frame=None):
        print("\nShutting down cluster...")
        for name, proc in reversed(processes):
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
            print(f"Stopped {name}")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown)

    # Wait for any process to exit (indicates a crash)
    try:
        while True:
            for name, proc in processes:
                ret = proc.poll()
                if ret is not None:
                    print(f"\n[!] {name} exited with code {ret}")
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
