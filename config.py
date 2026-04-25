# config.py - Cluster configuration
# This file contains all tunable parameters for the distributed system.
# Do not modify this file unless your extension requires it

# Network settings
NETWORK_HOST = "localhost"
NETWORK_PORT = 5000

# Cluster settings
CLUSTER_SIZE = 5
NODE_IDS = [f"node-{i}" for i in range(CLUSTER_SIZE)]

# Raft timing (in seconds)
HEARTBEAT_INTERVAL = 0.5 # Leader sends heartbeats this often
ELECTION_TIMEOUT_MIN = 1.5 # Minimum election timeout
ELECTION_TIMEOUT_MAX = 3.0 # Maximum election timeout

# Client settings
CLIENT_TIMEOUT = 5.0 # Client waits this long for a response
CLIENT_RETRY_DELAY = 1.0 # Delay before retrying after failure

# Network behaviour settings
NETWORK_BASE_DELAY = 0.01 # Perfect network delay (10ms)
NETWORK_DELAY_MIN = 0.05 # Delayed network minimum (50ms)
NETWORK_DELAY_MAX = 0.5 # Delayed network maximum (500ms)
NETWORK_DROP_RATE = 0.1 # Lossy network drop probability (10%)
PARTITION_DURATION = 10.0 # How long partitions last (seconds)
PARTITION_HEAL_DURATION = 10.0 # How long network is healthy between partitions

# Snapshot settings
SNAPSHOT_THRESHOLD = 100 # Snapshot after this many log entries

# Persistence settings
DATA_DIR = "data" # Directory for node state files
