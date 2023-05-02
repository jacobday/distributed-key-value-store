import logging
import random
import threading
import time

from distributed_kv_store.replica import Replica
from distributed_kv_store.utils import load_config

config, config_settings = load_config()

logging.basicConfig(filename="./logs/test.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(filename)s %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    encoding="utf-8",
                    level=config_settings["log_level"])


def start_replica(id, host, port, consistency_scheme, replica_addresses, sequencer_address):
    replica = Replica(id, host, port, consistency_scheme,
                      replica_addresses, sequencer_address)
    threading.Thread(target=replica.start).start()
    return replica


def test_eventual_consistency():
    logging.info("Starting eventual consistency test...")
    replica_addresses = [("localhost", 9500),
                         ("localhost", 9501), ("localhost", 9502)]

    replica0 = start_replica("replica_0", "localhost",
                             9500, "eventual", replica_addresses, None)
    replica1 = start_replica("replica_1", "localhost",
                             9501, "eventual", replica_addresses, None)
    replica2 = start_replica("replica_2", "localhost",
                             9502, "eventual", replica_addresses, None)

    # Set a = 1
    replica0.kv_store.set("a", "1")
    logging.debug("[replica0] Set a = 1")

    # The gossip interval is 3 seconds, therefore, replica1 and replica2 should not have the value yet
    time.sleep(1)

    replica0_value = replica0.kv_store.get("a")
    replica1_value = replica1.kv_store.get("a")
    replica2_value = replica2.kv_store.get("a")

    logging.debug(
        f"[replica0] a = {replica0_value}, expected: 1\n[replica1] a = {replica1_value}, expected: Key does not exist\n[replica2] a = {replica2_value}, expected: Key does not exist")

    assert replica0_value == "1", "replica0: eventual consistency failed"
    assert replica1_value == "Key does not exist", "replica1: eventual consistency failed"
    assert replica2_value == "Key does not exist", "replica2: eventual consistency failed"

    # After 3 seconds, all the replicas should have the value
    time.sleep(config_settings["replica"]["gossip_interval"])

    replica0_value = replica0.kv_store.get("a")
    replica1_value = replica1.kv_store.get("a")
    replica2_value = replica2.kv_store.get("a")

    logging.debug(
        f"[replica0] a = {replica0_value}, expected: 1\n[replica1] a = {replica1_value}, expected: 1\n[replica2] a = {replica2_value}, expected: 1")

    assert replica0_value == "1", "replica0: eventual consistency failed"
    assert replica1_value == "1", "replica1: eventual consistency failed"
    assert replica2_value == "1", "replica2: eventual consistency failed"

    logging.info("Eventual consistency test passed\n")
    print("Eventual consistency test passed\n")


def test_linear_consistency():
    logging.info("Starting linear consistency test...")

    replica_addresses = [("localhost", 9503),
                         ("localhost", 9504), ("localhost", 9505)]

    replica0 = start_replica("replica_0", "localhost",
                             9503, "linear", replica_addresses, None)
    replica1 = start_replica("replica_1", "localhost",
                             9504, "linear", replica_addresses, None)
    replica2 = start_replica("replica_2", "localhost",
                             9505, "linear", replica_addresses, None)

    # Set a = 1
    replica0.kv_store.set("a", "1")

    # Allow the value to propagate
    time.sleep(1)

    # All the replicas should have a = 1
    replica0_value = replica0.kv_store.get("a")
    replica1_value = replica1.kv_store.get("a")
    replica2_value = replica2.kv_store.get("a")

    logging.debug(
        f"[replica0] a = {replica0_value}, expected: 1\n[replica1] a = {replica1_value}, expected: 1\n[replica2] a = {replica2_value}, expected: 1")

    assert replica0_value == "1", "replica0: linear consistency failed"
    assert replica1_value == "1", "replica1: linear consistency failed"
    assert replica2_value == "1", "replica2: linear consistency failed"

    logging.info("Linear consistency test passed\n")
    print("Linear consistency test passed\n")


def test_sequential_consistency():
    logging.info("Starting sequential consistency test...")

    replica_addresses = [("localhost", 9506),
                         ("localhost", 9507), ("localhost", 9508)]
    sequencer_address = random.choice(replica_addresses)

    replica0 = start_replica("replica_0", "localhost",
                             9506, "sequential", replica_addresses, sequencer_address)
    replica1 = start_replica("replica_1", "localhost",
                             9507, "sequential", replica_addresses, sequencer_address)
    replica2 = start_replica("replica_2", "localhost",
                             9508, "sequential", replica_addresses, sequencer_address)

    # Set a = 1
    replica0.kv_store.set("a", "1")

    # Allow the sequencer to propagate the value
    time.sleep(1)

    # All the replicas should have a = 1
    replica0_value = replica0.kv_store.get("a")
    replica1_value = replica1.kv_store.get("a")
    replica2_value = replica2.kv_store.get("a")

    logging.debug(
        f"[replica0] a = {replica0_value}, expected: 1\n[replica1] a = {replica1_value}, expected: 1\n[replica2] a = {replica2_value}, expected: 1")

    assert replica0_value == "1", "replica0: sequential consistency failed"
    assert replica1_value == "1", "replica1: sequential consistency failed"
    assert replica2_value == "1", "replica2: sequential consistency failed"

    logging.info("Sequential consistency test passed\n")
    print("Sequential consistency test passed\n")


def test_causal_consistency():
    logging.info("Starting causal consistency test...")

    replica_addresses = [("localhost", 9509),
                         ("localhost", 9510), ("localhost", 9511)]

    replica0 = start_replica("replica_0", "localhost",
                             9509, "causal", replica_addresses, None)
    replica1 = start_replica("replica_1", "localhost",
                             9510, "causal", replica_addresses, None)
    replica2 = start_replica("replica_2", "localhost",
                             9511, "causal", replica_addresses, None)

    # Set a = 5 (at timestamp 5)
    replica0.kv_store.set("a", "5", "replica_1", 5)

    # Setting a = 1 (at timestamp 1) should not work because the timestamp is less than the previous timestamp
    replica0.kv_store.set("a", "1", "replica_1", 1)

    # Therefore, the replicas should all have a = 5
    replica0_value = replica0.kv_store.get("a")
    replica1_value = replica1.kv_store.get("a")
    replica2_value = replica2.kv_store.get("a")

    logging.debug(
        f"[replica0] a = {replica0_value}, expected: 5\n[replica1] a = {replica1_value}, expected: 5\n[replica2] a = {replica2_value}, expected: 5")

    assert replica0_value == "5", "replica0: causal consistency failed"
    assert replica1_value == "5", "replica1: causal consistency failed"
    assert replica2_value == "5", "replica2: causal consistency failed"

    logging.info("Causal consistency test passed\n")
    print("Causal consistency test passed\n")


def run_tests():
    print("\nRunning tests...\n  - View logs in ./logs/test.log\n")

    test_eventual_consistency()
    test_linear_consistency()
    test_sequential_consistency()
    test_causal_consistency()

    print("All tests passed")


run_tests()
