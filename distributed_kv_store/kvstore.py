import logging
import threading

from .utils import output_dict_to_file, send, simulate_latency


class KeyValueStore:
    def __init__(self):
        self.store = {}

    def get(self, key):
        if key in self.store:
            return self.store[key]
        else:
            return "Key does not exist"

    def set(self, key, value):
        self.store[key] = value
        return "Key-value pair added"

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            return "Key deleted"
        else:
            return "Key does not exist"

    def update(self, updates):
        for i in range(0, len(updates), 2):
            key = updates[i]
            value = updates[i + 1]
            self.store[key] = value

        print(self.replica.id)
        return "Update successful"

    def save(self, filename):
        output_dict_to_file(self.store, filename)
        return "Save successful"


class EventualConsistencyKVStore(KeyValueStore):
    def __init__(self, replica, replica_addresses):
        super().__init__()
        self.replica = replica
        self.replica_addresses = replica_addresses

        self.pending_updates = {address: []
                                for address in self.replica_addresses}

    def set(self, key, value):
        super().set(key, value)

        # Queue new key-value pair to be sent to each replica
        for address in self.replica_addresses:
            if address != (self.replica.host, self.replica.port):
                self.pending_updates.setdefault(
                    address, []).append((key, value))

        self.send_updates()
        return "Key-value pair added"

    # Send updates to each replica
    def send_updates(self):
        for target_replica, updates in self.pending_updates.items():
            # If there are pending updates for the target_replica, send them
            if updates:
                data = "update " + \
                    " ".join([f"{key} {value}" for key, value in updates])
                send(target_replica, data)

                # Clear pending updates
                self.pending_updates[target_replica] = []


class LinearConsistencyKVStore(KeyValueStore):
    def __init__(self, replica, replica_addresses):
        super().__init__()
        self.replica = replica
        self.replica_addresses = replica_addresses

    def set(self, key, value):
        super().set(key, value)

        # Keep track of which replicas have acknowledged the update
        acknowledgements = []

        # Send new key-value pair to each replica
        for address in self.replica_addresses:
            if address != (self.replica.host, self.replica.port):
                data = f"update {key} {value}"
                acknowledgements.append(self.send_updates(address, data))

        # Wait for all replicas to acknowledge the update
        for acknowledgement in acknowledgements:
            acknowledgement.wait()

        return "Key-value pair added"

    # Send updates to the target replica
    def send_updates(self, target_replica, data):
        acknowledge_event = threading.Event()

        # Set the acknowledge event when the acknowledgement is received
        def callback():
            simulate_latency()
            acknowledge_event.set()

        send(target_replica, data, callback=callback)

        return acknowledge_event


class SequentialConsistencyKVStore(KeyValueStore):
    def __init__(self, replica, replica_addresses, sequencer_address):
        super().__init__()
        self.replica = replica
        self.replica_addresses = replica_addresses
        self.sequencer_address = sequencer_address

    def set(self, key, value):
        if (self.replica.host, self.replica.port) == self.sequencer_address:
            # If this replica is the sequencer, set and broadcast the new key-value pair
            super().set(key, value)
            self.broadcast_update(key, value)

            return "Key-value pair added"
        else:
            # If this replica is not the sequencer, send the new key-value pair to the sequencer
            data = f"set {key} {value}"
            send(self.sequencer_address, data)

            return "Key-value pair forwarded to sequencer"

    def broadcast_update(self, key, value):
        # Send new key-value pair to each replica
        for address in self.replica_addresses:
            if address != (self.replica.host, self.replica.port):
                data = f"update {key} {value}"
                send(address, data)
