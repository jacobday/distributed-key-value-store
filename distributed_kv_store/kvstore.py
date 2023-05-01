import logging
import threading

from .utils import output_dict_to_file, send, simulate_latency


class KeyValueStore:
    def __init__(self):
        self.store = {}
        self.vector_clock = {}

    def get(self, key):
        if key in self.store:
            return self.store[key]
        else:
            return "Key does not exist"

    def set(self, key, value, replica_id=None, vector_clock=None):
        self.store[key] = value
        self.vector_clock[replica_id] = vector_clock
        return "Key-value pair added"

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            return "Key deleted"
        else:
            return "Key does not exist"

    def update(self, updates):
        # for i in range(0, len(updates), 2):
        #     key = updates[i]
        #     value = updates[i + 1]
        #     self.store[key] = value

        for i in range(0, len(updates), 4):
            key = updates[i]
            value = updates[i + 1]
            replica_id = updates[i + 2] if i + 2 < len(updates) else None
            vector_clock = int(updates[i + 3]) if i + 3 < len(updates) else None

            print(f"Updating key {key} with value {value} from replica {replica_id} with vector clock {vector_clock}")

            # If a replica ID or vector clock are not provided, update the key-value pair
            if replica_id is None or vector_clock is None:
                self.store[key] = value
            # If they are provided, only update the key-value pair if the vector clock is greater than the current vector clock
            elif replica_id not in self.vector_clock or self.vector_clock[replica_id] < vector_clock:
                self.store[key] = value

                # Update the vector clock
                self.vector_clock[replica_id] = vector_clock

        return "Update successful"

    def save(self, filename):
        output_dict_to_file(self.store, filename)
        return "Save successful"


class EventualConsistencyKVStore(KeyValueStore):
    def __init__(self, replica, replica_addresses, gossip_interval=2):
        super().__init__()
        self.replica = replica
        self.replica_addresses = replica_addresses
        self.gossip_interval = gossip_interval

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


class CasualConsistencyKVStore(KeyValueStore):
    def __init__(self, replica, replica_addresses, gossip_interval=2):
        super().__init__()
        self.replica = replica
        self.replica_addresses = replica_addresses
        self.gossip_interval = gossip_interval

        self.pending_updates = {address: []
                                for address in self.replica_addresses}

    def set(self, key, value, replica_id=None, vector_clock=None):
        # If the replica_id and vector_clock are not provided, generate them
        if replica_id is None and vector_clock is None:
            replica_id = self.replica.id
            self.vector_clock[replica_id] = self.vector_clock.get(replica_id, 0) + 1
            vector_clock = self.vector_clock[replica_id]

        super().set(key, value, replica_id, vector_clock)

        # Queue new key-value pair to be sent to each replica
        for address in self.replica_addresses:
            if address != (self.replica.host, self.replica.port):
                self.pending_updates.setdefault(address, []).append((key, value, replica_id, vector_clock))

        self.send_updates()
        return "Key-value pair added"


    def send_updates(self):
        for target_replica, updates in self.pending_updates.items():
            # If there are pending updates for the target_replica, send them
            if updates:
                formatted_updates = []
                
                # Format the updates to include the replica_id and vector_clock
                for update in updates:
                    key, value, replica_id, vector_clock = update
                    formatted_updates.append(f"{key} {value} {replica_id} {vector_clock}")

                data = "update " + " ".join(formatted_updates)
                send(target_replica, data)

                # Clear pending updates
                self.pending_updates[target_replica] = []