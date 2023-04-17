import logging
import random
import threading
import time

from .utils import output_dict_to_file, send


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

# pending_updates = {}

class EventualConsistencyKVStore(KeyValueStore):
    def __init__(self, replica, replica_addresses, gossip_interval):
        super().__init__()
        self.replica = replica
        self.replica_addresses = replica_addresses
        self.gossip_interval = gossip_interval

        # global pending_updates
        self.pending_updates = {address: []
                                for address in self.replica_addresses}

        # print(f"Replica addresses: {self.replica_addresses}")
        # print(f"init Pending updates: {self.pending_updates}")

        # gossip_thread = threading.Thread(target=self.gossip_loop)
        # gossip_thread.daemon = True
        # gossip_thread.start()

    def set(self, key, value):
        # global pending_updates
        super().set(key, value)

        # print(f"Replica addresses in set: {self.replica_addresses}")

        # Queue new key-value pair to be sent to each replica
        for address in self.replica_addresses:
            if address != (self.replica.host, self.replica.port):
                # self.pending_updates[address].append((key, value))
                # pending_updates[address].append((key, value))
                self.pending_updates.setdefault(address, []).append((key, value))

        print(f"Pending updates in set: {self.pending_updates}")
        self.send_updates()
        return "Key-value pair added"

    def send_updates(self):
        for target_replica, updates in self.pending_updates.items():
            if updates:
                data = "update " + \
                    " ".join([f"{key} {value}" for key, value in updates])
                send(target_replica, data)

                # Clear pending updates
                self.pending_updates[target_replica] = []
        

    # def gossip_loop(self):
    #     global pending_updates

    #     while True:
    #         # print(
    #         #     f"Pending updates at gossip_loop start: {self.pending_updates}")

    #         # Send pending updates every gossip_interval seconds
    #         time.sleep(self.gossip_interval)
    #         # pending_updates = self.get_pending_updates()

    #         # Choose a random replica to send updates to
    #         target_replica = random.choice([address for address in self.replica_addresses if address != (
    #             self.replica.host, self.replica.port)])

    #         # print(f"Target replica: {target_replica}")
    #         # print(f"Replica addresses in gossip_loop: {self.replica_addresses}")
    #         print(f"Pending updates in gossip_loop: {pending_updates}")

    #         # If the target replica has pending updates, send them
    #         updates = pending_updates[target_replica]

    #         if updates:
    #             data = "update " + \
    #                 " ".join([f"{key} {value}" for key, value in updates])
    #             send(target_replica, data)

    #             logging.debug(
    #                 f"{self.replica.id} sent {data} to {target_replica}")

    #             # Clear pending updates for the target replica
    #             # self.pending_updates[target_replica] = []
