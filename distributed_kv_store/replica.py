import logging
import socket
import threading

from .utils import load_config, send
from .kvstore import CasualConsistencyKVStore, KeyValueStore, EventualConsistencyKVStore, LinearConsistencyKVStore, SequentialConsistencyKVStore

config, config_settings = load_config()


class Replica:
    def __init__(self, id, host, port, consistency_scheme, replica_addresses, sequencer_address):
        self.id = id
        self.host = host
        self.port = port
        self.consistency_scheme = consistency_scheme
        self.replica_addresses = replica_addresses
        self.sequencer_address = sequencer_address

        self.output_location = config_settings["replica"]["output_location"]
        self.output_suffix = config_settings["replica"]["output_suffix"]

        # Initialize the chosen consistency scheme
        if consistency_scheme == "eventual":
            self.kv_store = EventualConsistencyKVStore(self, replica_addresses)
        elif consistency_scheme == "linear":
            self.kv_store = LinearConsistencyKVStore(self, replica_addresses)
        elif consistency_scheme == "sequential":
            self.kv_store = SequentialConsistencyKVStore(self, replica_addresses, sequencer_address)
        elif consistency_scheme == "casual":
            self.kv_store = CasualConsistencyKVStore(self, replica_addresses)
        else:
            self.kv_store = KeyValueStore()

        # Create socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))

        logging.debug(
            f"{self.id} initialized at {self.host}:{self.port}")

    # Handle commands from clients
    def handle_command(self, command):
        cmd = command.split()
        cmd_action = cmd[0]

        # print(f"{self.id} received command: {command}")

        if cmd_action == "get":
            return self.kv_store.get(cmd[1])
        elif cmd_action == "set":
            response = self.kv_store.set(cmd[1], cmd[2])
            self.kv_store.save(f"{self.output_location}/{self.id}_{self.output_suffix}")

            return response
        elif cmd_action == "delete":
            return self.kv_store.delete(cmd[1])
        elif cmd_action == "save":
            return self.kv_store.save(f"{self.output_location}/{self.id}_{self.output_suffix}")
        elif cmd_action == "update":
            return self.kv_store.update(cmd[1:])
        else:
            return "Invalid command"

    def handle_client(self, conn, addr):
        with conn:
            data = conn.recv(1024).decode()

            if data:
                logging.debug(f"{self.id} received \"{data}\" from {addr}")
                response = self.handle_command(data)
                # print(response)
                conn.sendall(response.encode())

    def listen(self):
        with self.socket as sock:
            sock.listen()
            logging.info(f"{self.id} listening on {self.host}:{self.port}")

            while True:
                conn, addr = sock.accept()

                # Create new thread for each client
                thread = threading.Thread(
                    target=self.handle_client, args=(conn, addr))
                thread.start()

    def run(self):
        # Listen for client commands
        self.listen()

        # Notify the main node that the replica is running
        main_settings = config_settings["main"]
        main_ip = main_settings["ip"]
        main_port = main_settings["port"]

        send((main_ip, main_port), "ready")

    def start(self):
        self.run()
