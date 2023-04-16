import logging
import socket
import threading

from .utils import load_config, send
from .kvstore import KeyValueStore

config, config_settings = load_config()


class Replica:
    def __init__(self, id, host, port, consistency_scheme):
        self.id = id
        self.host = host
        self.port = port
        self.consistency_scheme = consistency_scheme
        self.kv_store = KeyValueStore()

        # Create socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))

        logging.debug(
            f"{self.id} initialized at {self.host}:{self.port}")

    def handle_command(self, command):
        cmd = command.split()
        print(cmd)

        if cmd[0] == "get":
            return self.kv_store.get(cmd[1])
        elif cmd[0] == "set":
            return self.kv_store.set(cmd[1], cmd[2])
        elif cmd[0] == "delete":
            return self.kv_store.delete(cmd[1])
        else:
            return "Invalid command"

    def handle_client(self, conn, addr):
        with conn:
            data = conn.recv(1024).decode()

            if data:
                logging.debug(f"{self.id} received \"{data}\" from {addr}")
                response = self.handle_command(data)
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
        # Notify the main node that the replica is running
        main_settings = config_settings["main"]
        main_ip = main_settings["ip"]
        main_port = main_settings["port"]

        # Listen for client commands
        self.listen()
        send((main_ip, main_port), "ready")

    def start(self):
        self.run()
