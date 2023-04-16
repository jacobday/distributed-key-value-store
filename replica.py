import logging
import socket
import threading
import yaml

config = yaml.safe_load(open("./config.yml"))
config_settings = config["settings"]

kv_store = {}


class Replica:
    def __init__(self, id, host, port, consistency_scheme):
        self.id = id
        self.host = host
        self.port = port
        self.consistency_scheme = consistency_scheme

        # Create socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))

        logging.debug(
            f"{self.id} initialized at {self.host}:{self.port}")

    def get(self, key):
        if key in kv_store:
            return kv_store[key]
        else:
            return "Key does not exist"

    def set(self, key, value):
        kv_store[key] = value
        return "Key-value pair added"

    def delete(self, key):
        if key in kv_store:
            del kv_store[key]
            return "Key deleted"
        else:
            return "Key does not exist"

    # Send command to address
    def send(self, address, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)

        sock.send(data.encode())
        response = sock.recv(1024).decode()

        sock.close()
        return response

    def handle_command(self, command):
        cmd = command.split()
        print(cmd)

        if cmd[0] == "get":
            return self.get(cmd[1])
        elif cmd[0] == "set":
            return self.set(cmd[1], cmd[2])
        elif cmd[0] == "delete":
            return self.delete(cmd[1])
        else:
            return "Invalid command"

    def handle_client(self, conn, addr):
        with conn:
            logging.debug(f"{self.id} connected to {addr}")
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
        self.send((main_ip, main_port), "ready")

    def start(self):
        self.run()
