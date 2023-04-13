import logging
import socket
import threading
import yaml

config = yaml.safe_load(open("./config.yml"))
config_settings = config["settings"]


class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))

        logging.debug(f"Client initialized at {self.host}:{self.port}")

    # Send data to client
    def send(self, data):
        self.socket.send(data.encode())

    # Receive data from client
    def receive(self):
        return self.socket.recv(1024)

    # Receive data from client and sync with replicas
    def handle_new_connection(self, conn, addr):
        logging.debug(f"Client {self.host}:{self.port} connected to {addr}")

        # Receive data from client
        data = self.receive()
        logging.debug(f"Received data: {data}")

        # Send response back to client
        response = "Data received"
        self.send(response)

    # Listen for new connections
    def listen(self):
        with self.socket as sock:
            self.listen()

            logging.debug(
                f"Client {self.host}:{self.port} listening for connections...")

            while True:
                conn, addr = sock.accept()

                # Create a new thread for each connection
                thread = threading.Thread(
                    target=self.handle_new_connection, args=(conn, addr))
                thread.start()
