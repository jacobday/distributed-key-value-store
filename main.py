import logging
import multiprocessing
import socket

from distributed_kv_store import Client, Replica, load_config, send

config, config_settings = load_config()

logging.basicConfig(filename="./logs/main.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(filename)s %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    encoding="utf-8",
                    level=config_settings["log_level"])


class Main:
    def __init__(self):
        self.id = "main"
        self.logger = logging.getLogger(__name__)

        self.num_clients = config_settings["num_clients"]
        self.num_replicas = config_settings["num_replicas"]

        self.consistency_scheme = "sequential"
        self.consistency_schemes = config_settings["consistency_schemes"]

        self.host = config_settings["main"]["ip"]
        self.port = config_settings["main"]["port"]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))

        self.client_addresses = []
        self.replica_addresses = [('localhost', 9400), ('localhost', 9401), ('localhost', 9402)]

    def run(self):
        self.start_replicas()
        self.start_clients()

    # Start client processes and store their ip and port
    def start_clients(self):
        self.logger.info("Starting clients...")

        for i in range(self.num_clients):
            client_settings = config_settings["client"]

            client_id = f"client_{i}"
            client_ip = client_settings["ip"]
            client_port = client_settings["port"] + i

            # Start client process
            client = Client(client_id, client_ip, client_port)
            client_process = multiprocessing.Process(target=client.start)
            client_process.start()

            # Store client ip and port
            self.client_addresses.append((client_ip, client_port))

            # Wait for client to start before continuing
            self.wait_for_ready()

        # Tell clients to start sending commands
        for client_conn in self.client_addresses:
            send(client_conn, "run")

    # Wait for the client to respond with "ready"
    def wait_for_ready(self):
        self.sock.listen()

        cmd = ""

        while cmd != "ready":
            conn, addr = self.sock.accept()
            cmd = conn.recv(1024).decode()

    # Start replica processes and store their ip and port
    def start_replicas(self):
        self.logger.info("Starting replicas...")

        for i in range(self.num_replicas):
            replica_settings = config_settings["replica"]

            replica_id = f"replica_{i}"
            replica_ip = replica_settings["ip"]
            replica_port = replica_settings["port"] + i

            # Start replica process
            replica = Replica(replica_id, replica_ip,
                              replica_port, self.consistency_scheme, self.replica_addresses)
            replica_process = multiprocessing.Process(target=replica.start)
            replica_process.start()

            # Store client ip and port
            # self.replica_addresses.append((replica_ip, replica_port))

            # Update pending updates for each replica when a new replica is added
            # replica.kv_store.update_pending_updates()

    # Ask the user to choose a consistency scheme
    def set_run_options(self):
        print("Choose consistency scheme:")

        for i in range(len(self.consistency_schemes)):
            print(f"{i + 1}. {self.consistency_schemes[i].title()}")

        user_input = int(input("Enter number: "))

        if user_input < 1 or user_input > len(self.consistency_schemes):
            print("\nInvalid input\n")
            self.set_run_options()
        else:
            self.consistency_scheme = self.consistency_schemes[
                user_input - 1].lower()

            start_message = f"Starting distributed kv store with {self.consistency_scheme} consistency..."
            self.logger.info(start_message)
            print(
                f"\n{start_message}\n  View logs in main.log\n")

            self.run()


if __name__ == "__main__":
    main = Main()
    main.set_run_options()
