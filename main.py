import logging
import multiprocessing
import time
import yaml

from client import Client
from replica import Replica

config = yaml.safe_load(open("./config.yml"))
config_settings = config["settings"]

logging.basicConfig(filename="main.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(filename)s %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    encoding="utf-8",
                    level=config_settings["log_level"])


class Main:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.num_clients = config_settings["num_clients"]
        self.num_replicas = config_settings["num_replicas"]
        self.consistency_scheme = "sequential"
        self.consistency_schemes = config_settings["consistency_schemes"]

        self.client_ips = []
        self.client_ports = []
        self.replica_ips = []
        self.replica_ports = []

    def run(self):
        self.start_replicas()

        time.sleep(3)

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
            client_process = multiprocessing.Process(target=Client, args=(client_id,
                                                                          client_ip, client_port))
            client_process.start()

            # Store client ip and port
            self.client_ips.append(client_ip)
            self.client_ports.append(client_port)

            self.logger.info(
                f"Started client_{i} at {client_ip}:{client_port}")

    # Start replica processes and store their ip and port
    def start_replicas(self):
        self.logger.info("Starting replicas...")

        for i in range(self.num_replicas):
            replica_settings = config_settings["replica"]

            replica_id = f"replica_{i}"
            replica_ip = replica_settings["ip"]
            replica_port = replica_settings["port"] + i

            # Start replica process
            replica_process = multiprocessing.Process(target=Replica, args=(replica_id,
                                                                            replica_ip, replica_port, self.consistency_scheme))
            replica_process.start()

            # Store replica ip and port
            self.replica_ips.append(replica_ip)
            self.replica_ports.append(replica_port)

            self.logger.info(
                f"Started replica_{i} at {replica_ip}:{replica_port}")

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
