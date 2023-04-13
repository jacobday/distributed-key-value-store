import logging
import multiprocessing
import yaml

from client import Client

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
        # self.consistency_level = config_settings["consistency_level"]
        self.consistency_levels = config_settings["consistency_levels"]

        self.client_ips = []
        self.client_ports = []
        self.replica_ips = []
        self.replica_ports = []

    def run(self):
        start_message = f"Starting distributed kv store with {self.consistency_level} consistency..."
        self.logger.info(start_message)
        self.start_clients()

    # Start client processes and store their ip and port
    def start_clients(self):
        self.logger.info("Starting clients...")

        for i in range(self.num_clients):
            client_settings = config_settings["client"]
            client_ip = client_settings["ip"]
            client_port = client_settings["port"] + i

            # Start client process
            client_process = multiprocessing.Process(target=Client, args=(
                client_ip, client_port))
            client_process.start()

            # Store client ip and port
            self.client_ips.append(client_ip)
            self.client_ports.append(client_port)

            self.logger.info(
                f"Started client{i} at {client_ip}:{client_port}")

    # Ask the user to choose a consistency level
    def set_run_options(self):
        print("Choose consistency level:")

        for i in range(len(self.consistency_levels)):
            print(f"{i + 1}. {self.consistency_levels[i].title()}")

        user_input = int(input("Enter number: "))

        if user_input < 1 or user_input > len(self.consistency_levels):
            print("\nInvalid input\n")
            self.set_run_options()
        else:
            self.consistency_level = self.consistency_levels[
                user_input - 1].lower()
            self.run()


if __name__ == "__main__":
    main = Main()
    main.set_run_options()
