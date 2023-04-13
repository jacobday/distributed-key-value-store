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

        self.client_ips = []
        self.client_ports = []
        self.replica_ips = []
        self.replica_ports = []

    def run(self):
        self.logger.info("Starting distributed kv store...")
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


if __name__ == "__main__":
    main = Main()
    main.run()
