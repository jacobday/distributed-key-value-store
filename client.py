import logging
import socket
import threading
import time
import yaml

config = yaml.safe_load(open("./config.yml"))
config_settings = config["settings"]


class Client:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host
        self.port = port
        self.command_file = config_settings["client"]["command_file"]

        logging.debug(
            f"{self.id} initialized at {self.host}:{self.port}")

        self.execute_commands()

    # Send command to replica
    def send(self, address, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)

        sock.send(data.encode())
        response = sock.recv(1024).decode()

        sock.close()
        return response

    # Get replica address from replica id

    def get_replica_address(self, replica_id):
        replica_settings = config_settings["replica"]
        replica_ip = replica_settings["ip"]
        replica_port = replica_settings["port"] + int(replica_id.split("_")[1])

        return (replica_ip, replica_port)

    # Return client commands from file
    def read_commands_from_file(self, file_name):
        with open(file_name, "r") as f:
            commands = [line.strip().split() for line in f.readlines()]
            return commands

    # Send client commands to replicas
    def execute_commands(self):
        commands = self.read_commands_from_file(self.command_file)

        for command in commands:
            client_id, replica_id, cmd = command[0], command[1], command[2:]

            if self.id == client_id:
                data = " ".join(cmd)
                logging.info(
                    f"{self.id} sending command \"{data}\" to {replica_id}")

                response = self.send(
                    self.get_replica_address(replica_id), data)

                logging.info(
                    f"{self.id} received response \"{response}\" from {replica_id}")

                time.sleep(3)
