import logging
import socket
import time

from utils import load_config, send, read_commands_from_file, get_replica_address

config, config_settings = load_config()


class Client:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host
        self.port = port
        self.command_file = config_settings["client"]["command_file"]

        logging.debug(
            f"{self.id} initialized at {self.host}:{self.port}")

    # Send client commands to replicas
    def execute_commands(self):
        commands = read_commands_from_file(self.command_file)

        for command in commands:
            try:
                client_id, replica_id, cmd = command[0], command[1], command[2:]
            except:
                logging.error("Invalid entry in command file")
                continue

            if self.id == client_id:
                data = " ".join(cmd)
                logging.info(
                    f"{self.id} sending command \"{data}\" to {replica_id}")

                response = send(
                    get_replica_address(replica_id, config_settings["replica"]), data)

                logging.info(
                    f"{self.id} received response \"{response}\" from {replica_id}")

                time.sleep(2)

    def run(self):
        # Notify the main node that the client is running
        main_settings = config_settings["main"]
        main_ip = main_settings["ip"]
        main_port = main_settings["port"]

        send((main_ip, main_port), "ready")

        # Wait for run command from main node
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))

        sock.listen()
        cmd = ""

        while cmd != "run":
            conn, addr = sock.accept()
            cmd = conn.recv(1024).decode()

        # Execute client commands
        self.execute_commands()

    def start(self):
        self.run()
