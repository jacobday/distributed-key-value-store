import logging
import random
import socket
import time
import yaml


# Return the config settings in config.yml
def load_config():
    with open("config/config.yml", "r") as f:
        config = yaml.safe_load(f)
        config_settings = config["settings"]
        return config, config_settings

# Send data to an address and return the response
def send(address, data, callback=None, timeout=5):
    host, port = address
    response = None

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)

        try:
            sock.connect(address)
            sock.send(data.encode())
            response = sock.recv(1024).decode()

            if callback:
                callback()
        except socket.timeout:
            response = f"Connection to {host}:{port} timed out"

            logging.error(response)
            print(response)
        except Exception as e:
            response = f"Error connecting to {host}:{port}: {e}"

            logging.error(response)
            print(response)

    return response

# Return client commands from file
def read_commands_from_file(file_name):
    with open(file_name, "r") as f:
        commands = [line.strip().split() for line in f.readlines()]
        return commands

# Write output to file
def output_dict_to_file(dictionary, file_name):
    dictionary = dict(sorted(dictionary.items()))

    with open(file_name, "w") as f:
        for key, value in dictionary.items():
            f.write(f"{key} {value}\n")


# Get replica address from replica id
def get_replica_address(replica_id, replica_settings):
    replica_ip = replica_settings["ip"]
    replica_port = replica_settings["port"] + int(replica_id.split("_")[1])

    return (replica_ip, replica_port)


# Simulate network latency by sleeping for a random amount of time
def simulate_latency():
    sleep_time = random.uniform(0, 3)
    time.sleep(sleep_time)
