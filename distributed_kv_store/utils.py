import socket
import yaml


# Return the config settings in config.yml
def load_config():
    with open("config/config.yml", "r") as f:
        config = yaml.safe_load(f)
        config_settings = config["settings"]
        return config, config_settings

# Send data to an address and return the response
def send(address, data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)

    sock.send(data.encode())
    response = sock.recv(1024).decode()

    sock.close()
    return response

# Return client commands from file
def read_commands_from_file(file_name):
    with open(file_name, "r") as f:
        commands = [line.strip().split() for line in f.readlines()]
        return commands

# Write output to file
def output_dict_to_file(dictionary, file_name):
    with open(file_name, "w") as f:
        for key, value in dictionary.items():
            f.write(f"{key} {value}")


# Get replica address from replica id
def get_replica_address(replica_id, replica_settings):
    replica_ip = replica_settings["ip"]
    replica_port = replica_settings["port"] + int(replica_id.split("_")[1])

    return (replica_ip, replica_port)
