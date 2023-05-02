# Distributed Multi-Consistency Key-Value Store

## How to Run

1. Install the necessary dependencies by executing `pip3 install -r requirements.txt`
1. Configure the number of client and replica processes in the [config.yml](./config.yml) file

### Main Program

1. Define the commands to be executed in the [client-commands.txt](./commands/client-commands.txt)file, following this template:

    `client_id replica_id set/get key value`

1. Run the [main.py](./main.py) script
1. Select the desired consistency scheme in the console:

    Consistency schemes:
    1. Sequential
    1. Eventual
    1. Linear
    1. Causal
1. Review the logs in the [main.log](./logs/main.log) file and view the final key-value store of each replica in the [./results](./results/) folder

### Test Program

1. Run the [test.py](./test.py) script:
    - The test program will verify each consistency scheme is functioning correctly.
1. Check the testing logs in the [test.log](./logs/test.log) file and view the final key-value store of each replica in the [./results](./results/) folder

## Additional Notes

- To generate or update the [requirements.txt](./requirements.txt) file, run `pip3 freeze > requirements.txt`
