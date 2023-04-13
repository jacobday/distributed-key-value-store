# Distributed Multi-Consistency Key-Value Store

## How to run

### Manually

1. Install dependencies by running `pip3 install -r requirements.txt`
1. Define the number of client and replica processes to spawn in [config.yml](./config.yml)
1. Choose which consistency level to use in the console

    Consistency levels:
    1. Sequential
    1. Eventual
    1. Linear
    1. Casual

## Notes

- Generate/update [requirements.txt](./requirements.txt) by running `pip3 freeze > requirements.txt`
