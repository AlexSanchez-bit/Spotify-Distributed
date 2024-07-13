import sys
from Database.database_connectiom import Playlist
from Kademlia.KBucket import Node
from Kademlia.KademliaNode import KademliaNode
import os
import time

default_nodes = [
    ("127.0.0.1", 8080),
    ("127.0.0.1", 8081),
    ("127.0.0.1", 8082),
    ("127.0.0.1", 8083),
    ("172.18.0.2", 8080),
    ("172.18.0.3", 8080),
    ("172.18.0.4", 8080),
    ("172.18.0.8", 8080),
    ("172.18.0.5", 8080),
    ("172.18.0.9", 8080),
]


def main():
    ip = os.getenv("NODE_IP", "127.0.0.1")  # Valor por defecto es 127.0.0.1
    port = int(os.getenv("NODE_PORT", 8082))
    node = KademliaNode(ip, port)
    node.start()
    while True:
        file_direction = input("ip: ")
        node.store_a_file(file_direction)
        time.sleep(1)


if __name__ == "__main__":
    main()
