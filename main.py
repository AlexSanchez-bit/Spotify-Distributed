import sys
from Database.database_connectiom import Playlist
from Kademlia.KBucket import Node
from Kademlia.KademliaNode import KademliaNode
import os
import time

from Kademlia.utils.StoreAction import StoreAction

default_nodes = [
    ("127.0.0.1", 8080),
    ("127.0.0.1", 8081),
    ("127.0.0.1", 8082),
]


def main():
    ip = os.getenv("NODE_IP", "127.0.0.1")  # Valor por defecto es 127.0.0.1
    port = int(os.getenv("NODE_PORT", 8081))
    node = KademliaNode(ip, port)
    node.start()
    while True:
        file_direction = input("ip: ")
        node.store_a_file(file_direction)
        time.sleep(1)


if __name__ == "__main__":
    main()
