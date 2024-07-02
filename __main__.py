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
    port = int(os.getenv("NODE_PORT", 8080))
    node = KademliaNode(ip, port)
    node.start()
    for ip, port in default_nodes:
        print(node.ping(Node(ip, port)))
    id = f"{time.time()}"
    node.store_playlist(
        StoreAction.INSERT, Playlist("ocaso de la lluvia vieja", "yo", id)
    )
    node.store_playlist(
        StoreAction.UPDATE,
        Playlist("mi nene es preciosa", "para mi nene", f"1719895112.9160979"),
    )
    node.store_playlist(
        StoreAction.DELETE,
        Playlist("", "", id),
    )
    while True:
        file_direction = input("ip: ")
        node.store_a_file(file_direction)
        time.sleep(1)


if __name__ == "__main__":
    main()
