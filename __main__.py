import sys
from Database.database_connectiom import Playlist
from Kademlia.KBucket import Node, sha1_hash
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
    time.sleep(0.2)
    id2 = f"{time.time()}"
    node.store_playlist(
        StoreAction.INSERT, Playlist("ocaso de la lluvia vieja", "yo", id)
    )
    node.store_playlist(
        StoreAction.INSERT, Playlist("la nene mas linda del mundo", "yo", id2)
    )
    node.store_playlist(
        StoreAction.DELETE,
        Playlist("", "", id),
    )
    node.get_playlist(id2)

    node.store_a_file("audio.mp3")
    node.get_a_file(sha1_hash("audio.mp3"))

    while True:
        file_direction = input("ip: ")
        node.store_a_file(file_direction)
        time.sleep(1)


if __name__ == "__main__":
    main()
