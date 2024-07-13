import sys
from Database.database_connectiom import Playlist
from Kademlia.KBucket import Node, sha1_hash
from Kademlia.KademliaNode import KademliaNode
import os
import time

from Kademlia.utils.StoreAction import StoreAction

default_nodes = []


def main():
    ip = os.getenv("NODE_IP", "127.0.0.1")  # Valor por defecto es 127.0.0.1
    port = int(os.getenv("NODE_PORT", 8080))
    node = KademliaNode(ip, port)
    node.start()

    for peer_ip in list(node.consensus.network_info.hosts())[:10]:
        node.ping(Node(str(peer_ip), port))
    node.node_lookup(node.id)


if __name__ == "__main__":
    main()
