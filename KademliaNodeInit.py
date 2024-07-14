import sys
from threading import Thread
from Database.database_connectiom import Playlist
from Kademlia.KBucket import Node, sha1_hash
from Kademlia.KademliaNode import KademliaNode
import os
import time

from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.Rpc import Rpc
from Kademlia.utils.RpcType import RpcType

default_nodes = []


def init_node():
    ip = os.getenv("NODE_IP", "127.0.0.1")
    port = int(os.getenv("NODE_PORT", 8080))
    node = KademliaNode(ip, port)
    node.start()

    discover_network(node)

    return node


def discover_network(node):
    print("initializing: ....starting network discovery")
    print(
        "initializing: broadcast address",
        node.consensus.broadcast_address
    )
    node.ping(Node(
        node.consensus.broadcast_address, node.port))
    node.ping(Node("224.1.1.1", node.port))
    # for adress in list(node.consensus.network_info.hosts())[:10]:
    #     print(node.ping(Node(str(adress), node.port)))
    time.sleep(1)
    result = node.node_lookup(node.id)
    print("initializing: .... network discovered: ", result)
