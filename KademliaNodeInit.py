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
from Kademlia.utils.StoreAction import StoreAction

default_nodes = []


def init_node():
    ip = os.getenv("NODE_IP", "127.0.0.1")
    port = int(os.getenv("NODE_PORT", 8080))
    node = KademliaNode(ip, port)
    node.start()

    discover_network(node)

    # in case the node go down , he will try to set the data to his new owners
    print("finding owner nodes of my data")
    for data in node.database.playlists:
        node.store_playlist(StoreAction.UPDATE, data)
    # Especifica la ruta de la carpeta que quieres listar
    song_folder = "/tmp/songs"

    # Obtén una lista de todos los elementos en la carpeta
    songs = os.listdir(song_folder)

    # Imprime cada elemento
    for song in songs:
        print("restoring: ", song, "   ", song.split(".")[0])
        node.store_a_file(song, int(song.split(".")[0], 16))

    return node


def discover_network(node):
    print("initializing: ....starting network discovery")
    print("initializing: broadcast address", node.consensus.broadcast_address)
    node.ping(Node(node.consensus.broadcast_address, node.port))
    node.consensus.send_broadcast_rpc(Rpc(RpcType.Ping, MessageType.Request, "Ping"))
    node.ping(Node("224.1.1.1", node.port))
    # for adress in list(node.consensus.network_info.hosts())[:10]:
    #     print(node.ping(Node(str(adress), node.port)))
    time.sleep(2)
    result = node.node_lookup(node.id)
    node.consensus.start_election()
    print("initializing: .... network discovered: ", result)
