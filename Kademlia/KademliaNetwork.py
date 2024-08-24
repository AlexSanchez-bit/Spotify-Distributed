import socket
import pickle
from time import sleep
import time
import struct
from Kademlia.KBucket import Node
from typing import Any, Tuple
import threading
from Kademlia.RoutingTable import RoutingTable
from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.Rpc import Rpc
from Kademlia.utils.RpcNode import RpcNode
from Kademlia.utils.RpcType import RpcType
from Kademlia.utils.Syncronization.LamportClock import LamportClock

lock = threading.Lock()


class KademliaNetwork:
    """
    Mantaining the routing info and managing the nodes network conections
    """

    def __init__(self, node: RpcNode):
        """
        Initializaes the sockets for comunication
        """

        self.node = node
        self.clock = LamportClock()
        self.server_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )
        ttl = struct.pack("b", 1)
        self.server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((node.ip, node.port))
        mreq = struct.pack("4sl", socket.inet_aton("224.1.1.1"), socket.INADDR_ANY)
        self.server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        self.sended_pings = []
        print(f"node {node.id} listenning on {node.ip}:{node.port}")

    def send_rpc(self, node: Node, rpc):
        """
        Send An Encoded rpc to the peer
        """
        message = pickle.dumps((rpc, self.clock.ticks))
        self.clock.tick()
        with lock:
            self.server_socket.sendto(message, (node.ip, node.port))

    def receive_rpc(self):
        """
        Waits for rpc and manages messages
        """
        while True:
            message, address = self.server_socket.recvfrom(4096)
            self.handle_rpc(message, address)
            time.sleep(0.00000005)

    def handle_rpc(self, message, address):
        try:
            print("received: ---", message.decode())
            if message.decode() == "client":  # received client broadcast
                self.server_socket.sendto(
                    "server".encode(), address
                )  # respond to client
                return
        except Exception:
            print("no client petition")

        ip, port = address
        sender = Node(ip, port)
        rpc, ticks = pickle.loads(message)
        self.clock.tick()
        self.clock.merge_ticks(ticks)

        respond_thread = threading.Thread(
            target=self.node.handle_rpc, args=[sender, rpc, self.clock.ticks]
        )
        respond_thread.start()

        refresh_thread = threading.Thread(target=self.refresh_k_buckets, args=[sender])
        refresh_thread.start()

    def refresh_k_buckets(self, node: Node):
        least = self.node.routing_table.add_node(node)
        if least is not None:
            result = self.node.ping(least, MessageType.Request)
            index = self.node.routing_table.get_bucket_index(least.id)
            if not result:
                self.node.routing_table.buckets[index].remove_node(least)
                self.node.routing_table.add_node(node)
            else:
                self.node.routing_table.buckets[index].remove_node(least)
                self.node.routing_table.add_node(least)

    def start(self):
        print("starting network")
        receiver_thread = threading.Thread(target=self.receive_rpc)
        receiver_thread.start()
