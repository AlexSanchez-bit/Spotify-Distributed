import socket
import pickle
from time import sleep
import time
from KBucket import Node
from typing import Any, Tuple
import threading
from RoutingTable import RoutingTable
from utils.MessageType import MessageType
from utils.Rpc import Rpc
from utils.RpcNode import RpcNode
from utils.RpcType import RpcType


class KademliaNetwork:
    """
    Mantaining the routing info and managing the nodes network conections
    """

    def __init__(self, node: RpcNode):
        """
        Initializaes the sockets for comunication
        """

        self.node = node
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind((node.ip, node.port))
        self.sended_pings = []
        print(f"node {node.id} listenning on {node.ip}:{node.port}")

    def send_rpc(self, node: Node, rpc: Rpc):
        """
        Send An Encoded rpc to the peer
        """
        self.refresh_k_buckets(node)
        message = pickle.dumps(rpc)
        self.server_socket.sendto(message, (node.ip, node.port))

    def receive_rpc(self):
        """
        Waits for rpc and manages messages
        """
        while True:
            message, address = self.server_socket.recvfrom(4096)
            ip, port = address
            sender = Node(ip, port)
            self.refresh_k_buckets(sender)
            rpc = pickle.loads(message)
            if (
                rpc[0] == RpcType.Ping
                and rpc[1] == MessageType.Response
                and sender in self.sended_pings
            ):
                # if the node on a k-bucket has responded delete it from the sended pings
                self.sended_pings.remove(sender)
            self.node.handle_rpc(sender, rpc)

    def refresh_k_buckets(self, node: Node):
        least = self.node.routing_table.add_node(node)
        if least is not None:
            self.node.ping(least)
            self.sended_pings.append(least)
            receiver_thread = threading.Thread(
                target=self.wait_to_response, args=[node, least]
            )
            receiver_thread.start()

    def wait_to_response(self, least, current):
        time.sleep(10)  # wait at least 10 secs
        print("dio ping uno viejo")
        if least in self.sended_pings:  # if least has responded we would have delete it
            # if still is on the list doesnt responde the ping
            self.node.routing_table.replace(current.id, least.id)
            self.sended_pings.remove(least)

    def start(self):
        print("starting network")
        receiver_thread = threading.Thread(target=self.receive_rpc)
        receiver_thread.start()
