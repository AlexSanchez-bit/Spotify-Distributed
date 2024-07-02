import socket
import pickle
from time import sleep
import time
from Kademlia.KBucket import Node
from typing import Any, Tuple
import threading
from Kademlia.RoutingTable import RoutingTable
from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.Rpc import Rpc
from Kademlia.utils.RpcNode import RpcNode
from Kademlia.utils.RpcType import RpcType

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
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind((node.ip, node.port))
        self.sended_pings = []
        print(f"node {node.id} listenning on {node.ip}:{node.port}")

    def send_rpc(self, node: Node, rpc):
        """
        Send An Encoded rpc to the peer
        """
        message = pickle.dumps(rpc)
        with lock:
            self.server_socket.sendto(message, (node.ip, node.port))

    def receive_rpc(self):
        """
        Waits for rpc and manages messages
        """
        while True:
            message, address = self.server_socket.recvfrom(4096)
            ip, port = address
            sender = Node(ip, port)
            rpc = pickle.loads(message)
            respond_thread = threading.Thread(
                target=self.node.handle_rpc, args=[sender, rpc]
            )
            respond_thread.start()
            self.refresh_k_buckets(sender)

    def refresh_k_buckets(self, node: Node):
        least = self.node.routing_table.add_node(node)
        if least is not None:
            self.node.ping(least, MessageType.Request)
            with lock:
                self.sended_pings.append(least)
                receiver_thread = threading.Thread(
                    target=self.wait_to_response, args=[node, least]
                )
                receiver_thread.start()

    def wait_to_response(self, least, current):
        print("dio ping uno viejo")
        count = 0
        start_time = time.time()
        while least not in self.sended_pings:
            time.sleep((3))
            count += 1
            if time.time() - start_time > 4:
                return
        with lock:
            self.node.routing_table.replace(current.id, least.id)
            self.sended_pings.remove(least)

    def start(self):
        print("starting network")
        receiver_thread = threading.Thread(target=self.receive_rpc)
        receiver_thread.start()
