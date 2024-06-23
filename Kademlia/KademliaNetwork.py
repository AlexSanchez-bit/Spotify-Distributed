import socket
import pickle
from KBucket import Node
from typing import Any, Tuple
import threading
from KademliaRpcNode import KademliaRpcNode


class KademliaNetwork:
    """
    Mantaining the routing info and managing the nodes network conections
    """

    def __init__(self, node: KademliaRpcNode):
        """
        Initializaes the sockets for comunication
        """

        self.node = node
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind((node.ip, node.port))
        print(f"node {node.id} listenning on {node.ip}:{node.port}")

    def send_rpc(self, node: Node, rpc: Tuple[str, Any]):
        """
        Send An Encoded rpc to the peer
        """
        print(f"sending {rpc} to {node}")
        message = pickle.dumps(rpc)
        self.server_socket.sendto(message, (node.ip, node.port))

    def receive_rpc(self):
        """
        Waits for rpc and manages messages
        """
        while True:
            print("hello socket")
            message, address = self.server_socket.recvfrom(4096)
            if message or address:
                print("receiving", message, address)
            else:
                print("waiting...")
            rpc = pickle.loads(message)
            response = self.node.handle_rpc(rpc)
            if response:
                response_message = pickle.dumps(response)
                self.server_socket.sendto(response_message, address)

    def start(self):
        print("starting network")
        receiver_thread = threading.Thread(target=self.receive_rpc)
        receiver_thread.start()
