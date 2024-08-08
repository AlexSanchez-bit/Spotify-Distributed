import ipaddress
from threading import Thread
import time
from Kademlia.KBucket import K, Node
from typing import List, Optional
from Kademlia.KademliaNetwork import KademliaNetwork
from Kademlia.RoutingTable import RoutingTable
from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.Rpc import Rpc
from Kademlia.utils.RpcType import RpcType
import netifaces as ni
import socket


class Server:
    def __init__(
        self, node: Node, network: KademliaNetwork, routing_table: RoutingTable
    ):
        self.node = node
        self.network = network
        self.routing_table = routing_table
        self.calculate_broadcast_address()
        print("broadcast----", (self.broadcast_address))
        self.broadcast_address = "255.255.255.255"
        Thread(target=self.receive_broadcast).start()

    def send_broadcast_rpc(self, rpc):
        try:
            self.network.send_rpc(
                Node(
                    self.broadcast_address,
                    self.node.port,
                ),
                rpc,
            )
        except Exception as e:
            print("raft: no se pudo hacer broadcast", e)
            for peer_ip in list(self.network_info.hosts())[:100]:
                self.network.send_rpc(Node(str(peer_ip), self.node.port), rpc)

    def receive_broadcast(self):
        sock = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Permitir la reutilización de la dirección

        # Habilitar el modo de broadcast
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Vincular el socket a todas las interfaces y al puerto 5005
        sock.bind(("", self.node.port))

        while True:
            # Recibir datos
            data, addr = sock.recvfrom(1024)
            print(f"broadcast recibido desde: {addr}")
            self.network.handle_rpc(data, addr)
            time.sleep(0.05)

    def calculate_broadcast_address(self):
        netmask = "255.255.255.0"
        gateway = ni.gateways()["default"][ni.AF_INET][0]
        network = ipaddress.IPv4Network(f"{gateway}/{netmask}", strict=False)
        self.network_info = network
        self.netmask = netmask
        self.broadcast_address = str(network.broadcast_address)
        return str(network.broadcast_address)
