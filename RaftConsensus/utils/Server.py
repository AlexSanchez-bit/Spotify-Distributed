import ipaddress
from threading import Thread
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

    def send_broadcast_rpc(self, rpc):
        try:
            self.network.send_rpc(
                Node(
                    self.broadcast_address,
                    self.node.port,
                ),
                rpc,
            )
            for peer_ip in list(self.network_info.hosts())[:100]:
                self.network.send_rpc(Node(str(peer_ip), self.node.port), rpc)
        except Exception as e:
            print("raft: no se pudo hacer broadcast", e)
            for peer_ip in list(self.network_info.hosts())[:100]:
                self.network.send_rpc(Node(str(peer_ip), self.node.port), rpc)

    def calculate_broadcast_address(self):
        netmask = "255.255.255.0"
        gateway = ni.gateways()["default"][ni.AF_INET][0]
        network = ipaddress.IPv4Network(
            f"{'172.19.0.255'}/{netmask}", strict=False)
        self.network_info = network
        self.netmask = netmask
        self.broadcast_address = str(network.broadcast_address)
        return str(network.broadcast_address)
