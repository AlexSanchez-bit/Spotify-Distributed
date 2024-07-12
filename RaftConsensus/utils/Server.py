import ipaddress
from Kademlia.KBucket import Node
from typing import List, Optional
from Kademlia.KademliaNetwork import KademliaNetwork
from Kademlia.RoutingTable import RoutingTable
import netifaces


class Server:
    def __init__(
        self, node: Node, network: KademliaNetwork, routing_table: RoutingTable
    ):
        self.node = node
        self.network = network
        self.routing_table = routing_table
        self.calculate_broadcast_address()

    def send_broadcast_rpc(self, rpc):
        try:
            self.network.send_rpc(
                Node(
                    self.broadcast_address,
                    self.node.port,
                    self.node.id,
                ),
                rpc,
            )
        except Exception as e:
            print("no se pudo hacer broadcast", e)
            for bucket in self.routing_table.buckets:
                for node in bucket.get_nodes():
                    self.network.send_rpc(node, rpc)

    def calculate_broadcast_address(self):
        netmask = "255.255.255.0"
        for interface in netifaces.interfaces():
            addrs = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addrs:
                for addr in addrs[netifaces.AF_INET]:
                    if addr["addr"] == self.node.ip:
                        netmask = addr["netmask"]
        network = ipaddress.IPv4Network(f"{self.node.ip}/{netmask}", strict=False)
        self.network_info = network
        self.netmask = netmask
        self.broadcast_address = str(network.broadcast_address)
        return str(network.broadcast_address)
