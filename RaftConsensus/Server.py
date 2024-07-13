import ipaddress
from Kademlia.KBucket import Node
from typing import List, Optional
from Kademlia.KademliaNetwork import KademliaNetwork
from Kademlia.RoutingTable import RoutingTable
from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.Rpc import Rpc
from RaftConsensus.utils.RaftRpcs import RaftRpc
from RaftConsensus.utils.States import RaftState
import random
import time
import threading
import netifaces

lock = threading.Lock()


class Server:
    def __init__(
        self, node: Node, network: KademliaNetwork, routing_table: RoutingTable
    ):
        self.node = node
        self.network = network
        self.routing_table = routing_table
        self.logs: List[str] = []
        self.state: RaftState = RaftState.Follower
        self.current_term = 0
        self.voted_for = None
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}
        self.heartbeat_timeout = random.randint(10, 20)
        self.leader_wait_timeout = random.randint(15, 25)

        self.leader: Optional[Node] = None

        self.calculate_broadcast_address()
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        print("iniciando heartbeat de raft")

    def wait_leader_heartbeat(self):
        while True:
            print("waiting for leader")
            time.sleep(self.leader_wait_timeout)
            if self.leader is not None:
                print("calling elections")
            else:
                self.leader_wait_timeout = random.randint(10, 20)

    def send_heartbeat(self):
        while True:
            time.sleep(self.heartbeat_timeout)
            print("sending broadcast")
            with lock:
                if self.state == RaftState.Leader:
                    self.send_broadcast_rpc(
                        Rpc(
                            RaftRpc.LeaderHeartBeat,
                            MessageType.Request,
                            self.current_term,
                        )
                    )

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
            for bucket in self.network.node.routing_table.buckets:
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
