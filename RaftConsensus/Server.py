from socket import socket
from Kademlia.KBucket import Node
from typing import List
from Kademlia.KademliaNetwork import KademliaNetwork
from Kademlia.RoutingTable import RoutingTable
from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.Rpc import Rpc
from RaftConsensus.utils.RaftRpcs import RaftRpc
from RaftConsensus.utils.States import RaftState
import random
import time
import threading


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

        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()

        self.broadcast_direction = "172.19.0.255"
        print("iniciando heartbeat de raft")

    def send_heartbeat(self):
        while True:
            time.sleep(self.heartbeat_timeout)
            self.network.send_rpc(
                Node(self.broadcast_direction, self.node.port, self.node.id),
                Rpc(RaftRpc.LeaderHeartBeat, MessageType.Request, "OK"),
            )
