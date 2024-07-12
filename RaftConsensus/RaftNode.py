import threading
import time
import random
from typing import Optional, List
from RaftConsensus.utils.States import RaftState
from RaftConsensus.utils.RaftRpcs import RaftRpc
from Kademlia.utils.Rpc import Rpc
from Kademlia.utils.MessageType import MessageType
from RaftConsensus.utils.Server import Server
from Kademlia.KademliaNetwork import KademliaNetwork
from Kademlia.RoutingTable import RoutingTable
from Kademlia.KBucket import Node

lock = threading.Lock()


class RaftNode(Server):
    def __init__(
        self, node: Node, network: KademliaNetwork, routing_table: RoutingTable
    ):
        super().__init__(node, network, routing_table)
        self.logs: List[str] = []
        self.state: RaftState = RaftState.Follower
        self.current_term = 0
        self.voted_for = None
        self.votes = 0

        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}
        self.heartbeat_timeout = random.randint(10, 20)
        self.leader_wait_timeout = random.randint(15, 25)

        self.leader: Optional[Node] = None
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        self.leader_wait_thread = threading.Thread(target=self.wait_leader_heartbeat)
        self.leader_wait_thread.start()
        print("raft: iniciando heartbeat de raft")

    def send_heartbeat(self):
        while True:
            time.sleep(self.heartbeat_timeout)
            with lock:
                if self.state == RaftState.Leader:
                    print("raft: sending broadcast")
                    self.send_broadcast_rpc(
                        Rpc(
                            RaftRpc.LeaderHeartBeat,
                            MessageType.Request,
                            (self.current_term, len(self.logs)),
                        )
                    )

    def wait_leader_heartbeat(self):
        start_time = time.time()
        while time.time() - start_time < self.leader_wait_timeout:
            print("raft: waiting for leader")
            time.sleep(self.leader_wait_timeout)
            if self.leader is None:
                print("raft: calling to elections")
                self.current_term += 1
                self.voted_for = self.node.id
                self.send_broadcast_rpc(
                    Rpc(
                        RaftRpc.RequestVote,
                        MessageType.Request,
                        (self.current_term, len(self.logs)),
                    )
                )
                time.sleep(self.leader_wait_timeout)
                if self.votes > self.routing_table.get_node_count() / 2:
                    self.state = RaftState.Leader
                    print(f"raft: {self.node.id} fue seleccionado lider ")
            else:
                self.leader_wait_timeout = random.randint(10, 20)
                start_time = time.time()

    def handle_raft_rpc(self, address, rpc, clock_ticks):
        print("raft: rpc  ", rpc, clock_ticks)
        rpc_type, message_type, payload = rpc
        if rpc_type == RaftRpc.RequestVote:
            if message_type == MessageType.Request:
                peer_term, peer_log = payload
                if peer_term >= self.current_term and peer_log >= len(self.logs):
                    print("raft: voting for ", address.id)
                    self.network.send_rpc(
                        address,
                        Rpc(RaftRpc.RequestVote, MessageType.Response, "Vote"),
                    )
                else:
                    print("raft: not voting for ", address.id)
                    self.network.send_rpc(
                        address,
                        Rpc(RaftRpc.RequestVote, MessageType.Response, "NoVote"),
                    )
            else:
                if payload == "Vote":
                    print("raft: recibing vote from for ", address.id)
                    with lock:
                        self.votes += 1
        if rpc_type == RaftRpc.LeaderHeartBeat:
            peer_term, peer_log = payload
            if peer_term >= self.current_term and peer_log >= len(self.logs):
                self.leader = address
                print(f"raft: recibido ping del leader {self.leader.id}  ")
