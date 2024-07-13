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
        self.heartbeat_timeout = random.randint(2, 5)
        self.leader_wait_timeout = random.randint(5, 10)

        self.leader: Optional[Node] = None
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        self.leader_wait_thread = threading.Thread(target=self.wait_leader_heartbeat)
        self.leader_wait_thread.start()
        self.apply_thread = threading.Thread(target=self.apply_log)
        self.apply_thread.start()
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
        while True:
            time.sleep(self.leader_wait_timeout)
            with lock:
                if self.state != RaftState.Leader:
                    if self.leader is None:
                        self.call_elections()
                    self.leader_wait_timeout = random.randint(5, 10)

    def call_elections(self):
        with lock:
            self.state = RaftState.Candidate
            print("raft: calling to elections")
            self.current_term += 1
            self.voted_for = self.node.id
            self.votes = 1  # vote for self
            self.send_broadcast_rpc(
                Rpc(
                    RaftRpc.RequestVote,
                    MessageType.Request,
                    (self.current_term, len(self.logs)),
                )
            )

    def handle_raft_rpc(self, address, rpc, clock_ticks):
        print("raft: rpc  ", rpc, clock_ticks)
        rpc_type, message_type, payload = rpc
        if rpc_type == RaftRpc.RequestVote:
            if message_type == MessageType.Request:
                peer_term, peer_log = payload
                with lock:
                    if peer_term > self.current_term:
                        self.current_term = peer_term
                        self.voted_for = None
                    if (
                        self.voted_for is None or self.voted_for == address.id
                    ) and peer_log >= len(self.logs):
                        print("raft: voting for ", address.id)
                        self.voted_for = address.id
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
            elif message_type == MessageType.Response:
                if payload == "Vote":
                    print("raft: receiving vote from ", address.id)
                    with lock:
                        self.votes += 1
                        if self.votes > self.routing_table.get_node_count() / 2:
                            self.state = RaftState.Leader
                            self.leader = self.node
                            print(f"raft: {self.node.id} fue seleccionado lider ")
                            self.send_heartbeat()
        elif rpc_type == RaftRpc.LeaderHeartBeat:
            peer_term, peer_log = payload
            with lock:
                if peer_term >= self.current_term:
                    self.current_term = peer_term
                    self.leader = address
                    self.state = RaftState.Follower
                    print(f"raft: recibido ping del leader {self.leader.id}")
                    # Responder al líder para confirmar que el heartbeat fue recibido
                    self.network.send_rpc(
                        address,
                        Rpc(
                            RaftRpc.LeaderHeartBeat,
                            MessageType.Response,
                            (self.current_term, len(self.logs)),
                        ),
                    )
        elif rpc_type == RaftRpc.AppendEntries:
            response = self.append_entries(address, rpc)
            self.network.send_rpc(address, response)
        elif rpc_type == RaftRpc.AppendEntries and message_type == MessageType.Response:
            term, success = payload
            with lock:
                if term > self.current_term:
                    self.current_term = term
                    self.state = RaftState.Follower
                    self.leader = address
                if success:
                    # Actualizar los índices `next_index` y `match_index`
                    self.next_index[address.id] = len(self.logs)
                    self.match_index[address.id] = len(self.logs) - 1
                    # Verificar si una nueva entrada puede ser comprometida
                    for index in range(self.commit_index + 1, len(self.logs)):
                        count = sum(1 for n in self.match_index.values() if n >= index)
                        if count > self.routing_table.get_node_count() / 2:
                            self.commit_index = index
                            self.apply_log()
                else:
                    self.next_index[address.id] -= 1

    def append_entries(self, address, rpc):
        rpc_type, message_type, payload = rpc
        if rpc_type == RaftRpc.AppendEntries:
            term, prev_log_index, prev_log_term, entries, leader_commit = payload
            with lock:
                if term < self.current_term:
                    return Rpc(
                        RaftRpc.AppendEntries,
                        MessageType.Response,
                        (self.current_term, False),
                    )
                self.leader = address
                self.current_term = term
                self.state = RaftState.Follower
                if prev_log_index >= len(self.logs) or (
                    prev_log_index >= 0
                    and self.logs[prev_log_index][0] != prev_log_term
                ):
                    return Rpc(
                        RaftRpc.AppendEntries,
                        MessageType.Response,
                        (self.current_term, False),
                    )
                self.logs = self.logs[: prev_log_index + 1] + entries
                if leader_commit > self.commit_index:
                    self.commit_index = min(leader_commit, len(self.logs) - 1)
                return Rpc(
                    RaftRpc.AppendEntries,
                    MessageType.Response,
                    (self.current_term, True),
                )

    def apply_log(self):
        while True:
            with lock:
                if self.commit_index > self.last_applied:
                    self.last_applied += 1
                    print(f"raft: Aplicando log de índice {self.last_applied}")
            time.sleep(1)

    def run(self):
        while True:
            with lock:
                if self.state == RaftState.Leader:
                    self.send_heartbeat()
                elif self.state == RaftState.Follower:
                    self.wait_leader_heartbeat()
                elif self.state == RaftState.Candidate:
                    self.call_elections()
            time.sleep(1)
