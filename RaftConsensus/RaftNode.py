import threading
import time
import random
from typing import Optional, List, Tuple, Any
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
        self.initialize_state()
        self.start_threads()

    def initialize_state(self):
        self.logs: List[str] = []
        self.state: RaftState = RaftState.Follower
        self.current_term = 0
        self.voted_for = None
        self.votes = 0

        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}
        self.heartbeat_timeout = random.randint(2, 4)
        self.leader_wait_timeout = random.randint(2, 4)

        self.leader: Optional[Node] = None

    def start_threads(self):
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        self.leader_wait_thread = threading.Thread(target=self.wait_leader_heartbeat)
        self.leader_wait_thread.start()
        self.apply_thread = threading.Thread(target=self.apply_log)
        self.apply_thread.start()

    def send_heartbeat(self):
        while True:
            time.sleep(self.heartbeat_timeout)
            with lock:
                if self.state == RaftState.Leader:
                    self.broadcast_heartbeat()

    def broadcast_heartbeat(self):
        print(f"raft: Node {self.node.id} sending heartbeat")
        self.send_broadcast_rpc(
            Rpc(
                RaftRpc.LeaderHeartBeat,
                MessageType.Request,
                (self.current_term, self.logs),
            )
        )

    def wait_leader_heartbeat(self):
        while True:
            time.sleep(self.leader_wait_timeout)
            self.check_leader_heartbeat()
            self.leader_wait_timeout = random.randint(2, 4)

    def check_leader_heartbeat(self):
        print(f"raft: Node {self.node.id} waiting for leader heartbeat")
        if self.state != RaftState.Leader and self.leader is None:
            self.call_elections()

    def call_elections(self):
        with lock:
            print(f"raft: Node {self.node.id} initiating election")
            self.state = RaftState.Candidate
            self.current_term += 1
            self.voted_for = self.node.id
            self.votes = 1  # vote for self
            self.send_broadcast_rpc(
                Rpc(
                    RaftRpc.RequestVote,
                    MessageType.Request,
                    (self.current_term, self.logs),
                )
            )

    def handle_raft_rpc(self, address: Node, rpc, clock_ticks: Any):
        rpc_type, message_type, payload = rpc
        if rpc_type == RaftRpc.RequestVote:
            self.handle_request_vote(address, message_type, payload)
        elif rpc_type == RaftRpc.LeaderHeartBeat:
            self.handle_leader_heartbeat(address, payload)
        elif rpc_type == RaftRpc.AppendEntries:
            self.handle_append_entries(address, message_type, payload)

    def handle_request_vote(
        self, address: Node, message_type: MessageType, payload: Any
    ):
        if message_type == MessageType.Request:
            peer_term, peer_log = payload
            with lock:
                self.process_vote_request(address, peer_term, peer_log)
        elif message_type == MessageType.Response:
            with lock:
                self.process_vote_response(address, payload)

    def process_vote_request(self, address: Node, peer_term: int, peer_log: List[str]):
        if peer_term > self.current_term:
            self.update_term(peer_term)
            self.voted_for = address.id
            print("raft election votando por: ", address)
            self.network.send_rpc(
                address, Rpc(RaftRpc.RequestVote, MessageType.Response, "Vote")
            )
        else:
            print("raft election rechazando votar a : ", address)
            self.network.send_rpc(
                address, Rpc(RaftRpc.RequestVote, MessageType.Response, "NoVote")
            )

    def process_vote_response(self, address: Node, payload: str):
        if payload == "Vote":
            print("raft: election recibido el voto de : ", address)
            self.votes += 1
            if self.votes > self.routing_table.get_node_count() / 2:
                self.become_leader()

    def handle_leader_heartbeat(self, address: Node, payload: Tuple[int, List[str]]):
        peer_term, peer_log = payload
        if peer_term > self.current_term:
            print(f"cambiando de lider de: {self.leader} to {address}")
            self.update_term(peer_term)
            with lock:
                self.leader = address
                self.state = RaftState.Follower
        self.network.send_rpc(
            address,
            Rpc(
                RaftRpc.LeaderHeartBeat,
                MessageType.Response,
                (self.current_term, self.logs),
            ),
        )

    def handle_append_entries(
        self,
        address: Node,
        message_type: MessageType,
        payload: Tuple[int, int, int, List[str], int],
    ):
        if message_type == MessageType.Request:
            response = self.append_entries(address, payload)
            self.network.send_rpc(address, response)
            if self.state == RaftState.Leader:
                self.send_broadcast_rpc(
                    Rpc(RaftRpc.AppendEntries, MessageType.Request, payload)
                )
        elif message_type == MessageType.Response:
            self.process_append_entries_response(address, payload)

    def append_entries(
        self, address: Node, payload: Tuple[int, int, int, List[str], int]
    ):
        term, prev_log_index, prev_log_term, entries, leader_commit = payload
        if term < self.current_term:
            return Rpc(
                RaftRpc.AppendEntries,
                MessageType.Response,
                (self.current_term, False),
            )
        with lock:
            self.update_term(term)
        if self.is_log_consistent(prev_log_index, prev_log_term):
            self.update_log(entries, leader_commit)
            return Rpc(
                RaftRpc.AppendEntries,
                MessageType.Response,
                (self.current_term, True),
            )
        else:
            return Rpc(
                RaftRpc.AppendEntries,
                MessageType.Response,
                (self.current_term, False),
            )

    def send_entry_to_leader(self, entry: Tuple[str, str]):
        if self.state == RaftState.Follower and self.leader is not None:
            # Enviar la entrada al líder
            print("enviando log al lider")
            self.network.send_rpc(
                self.leader,
                Rpc(
                    RaftRpc.AppendEntries,
                    MessageType.Request,
                    (
                        self.current_term,
                        len(self.logs) - 1,
                        self.logs[-1] if self.logs else -1,
                        [entry],
                        self.commit_index,
                    ),
                ),
            )

    def process_append_entries_response(self, address: Node, payload: Tuple[int, bool]):
        term, success = payload
        if term > self.current_term:
            with lock:
                self.update_term(term)
            self.state = RaftState.Follower
            self.leader = address
        if success:
            self.update_indices(address.id)
            self.commit_entries()

    def update_term(self, term: int):
        self.current_term = term
        self.voted_for = None

    def is_log_consistent(self, prev_log_index: int, prev_log_term: int) -> bool:
        return prev_log_index < len(self.logs) and (
            prev_log_index == -1 or self.logs[prev_log_index][0] == prev_log_term
        )

    def update_log(self, entries: List[str], leader_commit: int):
        self.logs = self.logs[: len(entries)] + entries
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.logs) - 1)

    def update_indices(self, node_id: int):
        self.next_index[node_id] = len(self.logs)
        self.match_index[node_id] = len(self.logs) - 1

    def commit_entries(self):
        for index in range(self.commit_index + 1, len(self.logs)):
            count = sum(
                1 for match_index in self.match_index.values() if match_index >= index
            )
            if count > self.routing_table.get_node_count() / 2:
                self.commit_index = index
                self.apply_log()

    def become_leader(self):
        self.state = RaftState.Leader
        self.leader = self.node
        self.initialize_leader_state()
        print(f"raft: Node {self.node.id} elected as leader")
        self.update_term(self.current_term + 1)

    def initialize_leader_state(self):
        for node in self.routing_table.get_all_nodes():
            if node.id != self.node.id:
                self.next_index[node.id] = len(self.logs)
                self.match_index[node.id] = 0

    def apply_log(self):
        while True:
            with lock:
                if self.commit_index > self.last_applied:
                    self.last_applied += 1
                    print(
                        f"raft: Node {self.node.id}",
                        f"raft: applying log index {self.last_applied}",
                    )
            time.sleep(1)
