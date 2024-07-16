from enum import Enum
from threading import Thread, Lock
import time
from Kademlia.KBucket import Node
from Kademlia.KademliaNetwork import KademliaNetwork
from Kademlia.RoutingTable import RoutingTable
from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.Rpc import Rpc
from Kademlia.utils.RpcType import RpcType
from RaftConsensus.utils.Server import Server


class NodeState(Enum):
    FOLLOWER = "Follower"
    LEADER = "Leader"
    ELECTION = "Election"


class BullyRpcType(Enum):
    ELECTION = "Election"
    COORDINATOR = "Coordinator"


class BullyConsensus(Server):
    def __init__(
        self, node: Node, network: KademliaNetwork, routing_table: RoutingTable
    ):
        super().__init__(node, network, routing_table)
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        self.lock = Lock()
        self.election_timeout = 4

        # Thread(target=self.monitor_leader, daemon=True).start()

    def monitor_leader(self):
        while True:
            time.sleep(self.election_timeout)
            if not self.ping_leader():
                self.start_election()

    def ping_leader(self):
        if self.leader_id is None:
            return False
        leader_node = self.routing_table.get_node_by_id(self.leader_id)
        if leader_node:
            return self.ping(leader_node)
        return False

    def ping(self, node: Node):
        rpc = Rpc(RpcType.Ping, MessageType.Request, "Ping")
        try:
            self.network.send_rpc(node, rpc)
            return True
        except Exception as e:
            print(f"bully: an exception occurred {e}")
            return False

    def start_election(self):
        print("requesting start elections")
        # with self.lock:
        #     self.state = NodeState.ELECTION
        #     print(f"Node {self.node.id} starting election")
        #     higher_nodes = [
        #         node
        #         for node in self.routing_table.get_all_nodes()
        #         if node.id > self.node.id
        #     ]
        #
        #     if not higher_nodes:
        #         self.become_leader()
        #         return
        #
        #     responses = []
        #     threads = []
        #     for node in higher_nodes:
        #         thread = Thread(
        #             target=self.send_election_message, args=(node, responses)
        #         )
        #         thread.start()
        #         threads.append(thread)
        #
        #     for thread in threads:
        #         thread.join()
        #
        #     if not responses:
        #         self.become_leader()

<<<<<<< HEAD
    def send_election_message(self, node, responses):
        rpc = Rpc(BullyRpcType.ELECTION, MessageType.Request, self.node.id)
        try:
            response = self.network.send_rpc(node, rpc)
            if response:
                responses.append(response)
        except Exception as e:
            print(f"bully send election error: {e}")
=======
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
                    (self.current_term, len(self.logs)),
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
        with lock:
            if message_type == MessageType.Request:
                peer_term, peer_log = payload
                self.process_vote_request(address, peer_term, peer_log)
            elif message_type == MessageType.Response:
                self.process_vote_response(address, payload)

    def process_vote_request(self, address: Node, peer_term: int, peer_log: int):
        if peer_term > self.current_term:
            self.update_term(peer_term)
        if (self.voted_for is None or self.voted_for == address.id) and peer_log >= len(
            self.logs
        ):
            self.voted_for = address.id
            self.network.send_rpc(
                address, Rpc(RaftRpc.RequestVote, MessageType.Response, "Vote")
            )
        else:
            self.network.send_rpc(
                address, Rpc(RaftRpc.RequestVote, MessageType.Response, "NoVote")
            )

    def process_vote_response(self, address: Node, payload: str):
        if payload == "Vote":
            with lock:
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
>>>>>>> c3cfb9ec7084a57948084ab3248b1e53fb347232

    def become_leader(self):
        with self.lock:
            self.state = NodeState.LEADER
            self.leader_id = self.node.id
            print(f"Node {self.node.id} became the leader")
            self.announce_leadership()

    def announce_leadership(self):
        nodes = self.routing_table.get_all_nodes()
        for node in nodes:
            rpc = Rpc(BullyRpcType.COORDINATOR, MessageType.Request, self.node.id)
            self.network.send_rpc(node, rpc)

    def handle_rpc(self, address, rpc, clock_ticks):
        rpc_type, message_type, payload = rpc
        if rpc_type == BullyRpcType.ELECTION:
            self.handle_election_rpc(address, message_type, payload)
        elif rpc_type == BullyRpcType.COORDINATOR:
            self.handle_coordinator_rpc(address, message_type, payload)
        else:
            print(f"Unknown RPC type: {rpc_type}")

    def handle_election_rpc(self, node, message_type, payload):
        candidate_id = payload
        if candidate_id is not None and candidate_id < self.node.id:
            rpc = Rpc(BullyRpcType.ELECTION, MessageType.Response, self.node.id)
            self.network.send_rpc(node, rpc)
            self.start_election()
        else:
            rpc = Rpc(BullyRpcType.ELECTION, MessageType.Response, None)
            self.network.send_rpc(node, rpc)

    def handle_coordinator_rpc(self, node, message_type, payload):
        leader_id = payload
        with self.lock:
            self.leader_id = leader_id
            self.state = NodeState.FOLLOWER
        print(
            f"Node {self.node.id} ",
            f" acknowledged {self.leader_id} as the leader",
        )

    def send_broadcast_rpc(self, rpc):
        super().send_broadcast_rpc(rpc)
        print("Bully consensus broadcast RPC sent.")
