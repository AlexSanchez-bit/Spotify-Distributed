from KBucket import Node
from typing import Tuple, Any, List
from RoutingTable import RoutingTable
from utils.Rpc import Rpc
from utils.RpcNode import RpcNode
from utils.RpcType import RpcType
from utils.MessageType import MessageType
from KademliaNetwork import KademliaNetwork


class KademliaRpcNode(Node, RpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port)
        self.routing_table = RoutingTable(self.id)
        self.network = KademliaNetwork(self)

    def ping(self, node: Node, type: MessageType = MessageType.Request):
        try:
            self.network.send_rpc(node, Rpc(RpcType.Ping, type, "Ping"))
            return True
        except Exception:
            return False

    def store(self, key: str, value: str):
        pass

    def find_node(self, target_id: int, node=None) -> List[Node]:
        return []

    def find_value(self, key: str) -> str:
        return ""

    def handle_rpc(self, address, rpc: Rpc):
        print("llego: ", rpc, "de", address)
        rpc_type, message_type, payload = rpc
        if rpc_type == RpcType.Ping:
            self.handle_ping(address, message_type)

    def handle_ping(self, node, message_type):
        if message_type == MessageType.Request:
            self.ping(node, type=MessageType.Response)
        if message_type == MessageType.Response:
            print("me respondio", node)
