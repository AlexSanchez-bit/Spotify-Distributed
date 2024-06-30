from KBucket import Node
from utils.MessageType import MessageType
from utils.StoreAction import StoreAction


class RpcNode(Node):
    def __init__(self, ip, port, routing_table):
        super().__init__(ip, port)
        self.routing_table = routing_table

    def ping(self, node: Node, type: MessageType):
        pass

    def store(self, key, node, value):
        pass

    def find_node(self, target_id: int, node=None):
        pass

    def find_value(self, key: str):
        pass

    def handle_rpc(self, address, rpc):
        pass
