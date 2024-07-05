from Kademlia.KBucket import Node
from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.StoreAction import StoreAction


class RpcNode(Node):
    def __init__(self, ip, port, routing_table):
        super().__init__(ip, port)
        self.routing_table = routing_table

    def ping(self, node: Node, type: MessageType = MessageType.Request):
        pass

    def store(self, key, node, value):
        pass

    def find_node(self, target_id: int, node=None):
        pass

    def find_value(self, key: int, node: Node):
        pass

    def handle_rpc(self, address, rpc, clock_ticks):
        pass
