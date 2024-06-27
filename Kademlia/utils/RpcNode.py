from KBucket import Node
from utils.Rpc import Rpc


class RpcNode:
    def ping(self, node: Node):
        pass

    def store(self, key: str, value: str):
        pass

    def find_node(self, target_id: int, node=None):
        pass

    def find_value(self, key: str):
        pass

    def handle_rpc(self, address, rpc: Rpc):
        pass
