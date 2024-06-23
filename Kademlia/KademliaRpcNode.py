from KBucket import Node
from typing import Tuple, Any


class KademliaRpcNode(Node):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port)

    def handle_rpc(self, rpc: Tuple[str, Any]) -> Any:
        pass
