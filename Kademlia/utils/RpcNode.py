from KBucket import Node


class RpcNode(Node):
    def __init__(self, ip, port, routing_table):
        super().__init__(ip, port)
        self.routing_table = routing_table

    def ping(self, node: Node):
        pass

    def store(self, key: str, value: str):
        pass

    def find_node(self, target_id: int, node=None):
        pass

    def find_value(self, key: str):
        pass

    def handle_rpc(self, address, rpc):
        pass
