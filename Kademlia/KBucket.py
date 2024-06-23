import hashlib
from typing import List

K = 20  # Número de nodos en cada k-bucket
ID_LENGTH = 160  # Longitud de los identificadores en bits


def sha1_hash(data: str) -> int:
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)


class Node:
    def __init__(self, ip: str, port: int):
        self.id = sha1_hash(f"{ip}:{port}")
        self.ip = ip
        self.port = port

    def __repr__(self):
        return f"Node({self.id}, {self.ip}, {self.port})"


class KBucket:
    def __init__(self, range_start: int, range_end: int):
        self.range_start = range_start
        self.range_end = range_end
        self.nodes = []

    def add_node(self, node: Node):
        if node in self.nodes:
            self.nodes.remove(node)
        elif len(self.nodes) >= K:
            self.nodes.pop(0)
        self.nodes.append(node)

    def remove_node(self, node: Node):
        if node in self.nodes:
            self.nodes.remove(node)

    def get_nodes(self) -> List[Node]:
        return self.nodes
