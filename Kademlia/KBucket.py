import hashlib
from threading import Lock
from typing import List

lock = Lock()

K = 3  # Número de nodos en cada k-bucket
ID_LENGTH = 160  # Longitud de los identificadores en bits


def sha1_hash(data: str) -> int:
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)


class Node:
    def __init__(self, ip: str, port: int, id=None):
        if id is not None:
            self.id = id
        else:
            self.id = sha1_hash(f"{ip}:{port}")
        self.ip = ip
        self.port = port

    def __repr__(self):
        return f"Node({self.id}, {self.ip}, {self.port})"

    def __eq__(self, o):
        return self.id == o.id

    def __ne__(self, o):
        return self.id != o.id

    def __hash__(self) -> int:
        return self.id


class KBucket:
    def __init__(self, range_start: int, range_end: int):
        self.range_start = range_start
        self.range_end = range_end
        self.nodes = []

    def add_node(self, node: Node):
        with lock:
            if node in self.nodes:
                return None

            if node not in self.nodes and len(self.nodes) < K:
                self.nodes.append(node)
                return None
            else:
                return self.nodes[0]

    def remove_node(self, node: Node):
        print("into removing node")
        with lock:
            if node in self.nodes:
                self.nodes.remove(node)
        print("finish removing node")
        print("removing node -- : ", node, " from k-bucket ", self.nodes)

    def get_nodes(self) -> List[Node]:
        return [node for node in self.nodes]
