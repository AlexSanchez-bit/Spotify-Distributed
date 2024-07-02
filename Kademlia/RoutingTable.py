from typing import List
from Kademlia.KBucket import KBucket, Node, ID_LENGTH


class RoutingTable:
    """
    Routing table is responsable of the routing info and the maintain the k-buckets
    """

    def __init__(self, node_id: int):
        """
        The constructior initializes the k-buckets with all the ranges of the key-space
        """
        self.node_id = node_id
        self.buckets = [KBucket(2**i, 2 ** (i + 1)) for i in range(ID_LENGTH)]

    def add_node(self, node: Node):
        """
        this adds a node to the k-buckets
        """

        bucket_index = self.get_bucket_index(node.id)
        if bucket_index >= 0:
            self.buckets[bucket_index].add_node(node)

    def replace(self, id, o):
        print(f"replacing {id} with {0}")
        idx = self.get_bucket_index(id)
        self.buckets[idx].remove_node(Node("", 0, id))
        self.buckets[idx].add_node(o)

    def get_bucket_index(self, node_id: int) -> int:
        """
        Using XOR metric to calculate distances we get the nearest k-bucket for a given key
        """

        distance = self.node_id ^ node_id
        return distance.bit_length() - 1

    def find_closest_nodes(self, target_id: int, count: int) -> List[Node]:
        """
        Using BinarySearch we look for the nearest node on our k-buckets
        """
        bucket_index = self.get_bucket_index(target_id)
        closest_nodes = self.buckets[bucket_index].get_nodes()
        for i in range(1, ID_LENGTH):
            if len(closest_nodes) >= count:
                break
            if bucket_index - i >= 0:
                closest_nodes.extend(self.buckets[bucket_index - i].get_nodes())
            if bucket_index + i < ID_LENGTH:
                closest_nodes.extend(self.buckets[bucket_index + i].get_nodes())
        closest_nodes = sorted(closest_nodes, key=lambda node: node.id ^ target_id)
        return closest_nodes[:count]
