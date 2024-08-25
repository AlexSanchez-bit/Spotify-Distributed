import enum
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
        self.buckets = [KBucket(0, 2)] + [
            KBucket(2**i, 2 ** (i + 1)) for i in range(ID_LENGTH)
        ]

    def get_node_by_id(self, node_id):
        index = self.get_bucket_index(node_id)
        for node in self.buckets[index].get_nodes():
            if node.id == node_id:
                return node
        return None

    def add_node(self, node: Node):
        """
        this adds a node to the k-buckets
        """

        bucket_index = self.get_bucket_index(node.id)
        print("buckets ", bucket_index)
        if bucket_index >= 0:
            return self.buckets[bucket_index].add_node(node)
        return "ID Not In Range"

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
        # Determinar el índice del bucket
        # Buscar el primer bit '1' en la representación binaria
        print(
            f"routingtable: distancia entre"
            + f" {self.node_id} y {node_id} es: {distance}"
        )
        print("routingtable: ", distance.bit_length())
        if distance == 0:
            print("routingtable:guardando en el bucket:--- ", 0)
            return 0  # Si son iguales, no se asigna a ningún bucket
        bucket_idx = distance.bit_length() - 1
        print("routingtable:guardando en el bucket: ", bucket_idx)
        return bucket_idx

    def find_closest_nodes(self, target_id: int, count: int) -> List[Node]:
        """
        Using BinarySearch we look for the nearest node on our k-buckets
        """
        bucket_index = self.get_bucket_index(target_id)
        closest_nodes = []
        print("closest bucket index to {target_id}: ", bucket_index)
        for node in self.buckets[bucket_index].get_nodes():
            closest_nodes.append(node)
        if len(closest_nodes) == count:
            return closest_nodes
        queue = []
        if bucket_index - 1 >= 0:
            queue.append(bucket_index - 1)
        if bucket_index + 1 >= 0:
            queue.append(bucket_index + 1)
        while len(queue) > 0 and len(closest_nodes) < count:
            bucket = queue.pop(0)
            for node in self.buckets[bucket].get_nodes():
                closest_nodes.append(node)
            if bucket - 1 >= 0 and bucket not in queue:
                queue.append(bucket - 1)
            if bucket + 1 >= 0 and bucket not in queue:
                queue.append(bucket + 1)
        return closest_nodes

    def get_node_count(self):
        count = 0
        for bucket in self.buckets:
            for _ in bucket.get_nodes():
                count += 1
        return count

    def get_all_nodes(self):
        nodes = []
        for bucket in self.buckets:
            for node in bucket.get_nodes():
                nodes.append(node)
        return nodes
