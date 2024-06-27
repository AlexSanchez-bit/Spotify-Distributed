from collections import defaultdict
from typing import Any, List, Tuple
from KBucket import Node, K
from KademliaRpcNode import KademliaRpcNode
import threading
import time

import threading

alpha = 3


class KademliaNode(KademliaRpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port)
        self.data_store = defaultdict(str)

    def refresh_buckets(self):
        while True:
            print("revising buckets")
            for bucket in self.routing_table.buckets:
                for node in bucket.get_nodes():
                    print("buckets", node)
            time.sleep(600)

    def start(self):
        self.network.start()
        refresh_thread = threading.Thread(target=self.refresh_buckets)
        refresh_thread.start()
        print("starting bucket refresh subroutine")
