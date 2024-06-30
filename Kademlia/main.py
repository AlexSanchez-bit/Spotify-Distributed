import sys
import time
from KademliaNode import KademliaNode
from KBucket import Node, sha1_hash
import os


def main():
    ip = os.getenv("NODE_IP", "127.0.0.1")  # Valor por defecto es 127.0.0.1
    port = int(os.getenv("NODE_PORT", "8081"))
    node = KademliaNode(ip, port)
    node.start()
    if len(sys.argv) == 3:
        ip = sys.argv[1]
        port = int(sys.argv[2])
        node.ping(Node(ip, port))
    while True:
        file_direction = input("file direction: ")
        key = sha1_hash(file_direction)
        node.node_lookup(key)
        time.sleep(1)


main()
