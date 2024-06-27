import sys
from KademliaNode import KademliaNode
from KBucket import Node, sha1_hash
import os


def main():
    ip = os.getenv("NODE_IP", "127.0.0.1")  # Valor por defecto es 127.0.0.1
    port = int(os.getenv("NODE_PORT", "8082"))
    node = KademliaNode(ip, port)
    node.start()
    if len(sys.argv) == 3:
        ip = sys.argv[1]
        port = int(sys.argv[2])
        node.ping(Node(ip, port))
        print("heyyy")


main()
