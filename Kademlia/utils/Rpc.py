from Kademlia.utils.RpcType import RpcType
from Kademlia.utils.MessageType import MessageType


def Rpc(rpctype: RpcType, messageType: MessageType, payload):
    return (rpctype, messageType, payload)
