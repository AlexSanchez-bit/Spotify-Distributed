from Kademlia.utils.RpcType import RpcType
from Kademlia.utils.MessageType import MessageType
from RaftConsensus.utils.RaftRpcs import RaftRpc


def Rpc(rpctype: RpcType | RaftRpc, messageType: MessageType, payload):
    return (rpctype, messageType, payload)
