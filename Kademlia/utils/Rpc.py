from utils.RpcType import RpcType
from utils.MessageType import MessageType


def Rpc(rpctype: RpcType, messageType: MessageType, payload):
    return (rpctype, messageType, payload)
