from enum import Enum


class RpcType(Enum):
    Ping = "Ping"
    FindNode = "FindNode"
    FindValue = "FindValue"
    Store = "Store"
