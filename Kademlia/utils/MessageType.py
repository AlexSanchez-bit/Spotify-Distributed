from enum import Enum


class MessageType(Enum):
    Abort = "Abort"
    Request = "Request"
    Response = "Response"
