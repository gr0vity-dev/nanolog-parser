from . import ConfirmAckMessage
from .base_message import Message


class BaseMessage():

    def __init__(self, message_dict):
        self.__dict__.update(message_dict)
        self.class_name = self.__class__.__name__

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        else:
            raise AttributeError("No such attribute: " + name)


class ChannelMessage(BaseMessage):
    pass


class ChannelConfirmAck(ChannelMessage):
    pass
