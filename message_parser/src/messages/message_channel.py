from . import ConfirmAckMessage
from .base_message import Message


class ChannelMessage():
    pass


class ConfirmAckMessageSent(ChannelMessage):

    def __init__(self, message_dict):
        self.class_name = self.__class__.__name__
        self.action = "message_sent"
        self.content = message_dict


class ConfirmAckMessageDropped(ChannelMessage):

    def __init__(self, message_dict):
        self.class_name = self.__class__.__name__
        self.action = "message_dropped"
        self.content = message_dict