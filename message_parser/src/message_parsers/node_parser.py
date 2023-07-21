from .base_parser import BaseParser
from src.messages import NodeProcessConfirmedMessage, UnknownMessage


class NodeParser(BaseParser):
    MESSAGE_TYPES = {
        'process_confirmed': NodeProcessConfirmedMessage,
        'unknown': UnknownMessage,
    }

    def get_message_type_regex(self):
        return r'\[(node)\] \[\w+\]'

    def get_message_type_patterns(self):
        return {"process_confirmed": r'\[trace\] "(\w+)"'}

    def get_default_message_type(self):
        return 'unknown'
