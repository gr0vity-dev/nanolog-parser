from .base_parser import BaseParser
from src.messages import NodeProcessConfirmedMessage


class NodeParser(BaseParser):
    MESSAGE_TYPES = {'process_confirmed': NodeProcessConfirmedMessage}

    def get_message_type_regex(self):
        return r'\[node\] \[\w+\] "(\w+)"'
