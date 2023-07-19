from .base_parser import BaseParser
from src.messages.message_node import NodeProcessConfirmedMessage


class NodeParser(BaseParser):
    MESSAGE_TYPES = {'process_confirmed': NodeProcessConfirmedMessage}

    def get_message_type_regex(self):
        return r'(process_confirmed)'
