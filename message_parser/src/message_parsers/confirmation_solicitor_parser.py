# src/parsers/confirmation_solicitor_parser.py
from .base_parser import BaseParser
from src.messages import BroadcastMessage


class ConfirmationSolicitorParser(BaseParser):
    MESSAGE_TYPES = {
        'broadcast': BroadcastMessage,
    }

    def get_message_type_regex(self):
        return r'\[confirmation_solicitor\] \[trace\] "(\w+)"'
