from .base_parser import BaseParser
from src.messages import BroadcastMessage, FlushMessage, UnknownMessage


class ConfirmationSolicitorParser(BaseParser):
    MESSAGE_TYPES = {
        'broadcast': BroadcastMessage,
        'flush': FlushMessage,
        'unknown': UnknownMessage,
    }

    def get_message_type_regex(self):
        return r'\[(confirmation_solicitor)\] \[\w+\]'

    def get_message_type_patterns(self):
        return {
            "broadcast": r'\[trace\] "broadcast"',
            "flush": r'\[trace\] "flush"'
        }

    def get_default_message_type(self):
        return 'unknown'
