from .base_parser import BaseParser
from src.messages import BroadcastMessage, UnknownMessage


class ConfirmationSolicitorParser(BaseParser):
    MESSAGE_TYPES = {
        'broadcast': BroadcastMessage,
        'unknown': UnknownMessage,
    }

    def get_message_type_regex(self):
        return r'\[(confirmation_solicitor)\] \[\w+\]'

    def get_message_type_patterns(self):
        return {"broadcast": r'\[trace\] "(\w+)"'}

    def get_default_message_type(self):
        return 'unknown'
