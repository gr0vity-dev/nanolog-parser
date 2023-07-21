from .base_parser import BaseParser
from src.messages import ActiveStartedMessage, ActiveStoppedMessage, UnknownMessage


class ActiveTransactionsParser(BaseParser):

    MESSAGE_TYPES = {
        'active_started': ActiveStartedMessage,
        'active_stopped': ActiveStoppedMessage,
        'unknown': UnknownMessage,
    }

    def get_message_type_regex(self):
        return r'\[(active_transactions)\] \[\w+\]'

    def get_message_type_patterns(self):
        return {
            "active_started": r'\[trace\] "active_started"',
            "active_stopped": r'\[trace\] "active_stopped"',
        }

    def get_default_message_type(self):
        return 'unknown'