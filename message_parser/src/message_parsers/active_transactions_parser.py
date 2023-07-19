from .base_parser import BaseParser
from src.messages.message_active_transactions import ActiveStartedMessage, ActiveStoppedMessage


class ActiveTransactionsParser(BaseParser):

    MESSAGE_TYPES = {
        'active_started': ActiveStartedMessage,
        'active_stopped': ActiveStoppedMessage,
    }

    def get_message_type_regex(self):
        return r'("active_started"|"active_stopped")'
