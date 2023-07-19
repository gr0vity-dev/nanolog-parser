from src.messages.message_active_transactions import ActiveStartedMessage, ActiveStoppedMessage
import re


class ActiveTransactionsParser:
    MESSAGE_TYPES = {
        'active_started': ActiveStartedMessage,
        'active_stopped': ActiveStoppedMessage,
    }

    def parse_message(self, line, filename=None):
        regex = r'("active_started"|"active_stopped")'
        message_type_match = re.search(regex, line)

        if not message_type_match:
            raise ValueError(f"No message type found. Wrong log format.")

        message_type = message_type_match.group(1).replace('"', '')
        message_class = self.MESSAGE_TYPES.get(message_type)

        if message_class is None:
            raise ValueError(f"Unknown message type {message_type}.")

        return message_class(filename).parse(line)