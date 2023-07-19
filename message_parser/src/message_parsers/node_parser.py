from src.messages.message_node import NodeProcessConfirmedMessage
import re


class NodeParser:
    MESSAGE_TYPES = {'process_confirmed': NodeProcessConfirmedMessage}

    def parse_message(self, line, filename=None):
        regex = r'(process_confirmed)'
        message_type_match = re.search(regex, line)

        if not message_type_match:
            raise ValueError(f"No message type found. Wrong log format.")

        message_type = message_type_match.group(0)
        message_class = self.MESSAGE_TYPES.get(message_type)

        if message_class is None:
            raise ValueError(f"Unknown message type {message_type}.")

        return message_class(filename).parse(line)
