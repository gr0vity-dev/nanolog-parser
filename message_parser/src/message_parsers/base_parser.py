import re
from abc import ABC, abstractmethod


class BaseParser(ABC):

    MESSAGE_TYPES = {}

    @abstractmethod
    def get_message_type_regex(self):
        pass

    def parse_message(self, line, filename=None):
        regex = self.get_message_type_regex()
        message_type_match = re.search(regex, line)

        # if not message_type_match:
        #     raise ValueError(f"No message type found. Wrong log format.")

        message_type = message_type_match.group(
            1) if message_type_match else "unknown"
        message_class = self.MESSAGE_TYPES.get(message_type)

        if message_class is None:
            raise ValueError(f"Unknown message type {message_type}.")

        return message_class(filename).parse(line)
