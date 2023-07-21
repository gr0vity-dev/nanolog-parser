import re
from abc import ABC, abstractmethod


class BaseParser(ABC):

    MESSAGE_TYPES = {}

    @abstractmethod
    def get_message_type_regex(self):
        pass

    @abstractmethod
    def get_message_type_patterns(self):
        pass

    @abstractmethod
    def get_default_message_type(self):
        pass

    def determine_message_type(self, line):
        for message_type, pattern in self.get_message_type_patterns().items():
            if re.search(pattern, line):
                return message_type
        return self.get_default_message_type()

    def parse_message(self, line, filename=None):
        message_type = self.determine_message_type(line)
        message_class = self.MESSAGE_TYPES.get(message_type)
        if message_class is None:
            raise ValueError(f"Unknown message type {message_type}.")
        return message_class(filename).parse(line)
