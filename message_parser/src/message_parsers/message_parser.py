from collections import defaultdict
from src.messages import MessageAttributeParser

from abc import ABC, abstractmethod
import re


class IMessageParser(ABC):

    @abstractmethod
    def register_parser(self, message_class, pattern, parse_dynamic=True):
        pass

    @abstractmethod
    def parse_message(self, line, message_type, file_name=None):
        pass


class MessageParser(IMessageParser):

    def __init__(self):
        self.parsers = {}

    def register_parser(self, message_class, pattern, parse_dynamic=True):
        self.parsers[pattern] = (message_class, parse_dynamic)

    def _determine_message_type(self, line):
        for pattern, (message_class, parse_dynamic) in self.parsers.items():
            if re.search(pattern, line):
                return message_class, parse_dynamic

    def parse_message(self, line, file_name=None):
        message_class, parse_dynamic = self._determine_message_type(line)
        attributes = MessageAttributeParser.parse_base_attributes(
            line, file_name)

        if parse_dynamic:
            message_attributes = MessageAttributeParser.extract_attributes(
                attributes["content"])
            attributes.pop("content")
            attributes.update(message_attributes)

        return message_class(attributes) if message_class else None
