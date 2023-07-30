from collections import defaultdict
from src.messages import MessageAttributeParser

from abc import ABC, abstractmethod


class IMessageParser(ABC):

    @abstractmethod
    def register_parser(self, message_type, message_class, key, parser):
        pass

    @abstractmethod
    def parse_message(self, line, message_type, file_name=None):
        pass


class MessageParser(IMessageParser):

    def __init__(self):
        self.parsers = defaultdict(list)

    def register_parser(self, message_type, message_class, parser_dict):
        self.message_class = message_class
        for parser, keys in parser_dict.items():
            for key in keys:
                self.parsers[message_type].append((key, parser))

    def parse_message(self, line, message_type, file_name=None):
        attributes = MessageAttributeParser.parse_base_attributes(
            line, file_name)
        parser_applied = False

        for key, parser in self.parsers.get(message_type, []):
            attributes[key] = parser(attributes.get("content"), key)
            parser_applied = True

        if parser_applied:
            attributes.pop("content")

        return self.message_class(attributes)