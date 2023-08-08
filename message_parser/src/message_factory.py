# file: message_factory.py
from src.message_parsers import *
# from src.message_parsers.message_parser import MessageParser
from src.message_parsers.log_parser import MessageToJsonConverter, MessageTypeIdentifier
import re


class MessageFactory:

    @staticmethod
    def create_message(line, filename=None):

        json_converter = MessageToJsonConverter()
        identifier = MessageTypeIdentifier()
        log_parser = LogParser(json_converter, identifier)

        return log_parser.parse_log(line,
                                    filename)  # pass filename to parse_message
