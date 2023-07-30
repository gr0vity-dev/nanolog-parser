# file: message_factory.py
from src.message_parsers import *
from src.message_parsers.message_parser import MessageParser
import re


class MessageFactory:

    @staticmethod
    def create_message(line, filename=None):

        parser = MessageParser()
        log_parser = LogParser(parser)

        return log_parser.parse_log(line,
                                    filename)  # pass filename to parse_message
