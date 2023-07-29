# file: message_factory.py
from src.message_parsers import *
import re


class MessageFactory:

    @staticmethod
    def create_message(line, filename=None):

        parser = LogParser()
        return parser.parse_message(line,
                                    filename)  # pass filename to parse_message
