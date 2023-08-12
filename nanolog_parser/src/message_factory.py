from nanolog_parser.src.message_parsers import *
from nanolog_parser.src.message_parsers.log_parser import MessageToJsonConverter, MessageTypeIdentifier


json_converter = MessageToJsonConverter()
identifier = MessageTypeIdentifier()


class MessageFactory:

    @staticmethod
    def create_message(line, filename=None):

        log_parser = LogParser(json_converter, identifier)

        return log_parser.parse_log(line,
                                    filename)  # pass filename to parse_message
