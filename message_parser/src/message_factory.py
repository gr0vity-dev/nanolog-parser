# file: message_factory.py
from src.message_parsers import BlockprocessorParser, NetworkParser
import re


class MessageFactory:
    PARSERS = {
        'network': NetworkParser,
        'blockprocessor': BlockprocessorParser
        # add more parsers here
    }

    @staticmethod
    def get_parser(log_source):
        parser_class = MessageFactory.PARSERS.get(log_source)

        if parser_class is None:
            raise ValueError(f"Unknown log source {log_source}")

        return parser_class()

    @staticmethod
    def create_message(line):
        log_sources = '|'.join(MessageFactory.PARSERS.keys())
        log_source_match = re.search(r'\[(' + log_sources + ')]', line)

        if not log_source_match:
            raise ValueError("No log source found. Wrong log format")

        log_source = log_source_match.group(1)
        parser = MessageFactory.get_parser(log_source)
        return parser.parse_message(line)