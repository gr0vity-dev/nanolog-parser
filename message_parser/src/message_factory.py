# file: message_factory.py
from src.message_parsers import *
import re


class MessageFactory:
    PARSERS = {
        'network': NetworkParser,
        'blockprocessor': BlockprocessorParser,
        'node': NodeParser,
        'active_transactions': ActiveTransactionsParser,
        'confirmation_solicitor': ConfirmationSolicitorParser,
        'election': ElectionParser,
        'unknown': UnknownParser,
    }

    @staticmethod
    def get_parser(log_source):
        parser_class = MessageFactory.PARSERS.get(log_source)

        if parser_class is None:
            raise ValueError(f"Unknown log source {log_source}")

        return parser_class()

    @staticmethod
    def create_message(line, filename=None):
        log_sources = '|'.join(MessageFactory.PARSERS.keys())
        log_source_match = re.search(r'\[(' + log_sources + ')]', line)

        #default to UnknownParser if no other Parser was found
        log_source = log_source_match.group(
            1) if log_source_match else 'unknown'

        parser = MessageFactory.get_parser(log_source)
        return parser.parse_message(line,
                                    filename)  # pass filename to parse_message
