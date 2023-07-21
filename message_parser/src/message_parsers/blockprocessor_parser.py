from .base_parser import BaseParser
from src.messages import *
import re


class BlockprocessorParser(BaseParser):

    MESSAGE_TYPES = {
        'block_processed': BlockProcessedMessage,
        'processed_blocks': ProcessedBlocksMessage,
        'blocks_in_queue': BlocksInQueueMessage,
        'default': BlockProcessorMessage
    }

    def get_message_type_regex(self):
        return r'\[(blockprocessor)\] \[\w+\]'

    def parse_message(self, line, filename=None):
        message_type = self.determine_message_type(line)
        message_class = self.MESSAGE_TYPES.get(message_type)

        if message_class is None:
            raise ValueError(f"Unknown message type {message_type}.")

        return message_class(filename).parse(line)

    def determine_message_type(self, line):
        # define regex patterns for each type
        patterns = {
            'block_processed':
            r'\[blockprocessor\] \[trace\] "(block_processed)"',
            'processed_blocks': r'Processed \d+ blocks',
            'blocks_in_queue': r'in processing queue',
        }

        for message_type, pattern in patterns.items():
            if re.search(pattern, line):
                return message_type

        # default
        return "default"