from .base_parser import BaseParser
from src.messages import *


class BlockprocessorParser(BaseParser):

    MESSAGE_TYPES = {
        'block_processed': BlockProcessedMessage,
        'processed_blocks': ProcessedBlocksMessage,
        'blocks_in_queue': BlocksInQueueMessage,
        'default': BlockProcessorMessage
    }

    def get_message_type_regex(self):
        return r'\[(blockprocessor)\] \[\w+\]'

    def get_message_type_patterns(self):
        return {
            'block_processed':
            r'\[blockprocessor\] \[trace\] "block_processed"',
            'processed_blocks': r'Processed \d+ blocks',
            'blocks_in_queue': r'in processing queue',
        }

    def get_default_message_type(self):
        return 'default'
