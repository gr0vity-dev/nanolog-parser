from .base_parser import BaseParser
from src.messages.message_blockprocessor import BlockProcessorMessage
import re


class BlockprocessorParser(BaseParser):

    MESSAGE_TYPES = {'block_processed': BlockProcessorMessage}

    def get_message_type_regex(self):
        return r'(block_processed)'