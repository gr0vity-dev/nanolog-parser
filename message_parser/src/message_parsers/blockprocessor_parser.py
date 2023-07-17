from src.messages.blockprocessor_messages import BlockProcessorMessage
import re


class BlockprocessorParser:
    MESSAGE_TYPES = {'block_processed': BlockProcessorMessage}

    @staticmethod
    def register_message_type(key, message_type):
        BlockprocessorParser.MESSAGE_TYPES[key] = message_type

    def parse_message(self, line):
        regex = r'(block_processed)'
        message_type_match = re.search(regex, line)

        if not message_type_match:
            raise ValueError(f"No message type found. Wrong log format.")

        message_type = message_type_match.group(0)
        message_class = self.MESSAGE_TYPES.get(message_type)

        if message_class is None:
            raise ValueError(f"Unknown message type {message_type}.")

        return message_class().parse(line)