from .base_message import Message
from .mixins import BaseAttributesMixin
import re
import json


class BlockProcessorMessage(Message, BaseAttributesMixin):

    def __init__(self, filename=None):
        super().__init__(filename)

    def parse_common(self, remainder):
        pass

    def parse_specific(self, remainder):
        # Extract remainder
        self.content = remainder


class BlockProcessedMessage(BlockProcessorMessage):

    def parse_specific(self, line):
        self.result = self.extract_result(line)
        self.forced = self.extract_forced(line)

        block_json = self.extract_json(line, "block")

        self.block_type = block_json.get('type')
        self.hash = block_json.get('hash')
        self.account = block_json.get('account')
        self.previous = block_json.get('previous')
        self.representative = block_json.get('representative')
        self.balance = block_json.get('balance')
        self.link = block_json.get('link')
        self.signature = block_json.get('signature')
        self.work = str(block_json.get('work'))

    def extract_result(self, line):
        match = re.search(r'result="(.*?)"', line)
        if match:
            return match.group(1)

    def extract_forced(self, line):
        match = re.search(r'forced=(\w+)', line)
        if match:
            return match.group(1) == 'true'


class ProcessedBlocksMessage(BlockProcessorMessage):

    def parse_specific(self, line):
        self.processed_blocks = self.extract_processed_blocks(line)
        self.forced_blocks = self.extract_forced_blocks(line)
        self.process_time = self.extract_process_time(line)

    def extract_processed_blocks(self, line):
        match = re.search(r'Processed (\d+) blocks', line)
        if match:
            return int(match.group(1))

    def extract_forced_blocks(self, line):
        match = re.search(r'\((\d+) forced\)', line)
        if match:
            return int(match.group(1))

    def extract_process_time(self, line):
        match = re.search(r'in (\d+)milliseconds', line)
        if match:
            return int(match.group(1))


class BlocksInQueueMessage(BlockProcessorMessage):

    def parse_specific(self, line):
        self.blocks_in_queue = self.extract_blocks_in_queue(line)
        self.state_blocks = self.extract_state_blocks(line)
        self.forced_blocks = self.extract_forced_blocks(line)

    def extract_blocks_in_queue(self, line):
        match = re.search(r'(\d+) blocks', line)
        if match:
            return int(match.group(1))

    def extract_state_blocks(self, line):
        match = re.search(r'\[\+ (\d+) state blocks\]', line)
        if match:
            return int(match.group(1))

    def extract_forced_blocks(self, line):
        match = re.search(r'\[\+ (\d+) forced\]', line)
        if match:
            return int(match.group(1))
