from .base_message import Message
from .mixins import BaseAttributesMixin
import re
import json


def extract_block(line):
    # Using a regular expression to find the message part
    matches = re.findall(r'block=\{(.*)\}', line)
    match = "{" + matches[0] + "}" if matches else None
    return match


def fix_json_keys(string):
    # Replace = with :
    string = re.sub(r'\s*=\s*', ':', string)
    # Replace keys with "key". Lookbehind and lookahead are used to avoid replacing substrings within double quotes.
    string = re.sub(r'(?<=\{|,|\[)\s*([a-zA-Z0-9_]+)\s*(?=:)', r'"\1"', string)
    return string


class BlockProcessorMessage(Message, BaseAttributesMixin):

    def __init__(self, filename=None):
        super().__init__(filename)

    def parse_specific(self, line):
        raise NotImplementedError()


class BlockProcessedMessage(BlockProcessorMessage):

    def parse_specific(self, line):
        self.result = self.extract_result(line)
        self.forced = self.extract_forced(line)

        block_text = extract_block(line)
        block_content = fix_json_keys(block_text)
        block_json = json.loads(block_content)

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
