from .base import Message
from .mixins import BaseAttributesMixin
import re


class BlockProcessorMessage(Message, BaseAttributesMixin):

    def __init__(self):
        super().__init__()
        self.result = None
        self.forced = None
        self.block = None

    def parse(self, line):
        self.parse_base_attributes(line)
        self.parse_specific(line)
        return self

    def parse_specific(self, line):
        self.result = self.extract_result(line)
        self.forced = self.extract_forced(line)
        self.block = self.extract_block(line)

    def extract_result(self, line):
        match = re.search(r'result="(.*?)"', line)
        if match:
            return match.group(1)

    def extract_forced(self, line):
        match = re.search(r'forced=(\w+)', line)
        if match:
            return match.group(1) == 'true'

    def extract_block(self, line):
        match = re.search(r'block={(.*)}, forced', line)
        if match:
            return match.group(1)
