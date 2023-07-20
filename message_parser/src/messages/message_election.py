# src/messages/message_generate_vote.py
from .base_message import Message
import re


class GenerateVoteNormalMessage(Message):

    def parse_specific(self, line):
        regex = r'root="(?P<root>[^"]+)", hash="(?P<hash>[^"]+)"'
        match = re.search(regex, line)

        if match:
            self.root = match.group('root')
            self.hash = match.group('hash')


class GenerateVoteFinalMessage(Message):

    def parse_specific(self, line):
        regex = r'root="(?P<root>[^"]+)", hash="(?P<hash>[^"]+)"'
        match = re.search(regex, line)

        if match:
            self.root = match.group('root')
            self.hash = match.group('hash')
