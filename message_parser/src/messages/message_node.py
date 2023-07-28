from .base_message import Message
from .mixins import BaseAttributesMixin
import re
import json


class NodeMessage(Message):

    def parse_common(self, remainder):
        pass

    def parse_specific(self, remainder):
        pass


class NodeProcessConfirmedMessage(NodeMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse_specific(self, line):
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
        self.sideband = block_json.get('sideband')
