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


class NodeProcessConfirmedMessage(Message, BaseAttributesMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.block_type = None
        self.hash = None
        self.account = None
        self.previous = None
        self.representative = None
        self.balance = None
        self.link = None
        self.signature = None
        self.work = None
        self.sideband = None

    def parse(self, line):
        self.parse_base_attributes(line)
        self.parse_specific(line)
        return self

    def parse_specific(self, line):
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
        self.sideband = block_json.get('sideband')
