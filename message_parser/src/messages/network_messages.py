import re
import json
from .base import Message
from .mixins import BaseAttributesMixin, HeaderMixin


def extract_message(line):
    # Using a regular expression to find the message part
    matches = re.findall(r'message=\{(.*)\}', line)
    match = "{" + matches[0] + "}" if matches else None
    print("match found was:", match)
    return match


def fix_json_keys(string):
    # Replace = with :
    string = re.sub(r'\s*=\s*', ':', string)
    # Replace keys with "key". Lookbehind and lookahead are used to avoid replacing substrings within double quotes.
    string = re.sub(r'(?<=\{|,|\[)\s*([a-zA-Z0-9_]+)\s*(?=:)', r'"\1"', string)
    return string


class NetworkMessage(Message, BaseAttributesMixin, HeaderMixin):

    def __init__(self):
        super().__init__()

    def parse(self, line):
        self.parse_base_attributes(line)
        message_text = extract_message(line)
        message_content = fix_json_keys(message_text)
        message_dict = json.loads(message_content)
        self.parse_header(message_dict['header'])
        self.parse_specific(message_dict)  # Step deferred to subclass
        return self

    def parse_specific(self, message_dict):
        pass  # Can be overridden by subclasses


class ConfirmAckMessage(NetworkMessage):

    def __init__(self):
        super().__init__()
        self.account = None
        self.timestamp = None
        self.hashes = []

    def parse_specific(self,
                       message_dict):  # Overriding method from NetworkMessage
        # Parse the vote details
        self.account = message_dict['vote']['account']
        self.timestamp = self.normalize_timestamp(
            message_dict['vote']['timestamp'])
        self.hashes = message_dict['vote']['hashes']


class ConfirmReqMessage(NetworkMessage):

    def __init__(self):
        super().__init__()
        self.roots = []

    def parse_specific(self,
                       message_dict):  # Overriding method from NetworkMessage
        # Parse the roots
        self.roots = message_dict['roots']


class PublishMessage(NetworkMessage):

    def __init__(self):
        super().__init__()
        self.block_type = None
        self.hash = None
        self.account = None
        self.previous = None
        self.representative = None
        self.balance = None
        self.link = None
        self.signature = None
        self.work = None

    def parse_specific(self, message_dict):
        # Parse the block details
        self.block_type = message_dict['block']['type']
        self.hash = message_dict['block']['hash']
        self.account = message_dict['block']['account']
        self.previous = message_dict['block']['previous']
        self.representative = message_dict['block']['representative']
        self.balance = message_dict['block']['balance']
        self.link = message_dict['block']['link']
        self.signature = message_dict['block']['signature']
        self.work = message_dict['block']['work']


class KeepAliveMessage(NetworkMessage):

    def __init__(self):
        super().__init__()
        self.peers = []

    def parse_specific(self, message_dict):
        # Add the parsed peers list
        self.peers = message_dict["peers"]


class AscPullAckMessage(NetworkMessage):

    def __init__(self):
        super().__init__()
        self.id = None
        self.blocks = []

    def parse_specific(self, message_dict):
        # Parse the id
        self.id = message_dict['id']

        # Parse the blocks
        for block in message_dict['blocks']:
            block_dict = {
                'type': block['type'],
                'hash': block['hash'],
                'account': block['account'],
                'previous': block['previous'],
                'representative': block['representative'],
                'balance': block['balance'],
                'link': block['link'],
                'signature': block['signature'],
                'work': block['work'],
            }
            self.blocks.append(block_dict)


class AscPullReqMessage(NetworkMessage):

    def __init__(self):
        super().__init__()
        self.id = None
        self.start = None
        self.start_type = None
        self.count = None

    def parse_specific(self, message_dict):
        # Parse the additional fields
        self.id = message_dict['id']
        self.start = message_dict['start']
        self.start_type = message_dict['start_type']
        self.count = message_dict['count']
