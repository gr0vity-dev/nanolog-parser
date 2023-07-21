import re
import json
from .base_message import Message
from .mixins import BaseAttributesMixin, HeaderMixin


def extract_message(line):
    # Using a regular expression to find the message part
    matches = re.findall(r'message=\{(.*)\}', line)
    match = "{" + matches[0] + "}" if matches else None
    return match


def fix_json_keys(string):
    # Replace = with :
    string = re.sub(r'\s*=\s*', ':', string)
    # Replace keys with "key". Lookbehind and lookahead are used to avoid replacing substrings within double quotes.
    string = re.sub(r'(?<=\{|,|\[)\s*([a-zA-Z0-9_]+)\s*(?=:)', r'"\1"', string)
    return string


def convert_message_to_json(log_message):
    log_message = extract_message(log_message)
    message_content = fix_json_keys(log_message)
    return json.loads(message_content)


class NetworkMessage(Message, BaseAttributesMixin, HeaderMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse_common(self, line):  # Overridden method
        message_dict = convert_message_to_json(line)
        self.parse_header(message_dict['header'])

    def parse_specific(self, message_dict):  # Overridden method
        self.content = message_dict


class ConfirmAckMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account = None
        self.timestamp = None
        self.hashes = []
        self.hash_count = None
        self.vote_type = None

    def parse_specific(self, line):
        message_dict = convert_message_to_json(line)
        self.account = message_dict['vote']['account']
        self.timestamp = self.normalize_timestamp(
            message_dict['vote']['timestamp'])
        self.hashes = message_dict['vote']['hashes']
        self.hash_count = len(self.hashes)
        self.vote_type = "final" if self.timestamp == -1 else "normal"


class ConfirmReqMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.roots = []
        self.root_count = None

    def parse_specific(self, line):
        message_dict = convert_message_to_json(line)
        self.roots = message_dict['roots']
        self.root_count = len(self.roots)


class PublishMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse_specific(self, line):
        message_dict = convert_message_to_json(line)
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.peers = []

    def parse_specific(self, line):
        message_dict = convert_message_to_json(line)
        self.peers = message_dict["peers"]


class AscPullAckMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blocks = []

    def parse_specific(self, line):
        message_dict = convert_message_to_json(line)
        self.id = str(message_dict['id'])

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
                'work': str(block['work']),
            }
            self.blocks.append(block_dict)


class AscPullReqMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse_specific(self, line):
        message_dict = convert_message_to_json(line)
        self.id = str(message_dict['id'])
        self.start = message_dict['start']
        self.start_type = message_dict['start_type']
        self.count = message_dict['count']
