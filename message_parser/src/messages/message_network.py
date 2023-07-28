from .base_message import Message
from .mixins import BaseAttributesMixin, HeaderMixin


class NetworkMessage(Message, BaseAttributesMixin, HeaderMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set_message_dict(self, message):
        self.message = message

    def parse_common(self, line):  # Overridden method
        self.message_dict = self.extract_json(line, "message")
        self.parse_header(self.message_dict['header'])

    def parse_specific(self, line):  # Overridden method
        self.content = line


class ConfirmAckMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account = None
        self.timestamp = None
        self.hashes = []
        self.hash_count = None
        self.vote_type = None

    def parse_specific(self, _):
        self.account = self.message_dict['vote']['account']
        self.timestamp = self.normalize_timestamp(
            self.message_dict['vote']['timestamp'])
        self.hashes = self.message_dict['vote']['hashes']
        self.hash_count = len(self.hashes)
        self.vote_type = "final" if self.timestamp == -1 else "normal"


class ConfirmReqMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.roots = []
        self.root_count = None

    def parse_specific(self, _):
        self.roots = self.message_dict['roots']
        self.root_count = len(self.roots)


class PublishMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse_specific(self, _):
        self.block_type = self.message_dict['block']['type']
        self.hash = self.message_dict['block']['hash']
        self.account = self.message_dict['block']['account']
        self.previous = self.message_dict['block']['previous']
        self.representative = self.message_dict['block']['representative']
        self.balance = self.message_dict['block']['balance']
        self.link = self.message_dict['block']['link']
        self.signature = self.message_dict['block']['signature']
        self.work = self.message_dict['block']['work']


class KeepAliveMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.peers = []

    def parse_specific(self, _):
        self.peers = self.message_dict["peers"]


class AscPullAckMessage(NetworkMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blocks = []

    def parse_specific(self, _):
        self.id = str(self.message_dict['id'])

        # Parse the blocks
        for block in self.message_dict['blocks']:
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

    def parse_specific(self, _):
        self.id = str(self.message_dict['id'])
        self.start = self.message_dict['start']
        self.start_type = self.message_dict['start_type']
        self.count = self.message_dict['count']
