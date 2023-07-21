from .base_parser import BaseParser
from src.messages import *


class NetworkParser(BaseParser):
    MESSAGE_TYPES = {
        'confirm_ack': ConfirmAckMessage,
        'confirm_req': ConfirmReqMessage,
        'publish': PublishMessage,
        'keepalive': KeepAliveMessage,
        'asc_pull_ack': AscPullAckMessage,
        'asc_pull_req': AscPullReqMessage,
        'default': NetworkMessage
    }

    def get_message_type_regex(self):
        return r'"message_received" message={ header={ type="(.*?)",'

    def get_message_type_patterns(self):
        return {
            'confirm_ack': r'confirm_ack',
            'confirm_req': r'confirm_req',
            'publish': r'publish',
            'keepalive': r'keepalive',
            'asc_pull_ack': r'asc_pull_ack',
            'asc_pull_req': r'asc_pull_req'
        }

    def get_default_message_type(self):
        return 'default'
