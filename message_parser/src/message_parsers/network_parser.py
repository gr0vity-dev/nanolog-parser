from .base_parser import BaseParser
from src.messages import ConfirmAckMessage, ConfirmReqMessage, PublishMessage, KeepAliveMessage, AscPullAckMessage, AscPullReqMessage


class NetworkParser(BaseParser):
    MESSAGE_TYPES = {
        'confirm_ack': ConfirmAckMessage,
        'confirm_req': ConfirmReqMessage,
        'publish': PublishMessage,
        'keepalive': KeepAliveMessage,
        'asc_pull_ack': AscPullAckMessage,
        'asc_pull_req': AscPullReqMessage
        # add more network message types here
    }

    def get_message_type_regex(self):
        return r'"message_received" message={ header={ type="(.*?)",'
