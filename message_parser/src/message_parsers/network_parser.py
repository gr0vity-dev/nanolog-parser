from src.messages.message_network import ConfirmAckMessage, ConfirmReqMessage, PublishMessage, KeepAliveMessage, AscPullAckMessage, AscPullReqMessage
import re


class NetworkParser:
    MESSAGE_TYPES = {
        'confirm_ack': ConfirmAckMessage,
        'confirm_req': ConfirmReqMessage,
        'publish': PublishMessage,
        'keepalive': KeepAliveMessage,
        'asc_pull_ack': AscPullAckMessage,
        'asc_pull_req': AscPullReqMessage
        # add more network message types here
    }

    @staticmethod
    def register_message_type(key, message_type):
        NetworkParser.MESSAGE_TYPES[key] = message_type

    def parse_message(self, line):
        regex = r'"message_received" message={ header={ type="(.*?)",'
        message_type_match = re.search(regex, line)

        if not message_type_match:
            raise ValueError(f"No message type found. Wrong log format.")

        message_type = message_type_match.group(1)
        message_class = self.MESSAGE_TYPES.get(message_type)

        if message_class is None:
            raise ValueError(f"Unknown message type {message_type}.")

        return message_class().parse(line)