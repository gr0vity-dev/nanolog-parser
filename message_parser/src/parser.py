import re
from src.messages.network_messages import ConfirmAckMessage, ConfirmReqMessage, PublishMessage, KeepAliveMessage, AscPullAckMessage, AscPullReqMessage


class Parser:

    def __init__(self):
        self.parsed_messages = []
        self.ignored_lines = 0

    def load_and_parse_file(self, filename):
        with open(filename, 'r') as file:
            for line in file:
                try:
                    message = MessageFactory.create_message(line)
                    self.parsed_messages.append(message)
                except ValueError:
                    self.ignored_lines += 1

    def report(self):
        message_report = {}
        for message in self.parsed_messages:
            message_class_name = message.__class__.__name__
            if message_class_name not in message_report:
                message_report[message_class_name] = 1
            else:
                message_report[message_class_name] += 1

        return {
            "message_report": message_report,
            "ignored_lines": self.ignored_lines
        }


MESSAGE_CLASSES = {
    'confirm_ack': ConfirmAckMessage,
    'confirm_req': ConfirmReqMessage,
    'publish': PublishMessage,
    'keepalive': KeepAliveMessage,
    'asc_pull_ack': AscPullAckMessage,
    'asc_pull_req': AscPullReqMessage
    # add more message types here
}


class MessageFactory:

    @staticmethod
    def create_message(line):
        # Extract the message type from the line
        matches = re.findall(r'type="(.*?)"', line)

        # If no matches were found, return None
        if not matches:
            raise ValueError(f"No type found. Wrong log format")

        message_type = matches[0]

        # Based on the message type, delegate to the right class
        message_class = MESSAGE_CLASSES.get(message_type)
        if message_class is None:
            raise ValueError(f"Unknown message type {message_type}")

        return message_class().parse(line)
