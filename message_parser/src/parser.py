from src.message_factory import MessageFactory
from src.parsing_utils import ParseException


class Parser:

    def __init__(self):
        self.parsed_messages = []
        self.ignored_lines = 0

    def load_and_parse_file(self, filename):
        with open(filename, 'r') as file:
            for line in file:
                try:
                    message = MessageFactory.create_message(
                        line,
                        filename.replace("node_spd_", "").replace(".log", ""))
                    self.parsed_messages.append(message)
                except ParseException as exc:
                    print(exc)
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