from nanolog_parser.src.message_factory import MessageFactory
from nanolog_parser.src.parsing_utils import ParseException
import json


class Parser:

    def __init__(self):
        self.parsed_messages = []
        self.ignored_lines = 0

    # def load_and_parse_file(self, filename, remove_prefix=""):
    #     with open(filename, 'r') as file:
    #         # Adjusting filename to suit your earlier logic
    #         filename = filename.replace(remove_prefix,
    #                                     "").replace("node.log",
    #                                                 "").replace(".log", "")

    #         for line in file:
    #             try:
    #                 # Parse the JSON line
    #                 log_entry = json.loads(line)

    #                 # Extract the 'log' value
    #                 # using strip() to remove any leading/trailing whitespace or newline
    #                 log_message = log_entry["log"].strip()

    #                 message = MessageFactory.create_message(
    #                     log_message, filename
    #                 )
    #                 self.parsed_messages.append(message)

    #             except json.JSONDecodeError:
    #                 print(f"Failed to decode JSON from line: {line}")
    #                 self.ignored_lines += 1
    #             except ParseException as exc:
    #                 print(exc)
    #                 self.ignored_lines += 1

    def load_and_parse_file(self, filename, remove_prefix=""):

        with open(filename, 'r') as file:
            # hacky way to put PR names in the log_filename column
            filename = filename.replace(remove_prefix,
                                        "").replace("node.log",
                                                    "").replace(".log", "")
            for line in file:
                try:
                    message = MessageFactory.create_message(
                        line, filename
                    )
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
