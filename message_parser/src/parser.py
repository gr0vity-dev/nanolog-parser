from src.message_factory import MessageFactory


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


# # Base parser
# class LogMessageParser:

#     def __init__(self, source):
#         self.source = source

#     def parse_message(self, line):
#         raise NotImplementedError

# # Network parser
# class NetworkLogMessageParser(LogMessageParser):
#     MESSAGE_TYPES = {
#         'confirm_ack': ConfirmAckMessage,
#         'confirm_req': ConfirmReqMessage,
#         'publish': PublishMessage,
#         'keepalive': KeepAliveMessage,
#         'asc_pull_ack': AscPullAckMessage,
#         'asc_pull_req': AscPullReqMessage
#         # add more network message types here
#     }

#     def parse_message(self, line):
#         regex = r'"message_received" message={ header={ type="(.*?)",'
#         message_type_match = re.search(regex, line)

#         if not message_type_match:
#             raise ValueError(
#                 f"No message type found. Wrong log format for {self.source}")

#         message_type = message_type_match.group(1)
#         message_class = self.MESSAGE_TYPES.get(message_type)

#         if message_class is None:
#             raise ValueError(
#                 f"Unknown message type {message_type} for log source {self.source}"
#             )

#         return message_class().parse(line)

# # Blockprocessor parser
# class BlockprocessorLogMessageParser(LogMessageParser):
#     MESSAGE_TYPES = {'block_processed': BlockProcessorMessage}

#     def parse_message(self, line):
#         regex = r'"block_processed"'
#         message_type_match = re.search(regex, line)

#         if not message_type_match:
#             raise ValueError(
#                 f"No message type found. Wrong log format for {self.source}")

#         message_type = message_type_match.group(0)
#         message_class = self.MESSAGE_TYPES.get(message_type)

#         if message_class is None:
#             raise ValueError(
#                 f"Unknown message type {message_type} for log source {self.source}"
#             )

#         return message_class().parse(line)

# # Factory class
# class MessageFactory:

#     PARSERS = {
#         'network': NetworkLogMessageParser,
#         'blockprocessor': BlockprocessorLogMessageParser
#         # add more parsers here
#     }

#     @staticmethod
#     def create_message(line):
#         log_sources = '|'.join(MessageFactory.PARSERS.keys())
#         log_source_match = re.search(r'\[(' + log_sources + ')]', line)

#         if not log_source_match:
#             raise ValueError("No log source found. Wrong log format")

#         log_source = log_source_match.group(1)
#         parser_class = MessageFactory.PARSERS.get(log_source)

#         if parser_class is None:
#             raise ValueError(f"Unknown log source {log_source}")

#         parser = parser_class(log_source)
#         return parser.parse_message(line)
