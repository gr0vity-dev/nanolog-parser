from .base_parser import BaseParser
from src.message_parsers.message_parser import IMessageParser
from src.messages import *


class LogParser(BaseParser):

    def __init__(self, parser: IMessageParser):
        self.parser = parser
        self._configure_parsers()

    def _configure_parsers(self):

        def network(name):
            return r'\[network\] \[trace\] "message_received" message={{ header={{ type="({}?)",'.format(name)

        def channel_sent(name):
            return r'\[channel\] \[trace\] "message_sent" message={{ header={{ type="({}?)",'.format(name)

        def common_pattern(name, event):
            return r'\[{}\] \[\w+\] "{}"'.format(name, event)

        # Define a mapping from message type to class and regex
        self.message_configurations = {
            ChannelConfirmAck: channel_sent("confirm_ack"),
            ConfirmAckMessage: network("confirm_ack"),
            ConfirmReqMessage: network("confirm_req"),
            PublishMessage: network("publish"),
            KeepAliveMessage: network("keepalive"),
            AscPullAckMessage: network("asc_pull_ack"),
            AscPullReqMessage: network("asc_pull_req"),
            NetworkMessage: r'\[network\] .*',
            ElectionGenerateVoteNormalMessage: common_pattern("election", "generate_vote_normal"),
            ElectionGenerateVoteFinalMessage: common_pattern("election", "generate_vote_final"),
            BroadcastMessage: common_pattern("confirmation_solicitor", "broadcast"),
            FlushMessage: common_pattern("confirmation_solicitor", "flush"),
            BlockProcessedMessage: common_pattern("blockprocessor", "block_processed"),
            ProcessedBlocksMessage: r'\[(blockprocessor)\] .* Processed \d+ blocks',
            BlocksInQueueMessage: r'\[(blockprocessor)\] .* in processing queue',
            BlockProcessorMessage: r'\[blockprocessor\] .*',
            ActiveStartedMessage: common_pattern("active_transactions", "active_started"),
            ActiveStoppedMessage: common_pattern("active_transactions", "active_stopped"),
            ProcessConfirmedMessage: common_pattern("node", "process_confirmed"),
            UnknownMessage: r''
        }

    def determine_message_type(self, line):
        for message_class, pattern in self.message_configurations.items():
            if re.search(pattern, line):
                return message_class
        return UnknownMessage

    def parse_log(self, line, file_name=None):
        message_class = self.determine_message_type(line)

        if message_class:
            self._register_parser_for_message_type(line, message_class)

        try:
            result = self.parser.parse_message(line, message_class.__name__,
                                               file_name)
        except Exception as ex:
            raise ParseException(
                f'Error parsing {self.__class__.__name__} message') from ex
        return result

    def get_message_type_patterns(self):
        return {k: v[1]
                for k, v in self.message_configurations.items()
                }  # Build the pattern dict from the tuples

    def get_default_message_type(self):
        return 'unknown'

    def _register_parser_for_message_type(self, line, message_class):
        parse_json = MessageAttributeParser.parse_json_attribute
        parse_string = MessageAttributeParser.parse_attribute

        parser_config = MessageAttributeParser.extract_attributes(line)
        attribute_parsers = {
            parse_json: parser_config["json"],
            parse_string: parser_config["string"]
        }
        self.parser.register_parser(
            message_class.__name__, message_class, attribute_parsers)
