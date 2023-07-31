from .base_parser import BaseParser
from src.message_parsers.message_parser import IMessageParser
from src.messages import *


class LogParser():

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

        # content is parsed dynamically (converted to json)
        message_configurations = {
            ChannelConfirmAck: channel_sent("confirm_ack"),
            ConfirmAckMessage: network("confirm_ack"),
            ConfirmReqMessage: network("confirm_req"),
            PublishMessage: network("publish"),
            KeepAliveMessage: network("keepalive"),
            AscPullAckMessage: network("asc_pull_ack"),
            AscPullReqMessage: network("asc_pull_req"),
            ElectionGenerateVoteNormalMessage: common_pattern("election", "generate_vote_normal"),
            ElectionGenerateVoteFinalMessage: common_pattern("election", "generate_vote_final"),
            BroadcastMessage: common_pattern("confirmation_solicitor", "broadcast"),
            FlushMessage: common_pattern("confirmation_solicitor", "flush"),
            BlockProcessedMessage: common_pattern("blockprocessor", "block_processed"),
            ActiveStartedMessage: common_pattern("active_transactions", "active_started"),
            ActiveStoppedMessage: common_pattern("active_transactions", "active_stopped"),
            ProcessConfirmedMessage: common_pattern(
                "node", "process_confirmed")
        }

        # content is parsed individually in the respective class
        static_message_configurations = {
            ProcessedBlocksMessage: r'\[(blockprocessor)\] .* Processed \d+ blocks',
            BlocksInQueueMessage: r'\[(blockprocessor)\] .* in processing queue',
        }

        # used as fallback if message_configurations is defined
        fallback_message_configurations = {
            BlockProcessorMessage: r'\[blockprocessor\] .*',
            NetworkMessage: r'\[network\] .*',
            UnknownMessage: r''
        }

        for message_class, message_regex in message_configurations.items():
            self.parser.register_parser(
                message_class, message_regex)

        for message_class, message_regex in static_message_configurations.items():
            self.parser.register_parser(
                message_class, message_regex,  parse_dynamic=False)

        for message_class, message_regex in fallback_message_configurations.items():
            self.parser.register_parser(
                message_class, message_regex,  parse_dynamic=False)

    def parse_log(self, line, file_name=None):

        try:
            result = self.parser.parse_message(line, file_name)
        except Exception as ex:
            raise ParseException(
                f'Error parsing {self.__class__.__name__} message') from ex
        return result

    def get_default_message_type(self):
        return 'unknown'
