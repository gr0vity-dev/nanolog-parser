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

        def channel_dropped(name):
            return r'\[channel\] \[trace\] "message_dropped" message={{ header={{ type="({}?)",'.format(name)

        def common_pattern(name, event):
            return r'\[{}\] \[\w+\] "{}"'.format(name, event)

        # content is parsed dynamically (converted to json)
        message_configurations = {
            channel_sent("confirm_ack"): ConfirmAckMessageSent,
            channel_sent("confirm_req"): ConfirmReqMessageSent,
            channel_sent("publish"): ChannelPublishMessage,
            channel_sent("keepalive"): ChannelKeepAliveMessage,
            channel_sent("asc_pull_ack"): ChannelAscPullAckMessage,
            channel_sent("asc_pull_req"): ChannelAscPullReqMessage,
            channel_dropped("confirm_ack"): ConfirmAckMessageDropped,
            channel_dropped("confirm_req"): ConfirmReqMessageDropped,
            channel_dropped("publish"): ChannelPublishMessageDropped,
            channel_dropped("keepalive"): ChannelKeepAliveMessageDropped,
            channel_dropped("asc_pull_ack"): ChannelAscPullAckMessageDropped,
            channel_dropped("asc_pull_req"): ChannelAscPullReqMessageDropped,
            network("confirm_ack"): ConfirmAckMessageReceived,
            network("confirm_req"): ConfirmReqMessage,
            network("publish"): PublishMessage,
            network("keepalive"): KeepAliveMessage,
            network("asc_pull_ack"): AscPullAckMessage,
            network("asc_pull_req"): AscPullReqMessage,
            common_pattern("election", "generate_vote_normal"): ElectionGenerateVoteNormalMessage,
            common_pattern("election", "generate_vote_final"): ElectionGenerateVoteFinalMessage,
            common_pattern("confirmation_solicitor", "broadcast"): BroadcastMessage,
            common_pattern("confirmation_solicitor", "flush"): FlushMessage,
            common_pattern("blockprocessor", "block_processed"): BlockProcessedMessage,
            common_pattern("active_transactions", "active_started"): ActiveStartedMessage,
            common_pattern("active_transactions", "active_stopped"): ActiveStoppedMessage,
            common_pattern("node", "process_confirmed"): ProcessConfirmedMessage,
        }

        # content is parsed individually in the respective class
        static_message_configurations = {
            r'\[(blockprocessor)\] .* Processed \d+ blocks': ProcessedBlocksMessage,
            r'\[(blockprocessor)\] .* in processing queue': BlocksInQueueMessage,
        }

        # used as fallback if message_configurations is defined
        # fallback_message_configurations = {
        #     BlockProcessorMessage: r'\[blockprocessor\] .*',
        #     NetworkMessage: r'\[network\] .*',
        # }

        for message_regex, message_class in message_configurations.items():
            self.parser.register_parser(
                message_class, message_regex, parse_dynamic=True)

        for message_regex, message_class in static_message_configurations.items():
            self.parser.register_parser(
                message_class, message_regex,  parse_dynamic=False)

        # for message_class, message_regex in fallback_message_configurations.items():
        #     self.parser.register_parser(
        #         message_class, message_regex, parse_dynamic=True)

        self.parser.register_parser(UnknownMessage, r'', parse_dynamic=False)

    def parse_log(self, line, file_name=None):

        try:
            result = self.parser.parse_message(line, file_name)
        except Exception as ex:
            raise ParseException(
                f'Error parsing {self.__class__.__name__} message') from ex
        return result

    def get_default_message_type(self):
        return 'unknown'
