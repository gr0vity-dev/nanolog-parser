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
            channel_sent("publish"): PublishMessageSent,
            channel_sent("keepalive"): KeepAliveMessageSent,
            channel_sent("asc_pull_ack"): AscPullAckMessageSent,
            channel_sent("asc_pull_req"): AscPullReqMessageSent,
            channel_sent("node_id_handshake"): NodeIdHandshakeMessageSent,
            channel_sent("telemetry_req"): TelemetryReqMessageSent,
            channel_sent("telemetry_ack"): TelemetryAckMessageSent,
            channel_sent("bulk_pull_account"): BulkPullAccountMessageSent,
            channel_sent("frontier_req"): FrontierReqMessageSent,
            channel_sent("bulk_push"): BulkPushMessageSent,
            channel_dropped("confirm_ack"): ConfirmAckMessageDropped,
            channel_dropped("confirm_req"): ConfirmReqMessageDropped,
            channel_dropped("publish"): PublishMessageDropped,
            channel_dropped("keepalive"): KeepAliveMessageDropped,
            channel_dropped("asc_pull_ack"): AscPullAckMessageDropped,
            channel_dropped("asc_pull_req"): AscPullReqMessageDropped,
            channel_dropped("node_id_handshake"): NodeIdHandshakeMessageDropped,
            channel_dropped("telemetry_req"): TelemetryReqMessageDropped,
            channel_dropped("telemetry_ack"): TelemetryAckMessageDropped,
            channel_dropped("bulk_pull_account"): BulkPullAccountMessageDropped,
            channel_dropped("frontier_req"): FrontierReqMessageDropped,
            channel_dropped("bulk_push"): BulkPushMessageDropped,
            network("confirm_ack"): ConfirmAckMessageReceived,
            network("confirm_req"): ConfirmReqMessageReceived,
            network("publish"): PublishMessageReceived,
            network("keepalive"): KeepAliveMessageReceived,
            network("asc_pull_ack"): AscPullAckMessageReceived,
            network("asc_pull_req"): AscPullReqMessageReceived,
            network("node_id_handshake"): NodeIdHandshakeMessageReceived,
            network("telemetry_req"): TelemetryReqMessageReceived,
            network("telemetry_ack"): TelemetryAckMessageReceived,
            network("bulk_pull_account"): BulkPullAccountMessageReceived,
            network("frontier_req"): FrontierReqMessageReceived,
            network("bulk_push"): BulkPushMessageReceived,
            common_pattern("election", "generate_vote_normal"): ElectionGenerateVoteNormalMessage,
            common_pattern("election", "generate_vote_final"): ElectionGenerateVoteFinalMessage,
            common_pattern("election", "election_confirmed"): ElectionConfirmedlMessage,
            common_pattern("confirmation_solicitor", "broadcast"): BroadcastMessage,
            common_pattern("confirmation_solicitor", "flush"): FlushMessage,
            common_pattern("blockprocessor", "block_processed"): BlockProcessedMessage,
            common_pattern("active_transactions", "active_started"): ActiveStartedMessage,
            common_pattern("active_transactions", "active_stopped"): ActiveStoppedMessage,
            common_pattern("node", "process_confirmed"): ProcessConfirmedMessage,
            common_pattern("vote_processor", "vote_processed"): VoteProcessedMessage,
            common_pattern("frontier_req_server", "sending_frontier"): SendingFrontierMessage,
            common_pattern("bulk_pull_account_client", "requesting_pending"): BulkPullAccountPendingMessage,
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
