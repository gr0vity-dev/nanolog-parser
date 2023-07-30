from .base_parser import BaseParser
from src.message_parsers.message_parser import IMessageParser
from src.messages import *


class LogParser(BaseParser):

    def __init__(self, parser: IMessageParser):
        self.parser = parser
        self._configure_parsers()

    def _configure_parsers(self):
        # Define message parsing configurations here
        # For example, for 'confirm_ack' messages:
        self.parser.register_parser("channel_sent", {
            MessageAttributeParser.parse_json_attribute:
            ["message", "channel"]
        })
        # Repeat for other message types...

    def parse_log(self, line, file_name=None):
        message_type = self.determine_message_type(line)
        message_dict = self.parser.parse_message(line, message_type, file_name)
        return self.MESSAGE_TYPES[message_type](message_dict)

    def get_message_type_patterns(self):
        network_regex = r'\[network\] \[trace\] "message_received" message={{ header={{ type="({}?)",'
        channel_sent_regex = r'\[channel\] \[trace\] "message_sent" message={{ header={{ type="({}?)",'
        channel_dropped_regex = r'\[channel\] .* message={{ header={{ type="({}?)",'

        return {
            #network
            'confirm_ack': network_regex.format("confirm_ack"),
            'channel_sent': channel_sent_regex.format("confirm_ack"),
            'confirm_req': network_regex.format("confirm_req"),
            'publish': network_regex.format("publish"),
            'keepalive': network_regex.format("keepalive"),
            'asc_pull_ack': network_regex.format("asc_pull_ack"),
            'asc_pull_req': network_regex.format("asc_pull_req"),
            'network_msg': r'\[network\] .*',
            #election
            "election_generate_vote_normal":
            r'\[(election)\] \[\w+\] "generate_vote_normal"',
            "election_generate_vote_final":
            r'\[(election)\] \[\w+\] "generate_vote_final"',
            #confirmation solicitor
            "broadcast": r'\[(confirmation_solicitor)\] \[\w+\] "broadcast"',
            "flush": r'\[(confirmation_solicitor)\] \[\w+\] "flush"',
            #block processor
            'block_processed':
            r'\[blockprocessor\] \[trace\] "block_processed"',
            'processed_blocks':
            r'\[(blockprocessor)\] .* Processed \d+ blocks',
            'blocks_in_queue': r'\[(blockprocessor)\] .* in processing queue',
            'block_processor': r'\[blockprocessor\] .*',
            "active_started":
            #active transactions
            r'\[active_transactions\] \[\w+\] "active_started"',
            "active_stopped":
            r'\[active_transactions\] \[\w+\] "active_stopped"',
            #node
            "process_confirmed": r'\[(node)\] \[\w+\] "(\w+)"',
            "unknown": r''
        }

    def get_default_message_type(self):
        return 'unknown'

    ACTIVE_TRANSACTION_MESSAGE_TYPES = {
        'active_started': ActiveStartedMessage,
        'active_stopped': ActiveStoppedMessage,
    }

    NODE_MESSAGE_TYPES = {
        'process_confirmed': NodeProcessConfirmedMessage,
    }

    BLOCKPROCESSOR_MESSAGE_TYPES = {
        'block_processed': BlockProcessedMessage,
        'processed_blocks': ProcessedBlocksMessage,
        'blocks_in_queue': BlocksInQueueMessage,
        'block_processor': BlockProcessorMessage
    }

    CONFIRMATION_SOLICITOR_MESSAGE_TYPES = {
        'broadcast': BroadcastMessage,
        'flush': FlushMessage,
    }

    ELECTION_MESSAGE_TYPES = {
        'election_generate_vote_normal': GenerateVoteNormalMessage,
        'election_generate_vote_final': GenerateVoteFinalMessage,
    }

    NETWORK_MESSAGE_TYPES = {
        'confirm_ack': ConfirmAckMessage,
        'confirm_req': ConfirmReqMessage,
        'publish': PublishMessage,
        'keepalive': KeepAliveMessage,
        'asc_pull_ack': AscPullAckMessage,
        'asc_pull_req': AscPullReqMessage,
        'network_msg': NetworkMessage
    }

    CHANNEL_MESSAGE_TYPES = {
        'channel_sent': ConfirmAckMessageSent,
        'confirm_ack_dropped': ConfirmAckMessageDropped
    }

    MESSAGE_TYPES = {
        **CHANNEL_MESSAGE_TYPES,
        **NETWORK_MESSAGE_TYPES,
        **ACTIVE_TRANSACTION_MESSAGE_TYPES,
        **NODE_MESSAGE_TYPES,
        **BLOCKPROCESSOR_MESSAGE_TYPES,
        **CONFIRMATION_SOLICITOR_MESSAGE_TYPES,
        **ELECTION_MESSAGE_TYPES, 'unknown': UnknownMessage
    }
