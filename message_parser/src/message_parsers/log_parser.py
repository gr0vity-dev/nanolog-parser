from .base_parser import BaseParser
from src.message_parsers.message_parser import IMessageParser
from src.messages import *


class LogParser(BaseParser):

    def __init__(self, parser: IMessageParser):
        self.parser = parser
        self._configure_parsers()

    def _configure_parsers(self):
        # Define a mapping from message type to class
        self.message_configurations = {
            "channel_confirm_ack": ChannelConfirmAck,
            "network_confirm_ack": ConfirmAckMessage,
            "network_confirm_req": ConfirmReqMessage,
            "network_publish": PublishMessage,
            "network_keepalive": KeepAliveMessage,
            "network_asc_pull_ack": AscPullAckMessage,
            "network_asc_pull_req": AscPullReqMessage,
            "network_msg": NetworkMessage,
            "election_generate_vote_normal": ElectionGenerateVoteNormalMessage,
            "election_generate_vote_final": ElectionGenerateVoteFinalMessage,
            "broadcast": BroadcastMessage,
            "flush": FlushMessage,
            "block_processed": BlockProcessedMessage,
            "processed_blocks": ProcessedBlocksMessage,
            "blocks_in_queue": BlocksInQueueMessage,
            "block_processor": BlockProcessorMessage,
            "active_started": ActiveStartedMessage,
            "active_stopped": ActiveStoppedMessage,
            "process_confirmed": ProcessConfirmedMessage,
            "unknown": UnknownMessage,
        }

    def parse_log(self, line, file_name=None):
        message_type = self.determine_message_type(line)
        message_class = self.message_configurations.get(message_type)

        if message_class:
            self._register_parser_for_message_type(line, message_type,
                                                   message_class)

        try:
            result = self.parser.parse_message(line, message_type, file_name)
        except Exception as ex:
            raise ParseException(
                f'Error parsing {self.__class__.__name__} message') from ex
        return result

    def _register_parser_for_message_type(self, line, message_type,
                                          message_class):
        parse_json = MessageAttributeParser.parse_json_attribute
        parse_string = MessageAttributeParser.parse_attribute

        parser_config = MessageAttributeParser.extract_attributes(line)
        attribute_parsers = {
            parse_json: parser_config["json"],
            parse_string: parser_config["string"]
        }
        self.parser.register_parser(message_type, message_class,
                                    attribute_parsers)

    def get_message_type_patterns(self):
        network_regex = r'\[network\] \[trace\] "message_received" message={{ header={{ type="({}?)",'
        channel_sent_regex = r'\[channel\] \[trace\] "message_sent" message={{ header={{ type="({}?)",'
        channel_dropped_regex = r'\[channel\] .* message={{ header={{ type="({}?)",'

        return {
            #network
            'network_confirm_ack': network_regex.format("confirm_ack"),
            'channel_confirm_ack': channel_sent_regex.format("confirm_ack"),
            'network_confirm_req': network_regex.format("confirm_req"),
            'network_publish': network_regex.format("publish"),
            'network_keepalive': network_regex.format("keepalive"),
            'network_asc_pull_ack': network_regex.format("asc_pull_ack"),
            'network_asc_pull_req': network_regex.format("asc_pull_req"),
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
