from nanolog_parser.src.message_parsers.parser_interface import IMessageToJsonConverter, IMessageTypeIdentifier
from nanolog_parser.src.messages import *
from nanolog_parser.src.parsing_utils import ParseException
import re


class MessageTypeIdentifier(IMessageTypeIdentifier):

    def __init__(self):
        self.common_pattern = {
            ('network_processed', 'confirm_ack'): ConfirmAckMessageReceived,
            ('network_processed', 'confirm_req'): ConfirmReqMessageReceived,
            ('network_processed', 'publish'): PublishMessageReceived,
            ('network_processed', 'keepalive'): KeepAliveMessageReceived,
            ('network_processed', 'asc_pull_ack'): AscPullAckMessageReceived,
            ('network_processed', 'asc_pull_req'): AscPullReqMessageReceived,
            ('network_processed', 'node_id_handshake'): NodeIdHandshakeMessageReceived,
            ('network_processed', 'telemetry_req'): TelemetryReqMessageReceived,
            ('network_processed', 'telemetry_ack'): TelemetryAckMessageReceived,
            ('network_processed', 'bulk_pull_account'): BulkPullAccountMessageReceived,
            ('network_processed', 'frontier_req'): FrontierReqMessageReceived,
            ('network_processed', 'bulk_push'): BulkPushMessageReceived,

            ('channel_sent', 'confirm_ack'): ConfirmAckMessageSent,
            ('channel_sent', 'confirm_req'): ConfirmReqMessageSent,
            ('channel_sent', 'publish'): PublishMessageSent,
            ('channel_sent', 'keepalive'): KeepAliveMessageSent,
            ('channel_sent', 'asc_pull_ack'): AscPullAckMessageSent,
            ('channel_sent', 'asc_pull_req'): AscPullReqMessageSent,
            ('channel_sent', 'node_id_handshake'): NodeIdHandshakeMessageSent,
            ('channel_sent', 'telemetry_req'): TelemetryReqMessageSent,
            ('channel_sent', 'telemetry_ack'): TelemetryAckMessageSent,
            ('channel_sent', 'bulk_pull_account'): BulkPullAccountMessageSent,
            ('channel_sent', 'frontier_req'): FrontierReqMessageSent,
            ('channel_sent', 'bulk_push'): BulkPushMessageSent,

            ("election", "generate_vote_normal"): ElectionGenerateVoteNormalMessage,
            ("election", "generate_vote_final"): ElectionGenerateVoteFinalMessage,
            ("election", "election_confirmed"): ElectionConfirmedMessage,
            ("election", "broadcast_vote"): ElectionBroadcastVoteMessage,
            ("confirmation_solicitor", "broadcast"): BroadcastMessage,
            ("confirmation_solicitor", "flush"): FlushMessage,
            ("blockprocessor", "block_processed"): BlockProcessedMessage,
            ("active_transactions", "active_started"): ActiveStartedMessage,
            ("active_transactions", "active_stopped"): ActiveStoppedMessage,
            ("node", "process_confirmed"): ProcessConfirmedMessage,
            ("vote_processor", "vote_processed"): VoteProcessedMessage,
            ("frontier_req_server", "sending_frontier"): SendingFrontierMessage,
            ("bulk_pull_account_client", "requesting_pending"): BulkPullAccountPendingMessage,
            ("election_scheduler", "block_activated"): SchedulerBlockActivatedMessage,
            ("vote_generator", "candidate_processed"): VoteGeneratorCandidateProcessedMessage,
            ("channel_send_result", "*"): ChannelSendResultMessage
        }
        self.content_based_identifiers = {
            "blockprocessor": {
                re.compile(r"Processed \d+ block"): ProcessedBlocksMessage,
                re.compile(r"in processing queue"): BlocksInQueueMessage,
            },
            "vote_processor": {
                re.compile(r"Processed \d+ votes "): VotesProcessedMessage,
            }
        }

    def identify_message_type(self, json: dict) -> type:
        log_process = json.get('log_process')
        log_event = json.get('log_event')

        # First, try direct lookup using log_process and log_event
        direct_result = self.common_pattern.get((log_process, log_event))
        if direct_result is not None:
            return direct_result

        # If direct lookup fails, check for wildcard patterns
        for (pattern_process, pattern_event), message_type in self.common_pattern.items():
            if '*' in (pattern_process, pattern_event):  # Check if either part has a wildcard
                if (pattern_process == log_process or pattern_process == '*') and \
                        (pattern_event == log_event or pattern_event == '*'):
                    return message_type

        # If both direct lookup and wildcard check fail, then check based on content
        if log_event is None:
            content = json.get('content')
            content_identifiers = self.content_based_identifiers.get(
                log_process, {})
            for pattern, message_class in content_identifiers.items():
                if pattern.search(content):
                    return message_class

        return UnknownMessage  # Return default if no match found


class MessageToJsonConverter(IMessageToJsonConverter):

    def __init__(self):
        self.attribute_parser = MessageAttributeParser()

    def convert_to_json(self, line: str, file_name: str = None) -> dict:
        attributes = self.attribute_parser.parse_base_attributes(
            line, file_name)
        message_attributes = self.attribute_parser.extract_attributes(
            attributes["content"])
        attributes.update(message_attributes)
        return attributes


class MessageAttributeParser:

    @staticmethod
    def _add_quotes_to_keys_fast(logline):
        words = logline.split(': ')
        for i in range(len(words) - 1):
            last_space = words[i].rfind(' ')
            if last_space != -1:
                pre_space = words[i][:last_space]
                post_space = words[i][last_space+1:]
                words[i] = f'{pre_space} "{post_space}"'
            else:
                words[i] = f'"{words[i]}"'
        return ':'.join(words)

    @staticmethod
    def extract_attributes(logline):
        # The function _add_quotes_to_keys_fast() is assumed to be part of the
        # MessageAttributeParser class and is used to make the logline JSON compatible.
        json_compatible = MessageAttributeParser._add_quotes_to_keys_fast(
            logline)

        try:
            # Attempt to load as JSON.
            json_object = json.loads("{" + json_compatible + "}")
        except json.JSONDecodeError:
            # If loading failed, create a new dictionary with a key named "content" and
            # the original string as the value.
            json_object = {"content": json_compatible}

        return json_object

    @staticmethod
    def parse_base_attributes(line, file_name=None):

        response = {}

        # define the pattern for the basic attributes
        pattern = r'\[(.+?)\] \[(.+?)\] \[(.+?)\]'
        base_attributes_match = re.match(pattern, line)

        # check if the base attributes match the pattern
        if base_attributes_match:
            response["log_timestamp"] = base_attributes_match.group(1)
            response["log_process"] = base_attributes_match.group(2)
            response["log_level"] = base_attributes_match.group(3).split(
                '"')[0].strip()
            response["log_file"] = file_name
        else:
            raise ValueError(f"Wrong log format for base attributes: {line}")

        # try to match the log_event
        event_pattern = r'\[{}\] \"(.+?)\"'.format(response["log_level"])
        log_event_match = re.search(event_pattern, line)

        # if log_event exists, assign it and cut it from the remainder, otherwise leave it None
        if log_event_match:
            response["log_event"] = log_event_match.group(1)
            response["content"] = str(line[base_attributes_match.end():log_event_match.start()] +
                                      line[log_event_match.end():]).strip()
        else:
            response["log_event"] = None
            response["content"] = str(
                line[base_attributes_match.end():]).strip()

        return response


class LogParser():

    def __init__(self, json_converter: IMessageToJsonConverter, message_identifier: IMessageTypeIdentifier):
        self.json_converter = json_converter
        self.message_identifier = message_identifier

    def parse_to_json(self, line: str, file_name: str = None) -> BaseMessage:
        try:
            json_log = self.json_converter.convert_to_json(line)
            json_log["log_file"] = file_name
            return json_log
        except Exception as exc:
            raise ParseException(message=exc) from exc

    def parse_log(self, line: str, file_name: str = None) -> BaseMessage:
        try:
            json_log = self.parse_to_json(line, file_name)
            message_class = self.message_identifier.identify_message_type(
                json_log)
            return message_class(json_log)
        except Exception as exc:
            raise ParseException(message=exc) from exc
