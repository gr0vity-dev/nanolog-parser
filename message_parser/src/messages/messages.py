from .base_message import BaseMessage
import re


class ChannelMessage(BaseMessage):
    pass


class ChannelConfirmAck(ChannelMessage):
    pass


class ChannelConfirmReq(ChannelMessage):
    pass


class ChannelPublishMessage(ChannelMessage):
    pass


class ChannelKeepAliveMessage(ChannelMessage):
    pass


class ChannelAscPullAckMessage(ChannelMessage):
    pass


class ChannelAscPullReqMessage(ChannelMessage):
    pass


class ChannelConfirmAckDropped(ChannelMessage):
    pass


class ChannelConfirmReqDropped(ChannelMessage):
    pass


class ChannelPublishMessageDropped(ChannelMessage):
    pass


class ChannelKeepAliveMessageDropped(ChannelMessage):
    pass


class ChannelAscPullAckMessageDropped(ChannelMessage):
    pass


class ChannelAscPullReqMessageDropped(ChannelMessage):
    pass


class UnknownMessage(BaseMessage):
    pass


class ConfirmAckMessage(BaseMessage):

    def post_init(self):
        self.vote_type = "final" if self.message["vote"][
            "timestamp"] == 18446744073709551615 else "normal"


class ConfirmReqMessage(BaseMessage):
    pass


class PublishMessage(BaseMessage):
    pass


class KeepAliveMessage(BaseMessage):
    pass


class AscPullAckMessage(BaseMessage):
    pass


class AscPullReqMessage(BaseMessage):
    pass


class NetworkMessage(BaseMessage):
    pass


class ElectionGenerateVoteNormalMessage(BaseMessage):
    pass


class ElectionGenerateVoteFinalMessage(BaseMessage):
    pass


class BroadcastMessage(BaseMessage):
    pass


class FlushMessage(BaseMessage):
    pass


class BlockProcessedMessage(BaseMessage):
    pass


class ProcessedBlocksMessage(BaseMessage):

    def post_init(self):
        self.processed_blocks, self.forced_blocks, self.process_time = self.extract_block_info(
            self.content)

    def extract_block_info(self, line):
        match = re.search(
            r'Processed (\d+) blocks \((\d+) forced\) in (\d+) milliseconds',
            line)
        if match:
            return int(match.group(1)), int(match.group(2)), int(
                match.group(3))
        else:
            return None, None, None


class BlocksInQueueMessage(BaseMessage):

    def post_init(self):
        self.blocks_in_queue, self.state_blocks, self.forced_blocks = self.extract_block_counts(
            self.content)

    def extract_block_counts(self, line):
        match = re.search(
            r'(\d+) blocks \(\+ (\d+) state blocks\) \(\+ (\d+) forced\)',
            line)
        if match:
            return int(match.group(1)), int(match.group(2)), int(
                match.group(3))
        else:
            return None, None, None


class BlockProcessorMessage(BaseMessage):
    pass


class ActiveStartedMessage(BaseMessage):
    pass


class ActiveStoppedMessage(BaseMessage):
    pass


class ProcessConfirmedMessage(BaseMessage):
    pass
