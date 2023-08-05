from src.messages import *
from src.storage.impl.sql_mappers import *


class MessageMapperRegistry:

    registry = {
        BlockProcessedMessage: BlockProcessedMessageMapper,
        ProcessedBlocksMessage: ProcessedBlocksMessageMapper,
        BlocksInQueueMessage: BlocksInQueueMessageMapper,
        BroadcastMessage: BroadcastMessageMapper,
        FlushMessage: FlushMessageMapper,
        ElectionGenerateVoteNormalMessage: ElectionGenerateVoteNormalMessageMapper,
        ElectionGenerateVoteFinalMessage: ElectionGenerateVoteFinalMessageMapper,
        ProcessConfirmedMessage: ProcessConfirmedMessageMapper,
        ActiveStartedMessage: ActiveStartedMessageMapper,
        ActiveStoppedMessage: ActiveStoppedMessageMapper,
        ConfirmAckMessageReceived: ConfirmAckMessageMapper,
        ConfirmAckMessageSent: ChannelConfirmAckMapper,
        ConfirmAckMessageDropped: ChannelConfirmAckMapper,
        ConfirmReqMessage: ConfirmReqMessageMapper,
        ConfirmReqMessageSent: ChannelConfirmReqkMapper,
        PublishMessage: PublishMessageMapper,
        KeepAliveMessage: KeepAliveMessageMapper,
        AscPullAckMessage: ASCPullAckMessageMapper,
        AscPullReqMessage: ASCPullReqMessageMapper,
        UnknownMessage: UnknownMessageMapper,
    }

    @classmethod
    def get_mapper_for_message(cls, message):
        mapper_class = cls.registry.get(type(message), UnknownMessageMapper)
        return mapper_class(message)
