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

        ConfirmReqMessageReceived: ConfirmReqMessageMapper,
        ConfirmReqMessageSent: ChannelConfirmReqkMapper,
        ConfirmReqMessageDropped: ChannelConfirmReqkMapper,

        PublishMessageReceived: PublishMessageMapper,
        PublishMessageSent: ChannelPublishMessageMapper,
        PublishMessageDropped: ChannelPublishMessageMapper,

        KeepAliveMessageReceived: KeepAliveMessageMapper,
        KeepAliveMessageSent: ChannelKeepAliveMessageMapper,
        KeepAliveMessageDropped: ChannelKeepAliveMessageMapper,

        AscPullAckMessageReceived: AscPullAckMessageMapper,
        AscPullAckMessageSent: ChannelAscPullAckMessageMapper,
        AscPullAckMessageDropped: ChannelAscPullAckMessageMapper,

        AscPullReqMessageReceived: AscPullReqMessageMapper,
        AscPullReqMessageSent: ChannelAscPullReqMessageMapper,
        AscPullReqMessageDropped: ChannelAscPullReqMessageMapper,

        NodeIdHandshakeMessageReceived: NodeIdHandshakeMessageMapper,
        NodeIdHandshakeMessageSent: ChannelNodeIdHandshakeMessageMapper,
        NodeIdHandshakeMessageDropped: ChannelNodeIdHandshakeMessageMapper,

        TelemetryReqMessageReceived: TelemetryReqMessageeMapper,
        TelemetryReqMessageSent: ChannelTelemetryReqMessageeMapper,
        TelemetryReqMessageDropped: ChannelTelemetryReqMessageeMapper,

        VoteProcessedMessage: VoteProcessedMessageMapper,
        SendingFrontierMessage: SendingFrontierMessageMapper,

        UnknownMessage: UnknownMessageMapper,
    }

    @classmethod
    def get_mapper_for_message(cls, message):
        mapper_class = cls.registry.get(type(message), UnknownMessageMapper)
        return mapper_class(message)
