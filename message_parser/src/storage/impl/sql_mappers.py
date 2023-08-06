from src.storage.impl.sql_mixins import SqlBaseMapperMixin, MessageMixin
from src.storage.impl.sql_mapper_interface import IMapper
from src.storage.impl.sql_relation import RelationsMixin


class BlockProcessedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"result", "hash", "forced"}
    sql_relation = {"block"}


class ProcessedBlocksMessageMapper(SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"processed_blocks", "forced_blocks", "process_time"}


class BlocksInQueueMessageMapper(SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"blocks_in_queue", "state_blocks", "forced_blocks"}


class ActiveStartedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {'root', 'behaviour', 'state', 'confirmed',
                   'winner', 'tally_amount', 'final_tally_amount', }
    sql_relation = {"election.blocks", "election.votes", "election.tally"}


class ActiveStoppedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {'root', 'behaviour', 'state', 'confirmed',
                   'winner', 'tally_amount', 'final_tally_amount', }
    sql_relation = {"election.blocks", "election.votes", "election.tally"}


class BroadcastMessageMapper(SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {'channel', 'hash'}


class ConfirmAckMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"vote_type", "vote_count"}
    sql_relation = {"message.header", "message.vote"}


class ChannelConfirmAckMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"vote_type", "vote_count"}
    sql_relation = {"message.header", "message.vote", "channel"}


class ConfirmReqMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"root_count"}
    sql_relation = {"message.header", "message.roots"}


class ChannelConfirmReqkMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"root_count", "message.block"}
    sql_relation = {"message.header", "message.roots", "channel"}


class ElectionGenerateVoteNormalMessageMapper(SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"root", "hash"}


class ElectionGenerateVoteFinalMessageMapper(SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"root", "hash"}


class UnknownMessageMapper(SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"content"}


class PublishMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"message.header", "message.block"}


class ChannelPublishMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"message.header", "message.block", "channel"}


class KeepAliveMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"message.header", "message.peers"}


class ChannelKeepAliveMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"message.header", "message.peers", "channel"}


class AscPullAckMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    SqlBaseMapperMixin.set_type("message.id", str)
    sql_columns = {"message.id", "message.type"}
    sql_relation = {"message.header", "message.blocks"}


class ChannelAscPullAckMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    SqlBaseMapperMixin.set_type("message.id", str)
    sql_columns = {"message.id", "message.type"}
    sql_relation = {"message.header", "message.blocks", "channel"}


class AscPullReqMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    SqlBaseMapperMixin.set_type("message.id", str)
    sql_columns = {"message.id", "message.start_type",
                   "message.start", "message.count"}
    sql_relation = {"message.header"}


class ChannelAscPullReqMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    SqlBaseMapperMixin.set_type("message.id", str)
    sql_columns = {"message.id", "message.start_type",
                   "message.start", "message.count"}
    sql_relation = {"message.header", "channel"}


class NodeIdHandshakeMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"message.header", "message.query", "message.response"}


class ChannelNodeIdHandshakeMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"message.header", "message.query",
                    "message.response", "channel"}


class TelemetryReqMessageeMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"message.header"}


class ChannelTelemetryReqMessageeMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"message.header", "channel"}


class FlushMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"root_count", "channel"}
    sql_relation = {"confirm_req.header",
                    "confirm_req.roots", "confirm_req.block"}


class ProcessConfirmedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_relation = {"block"}


class VoteProcessedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"status"}
    sql_relation = {"vote"}


class SendingFrontierMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin, IMapper):
    sql_columns = {"account", "frontier", "socket.remote_endpoint"}
