from abc import ABC, abstractmethod


class SQLDataNormalizer:
    def __init__(self):
        self._normalizers = {
            "blocks": BlockNormalizer(),
            "votes": VoteNormalizer(),
            "channels": ChannelNormalizer(),
        }

    def singularize(self, entity: str):
        if entity.endswith('ies'):
            return entity[:-3] + 'y'
        elif entity.endswith('s') and not entity.endswith('ss'):
            return entity[:-1]
        return entity

    def pluralize(self, entity: str):
        if entity.endswith('y'):
            return entity[:-1] + 'ies'
        elif entity.endswith('s'):
            return entity
        else:
            return entity + 's'

    def normalize_sql(self, entity, entries):
        table_name = self.pluralize(entity)
        normalizer = self._normalizers.get(table_name)
        return normalizer.normalize(entries) if normalizer else entries, table_name


class NormalizerInterface(ABC):

    @abstractmethod
    def normalize(self, data):
        pass


class BlockNormalizer(NormalizerInterface):
    @staticmethod
    def remove_sideband(block):
        if "sideband" in block:
            block.pop("sideband")
        return block

    @staticmethod
    def stringify_work(block):
        block["work"] = str(block["work"])
        return block

    def normalize(self, blocks):
        if not isinstance(blocks, list):
            blocks = [blocks]
        for block in blocks:
            block = self.remove_sideband(block)
            block = self.stringify_work(block)
        return blocks


class VoteNormalizer(NormalizerInterface):
    @staticmethod
    def adjust_max_timestamp(timestamp):
        if timestamp == 18446744073709551615:
            return -1
        return timestamp

    @staticmethod
    def update_hash(vote, hash_value):
        vote["hash"] = hash_value
        return vote

    @staticmethod
    def remove_hashes(vote):
        if "hashes" in vote:
            vote.pop("hashes")
        return vote

    @staticmethod
    def adjust_timestamp(vote):
        vote["timestamp"] = VoteNormalizer.adjust_max_timestamp(
            vote["timestamp"])
        return vote

    @staticmethod
    def set_vote_type(vote):
        vote["vote_type"] = "final" if vote["timestamp"] == -1 else "normal"
        return vote

    @staticmethod
    def set_vote_time(vote):
        if "time" in vote:
            vote.pop("time")
        return vote
        # vote["time"] = vote["time"] if "time" in vote else 0
        # return vote

    def normalize(self, votes):
        if not isinstance(votes, list):
            votes = [votes]

        normalized_votes = [
            self.normalize_individual_vote(vote, hash_value)
            for vote in votes
            for hash_value in (vote['hashes'] if "hashes" in vote else [vote["hash"]])
        ]

        return normalized_votes

    def normalize_individual_vote(self, vote, hash_value):
        vote = self.update_hash(vote.copy(), hash_value)
        vote = self.remove_hashes(vote)
        vote = self.adjust_timestamp(vote)
        vote = self.set_vote_type(vote)
        vote = self.set_vote_time(vote)
        return vote


class ChannelNormalizer(NormalizerInterface):
    @staticmethod
    def remove_socket(channel):
        channel.pop("socket")
        return channel

    def normalize(self, channels):
        if not isinstance(channels, list):
            channels = [channels]

        for channel in channels:
            channel = self.remove_socket(channel)

        return channels
