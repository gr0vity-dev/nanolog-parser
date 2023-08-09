from abc import ABC, abstractmethod


class SQLDataNormalizer:
    def __init__(self):
        self._normalizers = {
            "blocks": BlockNormalizer(),
            "votes": VoteNormalizer(),
            "channels": ChannelNormalizer(),
            "sidebands": SidebandNormalizer(),
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

        # Normalize main data and any nested relations
        normalized_entries = normalizer.normalize(
            entries) if normalizer else entries

        return normalized_entries, table_name

    # def normalize_sql(self, entity, entries):
    #     table_name = self.pluralize(entity)
    #     normalizer = self._normalizers.get(table_name)
    #     return normalizer.normalize(entries) if normalizer else entries, table_name


class NormalizerInterface(ABC):

    @abstractmethod
    def normalize(self, data):
        pass


class BlockNormalizer(NormalizerInterface):
    def __init__(self):
        self.sideband_normalizer = SidebandNormalizer()

    @staticmethod
    def extract_sideband(block):
        """Extract sideband from the block and return it."""
        return block.pop("sideband", None)

    @staticmethod
    def stringify_work(block):
        block["work"] = str(block["work"])
        return block

    def normalize(self, blocks):
        normalized_data = []

        if not isinstance(blocks, list):
            blocks = [blocks]

        for block in blocks:
            sideband = self.extract_sideband(block)
            sideband = self.sideband_normalizer.normalize(
                sideband)  # Normalize the sideband

            # Ensure to call this AFTER extracting sideband.
            block = self.stringify_work(block)
            normalized_data.append((block, sideband))

        return normalized_data  # Return a list of (block, sideband)

# class BlockNormalizer(NormalizerInterface):
#     @staticmethod
#     def remove_sideband(block):

#         block.pop("sideband", None)
#         return block

#     @staticmethod
#     def map_sideband(block):
#         sideband = block.get("sideband", {})

#         # Initialise with values, because sql doesn't treat 2 records with
#         # NULL values as identical resulting in duplicate entries of teh same block
#         block["sideband_height"] = 0
#         block["sideband_subtype"] = ''

#         # Map sideband_height
#         if "height" in sideband:
#             block["sideband_height"] = sideband["height"]

#         # Map sideband_subtype
#         details = sideband.get("details", {})
#         if details.get("is_receive", False):
#             block["sideband_subtype"] = "receive"
#         elif details.get("is_send", False):
#             block["sideband_subtype"] = "send"
#         elif details.get("is_send") == False and details.get("is_receive") == False:
#             block["sideband_subtype"] = "change"

#         # Remove sideband
#         block.pop("sideband", None)

#         return BlockNormalizer

    # @staticmethod
    # def stringify_work(block):
    #     block["work"] = str(block["work"])
    #     return block

    # def normalize(self, blocks):
    #     if not isinstance(blocks, list):
    #         blocks = [blocks]
    #     for block in blocks:
    #         block = self.remove_sideband(block)
    #         block = self.stringify_work(block)
    #     return blocks


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


class SidebandNormalizer(NormalizerInterface):
    @staticmethod
    def map_details(sideband):
        details = sideband.get("details", {})
        if details.get("is_receive", False):
            sideband["subtype"] = "receive"
        elif details.get("is_send", False):
            sideband["subtype"] = "send"
        elif details.get("is_send") == False and details.get("is_receive") == False:
            sideband["subtype"] = "change"

        sideband.pop("details", None)
        return sideband

    def normalize(self, sidebands):
        if not isinstance(sidebands, list):
            sidebands = [sidebands]

        for sideband in sidebands:
            sideband = self.map_details(sideband)

        return sideband
