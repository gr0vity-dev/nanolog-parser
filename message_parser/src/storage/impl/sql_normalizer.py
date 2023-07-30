class SQLDataNormalizer:

    @staticmethod
    def adjust_max_timestamp(timestamp):
        if timestamp == 18446744073709551615:
            return -1
        return timestamp

    @staticmethod
    def normalize_block_fields(blocks):

        def edit_block(block):
            block.pop("sideband")
            if "sideband" in block:
                block["sideband"] = str(block["sideband"])
            block["work"] = str(block["work"])

        if isinstance(blocks, list):
            for block in blocks:
                edit_block(block)
        else:
            edit_block(blocks)

    @staticmethod
    def normalize_vote_fields(votes):

        def edit_vote(vote):
            vote["timestamp"] = SQLDataNormalizer.adjust_max_timestamp(
                vote["timestamp"])
            if vote["timestamp"] == -1:
                vote["vote_type"] = "final"
            else:
                vote["vote_type"] = "normal"

        if isinstance(votes, list):
            for vote in votes:
                edit_vote(vote)
        else:
            edit_vote(votes)

    @staticmethod
    def normalize_vote(votes):

        def edit_vote(vote, hash_value):
            vote["hash"] = hash_value
            vote.pop("hashes")
            vote["timestamp"] = SQLDataNormalizer.adjust_max_timestamp(
                vote["timestamp"])
            if vote["timestamp"] == -1:
                vote["vote_type"] = "final"
            else:
                vote["vote_type"] = "normal"
            return vote

        normalized_votes = []
        if isinstance(votes, list):
            for vote in votes:
                for hash_value in vote['hashes']:
                    normalized_vote = edit_vote(vote.copy(), hash_value)
                    normalized_votes.append(normalized_vote)
        else:
            for hash_value in votes['hashes']:
                normalized_vote = edit_vote(votes.copy(), hash_value)
                normalized_votes.append(normalized_vote)

        return normalized_votes

    @staticmethod
    def normalize_channel(channels):

        def edit_channel(channel):
            channel.pop("socket")

        if isinstance(channels, list):
            for channel in channels:
                edit_channel(channel)
        else:
            edit_channel(channels)

        return channels
