class SQLDataNormalizer:

    @staticmethod
    def normalize_sql(table, entries):
        switcher = {
            "blocks": SQLDataNormalizer.normalize_block,
            "votes": SQLDataNormalizer.normalize_vote,
            "channels": SQLDataNormalizer.normalize_channel,
        }

        normalize_function = switcher.get(table)

        if normalize_function:
            return normalize_function(entries)
        return entries

    @staticmethod
    def adjust_max_timestamp(timestamp):
        if timestamp == 18446744073709551615:
            return -1
        return timestamp

    @staticmethod
    def normalize_block_fields(blocks):

        def edit_block(block):
            if "sideband" in block:
                # block["sideband"] = str(block["sideband"])
                block.pop("sideband")
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
            if "hashes" in vote:
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
                if "hashes" in vote:
                    for hash_value in vote['hashes']:
                        normalized_vote = edit_vote(vote.copy(), hash_value)
                        normalized_votes.append(normalized_vote)
                else:
                    normalized_vote = edit_vote(vote, vote["hash"])
                    normalized_votes.append(vote)
        else:
            if "hashes" in votes:
                for hash_value in votes['hashes']:
                    normalized_vote = edit_vote(votes.copy(), hash_value)
                    normalized_votes.append(normalized_vote)
            else:
                normalized_vote = edit_vote(votes, votes["hash"])
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

    @staticmethod
    def normalize_block(blocks):

        def edit_block(block):
            if "sideband" in block:
                # block["sideband"] = str(block["sideband"])
                block.pop("sideband")
            block["work"] = str(block["work"])

        if isinstance(blocks, list):
            for block in blocks:
                edit_block(block)
        else:
            edit_block(blocks)

        return blocks
