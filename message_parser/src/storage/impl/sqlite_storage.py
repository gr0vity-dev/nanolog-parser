import sqlite3
from src.messages import *
from src.storage.impl.sql_mappers import *
from src.storage.impl.sql_relation import HashableMapper, LinkMapper


class SQLiteStorage:

    def __init__(self, db_name):
        self.repository = SQLiteRepository(db_name)
        base_mappers = {
            'HashableMapper': HashableMapper,
            'LinkMapper': LinkMapper
        }
        self.mapper_registry = {
            **base_mappers,
            'BlockProcessedMessage': BlockProcessedMessageMapper,
            'ProcessedBlocksMessage': ProcessedBlocksMessageMapper,
            'BlocksInQueueMessage': BlocksInQueueMessageMapper,
            'BroadcastMessage': BroadcastMessageMapper,
            'FlushMessage': FlushMessageMapper,
            'GenerateVoteNormalMessage': GenerateVoteNormalMessageMapper,
            'GenerateVoteFinalMessage': GenerateVoteFinalMessageMapper,
            'NodeProcessConfirmedMessage': NodeProcessConfirmedMessageMapper,
            'ActiveStartedMessage': ActiveStartedMessageMapper,
            'ActiveStoppedMessage': ActiveStoppedMessageMapper,
            'ConfirmAckMessage': ConfirmAckMessageMapper,
            'ConfirmReqMessage': ConfirmReqMessageMapper,
            'PublishMessage': PublishMessageMapper,
            'KeepAliveMessage': KeepAliveMessageMapper,
            'AscPullAckMessage': ASCPullAckMessageMapper,
            'AscPullReqMessage': ASCPullReqMessageMapper,
            'NetworkMessage': NetworkMessageMapper,
            'UnknownMessage': UnknownMessageMapper,
            # ... add any other mappings here ...
        }

    def get_mapper_for_message(self, message):
        mapper_class = self.mapper_registry.get(
            type(message).__name__, MessageMapper)
        return mapper_class(message)

    def store_message(self, message):
        mapper = self.get_mapper_for_message(message)
        table_name, schema, data = mapper.handle_table()
        self.repository.create_table_if_not_exists(table_name, schema)
        mapper.sql_id = self.repository.insert_data(table_name, data)

        # handle related entities
        id_mappings = {}
        for related_mapper in mapper.get_related_entities():
            related_table_name, related_schema, related_data = related_mapper.handle_table(
            )

            self.repository.create_table_if_not_exists(related_table_name,
                                                       related_schema)
            if related_mapper.is_dependent():
                mapped_data = related_mapper.convert_related_ids(id_mappings)
                related_mapper.sql_id = self.repository.insert_data(
                    related_table_name, mapped_data)
            else:
                related_mapper.sql_id = self.repository.insert_data(
                    related_table_name, related_data)
                id_mappings[related_mapper.to_key()] = related_mapper.sql_id

        return mapper.sql_id


class SQLiteRepository:

    def __init__(self, db_name, batch_size=100):  # Add a batch_size parameter
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA journal_mode=WAL")
        self.batch_size = batch_size
        self.batch_count = 0

    def create_index(self, table_name, column_name):
        cursor = self.conn.cursor()
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS index_{table_name}_{column_name} ON {table_name}({column_name});"
        )
        self.conn.commit()

    def create_table_if_not_exists(self, table_name, table_schema):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f'{column} {dtype}' for column, dtype in table_schema])}
        );
        """
        self.cursor.execute(query)
        self.maybe_commit()

    def insert_data(self, table_name, data):
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = tuple(data.values())
        query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        """
        self.cursor.execute(query, values)
        self.maybe_commit()
        return self.cursor.lastrowid  # return the last inserted id

    def maybe_commit(self):
        # Increment the batch counter
        self.batch_count += 1
        # If we've reached the batch size, commit and reset the counter
        if self.batch_count >= self.batch_size:
            self.conn.commit()
            self.batch_count = 0

    def close(self):
        # When we're done, make sure to commit any outstanding changes
        self.conn.commit()
        self.conn.close()
