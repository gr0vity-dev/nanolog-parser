import sqlite3
from src.messages import *
from src.storage.impl.sql_message_mapper import *


class SQLiteStorage:

    def __init__(self, db_name):
        self.repository = SQLiteRepository(db_name)

    def store_message(self, message):
        mapper = self.get_mapper_for_message(message)
        table_name = mapper.get_table_name()
        schema = mapper.get_table_schema()
        self.repository.create_table_if_not_exists(table_name, schema)
        data = mapper.to_dict()
        parent_id = self.repository.insert_data(table_name, data)
        for related_data, related_mapper in mapper.get_related_entities():
            related_data[mapper.parent_entity_name + '_id'] = parent_id
            related_table_name = related_mapper.get_table_name()
            related_schema = related_mapper.get_table_schema()
            self.repository.create_table_if_not_exists(related_table_name,
                                                       related_schema)
            self.repository.insert_data(related_table_name, related_data)

    @staticmethod
    def get_mapper_for_message(message):

        if isinstance(message, BlockProcessorMessage):
            return BlockProcessorMessageMapper(message)
        elif isinstance(message, BroadcastMessage):
            return BroadcastMessageMapper(message)
        elif isinstance(message, NodeProcessConfirmedMessage):
            return NodeProcessConfirmedMessageMapper(message)
        elif isinstance(message, ActiveStartedMessage):
            return ActiveStartedMessageMapper(message)
        elif isinstance(message, ActiveStoppedMessage):
            return ActiveStoppedMessageMapper(message)
        elif isinstance(message, ConfirmAckMessage):
            return ConfirmAckMessageMapper(message)
        elif isinstance(message, ConfirmReqMessage):
            return ConfirmReqMessageMapper(message)
        elif isinstance(message, PublishMessage):
            return PublishMessageMapper(message)
        elif isinstance(message, KeepAliveMessage):
            return KeepAliveMessageMapper(message)
        elif isinstance(message, AscPullAckMessage):
            return ASCPullAckMessageMapper(message)
        elif isinstance(message, AscPullReqMessage):
            return ASCPullReqMessageMapper(message)
        elif isinstance(message, NetworkMessage):
            return NetworkMessageMapper(message)
        else:
            return MessageMapper(message)


class SQLiteRepository:

    def __init__(self, db_name, batch_size=500):  # Add a batch_size parameter
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA journal_mode=WAL")
        self.batch_size = batch_size
        self.batch_count = 0

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
