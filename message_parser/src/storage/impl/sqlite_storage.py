import sqlite3
from abc import ABC, abstractmethod
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
        self.repository.insert_data(table_name, data)

    @staticmethod
    def get_mapper_for_message(message):

        if isinstance(message, BlockProcessorMessage):
            return BlockProcessorMessageMapper(message)
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

    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def create_table_if_not_exists(self, table_name, table_schema):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f'{column} {dtype}' for column, dtype in table_schema])}
        );
        """
        self.cursor.execute(query)
        self.conn.commit()

    def insert_data(self, table_name, data):
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = tuple(data.values())
        query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        """
        self.cursor.execute(query, values)
        self.conn.commit()

    def get_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()
