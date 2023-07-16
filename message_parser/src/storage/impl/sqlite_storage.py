from ..storage import Storage
import sqlite3
import json


class SQLiteStorage:

    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)

    def _ensure_table_exists(self, message):
        """Ensure the table for this message type exists. If not, create one."""
        table_name = message.class_name.lower()
        column_defs = []

        # Add a column definition for each attribute of the message
        for attr, value in message.__dict__.items():
            if attr == 'hashes' or attr == 'roots':
                value = 'TEXT'  # We will store list as comma-separated text
            elif isinstance(value, int):
                value = 'INTEGER'
            elif isinstance(value, str):
                value = 'TEXT'
            else:
                raise ValueError(
                    f"Unhandled type for attribute {attr}: {type(value)}")
            column_defs.append(f"{attr} {value}")

        column_defs_sql = ', '.join(column_defs)

        self.conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {column_defs_sql}
        );
        """)

    def store_message(self, message):
        self._ensure_table_exists(message)

        # Prepare the data to be inserted
        data = {}
        for attr, value in message.__dict__.items():
            if attr == 'hashes' or attr == 'roots':
                # Convert list to a comma-separated string
                value = ', '.join([json.dumps(item) for item in value])
            data[attr] = value

        columns_sql = ', '.join(data.keys())
        placeholders_sql = ', '.join(['?'] * len(data))
        values = tuple(data.values())

        # Insert the data
        self.conn.execute(
            f"""
        INSERT INTO {message.class_name.lower()} ({columns_sql})
        VALUES ({placeholders_sql});
        """, values)

        self.conn.commit()

    def retrieve_message(self, id, message_type):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {message_type} WHERE id = ?;", (id, ))
        row = cursor.fetchone()

        if row is None:
            return None

        # Convert the timestamp back to integer
        timestamp_index = list(map(lambda x: x[0],
                                   cursor.description)).index('timestamp')
        row = list(row)
        row[timestamp_index] = int(row[timestamp_index])

        return row
