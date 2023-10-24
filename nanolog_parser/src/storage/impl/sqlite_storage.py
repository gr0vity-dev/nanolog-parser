import sqlite3
from nanolog_parser.src.storage.impl.sql_message_maps import MessageMapperRegistry
import re


class SQLiteStorage:

    def __init__(self, db_name):
        self.repository = SQLiteRepository(db_name, batch_size=25000)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def store_message(self, message):
        mapper = MessageMapperRegistry.get_mapper_for_message(message)
        table_name, schema, data = mapper.handle_table()
        self.repository.create_table_if_not_exists(mapper)
        mapper.sql_id = self.repository.insert_data(table_name, data)

        # handle related entities
        id_mappings = {}
        for related_mapper in mapper.get_related_entities():
            related_table_name, related_schema, related_data = related_mapper.handle_table()

            self.repository.create_table_if_not_exists(related_mapper)
            if related_mapper.is_dependent():
                mapped_data = related_mapper.convert_related_ids(
                    id_mappings, message)
                related_mapper.sql_id = self.repository.insert_data(
                    related_table_name, mapped_data)
            else:
                related_mapper.sql_id = self.repository.insert_data(
                    related_table_name, related_data)
                id_mappings[related_mapper.to_key()] = related_mapper.sql_id

        return mapper.sql_id

    def close(self):
        self.repository.close()


class SQLiteRepository:

    def __init__(self, db_name, batch_size=100):  # Add a batch_size parameter
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA journal_mode=WAL")
        self.batch_size = batch_size
        self.batch_count = 0
        self.created_tables = set()  # In-memory set to track created tables

    def create_table_if_not_exists(self, mapper):
        table_name = mapper.get_table_name()
        if table_name in self.created_tables:
            return

        table_schema = mapper.get_table_schema()
        table_schema = sorted(table_schema, key=lambda col: col[0])

        unique_constraints = mapper.get_unique_constraints()
        indices = mapper.get_indices()

        # Add all columns in unique_constraints into a single UNIQUE clause
        unique_constraints_str = f', UNIQUE ({", ".join(unique_constraints)})' if unique_constraints else ''

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f'{column} {dtype}' for column, dtype in table_schema])}{unique_constraints_str}
        );
        """
        self.cursor.execute(query)
        self.maybe_commit()

        for columns in indices:
            index_name = f"{table_name}_{'_'.join(columns)}_index"
            query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({', '.join(columns)});"
            self.cursor.execute(query)
            self.maybe_commit()

        self.created_tables.add(table_name)

    def insert_data(self, table_name, data):
        while True:
            try:
                return self._attempt_insert(table_name, data)
            except sqlite3.OperationalError as exc:
                missing_column = self._identify_missing_column(str(exc))
                if missing_column:
                    self._add_column_to_table(
                        table_name, missing_column, data[missing_column])
                else:
                    # if we can't identify the column or another error occurs, raise the error
                    raise exc

    def _attempt_insert(self, table_name, data):
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = tuple(data.values())
        query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        """
        try:
            self.cursor.execute(query, values)
            self.maybe_commit()
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:
            select_placeholders = ' AND '.join(
                [f'{column} = ?' for column in data.keys()])
            select_query = f"""
            SELECT sql_id FROM {table_name}
            WHERE {select_placeholders}
            """
            self.cursor.execute(select_query, values)
            row = self.cursor.fetchone()
            return row[0] if row else None

    def _identify_missing_column(self, error_message):
        match = re.search(r'has no column named (\w+)', error_message)
        return match.group(1) if match else None

    def _add_column_to_table(self, table_name, column, value):
        # This gets the sqlite column type based on the provided value (you can expand this method as needed)
        column_type = self._get_sqlite_column_type(value)
        alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column} {column_type}"
        self.cursor.execute(alter_query)
        self.maybe_commit()

    def _get_sqlite_column_type(self, value):
        TYPE_MAPPING = {
            int: 'INTEGER',
            float: 'REAL',
            str: 'TEXT',
            bool: 'INTEGER',
            bytes: 'BLOB',
            type(None): 'TEXT'
        }
        return TYPE_MAPPING.get(type(value), 'TEXT')

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
