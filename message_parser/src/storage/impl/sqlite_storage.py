import sqlite3
from src.storage.impl.sql_message_maps import MessageMapperRegistry


class SQLiteStorage:

    def __init__(self, db_name):
        self.repository = SQLiteRepository(db_name)

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

    def create_table_if_not_exists(self, mapper):
        table_name = mapper.get_table_name()
        table_schema = mapper.get_table_schema()
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

    def insert_data(self, table_name, data):
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
            return self.cursor.lastrowid  # return the last inserted id
        except sqlite3.IntegrityError:
            # Now we need to get the id of the existing or inserted row
            select_placeholders = ' AND '.join(
                [f'{column} = ?' for column in data.keys()])
            select_query = f"""
            SELECT id FROM {table_name}
            WHERE {select_placeholders}
            """
            self.cursor.execute(select_query, values)
            row = self.cursor.fetchone()
            return row[0] if row else None

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
