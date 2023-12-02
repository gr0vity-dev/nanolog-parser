#!/usr/bin/env python3

import argparse
from nanolog_parser.src.formatters import IFormatter, JsonFormatter, TextFormatter
from nanolog_parser.src.storage.impl.sqlite_storage import SQLiteStorage
from nanolog_parser.src.formatters.jsondb import JSONFlattener
from sqlalchemy import create_engine,Table, MetaData, Index, text
import pandas as pd
import time



def get_args():
    parser = argparse.ArgumentParser(description="NanoLog Parser")

    # Modify the argument here:
    parser.add_argument('--format', type=str, required=True,
                        choices=['json', 'text', 'flat'], help="Specify the format: json or text.")
    parser.add_argument('--db', type=str, default="parsed_messages.db",
                        help="Path to the database where parsed messages will be stored.")
    
    parser.add_argument('--file-node', nargs=2, action='append',
                        help="Pairs of file and node names", required=True)
    
    return parser.parse_args()


class NanoLogParser:

    def __init__(self, formatter: IFormatter, storage: SQLiteStorage):
        self.formatter = formatter
        self.storage = storage
        self.message_count = 0

    def _modify_filename(self, filename, remove_prefix=""):
        return filename.replace(remove_prefix, "").replace("node.log", "").replace(".log", "")

    def process(self, line, filename):  # Add filename parameter
        modified_filename = self._modify_filename(filename)
        message = self.formatter.format(line, modified_filename)
        if message:
            self.storage.store_message(message)
            self._increment_and_display_progress()
    
    def process_flat(self, line, filename):  # Add filename parameter
        modified_filename = self._modify_filename(filename)
        message = self.formatter.get(line, modified_filename)
        if message and message.get("log_level") == "trace" :
            message.pop("content")
        self._increment_and_display_progress()
        return message    

    def _increment_and_display_progress(self):
        self.message_count += 1
        if self.message_count % 10000 == 0:
            print(f"Processed: {self.message_count} messages", end="\r")

def create_db_engine(connection_string):    
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        connection.execute(text("PRAGMA journal_mode=WAL;"))
    return engine

def write_to_db(engine, dataframes):
    for table_name, df in dataframes.items():
        # Convert all columns of the dataframe to string type
        df = df.astype(str)
        
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
        except Exception as e:
            print(f"Error writing table {table_name}: {e}")


def main():
    args = get_args()

    if args.format == "json":
        formatter = JsonFormatter()
    elif args.format == "text":
        formatter = TextFormatter()
    elif args.format == "flat":  
        formatter = JsonFormatter()      
        flattener = JSONFlattener()
        flattener.add_key_mappings({"block": "blocks", "vote": "votes"})
    else:
        print(f"Unsupported format: {args.format}")
        return

    import time

    if args.format == "flat":
        start_time = time.time()

        json_lines = []
        log_parser = NanoLogParser(formatter, None)
        print(f"Storing messages in SQL database: {args.db}\n")

        file_read_time = 0
        for file_node_pair in args.file_node:
            file_name, node_name = file_node_pair
            file_start_time = time.time()
            with open(file_name, 'r', encoding='utf-8') as file:
                print(f"Processing {node_name}\n")
                for line in file:
                    json_lines.append(log_parser.process_flat(line.strip(), node_name))
            file_read_time += time.time() - file_start_time

        print(f"File reading and processing time: {file_read_time:.2f} seconds")

        json_to_tables_start_time = time.time()
        print(f"Converting to json tables \n")
        tables = flattener.to_json_tables(json_lines)
        print(f"Time taken to convert to JSON tables: {time.time() - json_to_tables_start_time:.2f} seconds")

        dataframes_start_time = time.time()
        print(f"Converting to dataframes\n")
        dataframes = {table_name: pd.DataFrame(rows) for table_name, rows in tables.items()}
        print(f"Time taken to convert to DataFrames: {time.time() - dataframes_start_time:.2f} seconds")

        db_write_start_time = time.time()
        connection_string = f"sqlite:///{args.db}"  # Or your PostgreSQL connection string
        engine = create_db_engine(connection_string)
        print(f"Writing to sqlite \n")
        write_to_db(engine, dataframes)
        print(f"Time taken to write to DB: {time.time() - db_write_start_time:.2f} seconds")

        total_time = time.time() - start_time
        print(f"Total execution time: {total_time:.2f} seconds")

    else:
        with SQLiteStorage(args.db) as storage:
            log_parser = NanoLogParser(formatter, storage)
            print(f"Storing messages in SQL database: {args.db}\n")              
            
            for file_node_pair in args.file_node:
                file_name, node_name = file_node_pair
                with open(file_name, 'r', encoding='utf-8') as file:
                    print(f"Processing {node_name}\n")
                    for line in file:
                        log_parser.process(line.strip(), node_name)

    print(f"\nTotal messages processed: {log_parser.message_count}")
    print(f"Messages stored in SQL database: {args.db}")


if __name__ == "__main__":
    main()
