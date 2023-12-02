#!/usr/bin/env python3

import argparse
from nanolog_parser.src.formatters import IFormatter, JsonFormatter, TextFormatter
from nanolog_parser.src.storage.impl.sqlite_storage import SQLiteStorage
from src.formatters.jsondb import JSONFlattener
from sqlalchemy import create_engine,Table, MetaData, Index
import pandas as pd


def get_args():
    parser = argparse.ArgumentParser(description="NanoLog Parser")

    # Modify the argument here:
    parser.add_argument('--format', type=str, required=True,
                        choices=['json', 'text', 'flat'], help="Specify the format: json or text.")
    parser.add_argument('--db', type=str, default="parsed_messages.db",
                        help="Path to the database where parsed messages will be stored.")
    parser.add_argument('--file', type=str,
                        help="Name of the file being parsed.", required=True)
    parser.add_argument('--node', type=str,
                        help="Name of node. If provided, instead of 'file', 'node' is written to log_filename ")

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
    return create_engine(connection_string)

def write_to_db(engine, dataframes):
    for table_name, df in dataframes.items():
        df.to_sql(table_name, engine, if_exists='replace', index=False)

def main():
    args = get_args()

    if args.format == "json":
        formatter = JsonFormatter()
    elif args.format == "text":
        formatter = TextFormatter()
    elif args.format == "flat":  
        formatter = JsonFormatter()      
        flattener = JSONFlattener()
    else:
        print(f"Unsupported format: {args.format}")
        return

    if args.format == "flat":
            json_lines = []
            with open(args.file, 'r', encoding='utf-8') as file:
                for line in file:
                    json_lines.append(log_parser.process_flat(line.strip(), args.node or args.file))
                
                # main, children, mapping = flattener.flatten_json(json_lines)
                print(f"Converting to json tables \n")
                tables = flattener.to_json_tables(json_lines)
                print(f"Converting to dataframes\n")
                dataframes = {table_name: pd.DataFrame(rows) for table_name, rows in tables.items()}
                connection_string = f"sqlite:///{args.db}"  # Or your PostgreSQL connection string
                engine = create_db_engine(connection_string)
                print(f"Writing to sqlite \n")
                write_to_db(engine, dataframes)
    else:
        with SQLiteStorage(args.db) as storage:
            log_parser = NanoLogParser(formatter, storage)
            print(f"Storing messages in SQL database: {args.db}\n")              
            
            with open(args.file, 'r', encoding='utf-8') as file:
                for line in file:
                    log_parser.process(line.strip(), args.node or args.file)

    print(f"\nTotal messages processed: {log_parser.message_count}")
    print(f"Messages stored in SQL database: {args.db}")


if __name__ == "__main__":
    main()
