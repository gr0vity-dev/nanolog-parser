

from src.storage.impl.sqlite_storage import SQLiteStorage
from src.messages import *


def test_store_BlockProcessedMessage():
    storage = SQLiteStorage(':memory:')
    data = {
        "log_timestamp": "2023-07-28 21:43:40.198",
        "log_process": "blockprocessor",
        "log_level": "trace",
        "log_file": "nl_pr4",
        "log_event": "block_processed",
        "result": "progress",
        "block": {
            "type": "state",
            "hash": "D8F85FB58D79544264611543ABF53B112CC2A1B1DF4A5FEA40F24E92A611930A",
            "account": "DBD0232BFCD34057431BCD3C1F9EAC8EB21179427411B29EED4EAD9E048FA689",
            "previous": "00321706787924D24C5552E8786BC2D6548987DD9FD7B70A1217731E4009F6A8",
            "representative": "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835",
            "balance": "00000000000000000000000000000001",
            "link": "0000000000000000000000000000000000000000000000000000000000000000",
            "signature": "5AC230239E58F8AA485F2B717413B0221A1300B68B368E404DD2D59B5CE3A82B53EDF047A636AE9BD6611A8196B4BDA9291CAFD971C36AC54630D14DEF56D60F",
            "work": "6183967265338905340"
        },
        "forced": False,
        "class_name": "BlockProcessedMessage"
    }
    message = BlockProcessedMessage(data)
    storage.store_message(message)
    storage.store_message(message)
    storage.store_message(message)

    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM BlockProcessedMessage;")
    rows = cursor.fetchall()
    assert len(rows) == 3
    cursor.execute(f"SELECT * FROM blocks;")
    rows = cursor.fetchall()
    print(rows)
    assert len(rows) == 1

    cursor.execute(f"SELECT * FROM message_links;")
    rows = cursor.fetchall()
    print(rows)
    assert len(rows) == 3
