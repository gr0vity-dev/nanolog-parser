

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


def test_store_different_messages_to_confirmack_table():
    data1 = {
        "log_timestamp": "2023-07-28 21:43:31.200",
        "log_process": "channel",
        "log_level": "trace",
        "log_file": None,
        "log_event": "message_sent",
        "message": {
            "header": {
                "type": "confirm_ack",
                "network": "test",
                "network_int": 21080,
                "version": 19,
                "version_min": 18,
                "version_max": 19,
                "extensions": 4352
            },
            "vote": {
                "account": "398562D3A2945BE17E6676B3E43603E160142A0A555E85071E5A10D04010D8EC",
                "timestamp": 18446744073709551615,
                "hashes": [
                    "B0B14D451CDC5623A8376741B9B63811F77B64EDFEB281DE18D05E958BD6B225"
                ]
            }
        },
        "channel": {
            "endpoint": "[::ffff:192.168.112.6]:17075",
            "peering_endpoint": "[::ffff:192.168.112.6]:17075",
            "node_id": "2C4327C0B3B302D1696E84D52480890E6FD5373523BACDF39BE45FC88C33FC78",
            "socket": {
                "remote_endpoint": "[::ffff:192.168.112.6]:17075",
                "local_endpoint": "[::ffff:192.168.112.4]:39184"
            }
        },
        "class_name": "ConfirmAckMessage",
        "vote_type": "final"
    }
    data2 = {
        "log_timestamp": "2023-07-15 14:19:44.951",
        "log_process": "network",
        "log_level": "trace",
        "log_file": None,
        "log_event": "message_received",
        "message": {
            "header": {
                "type": "confirm_ack",
                "network": "live",
                "network_int": 21059,
                "version": 19,
                "version_min": 18,
                "version_max": 19,
                "extensions": 4352
            },
            "vote": {
                "account": "399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D",
                "timestamp": 18446744073709551615,
                "hashes": [
                    "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B",
                    "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD9229"
                ]
            }
        },
        "class_name": "ConfirmAckMessage",
        "vote_type": "final"
    }

    message = ConfirmAckMessageSent(data1)  # has channels relation
    message2 = ConfirmAckMessageReceived(data2)  # no channels relation

    storage = SQLiteStorage(':memory:')
    storage.store_message(message)
    storage.store_message(message2)

    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM confirmackmessage;")
    rows = cursor.fetchall()
    assert len(rows) == 2

    cursor.execute(f"SELECT * FROM channels;")
    rows = cursor.fetchall()
    assert len(rows) == 1
