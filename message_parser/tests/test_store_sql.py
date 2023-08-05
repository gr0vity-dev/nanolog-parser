

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
        "log_timestamp": "2023-07-28 21:43:31.898",
        "log_process": "network",
        "log_level": "trace",
        "log_file": "nl_pr4",
        "log_event": "message_received",
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
                "account": "FCE16FA5F87645DD73C799B3E959F635752ACA6EF8D9F4918B34B3D5E00E0B56",
                "timestamp": 18446744073709551615,
                "hashes": [
                    "5B7181F80219011D4E65F93FA2C02FBA117A0FC667BCF9E7BF72BE5C1FAE9334"
                ]
            }
        },
        "class_name": "ConfirmAckMessage",
        "vote_type": "final",
        "vote_count": 1
    }
    data2 = {
        "log_timestamp": "2023-07-28 21:43:33.798",
        "log_process": "channel",
        "log_level": "trace",
        "log_file": "nl_pr4",
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
                "account": "04BD6D942F527F887196868C8927FF84340B4A9AC491BE69DB3AFC31AAF36F57",
                "timestamp": 18446744073709551615,
                "hashes": [
                    "E86EA7AF605BA00B2E82419728CB5C9EE511873A752999D436650CCEAAA2FB33"
                ]
            }
        },
        "channel": {
            "endpoint": "[::ffff:192.168.112.3]:17075",
            "peering_endpoint": "[::ffff:192.168.112.3]:17075",
            "node_id": "01F4C307028F5118F449AFED64DB25F5D7469E48312010429E90BA0B1274F607",
            "socket": {
                "remote_endpoint": "[::ffff:192.168.112.3]:17075",
                "local_endpoint": "[::ffff:192.168.112.2]:43138"
            }
        },
        "class_name": "ConfirmAckMessage",
        "vote_type": "final",
        "vote_count": 1
    }
    data3 = {
        "log_timestamp": "2023-07-28 21:43:31.898",
        "log_process": "network",
        "log_level": "trace",
        "log_file": "nl_pr4",
        "log_event": "message_received",
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
                "account": "FCE16FA5F87645DD73C799B3E959F635752ACA6EF8D9F4918B34B3D5E00E0B56",
                "timestamp": 18446744073709551615,
                "hashes": [
                    "5B7181F80219011D4E65F93FA2C02FBA117A0FC667BCF9E7BF72BE5C1FAE9334"
                ]
            }
        },
        "class_name": "ConfirmAckMessage",
        "vote_type": "final",
        "vote_count": 1
    }

    message = ConfirmAckMessageReceived(data1)  # no channels relation
    message2 = ConfirmAckMessageSent(data2)  # failed during prodrun
    message3 = ConfirmAckMessageReceived(data3)  # no channels relation

    storage = SQLiteStorage(':memory:')
    storage.store_message(message)
    storage.store_message(message2)
    storage.store_message(message3)

    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM confirmackmessage;")
    rows = cursor.fetchall()
    print(rows)
    assert len(rows) == 3

    cursor.execute(f"SELECT * FROM channels;")
    rows = cursor.fetchall()
    print(rows)
    assert len(rows) == 1
