

from nanolog_parser.src.storage.impl.sqlite_storage import SQLiteStorage
from nanolog_parser.src.messages import *


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


def test_store_active_Started():
    data = {
        "log_timestamp": "2023-07-28 10:49:26.805",
        "log_process": "active_transactions",
        "log_level": "trace",
        "log_file": None,
        "log_event": "active_started",
        "election": {
            "root": "4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF",
            "behaviour": "normal",
            "state": "passive",
            "confirmed": False,
            "winner": "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3",
            "tally_amount": "0",
            "final_tally_amount": "0",
            "blocks": [
                {
                    "type": "state",
                    "hash": "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3",
                    "sideband": {
                        "successor": "0000000000000000000000000000000000000000000000000000000000000000",
                        "account": "0000000000000000000000000000000000000000000000000000000000000000",
                        "balance": "00000000000000000000000000000000",
                        "height": 2,
                        "timestamp": 1690541366,
                        "source_epoch": "epoch_begin",
                        "details": {
                            "epoch": "epoch_2",
                            "is_send": False,
                            "is_receive": False,
                            "is_epoch": False
                        }
                    },
                    "account": "9697595FE72336CD35206C0D708F6523CFD06C40D79B439C00C9CC41670FBEBF",
                    "previous": "4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF",
                    "representative": "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835",
                    "balance": "00000000000000000000000000000001",
                    "link": "0000000000000000000000000000000000000000000000000000000000000000",
                    "signature": "C1DE613980803B4D34E1DF2E4F750AEE782CBFB9F4A2F9D09C27A29E29F7A6591E3AA0B5671C7806E50327B11F5EE993ED41B1CD75ED1C08AFD8ABF0D8EB0509",
                    "work": 5711933947752905247
                }
            ],
            "votes": [
                {
                    "account": "nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os",
                    "time": 5510393808356621,
                    "timestamp": 0,
                    "hash": "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3"
                }
            ],
            "tally": [
                {
                    "amount": "0",
                    "hash": "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3"
                }
            ]
        },
        "class_name": "ActiveStartedMessage"
    }

    message = ActiveStartedMessage(data)  # no channels relation
    storage = SQLiteStorage(':memory:')
    storage.store_message(message)
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM activestartedmessage;")
    stored_message = cursor.fetchone()

    stored_message_dict = dict(
        zip([column[0] for column in cursor.description], stored_message))
    # assert stored_message_dict["election_winner"] == "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3"
