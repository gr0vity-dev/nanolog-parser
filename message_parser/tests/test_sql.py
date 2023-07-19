from src.parser import MessageFactory
from src.messages import *
from src.storage.impl.sqlite_storage import SQLiteStorage
import json
import random
import string
import json
from datetime import datetime, timedelta


def test_store_nodeprocessconfirmed_message():
    line = '[2023-07-18 20:46:14.798] [node] [trace] "process_confirmed" block={ type="state", hash="85EE57C6AB8E09FFDD1E656F47F7CC6598ADD48BE2F7B9F8B811CD9096E77C06", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1689713164, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="4005DB9BB6BC221383E80FBA1D5924C73580EA8573349513DA2EFA30F2D1A23C", previous="2A38C093945A920DC68F35F45195A88446A37E58F110FF022C71FD61C10D4D1C", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="7A3D8EC7DA648010853C3F7BEEC8D6E760B7C8CC940D8393362068558A086230DFF14D1ED88921E41EEFE5AD57D66D2332D1250159758AFA31943CEA2B137D02", work=2438566069390192728 }'
    store_nodeprocessconfirmed_message(line)


def test_store_blockprocessor_message():
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    store_blockprocessor_message(line)


def test_store_confirm_ack_message():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'
    store_network_message(line)


def test_store_confirm_req_message():
    line = '[2023-07-15 14:19:44.805] [network] [trace] "message_received" message={ header={ type="confirm_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, roots=[ { root="3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash="54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA" } ] }'
    store_network_message(line)


def test_store_publish_message():
    line = '[2023-07-15 14:19:48.286] [network] [trace] "message_received" message={ header={ type="publish", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=1536 }, block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 } }'
    store_network_message(line)


def test_store_keepalive_message():
    line = '[2023-07-15 14:19:44.867] [network] [trace] "message_received" message={ header={ type="keepalive", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=0 }, peers=[ "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075", "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075", "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075", "[::ffff:13.213.221.153]:7075" ] }'
    store_network_message(line)


def test_store_asc_pull_ack_message():
    line = '[2023-07-15 14:19:45.772] [network] [trace] "message_received" message={ header={ type="asc_pull_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=218 }, type="blocks", id=9247237627708466530, blocks=[ { type="state", hash="A14902C8746C2098DEE0B537D28E9ACC57968124A68DA4C2BC642EBDDB201740", account="0000000000000000000000000000000000000000000000000000000000000DDE", previous="108C3F1CA6420149648BB083FDB14CB46AA690494A2B9E9F6BF56FB245F9D2E2", representative="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", link="65706F636820763220626C6F636B000000000000000000000000000000000000", signature="FC196E0D7C1F5FA1E38277F8E5CF154365B3C8914C946A3355BC11ED3011AC475898A6332A86404D88D83051D9F814FDC71F51816C8DD2584A464CB40CCA5F07", work=7926988356349568187 } ] }'
    store_network_message(line)


def test_store_asc_pull_req_message():
    line = '[2023-07-15 14:19:45.832] [network] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'
    store_network_message(line)


def store_network_message(line, filename=None):
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(line, filename)
    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store the message
    storage.store_message(message)

    # Retrieve the stored message
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
    stored_message = cursor.fetchone()

    # Check if the stored data is correct
    stored_message_dict = dict(
        zip([column[0] for column in cursor.description], stored_message))

    # Create list of common properties
    common_properties = [
        'log_timestamp', 'log_process', 'log_level', 'log_event', 'log_file',
        'message_type', 'network', 'network_int', 'version', 'version_min',
        'version_max', 'extensions'
    ]
    # Iterate over common properties and assert their correctness
    for property in common_properties:
        assert stored_message_dict[property] == getattr(message, property)

    # Additional checks based on specific message type
    if message.class_name.lower() == 'confirmackmessage':
        assert stored_message_dict['account'] == message.account
        assert stored_message_dict['hash_count'] == message.hash_count
        assert stored_message_dict['vote_type'] == message.vote_type
        assert stored_message_dict['timestamp'] == message.timestamp
        cursor.execute(
            f"SELECT hash FROM {message.class_name.lower()}_hashes WHERE confirmackmessage_id = ?;",
            (stored_message_dict['sql_id'], ))
        stored_hashes = [item[0] for item in cursor.fetchall()]
        assert stored_hashes == message.hashes

    elif message.class_name.lower() == 'confirmreqmessage':
        cursor.execute(
            f"SELECT root, hash FROM {message.class_name.lower()}_roots WHERE confirmreqmessage_id = ?;",
            (stored_message_dict['sql_id'], ))
        stored_roots = [{
            'root': item[0],
            'hash': item[1]
        } for item in cursor.fetchall()]
        assert stored_roots == message.roots

    elif message.class_name.lower() == 'publishmessage':
        assert stored_message_dict['block_type'] == message.block_type
        assert stored_message_dict['hash'] == message.hash
        assert stored_message_dict['account'] == message.account
        assert stored_message_dict['previous'] == message.previous
        assert stored_message_dict['representative'] == message.representative
        assert stored_message_dict['balance'] == message.balance
        assert stored_message_dict['link'] == message.link
        assert stored_message_dict['signature'] == message.signature

    elif message.class_name.lower() == 'keepalivemessage':
        assert stored_message_dict['peers'] == json.dumps(message.peers)

    elif message.class_name.lower() == 'ascpullackmessage':
        assert stored_message_dict['id'] == message.id
        assert stored_message_dict['blocks'] == json.dumps(message.blocks)
        #assert stored_message_dict['accounts'] == json.dumps(message.accounts)

    elif message.class_name.lower() == 'ascpullreqmessage':
        assert stored_message_dict['id'] == message.id
        assert stored_message_dict['start'] == message.start
        assert stored_message_dict['start_type'] == message.start_type
        assert stored_message_dict['count'] == message.count

    else:
        raise NotImplementedError(
            f"Check for {message.class_name} not implemented")


def store_blockprocessor_message(line):
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(line)
    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store the message
    storage.store_message(message)

    # Retrieve the stored message
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
    stored_message = cursor.fetchone()

    # Check if the stored data is correct
    stored_message_dict = dict(
        zip([column[0] for column in cursor.description], stored_message))

    # Create list of common properties
    common_properties = [
        'log_timestamp', 'log_process', 'log_level', 'log_event', 'result',
        'block_type', 'hash', 'account', 'previous', 'representative',
        'balance', 'link', 'signature', 'work', 'forced'
    ]

    # Iterate over common properties and assert their correctness
    for property in common_properties:
        assert stored_message_dict[property] == getattr(message, property)


def random_hex_string(length=32):
    """Generate a random hexadecimal string of the given length."""
    return ''.join(
        random.choice(string.hexdigits.lower()) for _ in range(length))


def random_timestamp():
    """Generate a random timestamp within the last 24 hours."""
    seconds_in_day = 24 * 60 * 60
    timestamp = datetime.now() - timedelta(
        seconds=random.randint(0, seconds_in_day))
    return timestamp.strftime(
        '%Y-%m-%d %H:%M:%S.%f')[:-3]  # trim microseconds to 3 digits


def test_store_random_confirm_ack():
    # Create a ConfirmAckMessage instance
    message = ConfirmAckMessage()

    # Assign random values to the fields
    message.log_timestamp = random_timestamp()
    message.log_process = 'network'
    message.log_level = 'trace'
    message.log_event = 'message_received'
    message.message_type = 'confirm_ack'
    message.network = 'live'
    message.network_int = random.randint(0, 65535)
    message.version = random.randint(0, 65535)
    message.version_min = random.randint(0, 65535)
    message.version_max = random.randint(0, 65535)
    message.extensions = random.randint(0, 65535)
    message.account = random_hex_string(64)
    message.timestamp = random.randint(0, 2**63 - 1)
    message.hashes = [
        random_hex_string(64) for _ in range(random.randint(1, 10))
    ]  # list of random hashes

    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store the message
    storage.store_message(message)

    # Retrieve the stored message
    # Retrieve the stored message
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
    stored_message = cursor.fetchone()

    # Check if the stored data is correct
    stored_message_dict = dict(
        zip([column[0] for column in cursor.description], stored_message))

    assert stored_message_dict['log_timestamp'] == message.log_timestamp
    assert stored_message_dict['log_process'] == message.log_process
    assert stored_message_dict['log_level'] == message.log_level
    assert stored_message_dict['log_event'] == message.log_event
    assert stored_message_dict['message_type'] == message.message_type
    assert stored_message_dict['network'] == message.network
    assert stored_message_dict['network_int'] == message.network_int
    assert stored_message_dict['version'] == message.version
    assert stored_message_dict['version_min'] == message.version_min
    assert stored_message_dict['version_max'] == message.version_max
    assert stored_message_dict['extensions'] == message.extensions
    assert stored_message_dict['account'] == message.account
    assert stored_message_dict['timestamp'] == message.timestamp

    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {message.class_name.lower()};")
    row_count = cursor.fetchone()[0]

    assert row_count == 1, f"Table {message.class_name.lower()} is empty after storing message."

    # Query the hashes
    cursor.execute(
        f"SELECT hash FROM {message.class_name.lower()}_hashes WHERE confirmackmessage_id = ?;",
        (stored_message_dict['sql_id'], ))
    stored_hashes = [item[0] for item in cursor.fetchall()]

    assert stored_hashes == message.hashes


def test_store_many_confirm_ack():
    num_messages = 1000  # number of messages to store

    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Create and store multiple messages
    for _ in range(num_messages):
        # Create a ConfirmAckMessage instance and assign random values to the fields
        message = ConfirmAckMessage()
        message.log_timestamp = random_timestamp()
        message.log_process = 'network'
        message.log_level = 'trace'
        message.log_event = 'message_received'
        message.message_type = 'confirm_ack'
        message.network = 'live'
        message.network_int = random.randint(0, 65535)
        message.version = random.randint(0, 65535)
        message.version_min = random.randint(0, 65535)
        message.version_max = random.randint(0, 65535)
        message.extensions = random.randint(0, 65535)
        message.account = random_hex_string(64)
        message.timestamp = random.randint(0, 2**63 - 1)
        message.hashes = [
            random_hex_string(64) for _ in range(random.randint(1, 12))
        ]

        # Store the message
        storage.store_message(message)

    # Retrieve all stored messages
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
    stored_messages = cursor.fetchall()

    # Check if the number of stored messages is correct
    assert len(stored_messages) == num_messages


def test_store_filename_in_message():
    filename = 'sample_log.log'
    line = '[2023-07-15 14:19:45.832] [network] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'
    store_network_message(line,
                          filename)  # pass filename to store_network_message


def store_nodeprocessconfirmed_message(line):
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(line)
    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store the message
    storage.store_message(message)

    # Retrieve the stored message
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
    stored_message = cursor.fetchone()

    # Check if the stored data is correct
    stored_message_dict = dict(
        zip([column[0] for column in cursor.description], stored_message))

    # Create list of common properties
    common_properties = [
        'log_timestamp', 'log_process', 'log_level', 'log_event', 'block_type',
        'hash', 'account', 'previous', 'representative', 'balance', 'link',
        'signature', 'work', 'sideband'
    ]

    # Iterate over common properties and assert their correctness
    for property in common_properties:
        if property == 'sideband':
            # Since sideband is stored as a json string, we need to load it back into a dictionary for comparison
            assert json.loads(stored_message_dict[property]) == getattr(
                message, property)
        else:
            assert stored_message_dict[property] == getattr(message, property)


def test_store_activetransactionsstarted_message():
    line_started = '[2023-07-19 08:24:43.500] [active_transactions] [trace] "active_started" root="385D9F01FCEBBE15F123FA80AEC4D86EEA7991EBBCCB6370A0E4260E2B8B920A385D9F01FCEBBE15F123FA80AEC4D86EEA7991EBBCCB6370A0E4260E2B8B920A", hash="CE40A97D9ACA6A6890F28B076ADE1CC6001B0BA017D3A629D02D31F1B2C03A98", behaviour="normal"'
    store_active_transactions_message_combined(line_started,
                                               'activestartedmessage')


def test_store_activetransactionsstopped_message():
    line_stopped = '[2023-07-19 08:24:43.749] [active_transactions] [trace] "active_stopped" root="68F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE3268F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE32", hashes=[ "77B0B617A49B12B6A5F1CE6D063337A1DD8B365EBCA1CD18FD92D761037D1F3E" ], behaviour="normal", confirmed=true'
    store_active_transactions_message_combined(line_stopped,
                                               'activestoppedmessage')


def store_active_transactions_message_combined(line, sql_class_name):
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(line)

    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store the message
    storage.store_message(message)

    # Retrieve the stored message
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {sql_class_name.lower()};")
    stored_message = cursor.fetchone()

    # Check if the stored data is correct
    stored_message_dict = dict(
        zip([column[0] for column in cursor.description], stored_message))

    # Create list of common properties
    common_properties = [
        'log_timestamp', 'log_process', 'log_level', 'log_event', 'root',
        'behaviour'
    ]

    if sql_class_name.lower() == 'activestartedmessage':
        common_properties.extend(['hash'])

    if sql_class_name.lower() == 'activestoppedmessage':
        cursor.execute(
            f"SELECT hash FROM {message.class_name.lower()}_hashes WHERE activestoppedmessage_id = ?;",
            (stored_message_dict['sql_id'], ))
        stored_hashes = [item[0] for item in cursor.fetchall()]
        assert stored_hashes == message.hashes
        #common_properties.extend(['hashes', 'confirmed'])

    # Iterate over common properties and assert their correctness
    for property in common_properties:
        assert stored_message_dict[property] == getattr(message, property)
