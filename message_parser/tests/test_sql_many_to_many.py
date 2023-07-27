from src.parser import MessageFactory
from src.messages import *
from src.storage.impl.sqlite_storage import SQLiteStorage
import json
import random
import string
import json
from datetime import datetime, timedelta

COMMON_PROPERTIES = ['log_timestamp', 'log_process', 'log_level']
NETWORK_COMMON_PROPERTIES = COMMON_PROPERTIES + [
    'log_event', 'log_file', 'message_type', 'network', 'network_int',
    'version', 'version_min', 'version_max', 'extensions'
]


def test_store_confirm_ack_message():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" , "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD9229" ] } }'
    properties = NETWORK_COMMON_PROPERTIES + [
        'account', 'hash_count', 'vote_type', 'timestamp'
    ]
    store_message_test(line, ConfirmAckMessage, properties, 'hashes')


def test_store_message_confirm_req():
    # Define sample lines for each message type
    line = '[2023-07-15 14:19:44.805] [network] [trace] "message_received" message={ header={ type="confirm_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, roots=[ { root="3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash="54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA" } , { root="3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash="54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEB" }] }'

    store_message_test(line, ConfirmReqMessage,
                       NETWORK_COMMON_PROPERTIES + ['root_count'], 'roots')


def test_store_flush_message():
    line = '[2023-07-24 08:24:57.000] [confirmation_solicitor] [trace] "flush" channel="[::ffff:192.168.96.6]:17075", confirm_req={ header={ type="confirm_req", network="test", network_int=21080, version=19, version_min=18, version_max=19, extensions=28928 }, block=null, roots=[ { root="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", hash="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E" }, { root="C8084749BBD422A8C946E934FDE0702471F850B817D34450BB0FE5E574C9E56E", hash="5583791E4DE40CCA877E394F471E605C494DB038BD5F2FFB5AB41FE709F463E9" }, { root="18136735ACAECDC6AC775F3D739E5A10C5101C132F990EB6F338F2F1493ACD5B", hash="1DD6232FA752C96A6F20AF451003B38EAA4799AB2A1837222E21C6EAF2C87ECB" }, { root="122A088010D2B6BC88E9658EE06C893DC02E3504B400D6486F9F13AC888698BB", hash="36687C628781978911AEC91FE95C249161BA11CCC804A4933751BE0B10CF780D" }, { root="260394945DACEDDF531AE01796278AEDD6C26A68FC56BAB05797EE5746B73D1C", hash="0A4BA7AB62B2C96987377860050687C7FCBD2DC0E0D24986EF128F928C655683" }, { root="3DC77E847676662685995955C8148F8B335624AF549863C108D5FE3A9AA38786", hash="E692A698B99E1136369B62401F4BB7B16098D7A0542DA7EF2438905C3F1E4B60" }, { root="4CB6B82860EF803C5CD77B18C3ECC9C2F414E28E1FDB6351DC165BA16D5D76EA", hash="A295A8E032EE234F1996311768712C86061322304F87F752A62D0B91717455B8" } ] }'
    properties = COMMON_PROPERTIES + ['log_event', 'channel']
    store_message_test(line, FlushMessage, properties, 'roots')


def test_store_activetransactionsstopped_message():
    line = '[2023-07-19 08:24:43.749] [active_transactions] [trace] "active_stopped" root="68F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE3268F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE32", hashes=[ "77B0B617A49B12B6A5F1CE6D063337A1DD8B365EBCA1CD18FD92D761037D1F3E" ], behaviour="normal", confirmed=true'
    properties = COMMON_PROPERTIES + ['log_event', 'root', 'behaviour']
    store_message_test(line, ActiveStoppedMessage, properties, 'hashes')


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
        f"SELECT hash FROM hashes INNER JOIN message_links ON hashes.id = message_links.relation_id WHERE message_id = ? AND message_type = ? AND relation_type = ?;",
        (stored_message_dict['sql_id'], message.class_name.lower(), "hashes"))
    stored_hashes = [item[0] for item in cursor.fetchall()]

    assert stored_hashes == message.hashes


def test_store_many_confirm_ack():
    num_messages = 1000  # number of messages to store

    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store all message ids and hashes for later check
    all_message_hashes = {}

    # Create and store multiple messages
    for i in range(num_messages):
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
        all_message_hashes[i + 1] = message.hashes

    # Retrieve all stored messages
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
    stored_messages = cursor.fetchall()

    # Check if the number of stored messages is correct
    assert len(stored_messages) == num_messages

    # Check if all hashes related to the messages are stored correctly
    for message_id, hashes in all_message_hashes.items():
        cursor.execute(
            f"SELECT hash FROM hashes INNER JOIN message_links ON hashes.id = message_links.relation_id WHERE message_id = ? AND message_type = ? AND relation_type = ?;",
            (message_id, message.class_name.lower(), "hashes"))
        stored_hashes = [item[0] for item in cursor.fetchall()]
        assert sorted(stored_hashes) == sorted(hashes)


################################
################################
################################
################################

#### Non TEST HELPER FUNCTIONS

################################
################################
################################
################################


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


def store_message_test(line,
                       message_class,
                       properties,
                       relationship,
                       filename=None):
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(line, filename)
    assert isinstance(message, message_class)

    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store the message
    storage.store_message(message)

    # Check if the stored data is correct
    assert_data_in_table(storage, message_class, message, properties)
    assert_related_entities_in_table(storage, message_class, relationship)


def assert_related_entities_in_table(storage, message_class, relationship):

    cursor = storage.repository.conn.cursor()

    cursor.execute(f'SELECT * FROM {relationship}')
    entities = cursor.fetchall()
    entities_columns = [column[0] for column in cursor.description]

    stored_entities_list = [
        dict(zip(entities_columns, row)) for row in entities
    ]

    assert len(stored_entities_list) >= 1

    cursor.execute(
        f"SELECT * FROM message_links WHERE relation_type = '{relationship}' ")
    message_entities = cursor.fetchall()
    message_entities_columns = [column[0] for column in cursor.description]

    stored_message_entities_list = [
        dict(zip(message_entities_columns, row)) for row in message_entities
    ]

    # Assert the entity in '{relationship}' table is correctly related to the message in 'message_{relationship}' table
    for i, entity in enumerate(stored_entities_list):
        assert stored_message_entities_list[i]['relation_id'] == entity['id']
        assert stored_message_entities_list[i][
            'message_type'] == message_class.__name__.lower()
        assert stored_message_entities_list[i]['message_id'] == 1
        assert stored_message_entities_list[i]['relation_id'] == i + 1


def assert_data_in_table(storage, message_class, message, properties):
    # Retrieve the stored message
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message_class.__name__.lower()};")

    stored_message = cursor.fetchone()
    print("DEBUG stored_message", stored_message)

    # Create a dict from the stored message data
    stored_message_dict = dict(
        zip([column[0] for column in cursor.description], stored_message))

    # Assert each property is correctly stored
    for property in properties:
        assert stored_message_dict[property] == getattr(message, property)
