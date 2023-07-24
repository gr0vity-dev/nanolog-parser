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


def test_store_nodeprocessconfirmed_message():
    line = '[2023-07-18 20:46:14.798] [node] [trace] "process_confirmed" block={ type="state", hash="85EE57C6AB8E09FFDD1E656F47F7CC6598ADD48BE2F7B9F8B811CD9096E77C06", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1689713164, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="4005DB9BB6BC221383E80FBA1D5924C73580EA8573349513DA2EFA30F2D1A23C", previous="2A38C093945A920DC68F35F45195A88446A37E58F110FF022C71FD61C10D4D1C", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="7A3D8EC7DA648010853C3F7BEEC8D6E760B7C8CC940D8393362068558A086230DFF14D1ED88921E41EEFE5AD57D66D2332D1250159758AFA31943CEA2B137D02", work=2438566069390192728 }'
    properties = COMMON_PROPERTIES + [
        'log_event', 'block_type', 'hash', 'account', 'previous',
        'representative', 'balance', 'link', 'signature', 'work'
    ]
    json_properties = ['sideband']
    store_message(line, NodeProcessConfirmedMessage, properties,
                  json_properties)


def test_store_blockprocessor_message():
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    properties = COMMON_PROPERTIES + [
        'log_event', 'result', 'block_type', 'hash', 'account', 'previous',
        'representative', 'balance', 'link', 'signature', 'work', 'forced'
    ]
    store_message(line, BlockProcessedMessage, properties)


def test_store_confirm_ack_message():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'
    properties = NETWORK_COMMON_PROPERTIES + [
        'account', 'hash_count', 'vote_type', 'timestamp'
    ]
    store_message(line, ConfirmAckMessage, properties)


def test_store_confirm_req_message():
    line = '[2023-07-15 14:19:44.805] [network] [trace] "message_received" message={ header={ type="confirm_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, roots=[ { root="3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash="54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA" } ] }'
    properties = NETWORK_COMMON_PROPERTIES + ["root_count"]
    store_message(line, ConfirmReqMessage, properties)


def test_store_publish_message():
    line = '[2023-07-15 14:19:48.286] [network] [trace] "message_received" message={ header={ type="publish", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=1536 }, block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 } }'
    properties = NETWORK_COMMON_PROPERTIES + [
        'block_type', 'hash', 'account', 'previous', 'representative',
        'balance', 'link', 'signature'
    ]
    store_message(line, PublishMessage, properties)


def test_store_keepalive_message():
    line = '[2023-07-15 14:19:44.867] [network] [trace] "message_received" message={ header={ type="keepalive", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=0 }, peers=[ "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075", "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075", "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075", "[::ffff:13.213.221.153]:7075" ] }'
    properties = NETWORK_COMMON_PROPERTIES
    json_properties = ['peers']
    store_message(line, KeepAliveMessage, properties, json_properties)


def test_store_asc_pull_ack_message():
    line = '[2023-07-15 14:19:45.772] [network] [trace] "message_received" message={ header={ type="asc_pull_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=218 }, type="blocks", id=9247237627708466530, blocks=[ { type="state", hash="A14902C8746C2098DEE0B537D28E9ACC57968124A68DA4C2BC642EBDDB201740", account="0000000000000000000000000000000000000000000000000000000000000DDE", previous="108C3F1CA6420149648BB083FDB14CB46AA690494A2B9E9F6BF56FB245F9D2E2", representative="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", link="65706F636820763220626C6F636B000000000000000000000000000000000000", signature="FC196E0D7C1F5FA1E38277F8E5CF154365B3C8914C946A3355BC11ED3011AC475898A6332A86404D88D83051D9F814FDC71F51816C8DD2584A464CB40CCA5F07", work=7926988356349568187 } ] }'
    properties = NETWORK_COMMON_PROPERTIES + ['id']
    json_properties = ['blocks']
    store_message(line, AscPullAckMessage, properties, json_properties)


def test_store_asc_pull_req_message():
    line = '[2023-07-15 14:19:45.832] [network] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'
    properties = NETWORK_COMMON_PROPERTIES + [
        'id', 'start', 'start_type', 'count'
    ]
    store_message(line, AscPullReqMessage, properties)


def test_store_filename_in_message():
    filename = 'sample_log.log'
    line = '[2023-07-15 14:19:45.832] [network] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'
    properties = NETWORK_COMMON_PROPERTIES + [
        'id', 'start', 'start_type', 'count'
    ]
    store_message(line, AscPullReqMessage, properties, filename=filename)


def test_store_activetransactionsstarted_message():
    line = '[2023-07-19 08:24:43.500] [active_transactions] [trace] "active_started" root="385D9F01FCEBBE15F123FA80AEC4D86EEA7991EBBCCB6370A0E4260E2B8B920A385D9F01FCEBBE15F123FA80AEC4D86EEA7991EBBCCB6370A0E4260E2B8B920A", hash="CE40A97D9ACA6A6890F28B076ADE1CC6001B0BA017D3A629D02D31F1B2C03A98", behaviour="normal"'
    properties = COMMON_PROPERTIES + ['log_event', 'root', 'behaviour', 'hash']
    store_message(line, ActiveStartedMessage, properties)


def test_store_activetransactionsstopped_message():
    line = '[2023-07-19 08:24:43.749] [active_transactions] [trace] "active_stopped" root="68F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE3268F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE32", hashes=[ "77B0B617A49B12B6A5F1CE6D063337A1DD8B365EBCA1CD18FD92D761037D1F3E" ], behaviour="normal", confirmed=true'
    properties = COMMON_PROPERTIES + ['log_event', 'root', 'behaviour']
    store_message(line, ActiveStoppedMessage, properties)


def test_store_broadcast_message():
    line = '[2023-07-20 08:37:49.297] [confirmation_solicitor] [trace] "broadcast" channel="[::ffff:192.168.160.6]:17075", hash="F39BF0D09AF3D80DF00253A47EA5C33CD15F70F9B748FD745C69DF5E3D22428D"'
    properties = COMMON_PROPERTIES + ['log_event', 'channel', 'hash']
    store_message(line, BroadcastMessage, properties)


def test_store_generate_vote_normal_message():
    line = '[2023-07-20 08:20:51.401] [election] [trace] "generate_vote_normal" root="686C685B1CEF83843D6A5AD85EE685A6F6C394CB7C2E3B2B611CFA2B4DA566A3", hash="3A8867A4E61F181FC3B43B8E6BE5CBC860E35E6C7D3204EBB3557B2B6A514423"'
    properties = COMMON_PROPERTIES + ['log_event', 'root', 'hash']
    store_message(line, GenerateVoteNormalMessage, properties)


def test_store_generate_vote_final_message():
    line = '[2023-07-20 08:41:38.398] [election] [trace] "generate_vote_final" root="355D17A4AC91A73D31BE8E4F2874298255F7A8905CCC11DDF43462E1A71FD0AE", hash="D05F1BB72F02E6F0C73D85DFCF09F8B8C32C258E9CA75943487CF74BD5C7B9A2"'
    properties = COMMON_PROPERTIES + ['log_event', 'root', 'hash']
    store_message(line, GenerateVoteFinalMessage, properties)


def test_store_unknown_message_with_event():
    line = '[2023-07-20 08:41:38.398] [unknown_message] [trace] "unkown_event" some text that should be stored as content in the sql column'
    properties = COMMON_PROPERTIES + ['log_event', 'content']
    store_message(line, UnknownMessage, properties)


def test_store_unknown_message_without_event():
    line = '[2023-07-20 08:41:38.398] [unknown_message] [info] some text that should be stored as content in the sql column'
    properties = COMMON_PROPERTIES + ['content']
    store_message(line, UnknownMessage, properties)


def test_store_processed_blocks_message():
    line = '[2023-07-20 08:41:11.799] [blockprocessor] [debug] Processed 159 blocks (0 forced) in 501milliseconds'
    store_message(line, ProcessedBlocksMessage,
                  ['processed_blocks', 'forced_blocks', 'process_time'])


def test_store_blocks_in_queue_message():
    line = '[2023-07-20 08:41:12.300] [blockprocessor] [debug] 101 blocks [+ 0 state blocks] [+ 0 forced] in processing queue'
    store_message(line, BlocksInQueueMessage,
                  ['blocks_in_queue', 'state_blocks', 'forced_blocks'])


def test_store_flush_message():
    line = '[2023-07-24 08:24:57.000] [confirmation_solicitor] [trace] "flush" channel="[::ffff:192.168.96.6]:17075", confirm_req={ header={ type="confirm_req", network="test", network_int=21080, version=19, version_min=18, version_max=19, extensions=28928 }, block=null, roots=[ { root="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", hash="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E" }, { root="C8084749BBD422A8C946E934FDE0702471F850B817D34450BB0FE5E574C9E56E", hash="5583791E4DE40CCA877E394F471E605C494DB038BD5F2FFB5AB41FE709F463E9" }, { root="18136735ACAECDC6AC775F3D739E5A10C5101C132F990EB6F338F2F1493ACD5B", hash="1DD6232FA752C96A6F20AF451003B38EAA4799AB2A1837222E21C6EAF2C87ECB" }, { root="122A088010D2B6BC88E9658EE06C893DC02E3504B400D6486F9F13AC888698BB", hash="36687C628781978911AEC91FE95C249161BA11CCC804A4933751BE0B10CF780D" }, { root="260394945DACEDDF531AE01796278AEDD6C26A68FC56BAB05797EE5746B73D1C", hash="0A4BA7AB62B2C96987377860050687C7FCBD2DC0E0D24986EF128F928C655683" }, { root="3DC77E847676662685995955C8148F8B335624AF549863C108D5FE3A9AA38786", hash="E692A698B99E1136369B62401F4BB7B16098D7A0542DA7EF2438905C3F1E4B60" }, { root="4CB6B82860EF803C5CD77B18C3ECC9C2F414E28E1FDB6351DC165BA16D5D76EA", hash="A295A8E032EE234F1996311768712C86061322304F87F752A62D0B91717455B8" } ] }'
    properties = COMMON_PROPERTIES + ['log_event', 'channel']
    store_message(line, FlushMessage, properties)


def store_message(
    line,
    message_class,
    properties,
    json_properties=None,
    filename=None,
):
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(line, filename)
    assert isinstance(message, message_class)

    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store the message
    storage.store_message(message)

    # Retrieve the stored message
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
    stored_message = cursor.fetchone()
    print("DEBUG stored_message", stored_message)

    # Check if the stored data is correct
    stored_message_dict = dict(
        zip([column[0] for column in cursor.description], stored_message))

    if json_properties is None:
        json_properties = []

    # Iterate over regular properties and assert their correctness
    for property in properties:
        assert stored_message_dict[property] == getattr(message, property)

    # Iterate over JSON properties and assert their correctness
    for property in json_properties:
        assert json.loads(stored_message_dict[property]) == getattr(
            message, property)

    # Special handling for ConfirmReqMessage
    query_message_relations(cursor, message_class, message,
                            stored_message_dict['sql_id'])


def query_message_relations(cursor, message_class, message, sql_id):
    relation_map = {
        ConfirmReqMessage: {
            'relation': 'roots',
            'fields': ['root', 'hash']
        },
        ConfirmAckMessage: {
            'relation': 'hashes',
            'fields': ['hash']
        },
        ActiveStoppedMessage: {
            'relation': 'hashes',
            'fields': ['hash']
        },
        FlushMessage: {
            'relation': 'roots',
            'fields': ['root', 'hash'],
            'attr': 'confirm_req'  #key with type ConfirmReqMessage
        }
    }

    if message_class in relation_map:
        relation = relation_map[message_class]['relation']
        attr = relation_map[message_class].get('attr')
        fields = relation_map[message_class]['fields']

        cursor.execute(
            f"SELECT {','.join(fields)} FROM {message_class.__name__.lower()}_{relation} WHERE {message_class.__name__.lower()}_id = ?;",
            (sql_id, ))
        stored_data = cursor.fetchall()

        if len(fields) > 1:
            stored_items = [{field: item[i]
                             for i, field in enumerate(fields)}
                            for item in stored_data]
        else:
            stored_items = [item[0] for item in stored_data]

        if attr:
            attr_obj = getattr(message, attr)
            assert stored_items == getattr(attr_obj, relation)
        else:
            assert stored_items == getattr(message, relation)


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
