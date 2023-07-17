import tempfile
from src.parser import Parser, MessageFactory
from src.messages.network_messages import ConfirmAckMessage, ConfirmReqMessage, PublishMessage, KeepAliveMessage, AscPullAckMessage, AscPullReqMessage
from src.messages.blockprocessor_messages import BlockProcessorMessage
from src.storage.impl.sqlite_storage import SQLiteStorage
import json
import random
import string
import json
from datetime import datetime, timedelta


def test_message_factory():
    # Test the create_message function of MessageFactory
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'
    message = MessageFactory.create_message(line)
    assert isinstance(message, ConfirmAckMessage)


def test_parser():
    parser = Parser()

    # Create a temporary file for testing
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write(
            '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }\n'
        )
        temp_file.write(
            '[2023-07-15 14:19:44.561] [daemon] [info] Version: V26.0\n')
        temp_file_name = temp_file.name

    # Load and parse the file
    parser.load_and_parse_file(temp_file_name)

    # Assert the file was parsed correctly
    assert len(parser.parsed_messages) == 1
    assert parser.ignored_lines == 1

    # Test the report
    report = parser.report()
    assert report["message_report"]["ConfirmAckMessage"] == 1
    assert report["ignored_lines"] == 1


def test_confirm_ack_message_parse():
    # Prepare a sample line
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'

    # Create a ConfirmAckMessage instance and parse the line
    message = ConfirmAckMessage().parse(line)

    # Check if the parsed data is correct
    assert message.account == "399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D"
    assert message.timestamp == -1
    assert message.hashes == [
        "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B"
    ]


# def test_store_message():
#     # Prepare a sample ConfirmAckMessage
#     line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'
#     # Create a Message instance and parse the line using MessageFactory
#     message = MessageFactory.create_message(line)
#     # Create a SQLiteStorage instance
#     storage = SQLiteStorage(':memory:')

#     # Store the message
#     storage.store_message(message)

#     # Retrieve the stored message
#     cursor = storage.repository.conn.cursor()
#     cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
#     stored_message = cursor.fetchone()

#     # Check if the stored data is correct
#     stored_message_dict = dict(
#         zip([column[0] for column in cursor.description], stored_message))

#     assert stored_message_dict['log_timestamp'] == message.log_timestamp
#     assert stored_message_dict['log_process'] == message.log_process
#     assert stored_message_dict['log_level'] == message.log_level
#     assert stored_message_dict['log_event'] == message.log_event
#     assert stored_message_dict['message_type'] == message.message_type
#     assert stored_message_dict['network'] == message.network
#     assert stored_message_dict['network_int'] == message.network_int
#     assert stored_message_dict['version'] == message.version
#     assert stored_message_dict['version_min'] == message.version_min
#     assert stored_message_dict['version_max'] == message.version_max
#     assert stored_message_dict['extensions'] == message.extensions
#     assert stored_message_dict['account'] == message.account
#     assert stored_message_dict['timestamp'] == message.timestamp
#     assert stored_message_dict['hashes'] == json.dumps(message.hashes)


def test_message_parsing():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(line)

    # Check if the parsed base attributes are correct
    assert message.log_timestamp == '2023-07-15 14:19:44.951'
    assert message.log_process == 'network'
    assert message.log_level == 'trace'
    assert message.log_event == 'message_received'


def test_confirm_req_message():
    line = '[2023-07-15 14:19:44.805] [network] [trace] "message_received" message={ header={ type="confirm_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, block=null, roots=[ { root="3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash="54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA" } ] }'
    message = ConfirmReqMessage()
    message.parse(line)
    assert message.log_timestamp == '2023-07-15 14:19:44.805'
    assert message.log_process == 'network'
    assert message.log_level == 'trace'
    assert message.log_event == 'message_received'
    assert message.message_type == 'confirm_req'
    assert message.network == 'live'
    assert message.network_int == 21059
    assert message.version == 19
    assert message.version_min == 18
    assert message.version_max == 19
    assert message.extensions == 4352
    assert message.roots == [{
        'root':
        "3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D",
        'hash':
        "54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA"
    }]


def test_publish_message():
    # Prepare a sample PublishMessage
    line = '[2023-07-15 14:19:48.286] [network] [trace] "message_received" message={ header={ type="publish", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=1536 }, block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 } }'

    # Create a PublishMessage instance and parse the line
    message = PublishMessage().parse(line)

    # Validate the parsed data
    assert message.log_timestamp == "2023-07-15 14:19:48.286"
    assert message.log_process == "network"
    assert message.log_level == "trace"
    assert message.log_event == "message_received"
    assert message.message_type == "publish"
    assert message.network == "live"
    assert message.network_int == 21059
    assert message.version == 19
    assert message.version_min == 18
    assert message.version_max == 19
    assert message.extensions == 1536
    assert message.block_type == "state"
    assert message.hash == "160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE"
    assert message.account == "EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060"
    assert message.previous == "9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075"
    assert message.representative == "F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2"
    assert message.balance == "00000000000000000000000000000000"
    assert message.link == "F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2"
    assert message.signature == "E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D"


def test_keepalive_message():
    # Prepare a sample KeepAliveMessage
    line = '[2023-07-15 14:19:44.867] [network] [trace] "message_received" message={ header={ type="keepalive", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=0 }, peers=[ "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075", "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075", "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075", "[::ffff:13.213.221.153]:7075" ] }'

    # Create a KeepAliveMessage instance and parse the line
    message = KeepAliveMessage().parse(line)

    # Validate the parsed data
    assert message.log_timestamp == "2023-07-15 14:19:44.867"
    assert message.log_process == "network"
    assert message.log_level == "trace"
    assert message.log_event == "message_received"
    assert message.message_type == "keepalive"
    assert message.network == "live"
    assert message.network_int == 21059
    assert message.version == 19
    assert message.version_min == 18
    assert message.version_max == 19
    assert message.extensions == 0
    assert message.peers == [
        "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075",
        "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075",
        "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075",
        "[::ffff:13.213.221.153]:7075"
    ]


def test_asc_pull_ack_message():
    # Prepare a sample AscPullAckMessage
    line = '[2023-07-15 14:19:45.772] [network] [trace] "message_received" message={ header={ type="asc_pull_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=218 }, type="blocks", id=9247237627708466530, blocks=[ { type="state", hash="A14902C8746C2098DEE0B537D28E9ACC57968124A68DA4C2BC642EBDDB201740", account="0000000000000000000000000000000000000000000000000000000000000DDE", previous="108C3F1CA6420149648BB083FDB14CB46AA690494A2B9E9F6BF56FB245F9D2E2", representative="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", link="65706F636820763220626C6F636B000000000000000000000000000000000000", signature="FC196E0D7C1F5FA1E38277F8E5CF154365B3C8914C946A3355BC11ED3011AC475898A6332A86404D88D83051D9F814FDC71F51816C8DD2584A464CB40CCA5F07", work=7926988356349568187 } ] }'

    # Create an AscPullAckMessage instance and parse the line
    message = AscPullAckMessage().parse(line)

    # Validate the parsed data
    assert message.log_timestamp == "2023-07-15 14:19:45.772"
    assert message.log_process == "network"
    assert message.log_level == "trace"
    assert message.log_event == "message_received"
    assert message.message_type == "asc_pull_ack"
    assert message.network == "live"
    assert message.network_int == 21059
    assert message.version == 19
    assert message.version_min == 18
    assert message.version_max == 19
    assert message.extensions == 218
    assert message.id == "9247237627708466530"
    assert message.blocks[0]['type'] == "state"
    assert message.blocks[0][
        'hash'] == "A14902C8746C2098DEE0B537D28E9ACC57968124A68DA4C2BC642EBDDB201740"
    # Continue with other assertions for block data


def test_asc_pull_req_message():
    # Prepare a sample AscPullReqMessage
    line = '[2023-07-15 14:19:45.832] [network] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, type="blocks", id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'

    # Create an AscPullReqMessage instance and parse the line
    message = AscPullReqMessage().parse(line)

    # Validate the parsed data
    assert message.log_timestamp == "2023-07-15 14:19:45.832"
    assert message.log_process == "network"
    assert message.log_level == "trace"
    assert message.log_event == "message_received"
    assert message.message_type == "asc_pull_req"
    assert message.network == "live"
    assert message.network_int == 21059
    assert message.version == 19
    assert message.version_min == 18
    assert message.version_max == 19
    assert message.extensions == 34
    assert message.id == "12094529471189612132"
    assert message.start == "62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0"
    assert message.start_type == "account"
    assert message.count == 128


# def test_blockprocessor_message_identification():
#     # Test that a blockprocessor log line is correctly identified
#     line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
#     message = MessageFactory.create_message(line)
#     assert isinstance(message, BlockProcessorMessage)


def test_blockprocessor_message_parsing():
    # Test that a blockprocessor message is correctly parsed
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    message = BlockProcessorMessage().parse(line)
    assert message.result == 'gap_previous'
    assert not message.forced
    assert message.block is not None


def test_store_confirm_ack_message():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'
    store_message(line)


def test_store_confirm_req_message():
    line = '[2023-07-15 14:19:44.805] [network] [trace] "message_received" message={ header={ type="confirm_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, roots=[ { root="3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash="54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA" } ] }'
    store_message(line)


def test_store_publish_message():
    line = '[2023-07-15 14:19:48.286] [network] [trace] "message_received" message={ header={ type="publish", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=1536 }, block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 } }'
    store_message(line)


def test_store_keepalive_message():
    line = '[2023-07-15 14:19:44.867] [network] [trace] "message_received" message={ header={ type="keepalive", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=0 }, peers=[ "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075", "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075", "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075", "[::ffff:13.213.221.153]:7075" ] }'
    store_message(line)


def test_store_asc_pull_ack_message():
    line = '[2023-07-15 14:19:45.772] [network] [trace] "message_received" message={ header={ type="asc_pull_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=218 }, type="blocks", id=9247237627708466530, blocks=[ { type="state", hash="A14902C8746C2098DEE0B537D28E9ACC57968124A68DA4C2BC642EBDDB201740", account="0000000000000000000000000000000000000000000000000000000000000DDE", previous="108C3F1CA6420149648BB083FDB14CB46AA690494A2B9E9F6BF56FB245F9D2E2", representative="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", link="65706F636820763220626C6F636B000000000000000000000000000000000000", signature="FC196E0D7C1F5FA1E38277F8E5CF154365B3C8914C946A3355BC11ED3011AC475898A6332A86404D88D83051D9F814FDC71F51816C8DD2584A464CB40CCA5F07", work=7926988356349568187 } ] }'
    store_message(line)


# def test_store_asc_pull_req_message():
#     line = '[2023-07-15 14:19:45.832] [network] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'
#     store_message(line)


def store_message(line):
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
        'log_timestamp', 'log_process', 'log_level', 'log_event',
        'message_type', 'network', 'network_int', 'version', 'version_min',
        'version_max', 'extensions'
    ]
    # Iterate over common properties and assert their correctness
    for property in common_properties:
        assert stored_message_dict[property] == getattr(message, property)

    # Additional checks based on specific message type
    if message.class_name.lower() == 'confirmackmessage':
        assert stored_message_dict['account'] == message.account
        assert stored_message_dict['timestamp'] == message.timestamp
        assert stored_message_dict['hashes'] == json.dumps(message.hashes)
    elif message.class_name.lower() == 'confirmreqmessage':
        assert stored_message_dict['roots'] == json.dumps(message.roots)
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


def test_store_random_message():
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
    assert stored_message_dict['hashes'] == json.dumps(message.hashes)


def test_store_multiple_messages():
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
