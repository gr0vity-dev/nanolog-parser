import pytest
import tempfile
from src.parser import Parser, MessageFactory
from src.messages import *
from src.parsing_utils import ParseException


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
    assert len(parser.parsed_messages) == 2

    # Test the report
    report = parser.report()
    print("REPORT", report)
    assert report["message_report"]["ConfirmAckMessageReceived"] == 1
    assert report["message_report"]["UnknownMessage"] == 1


def test_confirm_ack_message_parse():
    # Prepare a sample line
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'

    # Create a ConfirmAckMessage instance and parse the line
    log = MessageFactory.create_message(line)
    assert isinstance(log, ConfirmAckMessage)

    # Check if the parsed data is correct
    assert log.log_timestamp == '2023-07-15 14:19:44.951'
    assert log.log_process == 'network'
    assert log.log_level == 'trace'
    assert log.log_event == 'message_received'

    # Accessing elements of the message dictionary
    assert log.message["header"]["type"] == 'confirm_ack'
    assert log.message["header"]["network"] == 'live'
    assert log.message["header"]["network_int"] == 21059
    assert log.message["header"]["version"] == 19
    assert log.message["header"]["version_min"] == 18
    assert log.message["header"]["version_max"] == 19
    assert log.message["header"]["extensions"] == 4352
    assert log.message["vote"][
        "account"] == "399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D"
    assert log.message["vote"]["timestamp"] == 18446744073709551615
    assert log.message["vote"]["hashes"] == [
        "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B"
    ]
    # The vote_type is not present in the provided message structure
    assert log.vote_type == "final"


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
    log = MessageFactory.create_message(line)
    assert isinstance(log, ConfirmReqMessage)

    assert log.log_timestamp == '2023-07-15 14:19:44.805'
    assert log.log_process == 'network'
    assert log.log_level == 'trace'
    assert log.log_event == 'message_received'
    # Accessing the header and roots dictionary from the message dictionary
    assert log.message["header"]["type"] == 'confirm_req'
    assert log.message["header"]["network"] == 'live'
    assert log.message["header"]["network_int"] == 21059
    assert log.message["header"]["version"] == 19
    assert log.message["header"]["version_min"] == 18
    assert log.message["header"]["version_max"] == 19
    assert log.message["header"]["extensions"] == 4352
    assert log.message["roots"] == [{
        'root':
        "3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D",
        'hash':
        "54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA"
    }]


def test_publish_message():
    # Prepare a sample PublishMessage
    line = '[2023-07-15 14:19:48.286] [network] [trace] "message_received" message={ header={ type="publish", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=1536 }, block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 } }'
    log = MessageFactory.create_message(line)
    assert isinstance(log, PublishMessage)

    assert log.log_timestamp == '2023-07-15 14:19:48.286'
    assert log.log_process == 'network'
    assert log.log_level == 'trace'
    assert log.log_event == 'message_received'
    # Accessing the header and block dictionary from the message dictionary
    assert log.message["header"]["type"] == 'publish'
    assert log.message["header"]["network"] == 'live'
    assert log.message["header"]["network_int"] == 21059
    assert log.message["header"]["version"] == 19
    assert log.message["header"]["version_min"] == 18
    assert log.message["header"]["version_max"] == 19
    assert log.message["header"]["extensions"] == 1536
    # Check block data
    assert log.message["block"]["type"] == 'state'
    assert log.message["block"][
        "hash"] == '160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE'
    assert log.message["block"][
        "account"] == 'EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060'
    assert log.message["block"][
        "previous"] == '9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075'
    assert log.message["block"][
        "representative"] == 'F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2'
    assert log.message["block"][
        "balance"] == '00000000000000000000000000000000'
    assert log.message["block"][
        "link"] == 'F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2'
    assert log.message["block"][
        "signature"] == 'E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D'
    assert log.message["block"]["work"] == 10530317739669255306


def test_keepalive_message():
    # Prepare a sample KeepAliveMessage
    line = '[2023-07-15 14:19:44.867] [network] [trace] "message_received" message={ header={ type="keepalive", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=0 }, peers=[ "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075", "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075", "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075", "[::ffff:13.213.221.153]:7075" ] }'

    log = MessageFactory.create_message(line)
    assert isinstance(log, KeepAliveMessage)

    # Validate the parsed data
    assert log.log_timestamp == "2023-07-15 14:19:44.867"
    assert log.log_process == "network"
    assert log.log_level == "trace"
    assert log.log_event == "message_received"
    assert log.message['header']['type'] == "keepalive"
    assert log.message['header']['network'] == "live"
    assert log.message['header']['network_int'] == 21059
    assert log.message['header']['version'] == 19
    assert log.message['header']['version_min'] == 18
    assert log.message['header']['version_max'] == 19
    assert log.message['header']['extensions'] == 0
    assert log.message['peers'] == [
        "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075",
        "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075",
        "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075",
        "[::ffff:13.213.221.153]:7075"
    ]


def test_asc_pull_ack_message():
    # Prepare a sample AscPullAckMessage
    line = '[2023-07-15 14:19:45.772] [network] [trace] "message_received" message={ header={ type="asc_pull_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=218 }, type="blocks", id=9247237627708466530, blocks=[ { type="state", hash="A14902C8746C2098DEE0B537D28E9ACC57968124A68DA4C2BC642EBDDB201740", account="0000000000000000000000000000000000000000000000000000000000000DDE", previous="108C3F1CA6420149648BB083FDB14CB46AA690494A2B9E9F6BF56FB245F9D2E2", representative="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", link="65706F636820763220626C6F636B000000000000000000000000000000000000", signature="FC196E0D7C1F5FA1E38277F8E5CF154365B3C8914C946A3355BC11ED3011AC475898A6332A86404D88D83051D9F814FDC71F51816C8DD2584A464CB40CCA5F07", work=7926988356349568187 } ] }'
    log = MessageFactory.create_message(line)
    assert isinstance(log, AscPullAckMessage)

    # Validate the parsed data
    assert log.log_timestamp == "2023-07-15 14:19:45.772"
    assert log.log_process == "network"
    assert log.log_level == "trace"
    assert log.log_event == "message_received"
    assert log.message['header']['type'] == "asc_pull_ack"
    assert log.message['header']['network'] == "live"
    assert log.message['header']['network_int'] == 21059
    assert log.message['header']['version'] == 19
    assert log.message['header']['version_min'] == 18
    assert log.message['header']['version_max'] == 19
    assert log.message['header']['extensions'] == 218
    assert log.message['type'] == "blocks"
    assert log.message['id'] == 9247237627708466530
    assert log.message['blocks'][0]['type'] == "state"
    assert log.message['blocks'][0][
        'hash'] == "A14902C8746C2098DEE0B537D28E9ACC57968124A68DA4C2BC642EBDDB201740"
    assert log.message['blocks'][0][
        'account'] == "0000000000000000000000000000000000000000000000000000000000000DDE"
    assert log.message['blocks'][0][
        'previous'] == "108C3F1CA6420149648BB083FDB14CB46AA690494A2B9E9F6BF56FB245F9D2E2"
    assert log.message['blocks'][0][
        'representative'] == "0000000000000000000000000000000000000000000000000000000000000000"
    assert log.message['blocks'][0][
        'balance'] == "00000000000000000000000000000000"
    assert log.message['blocks'][0][
        'link'] == "65706F636820763220626C6F636B000000000000000000000000000000000000"
    assert log.message['blocks'][0][
        'signature'] == "FC196E0D7C1F5FA1E38277F8E5CF154365B3C8914C946A3355BC11ED3011AC475898A6332A86404D88D83051D9F814FDC71F51816C8DD2584A464CB40CCA5F07"
    assert log.message['blocks'][0]['work'] == 7926988356349568187


def test_asc_pull_ack_parse_error():
    line = '[2023-07-20 08:39:18.699] [network] [trace] "message_received" message={ header={ type="asc_pull_ack", network="test", network_int=21080, version=19, version_min=18, version_max=19, extensions=218 }, type="blocks", id=13826308554500678838, blocks=[ { type="state", hash="927ED91C0FB6219533C16D0FDC72054EF289B7E9310BAFDC915A45AD107D9011", account="139E91FF89FD3DFB56E484678C6F48B6D6CA9D646BCC7477067D0737DC5FA5C1", previous="0000000000000000000000000000000000000000000000000000000000000000", representative="FEEEC71E328CFC40E02F477CCE837A388CFCBEE7C08FFEAA6DEF9512C73501D0"'
    with pytest.raises(ParseException):
        MessageFactory.create_message(line)


def test_asc_pull_req_message():
    # Prepare a sample AscPullReqMessage
    line = '[2023-07-15 14:19:45.832] [network] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, type="blocks", id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'
    log = MessageFactory.create_message(line)
    assert isinstance(log, AscPullReqMessage)

    # Validate the parsed data
    assert log.log_timestamp == "2023-07-15 14:19:45.832"
    assert log.log_process == "network"
    assert log.log_level == "trace"
    assert log.log_event == "message_received"
    assert log.message['header']['type'] == "asc_pull_req"
    assert log.message['header']['network'] == "live"
    assert log.message['header']['network_int'] == 21059
    assert log.message['header']['version'] == 19
    assert log.message['header']['version_min'] == 18
    assert log.message['header']['version_max'] == 19
    assert log.message['header']['extensions'] == 34
    assert log.message['type'] == "blocks"
    assert log.message['id'] == 12094529471189612132
    assert log.message[
        'start'] == "62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0"
    assert log.message['start_type'] == "account"
    assert log.message['count'] == 128


def test_blockprocessor_message_identification():
    # Test that a blockprocessor log line is correctly identified
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    message = MessageFactory.create_message(line)
    assert isinstance(message, BlockProcessedMessage)


def test_blockprocessor_message_parsing():
    # Test that a blockprocessor message is correctly parsed
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    log = MessageFactory.create_message(line)
    assert isinstance(log, BlockProcessedMessage)
    print(log.__dict__)

    # Validate the parsed data
    assert log.log_timestamp == "2023-07-15 14:19:48.287"
    assert log.log_process == "blockprocessor"
    assert log.log_level == "trace"
    assert log.log_event == "block_processed"
    assert log.block['type'] == "state"
    assert log.block[
        'hash'] == "160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE"
    assert log.block[
        'account'] == "EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060"
    assert log.block[
        'previous'] == "9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075"
    assert log.block[
        'representative'] == "F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2"
    assert log.block['balance'] == "00000000000000000000000000000000"
    assert log.block[
        'link'] == "F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2"
    assert log.block[
        'signature'] == "E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D"
    assert log.block['work'] == 10530317739669255306
    assert log.result == "gap_previous"
    assert log.forced == False


def test_filename_parsing():
    filename = 'sample_log.log'
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(
        line, filename)  # pass filename to create_message
    # Check if the filename is parsed correctly
    assert message.log_file == filename


def test_node_process_confirmed_message_parsing():
    line = '[2023-07-18 20:46:14.798] [node] [trace] "process_confirmed" block={ type="state", hash="85EE57C6AB8E09FFDD1E656F47F7CC6598ADD48BE2F7B9F8B811CD9096E77C06", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1689713164, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="4005DB9BB6BC221383E80FBA1D5924C73580EA8573349513DA2EFA30F2D1A23C", previous="2A38C093945A920DC68F35F45195A88446A37E58F110FF022C71FD61C10D4D1C", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="7A3D8EC7DA648010853C3F7BEEC8D6E760B7C8CC940D8393362068558A086230DFF14D1ED88921E41EEFE5AD57D66D2332D1250159758AFA31943CEA2B137D02", work=2438566069390192728 }'
    log = MessageFactory.create_message(line)
    assert isinstance(log, ProcessConfirmedMessage)

    # Assertions for base attributes
    # Validate the parsed data
    assert log.log_timestamp == "2023-07-18 20:46:14.798"
    assert log.log_process == "node"
    assert log.log_level == "trace"
    assert log.log_event == "process_confirmed"
    assert log.block['type'] == "state"
    assert log.block[
        'hash'] == "85EE57C6AB8E09FFDD1E656F47F7CC6598ADD48BE2F7B9F8B811CD9096E77C06"
    assert log.block['sideband'][
        'successor'] == "0000000000000000000000000000000000000000000000000000000000000000"
    assert log.block['sideband'][
        'account'] == "0000000000000000000000000000000000000000000000000000000000000000"
    assert log.block['sideband'][
        'balance'] == "00000000000000000000000000000000"
    assert log.block['sideband']['height'] == 2
    assert log.block['sideband']['timestamp'] == 1689713164
    assert log.block['sideband']['source_epoch'] == "epoch_begin"
    assert log.block['sideband']['details']['epoch'] == "epoch_2"
    assert log.block['sideband']['details']['is_send'] is False
    assert log.block['sideband']['details']['is_receive'] is False
    assert log.block['sideband']['details']['is_epoch'] is False
    assert log.block[
        'account'] == "4005DB9BB6BC221383E80FBA1D5924C73580EA8573349513DA2EFA30F2D1A23C"
    assert log.block[
        'previous'] == "2A38C093945A920DC68F35F45195A88446A37E58F110FF022C71FD61C10D4D1C"
    assert log.block[
        'representative'] == "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835"
    assert log.block['balance'] == "00000000000000000000000000000001"
    assert log.block[
        'link'] == "0000000000000000000000000000000000000000000000000000000000000000"
    assert log.block[
        'signature'] == "7A3D8EC7DA648010853C3F7BEEC8D6E760B7C8CC940D8393362068558A086230DFF14D1ED88921E41EEFE5AD57D66D2332D1250159758AFA31943CEA2B137D02"
    assert log.block['work'] == 2438566069390192728


def test_confirmation_solicitor_broadcast_message_parsing():
    line = '[2023-07-20 08:37:49.297] [confirmation_solicitor] [trace] "broadcast" channel="[::ffff:192.168.160.6]:17075", hash="F39BF0D09AF3D80DF00253A47EA5C33CD15F70F9B748FD745C69DF5E3D22428D"'
    log = MessageFactory.create_message(line)
    assert isinstance(log, BroadcastMessage)
    print(log.__dict__)

    # Validate the parsed data
    assert log.log_timestamp == "2023-07-20 08:37:49.297"
    assert log.log_process == "confirmation_solicitor"
    assert log.log_level == "trace"
    assert log.log_event == "broadcast"
    assert log.channel == "[::ffff:192.168.160.6]:17075"
    assert log.hash == "F39BF0D09AF3D80DF00253A47EA5C33CD15F70F9B748FD745C69DF5E3D22428D"


def test_parse_generate_vote_normal_message():
    line = '[2023-07-20 08:20:51.401] [election] [trace] "generate_vote_normal" root="686C685B1CEF83843D6A5AD85EE685A6F6C394CB7C2E3B2B611CFA2B4DA566A3", hash="3A8867A4E61F181FC3B43B8E6BE5CBC860E35E6C7D3204EBB3557B2B6A514423"'
    log = MessageFactory.create_message(line)
    assert isinstance(log, ElectionGenerateVoteNormalMessage)
    print(log.__dict__)

    # Validate the parsed data
    assert log.log_timestamp == "2023-07-20 08:20:51.401"
    assert log.log_process == "election"
    assert log.log_level == "trace"
    assert log.log_event == "generate_vote_normal"
    assert log.root == "686C685B1CEF83843D6A5AD85EE685A6F6C394CB7C2E3B2B611CFA2B4DA566A3"
    assert log.hash == "3A8867A4E61F181FC3B43B8E6BE5CBC860E35E6C7D3204EBB3557B2B6A514423"


def test_parse_generate_vote_final_message():
    line = '[2023-07-20 08:41:38.398] [election] [trace] "generate_vote_final" root="355D17A4AC91A73D31BE8E4F2874298255F7A8905CCC11DDF43462E1A71FD0AE", hash="D05F1BB72F02E6F0C73D85DFCF09F8B8C32C258E9CA75943487CF74BD5C7B9A2"'

    log = MessageFactory.create_message(line)
    assert isinstance(log, ElectionGenerateVoteFinalMessage)

    # Validate the parsed data
    assert log.log_timestamp == "2023-07-20 08:41:38.398"
    assert log.log_process == "election"
    assert log.log_level == "trace"
    assert log.log_event == "generate_vote_final"
    assert log.root == "355D17A4AC91A73D31BE8E4F2874298255F7A8905CCC11DDF43462E1A71FD0AE"
    assert log.hash == "D05F1BB72F02E6F0C73D85DFCF09F8B8C32C258E9CA75943487CF74BD5C7B9A2"


def test_unknown_parser_with_event():
    line_unknown = '[2023-07-20 08:41:38.398] [unknown_message] [trace] "unkown_event" some text that should be stored as content in the sql column'
    message = MessageFactory.create_message(line_unknown)
    assert isinstance(message, UnknownMessage)

    assert message.log_timestamp == "2023-07-20 08:41:38.398"
    assert message.log_process == "unknown_message"
    assert message.log_level == "trace"
    assert message.content == "some text that should be stored as content in the sql column"


def test_unknown_parser():
    line_unknown = '[2023-07-20 08:41:38.398] [unknown_message] [info] some text that should be stored as content in the sql column'
    message = MessageFactory.create_message(line_unknown)

    assert isinstance(message, UnknownMessage)

    assert message.log_timestamp == "2023-07-20 08:41:38.398"
    assert message.log_process == "unknown_message"
    assert message.log_level == "info"
    assert message.content == "some text that should be stored as content in the sql column"


def test_processed_blocks_message():
    line = '[2023-07-28 21:43:42.099] [blockprocessor] [debug] Processed 140 blocks (0 forced) in 501 milliseconds'
    message = MessageFactory.create_message(line)

    assert isinstance(message, ProcessedBlocksMessage)
    assert message.processed_blocks == 140
    assert message.forced_blocks == 0
    assert message.process_time == 501


def test_blocks_in_queue_message():
    line = "[2023-07-28 21:43:41.598] [blockprocessor] [debug] 130 blocks (+ 0 state blocks) (+ 0 forced) in processing queue"
    log = MessageFactory.create_message(line)
    print(log.__dict__)

    assert isinstance(log, BlocksInQueueMessage)
    assert log.blocks_in_queue == 130
    assert log.state_blocks == 0
    assert log.forced_blocks == 0


def test_blockprocessor_message_without_parser():
    line = "[2023-07-20 08:41:12.300] [blockprocessor] [info] Message_without_a_specific_parser"
    log = MessageFactory.create_message(line)

    assert isinstance(log, UnknownMessage)
    assert log.content == "Message_without_a_specific_parser"


def testconfirmation_solicitor_flush_message():
    line = '[2023-07-24 08:24:57.000] [confirmation_solicitor] [trace] "flush" channel="[::ffff:192.168.96.6]:17075", confirm_req={ header={ type="confirm_req", network="test", network_int=21080, version=19, version_min=18, version_max=19, extensions=28928 }, block=null, roots=[ { root="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", hash="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E" }, { root="C8084749BBD422A8C946E934FDE0702471F850B817D34450BB0FE5E574C9E56E", hash="5583791E4DE40CCA877E394F471E605C494DB038BD5F2FFB5AB41FE709F463E9" }, { root="18136735ACAECDC6AC775F3D739E5A10C5101C132F990EB6F338F2F1493ACD5B", hash="1DD6232FA752C96A6F20AF451003B38EAA4799AB2A1837222E21C6EAF2C87ECB" }, { root="122A088010D2B6BC88E9658EE06C893DC02E3504B400D6486F9F13AC888698BB", hash="36687C628781978911AEC91FE95C249161BA11CCC804A4933751BE0B10CF780D" }, { root="260394945DACEDDF531AE01796278AEDD6C26A68FC56BAB05797EE5746B73D1C", hash="0A4BA7AB62B2C96987377860050687C7FCBD2DC0E0D24986EF128F928C655683" }, { root="3DC77E847676662685995955C8148F8B335624AF549863C108D5FE3A9AA38786", hash="E692A698B99E1136369B62401F4BB7B16098D7A0542DA7EF2438905C3F1E4B60" }, { root="4CB6B82860EF803C5CD77B18C3ECC9C2F414E28E1FDB6351DC165BA16D5D76EA", hash="A295A8E032EE234F1996311768712C86061322304F87F752A62D0B91717455B8" } ] }'
    log = MessageFactory.create_message(line)
    print(log.__dict__)

    # Validate the parsed data
    assert log.log_timestamp == "2023-07-24 08:24:57.000"
    assert log.log_process == "confirmation_solicitor"
    assert log.log_level == "trace"
    assert log.log_event == "flush"
    assert log.channel == "[::ffff:192.168.96.6]:17075"

    # Check confirm_req fields
    assert log.confirm_req["header"]["type"] == "confirm_req"
    assert log.confirm_req["header"]["network"] == "test"
    assert log.confirm_req["header"]["network_int"] == 21080
    assert log.confirm_req["header"]["version"] == 19
    assert log.confirm_req["header"]["version_min"] == 18
    assert log.confirm_req["header"]["version_max"] == 19
    assert log.confirm_req["header"]["extensions"] == 28928
    assert log.confirm_req["block"] is None

    # Check roots
    expected_roots = [{
        'root':
        '6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA',
        'hash':
        'F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E'
    }, {
        'root':
        'C8084749BBD422A8C946E934FDE0702471F850B817D34450BB0FE5E574C9E56E',
        'hash':
        '5583791E4DE40CCA877E394F471E605C494DB038BD5F2FFB5AB41FE709F463E9'
    }, {
        'root':
        '18136735ACAECDC6AC775F3D739E5A10C5101C132F990EB6F338F2F1493ACD5B',
        'hash':
        '1DD6232FA752C96A6F20AF451003B38EAA4799AB2A1837222E21C6EAF2C87ECB'
    }, {
        'root':
        '122A088010D2B6BC88E9658EE06C893DC02E3504B400D6486F9F13AC888698BB',
        'hash':
        '36687C628781978911AEC91FE95C249161BA11CCC804A4933751BE0B10CF780D'
    }, {
        'root':
        '260394945DACEDDF531AE01796278AEDD6C26A68FC56BAB05797EE5746B73D1C',
        'hash':
        '0A4BA7AB62B2C96987377860050687C7FCBD2DC0E0D24986EF128F928C655683'
    }, {
        'root':
        '3DC77E847676662685995955C8148F8B335624AF549863C108D5FE3A9AA38786',
        'hash':
        'E692A698B99E1136369B62401F4BB7B16098D7A0542DA7EF2438905C3F1E4B60'
    }, {
        'root':
        '4CB6B82860EF803C5CD77B18C3ECC9C2F414E28E1FDB6351DC165BA16D5D76EA',
        'hash':
        'A295A8E032EE234F1996311768712C86061322304F87F752A62D0B91717455B8'
    }]

    for i, root in enumerate(log.confirm_req["roots"]):
        assert root["root"] == expected_roots[i]["root"]
        assert root["hash"] == expected_roots[i]["hash"]
