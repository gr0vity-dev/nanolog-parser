import pytest
import tempfile
from src.parser import Parser, MessageFactory
from src.messages import *
from src.parsing_utils import ParseException
import src.message_parsers
from src.message_parsers.base_parser import BaseParser


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
    assert report["message_report"]["ConfirmAckMessage"] == 1
    assert report["message_report"]["UnknownMessage"] == 1


def test_confirm_ack_message_parse():
    # Prepare a sample line
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" ] } }'

    # Create a ConfirmAckMessage instance and parse the line
    message = ConfirmAckMessage().parse(line)

    # Check if the parsed data is correct
    assert message.log_timestamp == '2023-07-15 14:19:44.951'
    assert message.log_process == 'network'
    assert message.log_level == 'trace'
    assert message.log_event == 'message_received'
    assert message.message_type == 'confirm_ack'
    assert message.network == 'live'
    assert message.network_int == 21059
    assert message.version == 19
    assert message.version_min == 18
    assert message.version_max == 19
    assert message.extensions == 4352
    assert message.account == "399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D"
    assert message.timestamp == -1
    assert message.hashes == [
        "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B"
    ]
    assert message.vote_type == "final"


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


def test_asc_pull_ack_parse_error():
    line = '[2023-07-20 08:39:18.699] [network] [trace] "message_received" message={ header={ type="asc_pull_ack", network="test", network_int=21080, version=19, version_min=18, version_max=19, extensions=218 }, type="blocks", id=13826308554500678838, blocks=[ { type="state", hash="927ED91C0FB6219533C16D0FDC72054EF289B7E9310BAFDC915A45AD107D9011", account="139E91FF89FD3DFB56E484678C6F48B6D6CA9D646BCC7477067D0737DC5FA5C1", previous="0000000000000000000000000000000000000000000000000000000000000000", representative="FEEEC71E328CFC40E02F477CCE837A388CFCBEE7C08FFEAA6DEF9512C73501D0"'
    with pytest.raises(ParseException):
        message = AscPullAckMessage().parse(line)


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


def test_blockprocessor_message_identification():
    # Test that a blockprocessor log line is correctly identified
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    message = MessageFactory.create_message(line)
    assert isinstance(message, BlockProcessedMessage)


def test_blockprocessor_message_parsing():
    # Test that a blockprocessor message is correctly parsed
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    message = BlockProcessedMessage().parse(line)

    # Assertions for base attributes
    assert message.log_timestamp == '2023-07-15 14:19:48.287'
    assert message.log_process == 'blockprocessor'
    assert message.log_level == 'trace'
    assert message.log_event == 'block_processed'

    # Assertions for specific fields
    assert message.result == 'gap_previous'
    assert not message.forced
    assert message.block_type == 'state'
    assert message.hash == '160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE'
    assert message.account == 'EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060'
    assert message.previous == '9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075'
    assert message.representative == 'F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2'
    assert message.balance == '00000000000000000000000000000000'
    assert message.link == 'F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2'
    assert message.signature == 'E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D'
    assert message.work == "10530317739669255306"


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
    message = NodeProcessConfirmedMessage().parse(line)

    # Assertions for base attributes
    assert message.log_timestamp == '2023-07-18 20:46:14.798'
    assert message.log_process == 'node'
    assert message.log_level == 'trace'
    assert message.log_event == 'process_confirmed'

    # Assertions for specific fields
    assert message.block_type == 'state'
    assert message.hash == '85EE57C6AB8E09FFDD1E656F47F7CC6598ADD48BE2F7B9F8B811CD9096E77C06'
    assert message.account == '4005DB9BB6BC221383E80FBA1D5924C73580EA8573349513DA2EFA30F2D1A23C'
    assert message.previous == '2A38C093945A920DC68F35F45195A88446A37E58F110FF022C71FD61C10D4D1C'
    assert message.representative == '39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835'
    assert message.balance == '00000000000000000000000000000001'
    assert message.link == '0000000000000000000000000000000000000000000000000000000000000000'
    assert message.signature == '7A3D8EC7DA648010853C3F7BEEC8D6E760B7C8CC940D8393362068558A086230DFF14D1ED88921E41EEFE5AD57D66D2332D1250159758AFA31943CEA2B137D02'
    assert message.work == "2438566069390192728"
    assert message.sideband[
        'successor'] == '0000000000000000000000000000000000000000000000000000000000000000'
    assert message.sideband[
        'account'] == '0000000000000000000000000000000000000000000000000000000000000000'
    assert message.sideband['balance'] == '00000000000000000000000000000000'
    assert message.sideband['height'] == 2
    assert message.sideband['timestamp'] == 1689713164
    assert message.sideband['source_epoch'] == 'epoch_begin'
    assert message.sideband['details']['epoch'] == 'epoch_2'
    assert not message.sideband['details']['is_send']
    assert not message.sideband['details']['is_receive']
    assert not message.sideband['details']['is_epoch']


def test_active_transactions_started_message_parsing():
    #line_started = '[2023-07-19 08:24:43.500] [active_transactions] [trace] "active_started" root="385D9F01FCEBBE15F123FA80AEC4D86EEA7991EBBCCB6370A0E4260E2B8B920A385D9F01FCEBBE15F123FA80AEC4D86EEA7991EBBCCB6370A0E4260E2B8B920A", hash="CE40A97D9ACA6A6890F28B076ADE1CC6001B0BA017D3A629D02D31F1B2C03A98", behaviour="normal"'
    line = '[2023-07-28 10:49:26.805] [active_transactions] [trace] "active_started" election={ root="4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF", behaviour="normal", state="passive", confirmed=false, winner="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3", tally_amount="0", final_tally_amount="0", blocks=[ { type="state", hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1690541366, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="9697595FE72336CD35206C0D708F6523CFD06C40D79B439C00C9CC41670FBEBF", previous="4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="C1DE613980803B4D34E1DF2E4F750AEE782CBFB9F4A2F9D09C27A29E29F7A6591E3AA0B5671C7806E50327B11F5EE993ED41B1CD75ED1C08AFD8ABF0D8EB0509", work=5711933947752905247 } ], votes=[ { account="nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os", time=5510393808356621, timestamp=0, hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3" } ], tally=[ { amount="0", hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3" } ] }'
    message_started = ActiveStartedMessage().parse(line)

    # Assertions for base attributes
    assert message_started.log_timestamp == '2023-07-28 10:49:26.805'
    assert message_started.log_process == 'active_transactions'
    assert message_started.log_level == 'trace'
    assert message_started.log_event == 'active_started'

    # Assertions for specific fields
    assert message_started.root == '4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF'
    assert message_started.hash == '578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3'
    assert message_started.behaviour == 'normal'


def test_active_transactions_stopped_message_parsing():
    #line_stopped = '[2023-07-19 08:24:43.749] [active_transactions] [trace] "active_stopped" root="68F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE3268F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE32", hashes=[ "77B0B617A49B12B6A5F1CE6D063337A1DD8B365EBCA1CD18FD92D761037D1F3E" ], behaviour="normal", confirmed=true'
    line = '[2023-07-28 10:49:27.298] [active_transactions] [trace] "active_stopped" election={ root="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4EF4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E", behaviour="hinted", state="expired_confirmed", confirmed=true, winner="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", tally_amount="199987308019747226638731596728893410000", final_tally_amount="149987308019747226638731596728893410000", blocks=[ { type="state", hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1690541363, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="C8563DF2ADE096D4551819C3F4178C359C2DF8C8FE121E46ECCA9F9BD6E85C43", previous="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="616A9A2D255FCE81DDD3CBFF8DCD8DBB73B45007699D7393C4ABC9A442F6CDF6CC6987095153A287A55F6ED15CD8562B0CFF4A33872BDB12AAD169D43240FF03", work=11857312774462321081 } ], votes=[ { account="nano_137xfpc4ynmzj3rsf3nej6mzz33n3f7boj6jqsnxpgqw88oh8utqcq7nska8", time=5510392306330110, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_3sz3bi6mpeg5jipr1up3hotxde6gxum8jotr55rzbu9run8e3wxjq1rod9a6", time=5510394001887203, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_1ge7edbt774uw7z8exomwiu19rd14io1nocyin5jwpiit3133p9eaaxn74ub", time=5510391903726501, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_3z93fykzixk7uoswh8fmx7ezefdo7d78xy8sykarpf7mtqi1w4tpg7ejn18h", time=5510391606052922, timestamp=1690541364592, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os", time=5510391606051222, timestamp=0, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" } ], tally=[ { amount="199987308019747226638731596728893410000", hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" } ] }'
    message_stopped = ActiveStoppedMessage().parse(line)

    assert message_stopped.log_timestamp == '2023-07-19 08:24:43.749'
    assert message_stopped.log_process == 'active_transactions'
    assert message_stopped.log_level == 'trace'
    assert message_stopped.log_event == 'active_stopped'

    assert message_stopped.root == '68F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE3268F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE32'
    assert message_stopped.hashes == [
        '77B0B617A49B12B6A5F1CE6D063337A1DD8B365EBCA1CD18FD92D761037D1F3E'
    ]
    assert message_stopped.behaviour == 'normal'
    assert message_stopped.confirmed == True


def test_confirmation_solicitor_broadcast_message_parsing():
    line_broadcast = '[2023-07-20 08:37:49.297] [confirmation_solicitor] [trace] "broadcast" channel="[::ffff:192.168.160.6]:17075", hash="F39BF0D09AF3D80DF00253A47EA5C33CD15F70F9B748FD745C69DF5E3D22428D"'

    message_broadcast = BroadcastMessage().parse(line_broadcast)

    # Assertions for base attributes
    assert message_broadcast.log_timestamp == '2023-07-20 08:37:49.297'
    assert message_broadcast.log_process == 'confirmation_solicitor'
    assert message_broadcast.log_level == 'trace'
    assert message_broadcast.log_event == 'broadcast'

    # Assertions for specific fields
    assert message_broadcast.channel == '[::ffff:192.168.160.6]:17075'
    assert message_broadcast.hash == 'F39BF0D09AF3D80DF00253A47EA5C33CD15F70F9B748FD745C69DF5E3D22428D'


def test_parse_generate_vote_normal_message():
    line_normal = '[2023-07-20 08:20:51.401] [election] [trace] "generate_vote_normal" root="686C685B1CEF83843D6A5AD85EE685A6F6C394CB7C2E3B2B611CFA2B4DA566A3", hash="3A8867A4E61F181FC3B43B8E6BE5CBC860E35E6C7D3204EBB3557B2B6A514423"'
    message = GenerateVoteNormalMessage().parse(line_normal)

    assert isinstance(message, GenerateVoteNormalMessage)
    assert message.root == "686C685B1CEF83843D6A5AD85EE685A6F6C394CB7C2E3B2B611CFA2B4DA566A3"
    assert message.hash == "3A8867A4E61F181FC3B43B8E6BE5CBC860E35E6C7D3204EBB3557B2B6A514423"


def test_parse_generate_vote_final_message():
    line_final = '[2023-07-20 08:41:38.398] [election] [trace] "generate_vote_final" root="355D17A4AC91A73D31BE8E4F2874298255F7A8905CCC11DDF43462E1A71FD0AE", hash="D05F1BB72F02E6F0C73D85DFCF09F8B8C32C258E9CA75943487CF74BD5C7B9A2"'
    message = GenerateVoteFinalMessage().parse(line_final)

    assert isinstance(message, GenerateVoteFinalMessage)
    assert message.root == "355D17A4AC91A73D31BE8E4F2874298255F7A8905CCC11DDF43462E1A71FD0AE"
    assert message.hash == "D05F1BB72F02E6F0C73D85DFCF09F8B8C32C258E9CA75943487CF74BD5C7B9A2"


def test_unknown_parser_with_event():
    line_unknown = '[2023-07-20 08:41:38.398] [unknown_message] [trace] "unkown_event" some text that should be stored as content in the sql column'
    message = MessageFactory.create_message(line_unknown)

    # print out the type of message
    print(type(message))

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
    line = '[2023-07-20 08:41:11.799] [blockprocessor] [debug] Processed 159 blocks (0 forced) in 501milliseconds"'
    message = MessageFactory.create_message(line)

    assert isinstance(message, ProcessedBlocksMessage)
    assert message.processed_blocks == 159
    assert message.forced_blocks == 0
    assert message.process_time == 501


def test_blocks_in_queue_message():
    line = "[2023-07-20 08:41:12.300] [blockprocessor] [debug] 101 blocks [+ 0 state blocks] [+ 0 forced] in processing queue"
    message = MessageFactory.create_message(line)

    assert isinstance(message, BlocksInQueueMessage)
    assert message.blocks_in_queue == 101
    assert message.state_blocks == 0
    assert message.forced_blocks == 0


def test_blockprocessor_message_without_parser():
    line = "[2023-07-20 08:41:12.300] [blockprocessor] [info] Message_without_a_specific_parser"
    message = MessageFactory.create_message(line)

    assert isinstance(message, BlockProcessorMessage)
    assert message.content == "Message_without_a_specific_parser"


def testconfirmation_solicitor_flush_message():
    line = '[2023-07-24 08:24:57.000] [confirmation_solicitor] [trace] "flush" channel="[::ffff:192.168.96.6]:17075", confirm_req={ header={ type="confirm_req", network="test", network_int=21080, version=19, version_min=18, version_max=19, extensions=28928 }, block=null, roots=[ { root="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", hash="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E" }, { root="C8084749BBD422A8C946E934FDE0702471F850B817D34450BB0FE5E574C9E56E", hash="5583791E4DE40CCA877E394F471E605C494DB038BD5F2FFB5AB41FE709F463E9" }, { root="18136735ACAECDC6AC775F3D739E5A10C5101C132F990EB6F338F2F1493ACD5B", hash="1DD6232FA752C96A6F20AF451003B38EAA4799AB2A1837222E21C6EAF2C87ECB" }, { root="122A088010D2B6BC88E9658EE06C893DC02E3504B400D6486F9F13AC888698BB", hash="36687C628781978911AEC91FE95C249161BA11CCC804A4933751BE0B10CF780D" }, { root="260394945DACEDDF531AE01796278AEDD6C26A68FC56BAB05797EE5746B73D1C", hash="0A4BA7AB62B2C96987377860050687C7FCBD2DC0E0D24986EF128F928C655683" }, { root="3DC77E847676662685995955C8148F8B335624AF549863C108D5FE3A9AA38786", hash="E692A698B99E1136369B62401F4BB7B16098D7A0542DA7EF2438905C3F1E4B60" }, { root="4CB6B82860EF803C5CD77B18C3ECC9C2F414E28E1FDB6351DC165BA16D5D76EA", hash="A295A8E032EE234F1996311768712C86061322304F87F752A62D0B91717455B8" } ] }'
    message = MessageFactory.create_message(line)

    assert isinstance(message, FlushMessage)
    assert message.channel == "[::ffff:192.168.96.6]:17075"
    assert isinstance(message.confirm_req, ConfirmReqMessage)
    assert message.confirm_req.root_count == 7