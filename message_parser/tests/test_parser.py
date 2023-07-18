import tempfile
from src.parser import Parser, MessageFactory
from src.messages import *


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


def test_blockprocessor_message_identification():
    # Test that a blockprocessor log line is correctly identified
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    message = MessageFactory.create_message(line)
    assert isinstance(message, BlockProcessorMessage)


def test_blockprocessor_message_parsing():
    # Test that a blockprocessor message is correctly parsed
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    message = BlockProcessorMessage().parse(line)

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