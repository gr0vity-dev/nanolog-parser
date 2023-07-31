from src.parser import MessageFactory
from src.messages import *
from src.storage.impl.sqlite_storage import SQLiteStorage
import json
import random
import string
import json
from datetime import datetime, timedelta

COMMON_PROPERTIES = [
    'log_timestamp', 'log_process', 'log_level', 'log_event', 'log_file'
]
NETWORK_COMMON_PROPERTIES = COMMON_PROPERTIES + [
    'message_type', 'network', 'network_int', 'version', 'version_min',
    'version_max', 'extensions'
]


def test_store_activetransactionsstarted_message():
    line = '[2023-07-28 10:49:26.805] [active_transactions] [trace] "active_started" election={ root="4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF", behaviour="normal", state="passive", confirmed=false, winner="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3", tally_amount="0", final_tally_amount="0", blocks=[ { type="state", hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1690541366, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="9697595FE72336CD35206C0D708F6523CFD06C40D79B439C00C9CC41670FBEBF", previous="4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="C1DE613980803B4D34E1DF2E4F750AEE782CBFB9F4A2F9D09C27A29E29F7A6591E3AA0B5671C7806E50327B11F5EE993ED41B1CD75ED1C08AFD8ABF0D8EB0509", work=5711933947752905247 } ], votes=[ { account="nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os", time=5510393808356621, timestamp=0, hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3" } ], tally=[ { amount="0", hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3" } ] }'
    # line = '[2023-07-28 10:44:56.608] [active_transactions] [trace] "active_started" election={ root="0BF3D62DAEE43808555F6215A39EC7694120167F08FDDFD4C5E3C974107171A20BF3D62DAEE43808555F6215A39EC7694120167F08FDDFD4C5E3C974107171A2", behaviour="normal", state="passive", confirmed=false, winner="A4BC66235AB483E0F29D81EDBB062172B0C289825CDB827D590520CE06173153", tally_amount="100000000000000000000000000000000000000", final_tally_amount="0", blocks=[ { type="state", hash="A4BC66235AB483E0F29D81EDBB062172B0C289825CDB827D590520CE06173153", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1690541096, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="02A3D38D7B9A561BF61BC0D5D2C2C4F560A63B6AE8C04BEEF1E369A35D731995", previous="0BF3D62DAEE43808555F6215A39EC7694120167F08FDDFD4C5E3C974107171A2", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="F3B6E3E55A375FE1BDB8028A459EDE8864B18B91290DD38615E96BF5988AD4105C0D53B0A194AA0E70FAC8E9CCC41D015A643D3442B9A493EFE0C44CB0A7B303", work=10548966438472085574 } ], votes=[ { account="nano_3sz3bi6mpeg5jipr1up3hotxde6gxum8jotr55rzbu9run8e3wxjq1rod9a6", time=5510123611640189, timestamp=1690541095808, hash="A4BC66235AB483E0F29D81EDBB062172B0C289825CDB827D590520CE06173153" }, { account="nano_3z93fykzixk7uoswh8fmx7ezefdo7d78xy8sykarpf7mtqi1w4tpg7ejn18h", time=5510123611634459, timestamp=1690541093888, hash="A4BC66235AB483E0F29D81EDBB062172B0C289825CDB827D590520CE06173153" }, { account="nano_1qybcjn5tuyxz17fn73hkem4xfuip6t9pqjrh1bewnk7khm9qgnuzoh1gz8c", time=5510123611631209, timestamp=0, hash="A4BC66235AB483E0F29D81EDBB062172B0C289825CDB827D590520CE06173153" } ], tally=[ { amount="100000000000000000000000000000000000000", hash="A4BC66235AB483E0F29D81EDBB062172B0C289825CDB827D590520CE06173153" } ] }'
    properties = COMMON_PROPERTIES + ['root', 'behaviour', 'state',
                                      'confirmed', 'winner', 'tally_amount', 'final_tally_amount']
    store_message_test(line, ActiveStartedMessage, properties, [
                       'blocks', 'votes', 'tally'])


def test_store_asc_pull_req_message():
    line = '[2023-07-15 14:19:45.832] [network] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'
    properties = COMMON_PROPERTIES + [
        'id', 'start', 'start_type', 'count'
    ]
    store_message_test(line, AscPullReqMessage, properties, "headers")


def test_store_publish_message():
    line = '[2023-07-15 14:19:48.286] [network] [trace] "message_received" message={ header={ type="publish", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=1536 }, block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, PublishMessage, properties, ['blocks', 'headers'])


def test_store_blockprocessor_message():
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result="gap_previous", block={ type="state", hash="160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account="EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous="9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance="00000000000000000000000000000000", link="F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature="E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work=10530317739669255306 }, forced=false'
    properties = COMMON_PROPERTIES + ['result',  'hash', 'forced']
    store_message_test(line, BlockProcessedMessage, properties, "blocks")


def test_store_channel_sent():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-28 21:43:31.200] [channel] [trace] "message_sent" message={ header={ type="confirm_ack", network="test", network_int=21080, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="398562D3A2945BE17E6676B3E43603E160142A0A555E85071E5A10D04010D8EC", timestamp=18446744073709551615, hashes=[ "B0B14D451CDC5623A8376741B9B63811F77B64EDFEB281DE18D05E958BD6B225" ] } }, channel={ endpoint="[::ffff:192.168.112.6]:17075", peering_endpoint="[::ffff:192.168.112.6]:17075", node_id="2C4327C0B3B302D1696E84D52480890E6FD5373523BACDF39BE45FC88C33FC78", socket={ remote_endpoint="[::ffff:192.168.112.6]:17075", local_endpoint="[::ffff:192.168.112.4]:39184" } }'
    properties = COMMON_PROPERTIES + ['vote_count']
    store_message_test(line, ChannelConfirmAck, properties,
                       ['channels', 'votes', 'headers'])


def test_store_confirm_ack_message():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-15 14:19:44.951] [network] [trace] "message_received" message={ header={ type="confirm_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, vote={ account="399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D", timestamp=18446744073709551615, hashes=[ "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B" , "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD9229" ] } }'
    properties = COMMON_PROPERTIES + ['vote_count']
    store_message_test(line, ConfirmAckMessage, properties,
                       ['votes', 'headers'])


def test_store_message_confirm_req():
    # Define sample lines for each message type
    line = '[2023-07-15 14:19:44.805] [network] [trace] "message_received" message={ header={ type="confirm_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=4352 }, roots=[ { root="3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash="54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA" } , { root="3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash="54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEB" }] }'
    properties = COMMON_PROPERTIES + ['root_count']
    store_message_test(line, ConfirmReqMessage, properties,
                       ['roots', 'headers'])


def test_store_flush_message():
    line = '[2023-07-24 08:24:57.000] [confirmation_solicitor] [trace] "flush" channel="[::ffff:192.168.96.6]:17075", confirm_req={ header={ type="confirm_req", network="test", network_int=21080, version=19, version_min=18, version_max=19, extensions=28928 }, block=null, roots=[ { root="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", hash="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E" }, { root="C8084749BBD422A8C946E934FDE0702471F850B817D34450BB0FE5E574C9E56E", hash="5583791E4DE40CCA877E394F471E605C494DB038BD5F2FFB5AB41FE709F463E9" }, { root="18136735ACAECDC6AC775F3D739E5A10C5101C132F990EB6F338F2F1493ACD5B", hash="1DD6232FA752C96A6F20AF451003B38EAA4799AB2A1837222E21C6EAF2C87ECB" }, { root="122A088010D2B6BC88E9658EE06C893DC02E3504B400D6486F9F13AC888698BB", hash="36687C628781978911AEC91FE95C249161BA11CCC804A4933751BE0B10CF780D" }, { root="260394945DACEDDF531AE01796278AEDD6C26A68FC56BAB05797EE5746B73D1C", hash="0A4BA7AB62B2C96987377860050687C7FCBD2DC0E0D24986EF128F928C655683" }, { root="3DC77E847676662685995955C8148F8B335624AF549863C108D5FE3A9AA38786", hash="E692A698B99E1136369B62401F4BB7B16098D7A0542DA7EF2438905C3F1E4B60" }, { root="4CB6B82860EF803C5CD77B18C3ECC9C2F414E28E1FDB6351DC165BA16D5D76EA", hash="A295A8E032EE234F1996311768712C86061322304F87F752A62D0B91717455B8" } ] }'
    properties = COMMON_PROPERTIES + ['channel', 'root_count']
    store_message_test(line, FlushMessage, properties, ['headers', 'roots'])


def test_store_activetransactionsstopped_message():
    # line = '[2023-07-19 08:24:43.749] [active_transactions] [trace] "active_stopped" root="68F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE3268F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE32", hashes=[ "77B0B617A49B12B6A5F1CE6D063337A1DD8B365EBCA1CD18FD92D761037D1F3E" ], behaviour="normal", confirmed=true'
    line = '[2023-07-28 10:49:27.298] [active_transactions] [trace] "active_stopped" election={ root="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4EF4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E", behaviour="hinted", state="expired_confirmed", confirmed=true, winner="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", tally_amount="199987308019747226638731596728893410000", final_tally_amount="149987308019747226638731596728893410000", blocks=[ { type="state", hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1690541363, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="C8563DF2ADE096D4551819C3F4178C359C2DF8C8FE121E46ECCA9F9BD6E85C43", previous="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="616A9A2D255FCE81DDD3CBFF8DCD8DBB73B45007699D7393C4ABC9A442F6CDF6CC6987095153A287A55F6ED15CD8562B0CFF4A33872BDB12AAD169D43240FF03", work=11857312774462321081 } ], votes=[ { account="nano_137xfpc4ynmzj3rsf3nej6mzz33n3f7boj6jqsnxpgqw88oh8utqcq7nska8", time=5510392306330110, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_3sz3bi6mpeg5jipr1up3hotxde6gxum8jotr55rzbu9run8e3wxjq1rod9a6", time=5510394001887203, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_1ge7edbt774uw7z8exomwiu19rd14io1nocyin5jwpiit3133p9eaaxn74ub", time=5510391903726501, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_3z93fykzixk7uoswh8fmx7ezefdo7d78xy8sykarpf7mtqi1w4tpg7ejn18h", time=5510391606052922, timestamp=1690541364592, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os", time=5510391606051222, timestamp=0, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" } ], tally=[ { amount="199987308019747226638731596728893410000", hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" } ] }'
    properties = COMMON_PROPERTIES + ['root', 'behaviour', 'state',
                                      'confirmed', 'winner', 'tally_amount', 'final_tally_amount']
    store_message_test(line, ActiveStoppedMessage, properties,
                       ['blocks', 'votes', 'tally'])


def test_store_asc_pull_ack_message():
    line = '[2023-07-15 14:19:45.772] [network] [trace] "message_received" message={ header={ type="asc_pull_ack", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=218 }, type="blocks", id=9247237627708466530, blocks=[ { type="state", hash="A14902C8746C2098DEE0B537D28E9ACC57968124A68DA4C2BC642EBDDB201740", account="0000000000000000000000000000000000000000000000000000000000000DDE", previous="108C3F1CA6420149648BB083FDB14CB46AA690494A2B9E9F6BF56FB245F9D2E2", representative="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", link="65706F636820763220626C6F636B000000000000000000000000000000000000", signature="FC196E0D7C1F5FA1E38277F8E5CF154365B3C8914C946A3355BC11ED3011AC475898A6332A86404D88D83051D9F814FDC71F51816C8DD2584A464CB40CCA5F07", work=7926988356349568187 } ] }'
    properties = COMMON_PROPERTIES + ['id']
    store_message_test(line, AscPullAckMessage,
                       properties, ['headers', 'blocks'])


def test_store_nodeprocessconfirmed_message():
    line = '[2023-07-18 20:46:14.798] [node] [trace] "process_confirmed" block={ type="state", hash="85EE57C6AB8E09FFDD1E656F47F7CC6598ADD48BE2F7B9F8B811CD9096E77C06", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1689713164, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="4005DB9BB6BC221383E80FBA1D5924C73580EA8573349513DA2EFA30F2D1A23C", previous="2A38C093945A920DC68F35F45195A88446A37E58F110FF022C71FD61C10D4D1C", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="7A3D8EC7DA648010853C3F7BEEC8D6E760B7C8CC940D8393362068558A086230DFF14D1ED88921E41EEFE5AD57D66D2332D1250159758AFA31943CEA2B137D02", work=2438566069390192728 }'
    properties = COMMON_PROPERTIES
    store_message_test(line, ProcessConfirmedMessage, properties, 'blocks')


def test_store_keepalive_message():
    line = '[2023-07-15 14:19:44.867] [network] [trace] "message_received" message={ header={ type="keepalive", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=0 }, peers=[ "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075", "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075", "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075", "[::ffff:13.213.221.153]:7075" ] }'
    properties = COMMON_PROPERTIES
    store_message_test(line, KeepAliveMessage,
                       properties, ['headers', 'peers'])


# def test_store_random_confirm_ack():
#     # Create a ConfirmAckMessage instance
#     initial_data = {
#         "log_timestamp": "2023-07-15 14:19:44.951",
#         "log_process": "network",
#         "log_level": "trace",
#         "log_file": None,
#         "log_event": "message_received",
#         "message": {
#             "header": {
#                 "type": "confirm_ack",
#                 "network": "live",
#                 "network_int": 21059,
#                 "version": 19,
#                 "version_min": 18,
#                 "version_max": 19,
#                 "extensions": 4352
#             },
#             "vote": {
#                 "account": "399385203231BC15F0DFB54A28152F03912A084285BB1ED83437DEF8C7F4815D",
#                 "timestamp": 18446744073709551615,
#                 "hashes": [
#                     "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD922B",
#                     "58FF212FF44F1E7CEC4AEE6F9FAE3F9EBCC03D2EDA12BA25E26E4C0F3DBD9229"
#                 ]
#             }
#         },
#         "class_name": "ConfirmAckMessage",
#         "vote_type": "final"
#     }

#     message = ConfirmAckMessage(initial_data)
#     assert isinstance(message, ConfirmAckMessage)

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

#     cursor = storage.repository.conn.cursor()
#     cursor.execute(f"SELECT COUNT(*) FROM message_links where relation_type = 'blocks' ")
#     row_count = cursor.fetchone()[0]

#     assert row_count == 1, f"Table {message.class_name.lower()} is empty after storing message."

#     # Query the hashes
#     cursor.execute(
#         f"SELECT hash FROM hashes INNER JOIN message_links ON hashes.id = message_links.relation_id WHERE message_id = ? AND message_type = ? AND relation_type = ?;",
#         (stored_message_dict['sql_id'], message.class_name.lower(), "hashes"))
#     stored_hashes = [item[0] for item in cursor.fetchall()]

#     assert stored_hashes == message.hashes


# def test_store_many_confirm_ack():
#     num_messages = 1000  # number of messages to store

#     # Create a SQLiteStorage instance
#     storage = SQLiteStorage(':memory:')

#     # Store all message ids and hashes for later check
#     all_message_hashes = {}

#     # Create and store multiple messages
#     for i in range(num_messages):
#         # Create a ConfirmAckMessage instance and assign random values to the fields
#         message = ConfirmAckMessage()
#         message.log_timestamp = random_timestamp()
#         message.log_process = 'network'
#         message.log_level = 'trace'
#         message.log_event = 'message_received'
#         message.message_type = 'confirm_ack'
#         message.network = 'live'
#         message.network_int = random.randint(0, 65535)
#         message.version = random.randint(0, 65535)
#         message.version_min = random.randint(0, 65535)
#         message.version_max = random.randint(0, 65535)
#         message.extensions = random.randint(0, 65535)
#         message.account = random_hex_string(64)
#         message.timestamp = random.randint(0, 2**63 - 1)
#         message.hashes = [
#             random_hex_string(64) for _ in range(random.randint(1, 12))
#         ]

#         # Store the message
#         storage.store_message(message)
#         all_message_hashes[i + 1] = message.hashes

#     # Retrieve all stored messages
#     cursor = storage.repository.conn.cursor()
#     cursor.execute(f"SELECT * FROM {message.class_name.lower()};")
#     stored_messages = cursor.fetchall()

#     # Check if the number of stored messages is correct
#     assert len(stored_messages) == num_messages

#     # Check if all hashes related to the messages are stored correctly
#     for message_id, hashes in all_message_hashes.items():
#         cursor.execute(
#             f"SELECT hash FROM hashes INNER JOIN message_links ON hashes.id = message_links.relation_id WHERE message_id = ? AND message_type = ? AND relation_type = ?;",
#             (message_id, message.class_name.lower(), "hashes"))
#         stored_hashes = [item[0] for item in cursor.fetchall()]
#         assert sorted(stored_hashes) == sorted(hashes)


################################
################################
################################
################################

# Non TEST HELPER FUNCTIONS

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
                       relationships,
                       filename=None):
    # Create a Message instance and parse the line using MessageFactory
    message = MessageFactory.create_message(line, filename)
    assert isinstance(message, message_class)
    print(message.__dict__)

    # Create a SQLiteStorage instance
    storage = SQLiteStorage(':memory:')

    # Store the message
    storage.store_message(message)

    # Check if the stored data is correct
    assert_data_in_table(storage, message_class, message, properties)
    if isinstance(relationships, list):
        for relation in relationships:
            assert_related_entities_in_table(storage, message_class, relation)
    else:
        assert_related_entities_in_table(storage, message_class, relationships)


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
        if hasattr(message, property):
            assert stored_message_dict[property] == getattr(message, property)
        else:
            assert property in stored_message_dict
