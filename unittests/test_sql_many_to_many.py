from nanolog_parser.src.parser import MessageFactory
from nanolog_parser.src.messages import *
from nanolog_parser.src.storage.impl.sqlite_storage import SQLiteStorage
import json
import random
import string
import json
from datetime import datetime, timedelta

COMMON_PROPERTIES = [
    'log_timestamp', 'log_process', 'log_level', 'log_event', 'log_file'
]


def test_store_vote_generator_candidate_processed():
    line = '[2023-09-16 13:58:51.500] [vote_generator] [trace] "candidate_processed" block: { type: "state", hash: "3148EE89AB3FA8160529DB5944793E4663BE3FE916F7A9E1DA25DFD653816F96", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1694872731, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "7366B9475FA7E32376FC8DBBD34CE1CF00EAEEC551E44B4A37789570D5A9E941", previous: "252C463EB226E0F69C2BF52BFB5D107017F58D31E605C3074E27B4D759317F0A", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "9DBCB1C7964183B208AC02987B87FF83F428930CBCF694676F09AF85AF1EF4883BB6E68E6F96FE85C3FD2E8242F84E450CF8CFB9E66A2CBCF7164A118693DF08", work: 17116006658840245070 }, should_vote: true, is_final: true'
    properties = COMMON_PROPERTIES + \
        ["should_vote", "is_final"]
    store_message_test(line, VoteGeneratorCandidateProcessedMessage,
                       properties, ["blocks"])


def test_store_election_broadcast_vote():
    line = '[2023-09-16 13:58:51.513] [election] [trace] "broadcast_vote" id: "0x1000000000000009", root: "140F5ACD288ADF2A9A8FC9914B487955AAEDA65ED13B7DFF22913028A296F635140F5ACD288ADF2A9A8FC9914B487955AAEDA65ED13B7DFF22913028A296F635", winner: { type: "state", hash: "21AA9813223311735AB5AFF52B0A35FA1FE714F96059FCF314B8536558D30487", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1694872731, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "44871D0C6AA2E3EB7D7A64FFCADB99F35EB289AFE03282FE9B6C3C2CCF40B11E", previous: "140F5ACD288ADF2A9A8FC9914B487955AAEDA65ED13B7DFF22913028A296F635", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "B8609848EF65235A9A3432AE77656853247CF8DB6C30C13ACED148F9618337AF2B4C95438B4FC29F20C97388D57948364C13C1132B621797F29B5035582EDB0B", work: 10057957971934593070 }, type: "normal"'
    properties = COMMON_PROPERTIES + \
        ["id", "root", "type", "winner_hash"]
    store_message_test(line, ElectionBroadcastVoteMessage,
                       properties, ["winners"])


def test_store_block_activated():
    line = '[2023-09-16 13:58:51.235] [election_scheduler] [trace] "block_activated" account: "nano_3tu5e5q8znsp4rt565u9kf5t5uqqrb6tzcpf3c3stwpb44exb4ikm96x5faw", block: { type: "state", hash: "F44B5B0C51C5D723E95E92AE2E3D0BF004AE8E8073E7300D827683B6E81676F4", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1694872731, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "EB6360EE6FD3361634320F679347A1EEF7C249AFAACD0A839D72C91099D48A12", previous: "37AC550FD7E723749CCDD8C3EE5D6E142A2E0E0CFD1D1B66FA8E2C0FCA2D2754", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "73A7ACD8720CC4E2178332A03A49DCE9BBE6C891246579C347AAE1813E7B6030E9BBCABB0F2576FEDB2EEE462FD06F62E198773FB5B0021FE1045B3F2C56DE0F", work: 5750271876189529300 }, time: 1694872731, priority: "1"'
    properties = COMMON_PROPERTIES + \
        ["account", "time", "priority"]
    store_message_test(line, SchedulerBlockActivatedMessage,
                       properties, "blocks")


def test_store_votesprocessed_message():
    line = '[2023-07-28 21:44:20.599] [vote_processor] [debug] Processed 107 votes in 702 milliseconds (rate of 152 votes per second)'
    properties = COMMON_PROPERTIES + \
        ["blocks_processed", "process_duration", "process_rate"]
    store_message_test(line, VotesProcessedMessage,
                       properties, [])


def test_store_bulkpush_message_sent():
    line = '[2023-07-28 21:45:58.801] [channel_send] [trace] "bulk_push" message: { header: { type: "bulk_push", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 } }, channel: { endpoint: "[::ffff:192.168.112.4]:17075", peering_endpoint: "[::ffff:192.168.112.4]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.112.4]:17075", local_endpoint: "[::ffff:192.168.112.2]:50174" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, BulkPushMessageSent,
                       properties, ["headers", "channels"])


def test_store_bulkpush_message_dropped():
    line = '[2023-07-28 21:45:58.801] [channel_send] [trace] "bulk_push" message: { header: { type: "bulk_push", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 } }, channel: { endpoint: "[::ffff:192.168.112.4]:17075", peering_endpoint: "[::ffff:192.168.112.4]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.112.4]:17075", local_endpoint: "[::ffff:192.168.112.2]:50174" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, BulkPushMessageSent,
                       properties, ["headers", "channels"])


def test_store_frontierreq_message_sent():
    line = '[2023-07-28 21:43:29.697] [channel_send] [trace] "frontier_req" message: { header: { type: "frontier_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 }, start: "0000000000000000000000000000000000000000000000000000000000000000", age: 1690584209, count: 1048576 }, channel: { endpoint: "[::ffff:192.168.112.3]:17075", peering_endpoint: "[::ffff:192.168.112.3]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.112.3]:17075", local_endpoint: "[::ffff:192.168.112.2]:43144" } }'
    properties = COMMON_PROPERTIES + \
        ["message_start", "message_age", "message_count"]
    store_message_test(line, FrontierReqMessageSent,
                       properties, ["headers", "channels"])


def test_store_frontierreq_message_dropped():
    line = '[2023-07-28 21:43:29.697] [channel_send] [trace] "frontier_req" message: { header: { type: "frontier_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 }, start: "0000000000000000000000000000000000000000000000000000000000000000", age: 1690584209, count: 1048576 }, channel: { endpoint: "[::ffff:192.168.112.3]:17075", peering_endpoint: "[::ffff:192.168.112.3]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.112.3]:17075", local_endpoint: "[::ffff:192.168.112.2]:43144" } }'
    properties = COMMON_PROPERTIES + \
        ["message_start", "message_age", "message_count"]
    store_message_test(line, FrontierReqMessageSent,
                       properties, ["headers", "channels"])


def test_store_bulkpullaccount_message_sent():
    line = '[2023-07-28 21:45:58.801] [channel_send] [trace] "bulk_pull_account" message: { header: { type: "bulk_pull_account", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 }, account: "04BD6D942F527F887196868C8927FF84340B4A9AC491BE69DB3AFC31AAF36F57", minimum_amount: "000000000000D3C21BCECCEDA1000000", flags: 0 }, channel: { endpoint: "[::ffff:192.168.112.4]:17075", peering_endpoint: "[::ffff:192.168.112.4]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.112.4]:17075", local_endpoint: "[::ffff:192.168.112.2]:46806" } }'
    properties = COMMON_PROPERTIES + \
        ["message_account", "message_minimum_amount", "message_flags"]
    store_message_test(line, BulkPullAccountMessageSent,
                       properties, ["headers", "channels"])


def test_store_bulkpullaccount_message_dropped():
    line = '[2023-07-28 21:45:58.801] [channel_send] [trace] "bulk_pull_account" message: { header: { type: "bulk_pull_account", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 }, account: "04BD6D942F527F887196868C8927FF84340B4A9AC491BE69DB3AFC31AAF36F57", minimum_amount: "000000000000D3C21BCECCEDA1000000", flags: 0 }, channel: { endpoint: "[::ffff:192.168.112.4]:17075", peering_endpoint: "[::ffff:192.168.112.4]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.112.4]:17075", local_endpoint: "[::ffff:192.168.112.2]:46806" } }'
    properties = COMMON_PROPERTIES + \
        ["message_account", "message_minimum_amount", "message_flags"]
    store_message_test(line, BulkPullAccountMessageSent,
                       properties, ["headers", "channels"])


def test_store_electionconfirmed():
    line = '[2023-07-28 21:43:45.598] [election] [trace] "election_confirmed" root: "AE75487D696A575088A5A43B927DFE86C6B5EA831B015DAE50C1525AE16A8C56AE75487D696A575088A5A43B927DFE86C6B5EA831B015DAE50C1525AE16A8C56"'
    properties = COMMON_PROPERTIES + ["root"]
    store_message_test(line, ElectionConfirmedMessage, properties, [])


def test_store_bulkpullaccountclient_requesting_pending():
    line = '[2023-07-28 21:45:58.801] [bulk_pull_account_client] [trace] "requesting_pending" account: "nano_137xfpc4ynmzj3rsf3nej6mzz33n3f7boj6jqsnxpgqw88oh8utqcq7nska8", connection: { endpoint: "[::ffff:192.168.112.4]:17075", peering_endpoint: "[::ffff:192.168.112.4]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.112.4]:17075", local_endpoint: "[::ffff:192.168.112.2]:46806" } }'
    properties = COMMON_PROPERTIES + ["account"]
    store_message_test(line, BulkPullAccountPendingMessage,
                       properties, ['channels'])


def test_store_telemetryack_message():
    line = '[2023-07-28 21:44:39.700] [network_processed] [trace] "telemetry_ack" message: { header: { type: "telemetry_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 202 }, data: {  } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, TelemetryAckMessageReceived,
                       properties, ['headers'])


def test_store_telemetryack_message_sent():
    line = '[2023-07-28 21:44:44.698] [channel_send] [trace] "telemetry_ack" message: { header: { type: "telemetry_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 202 }, data: {  } }, channel: { endpoint: "[::ffff:192.168.112.2]:17075", peering_endpoint: "[::ffff:192.168.112.2]:17075", node_id: "83886EC0215F7478F0881C93295E3F43EB901CDD36EBD2EF1932DF484C66E66C", socket: { remote_endpoint: "[::ffff:192.168.112.2]:17075", local_endpoint: "[::ffff:192.168.112.4]:53356" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, TelemetryAckMessageSent,
                       properties, ['headers', 'channels'])


def test_store_telemetryack_message_dropped():
    line = '[2023-07-28 21:44:44.698] [channel_send] [trace] "telemetry_ack" message: { header: { type: "telemetry_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 202 }, data: {  } }, channel: { endpoint: "[::ffff:192.168.112.2]:17075", peering_endpoint: "[::ffff:192.168.112.2]:17075", node_id: "83886EC0215F7478F0881C93295E3F43EB901CDD36EBD2EF1932DF484C66E66C", socket: { remote_endpoint: "[::ffff:192.168.112.2]:17075", local_endpoint: "[::ffff:192.168.112.4]:53356" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, TelemetryAckMessageSent,
                       properties, ['headers', 'channels'])


def test_store_frontierreqserver_sending_frontier():
    line = '[2023-07-28 21:44:30.998] [frontier_req_server] [trace] "sending_frontier" account: "nano_11117xgbaut51xi54napargzjf15xjur5ecci7ach44o1qdpe61j7wfnyeur", frontier: "4B9B28EC618E9FDE3624617AA87373F06D99CEBBBF3FF2A17394C6459666C760", socket: { remote_endpoint: "[::ffff:192.168.112.6]:43808", local_endpoint: "0.0.0.0:0" }'
    properties = COMMON_PROPERTIES + \
        ["account", "frontier", "socket_remote_endpoint"]
    store_message_test(line, SendingFrontierMessage, properties, [])


def test_store_voteprocessor_vote_processed():
    line = '[2023-07-28 21:44:30.898] [vote_processor] [trace] "vote_processed" vote: { account: "FCE16FA5F87645DD73C799B3E959F635752ACA6EF8D9F4918B34B3D5E00E0B56", timestamp: 18446744073709551615, hashes: [ "5912FE306C30AA02BC3DB510E3DDF13D20B10668E752F3E4D747AE52986917CF", "5EE94C3EBA336F8FBA9E4E77AEB01458055F49630B5A383561E94855A147ADA0", "63E3E1BCFFBED281A9BD856AA8DCF563D77C79C3D11086E4A1BB66510BDB7D8D", "6C5E7A0FEA151212E6774B84A2374597B787AE76DE0A5E3EFBF49471EAFC280E", "6FE109CFE4B2E516A071DA6EC4CB376EE52ECC2A6072113A5F7218944FB5AFF4", "74C3A83929535174DE33D7065BB1C07596E9A6F0C2B56DFBBFF5A93C9CA26AF2", "7AB9FC09B094E33523E7F84F1A13F72405E828EC80ACC0363377B248479507E6", "7B06CA33CBDA21F13C591A7BFC681205329D9EAECBE81A285044A117D340E6BE", "83EE9BE0892D78FF5C45BFD4D9351A4C5640ABB1EAA671F1AE83D2D6541FFA11", "A2EE0BDF7E5E8086913D3028E5578536442E369B0194CC5D438964C88430B7DD", "A72CEDE3AFA2117D3F7D1F3BA864B64651A373DB0670AA97B242ACDEAB9D6088", "AFFCF32C4E0F6C666A94F826BD8856F350286AE48460E4E592A59FB6519284CD" ] }, status: "replay"'
    properties = COMMON_PROPERTIES + ["status"]
    store_message_test(line, VoteProcessedMessage,
                       properties, ['votes'])


def test_store_telemetryreq_message():
    line = '[2023-07-28 21:44:30.798] [network_processed] [trace] "telemetry_req" message: { header: { type: "telemetry_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, TelemetryReqMessageReceived,
                       properties, ['headers'])


def test_store_telemetryreq_message_sent():
    line = '[2023-07-28 21:44:24.697] [channel_send] [trace] "telemetry_req" message: { header: { type: "telemetry_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 } }, channel: { endpoint: "[::ffff:192.168.112.5]:17075", peering_endpoint: "[::ffff:192.168.112.5]:17075", node_id: "14C42D84999ED4D36A885B0B1E0B8AE9032F1EBCAE6037DD6652A0B8C6EECD8C", socket: { remote_endpoint: "[::ffff:192.168.112.5]:17075", local_endpoint: "[::ffff:192.168.112.4]:37168" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, TelemetryReqMessageSent,
                       properties, ['headers', 'channels'])


def test_store_telemetryreq_message_dropped():
    line = '[2023-07-28 21:44:24.697] [channel_send] [trace] "telemetry_req" message: { header: { type: "telemetry_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 } }, channel: { endpoint: "[::ffff:192.168.112.5]:17075", peering_endpoint: "[::ffff:192.168.112.5]:17075", node_id: "14C42D84999ED4D36A885B0B1E0B8AE9032F1EBCAE6037DD6652A0B8C6EECD8C", socket: { remote_endpoint: "[::ffff:192.168.112.5]:17075", local_endpoint: "[::ffff:192.168.112.4]:37168" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, TelemetryReqMessageSent,
                       properties, ['headers', 'channels'])


def test_store_nodeidhandshake_message_sent():
    line = '[2023-09-21 20:55:07.026] [channel_send] [trace] "node_id_handshake" message: { header: { type: "node_id_handshake", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 5 }, query: { cookie: "FF31CEB8AA8CA6D38CB9A632485007C0F94AD867E3D50FB2E8C20AC282163FD5" }, response: null }, channel: { endpoint: "[::ffff:192.168.0.3]:17075", peering_endpoint: "[::ffff:192.168.0.3]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.0.3]:17075", local_endpoint: "[::ffff:192.168.0.4]:45670" } }, dropped: false, traffic_type: "generic", drop_policy: "limiter", size: 40, should_pass: true, id: "0x10612683faa8b9a6"'
    properties = COMMON_PROPERTIES + ['should_pass']
    store_message_test(line, NodeIdHandshakeMessageSent,
                       properties, ['headers', 'queries', 'channels'])


def test_store_nodeidhandshake_message_dropped():
    line = '[2023-07-28 21:43:24.607] [channel_send] [trace] "node_id_handshake" message: { header: { type: "node_id_handshake", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 5 }, query: { cookie: "F2767A2293D3706CCF1D984EC55820A10373AB74CBCB4FF71D127005DC13E417" }, response: null }, channel: { endpoint: "[::ffff:192.168.112.4]:17075", peering_endpoint: "[::ffff:192.168.112.4]:17075", node_id: "0000000000000000000000000000000000000000000000000000000000000000", socket: { remote_endpoint: "[::ffff:192.168.112.4]:17075", local_endpoint: "[::ffff:192.168.112.4]:59604" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, NodeIdHandshakeMessageSent,
                       properties, ['headers', 'queries', 'channels'])


def test_store_nodeidhandshake_message():
    line = '[2023-07-28 21:43:24.698] [network_processed] [trace] "node_id_handshake" message: { header: { type: "node_id_handshake", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 6 }, query: null, response: { node_id: "01F4C307028F5118F449AFED64DB25F5D7469E48312010429E90BA0B1274F607", signature: "F038FEF309DB85B07D163DDC184A5A2619A17D7E5ED30AAB536F3476734CDA800E14377117CF9CD6D3B466C028F7DA8C013B40C59EAFCC8FB8FFD4F80EF1C104", v2: true, salt: "29D1D1BA6B2487E98379CBEAC6D3EB7921612EAEA24C00E2DDE7A58B09D050ED", genesis: "E670DF81878460B76B3425EC399800E1219A4387B11A4841B16CE260A9F36917" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, NodeIdHandshakeMessageReceived,
                       properties, ['headers', 'responses'])


def test_store_activetransactionsstarted_message():
    # line = '[2023-07-28 10:49:26.805] [active_transactions] [trace] "active_started" election: { root: "4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF", behaviour: "normal", state: "passive", confirmed: false, winner: "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3", tally_amount: "0", final_tally_amount: "0", blocks: [ { type: "state", hash: "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1690541366, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "9697595FE72336CD35206C0D708F6523CFD06C40D79B439C00C9CC41670FBEBF", previous: "4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "C1DE613980803B4D34E1DF2E4F750AEE782CBFB9F4A2F9D09C27A29E29F7A6591E3AA0B5671C7806E50327B11F5EE993ED41B1CD75ED1C08AFD8ABF0D8EB0509", work: 5711933947752905247 } ], votes: [ { account: "nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os", time: 5510393808356621, timestamp: 0, hash: "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3" } ], tally: [ { amount: "0", hash: "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3" } ] }'
    line = '[2023-09-20 20:26:23.704] [active_transactions] [trace] "active_started" election: { id: "0x5f750ffbb8afb7ee", root: "783DF67B392DA26786DA0A3446C492CF265AAB208FBFFAD9F8EF1C12FC3029D1783DF67B392DA26786DA0A3446C492CF265AAB208FBFFAD9F8EF1C12FC3029D1", behaviour: "normal", state: "passive", confirmed: false, winner: "998D308FB0F0D2EF8FEBEC796A9326F114783EFAD7C6AB00155D5420929203E7", tally_amount: "0", final_tally_amount: "0", blocks: [ { type: "state", hash: "998D308FB0F0D2EF8FEBEC796A9326F114783EFAD7C6AB00155D5420929203E7", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1695241582, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "E4AB88CB24094C3BCBDB5B6AB87C16BA16BAE848B3ADEE5DDBAB6D0F76DEED6E", previous: "783DF67B392DA26786DA0A3446C492CF265AAB208FBFFAD9F8EF1C12FC3029D1", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "EECFF3501834CFA28D801B5950B6AA27918426FB5E6D8A5ADC4F706AF2C619D84FCCFE9B1CC9247B0331E24D926C8F8616E89B4A7DF74079A079C88097D09406", work: 16381090310514122296 } ], votes: [ { account: "nano_3cti94ds36kiomw97ohs59i5hjwikmxxch3yyxd7hq7qwju1kpnm68u1mkx9", time: 10210610707626732, timestamp: 0, hash: "998D308FB0F0D2EF8FEBEC796A9326F114783EFAD7C6AB00155D5420929203E7" } ], tally: [ { amount: "0", hash: "998D308FB0F0D2EF8FEBEC796A9326F114783EFAD7C6AB00155D5420929203E7" } ] }'
    properties = COMMON_PROPERTIES + ['election_root', 'election_behaviour', 'election_state',
                                      'election_confirmed', 'election_winner', 'election_tally_amount', 'election_final_tally_amount']
    store_message_test(line, ActiveStartedMessage, properties, [
                       'blocks', 'votes', 'tallies'])


def test_store_asc_pull_req_message():
    # line = '[2023-07-15 14:19:45.832] [network_processed] [trace] "message_received" message={ header={ type="asc_pull_req", network="live", network_int=21059, version=19, version_min=18, version_max=19, extensions=34 }, id=12094529471189612132, start="62D480D111E8D81423BEAD85C869AD22AE1430D7BA11A4A1158F7FF316AB5EC0", start_type="account", count=128 }'
    line = '[2023-08-08 19:10:00.332] [network_processed] [trace] "asc_pull_req" message: { header: { type: "asc_pull_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 34 }, type: "blocks", id: 11410925922915025654, start: "42FBDAD4C956B3E2B6576E85C14CFA6D4C8A007E497BDDAA6F23E741A3F56CFF", start_type: "block", count: 128 }'
    properties = COMMON_PROPERTIES + [
        'message_id', 'message_start', 'message_start_type', 'message_type', 'message_count'
    ]
    store_message_test(line, AscPullReqMessageReceived, properties, "headers")


def test_store_asc_pull_req_message_sent():
    line = '[2023-07-28 21:44:01.800] [channel_send] [trace] "asc_pull_req" message: { header: { type: "asc_pull_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 34 }, type: "blocks", id: 1057808171588687116, start: "1C5A1CDCE811281296FAED6252834B33DDA28B97B7E0C5A01B710F5452BA32FD", start_type: "block", count: 128 }, channel: { endpoint: "[::ffff:192.168.112.6]:17075", peering_endpoint: "[::ffff:192.168.112.6]:17075", node_id: "2C4327C0B3B302D1696E84D52480890E6FD5373523BACDF39BE45FC88C33FC78", socket: { remote_endpoint: "[::ffff:192.168.112.6]:17075", local_endpoint: "[::ffff:192.168.112.4]:39184" } }'
    properties = COMMON_PROPERTIES + [
        'message_id', 'message_start', 'message_start_type', 'message_type', 'message_count'
    ]
    store_message_test(line, AscPullReqMessageSent, properties, "headers")


def test_store_asc_pull_req_message_dropped():
    line = '[2023-07-28 21:44:01.800] [channel_send] [trace] "asc_pull_req" message: { header: { type: "asc_pull_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 34 }, type: "blocks", id: 1057808171588687116, start: "1C5A1CDCE811281296FAED6252834B33DDA28B97B7E0C5A01B710F5452BA32FD", start_type: "block", count: 128 }, channel: { endpoint: "[::ffff:192.168.112.6]:17075", peering_endpoint: "[::ffff:192.168.112.6]:17075", node_id: "2C4327C0B3B302D1696E84D52480890E6FD5373523BACDF39BE45FC88C33FC78", socket: { remote_endpoint: "[::ffff:192.168.112.6]:17075", local_endpoint: "[::ffff:192.168.112.4]:39184" } }'
    properties = COMMON_PROPERTIES + [
        'message_id', 'message_start', 'message_start_type', 'message_count'
    ]
    store_message_test(line, AscPullReqMessageSent, properties, "headers")


def test_store_publish_message():
    line = '[2023-07-15 14:19:48.286] [network_processed] [trace] "publish" message: { header: { type: "publish", network: "live", network_int: 21059, version: 19, version_min: 18, version_max: 19, extensions: 1536 }, block: { type: "state", hash: "160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account: "EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous: "9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative: "F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance: "00000000000000000000000000000000", link: "F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature: "E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work: 10530317739669255306 } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, PublishMessageReceived,
                       properties, ['blocks', 'headers'])


def test_store_publish_message_dropped():
    line = '[2023-07-28 21:43:53.099] [channel_send] [trace] "publish" message: { header: { type: "publish", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 1536 }, block: { type: "state", hash: "26A645EC0B4EA9FF2E3D556AD2D4A894DAF0424656FBDC524F16966A268F5FB8", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "28BA0D116F68E073D5023FBA2B969512328DF3409828BB7E089F25C270C2C504", balance: "00000000000000000000000000000000", height: 2, timestamp: 1690580632, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "28BA0D116F68E073D5023FBA2B969512328DF3409828BB7E089F25C270C2C504", previous: "0BEA1DFA4AFEEF283BDBECD0DA9DB5BDDF33523A11A785B6F817A311E1C13BC8", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "030FA339C757E6793CB8C614974DE3973E6AB47CD166E076943495A898D42B068BECB2B58724957F2A8C57C83C33C8F97E72886C3539BCE0CB22E51AC8034008", work: 1801355901321131775 } }, channel: { endpoint: "[::ffff:192.168.112.1]:60088", peering_endpoint: "[::ffff:192.168.112.1]:60088", node_id: "BD1AD5D83E319486B35C09B2BAFB2982C74ADF64E271267BF074BD9D5FBA21F9", socket: { remote_endpoint: "[::ffff:192.168.112.1]:60088", local_endpoint: "0.0.0.0:0" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, PublishMessageSent,
                       properties, ['blocks', 'headers'])


def test_store_publish_message_sent():
    line = '[2023-07-28 21:43:53.099] [channel_send] [trace] "publish" message: { header: { type: "publish", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 1536 }, block: { type: "state", hash: "544DB945325DDE4571AA7C71501ADBC7E09D20463E4D8D55FAE17E418F370174", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "E1A87D265A51194059F41F0601FE7BD2F5F985C2CA71253FDD83F5CD11F70F12", balance: "00000000000000000000000000000000", height: 2, timestamp: 1690580632, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "E1A87D265A51194059F41F0601FE7BD2F5F985C2CA71253FDD83F5CD11F70F12", previous: "C9BAA873949BE8E1C892112A3AFCF6C2903A8C3CF6E5C4DDDE43E36E0E13ADA1", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "88B30AEED44A89650BD6603FA1C7A3FD926DB8C92A7E33139F0ED646B658789E659B9D00203BDE0417894CFFD24AB8291FF2335B514EC7156EFD8B007826CC03", work: 11859225291316017006 } }, channel: { endpoint: "[::ffff:192.168.112.2]:17075", peering_endpoint: "[::ffff:192.168.112.2]:17075", node_id: "83886EC0215F7478F0881C93295E3F43EB901CDD36EBD2EF1932DF484C66E66C", socket: { remote_endpoint: "[::ffff:192.168.112.2]:17075", local_endpoint: "[::ffff:192.168.112.4]:53356" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, PublishMessageSent,
                       properties, ['blocks', 'headers'])


def test_store_blockprocessor_message():
    line = '[2023-07-15 14:19:48.287] [blockprocessor] [trace] "block_processed" result: "gap_previous", block: { type: "state", hash: "160F1EF61CFC73D2DBF2B249AA38B9965BF441EEF4312E9A89BDB58A22CF32FE", account: "EBB66C545B0ED5F248256E281E13B09829518435C4C05E705BB70F2DF625E060", previous: "9C490F4525EA5E6EAA4E76869B7073D5BD452D11B2CEB6CC34353856519D2075", representative: "F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", balance: "00000000000000000000000000000000", link: "F11A22A0340C7931C6C6288280A0F6ACF8F052BED2C929493883388B1776ADA2", signature: "E7B0E3315C52085F4EB4C00462B3394983B84216860370B50DF85A17664CEB58ED76F0EA2699BBFFD15BB84578681C4A5E0FCA67685BB882F80C329C5C818F0D", work: 10530317739669255306 }, forced: false'
    properties = COMMON_PROPERTIES + ['result',  'block_hash', 'forced']
    store_message_test(line, BlockProcessedMessage, properties, "blocks")


def test_store_channel_confirm_ack_sent():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-09-21 18:46:17.021] [channel_send] [trace] "confirm_ack" message: { header: { type: "confirm_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 49408 }, vote: { account: "FCE16FA5F87645DD73C799B3E959F635752ACA6EF8D9F4918B34B3D5E00E0B56", timestamp: 18446744073709551615, hashes: [ "9428AC5276BE6B472F9F2F5D5BF5DA54FB115D56381EA6A1DD0DE2FB85D5DF87", "94D061A755E4326DE80B3DD59FDB1838D17E491672FACBAF675100AC031CB469", "94D84AF8DDB93620B1D450C374F6EC683906D49618D541791A5621CBB5C2E06A", "9571FFA2C8DBE78AC5F3C9885F90EEEE6CECBC9E2F86F812FFE598BAD0EC3906", "DC3A2A6D9FD1D13D016682526D1B7036DA71B630C0164B9A771C68AF4E58564C", "DC6919FB2FE7877756F74D51ED85E96D45B0C4C6E1B29ABCABC4253456E3A151", "DC6B3E381DCE14E3CC88085FD2C70C1AD027E98156BCC75C7265F096A7964731", "DDDCBEE0355A09AA418C078122B91D611B9B28CD9A22329E84F8CA8892F8F2ED", "DDF99ECD7CF0872B78B1A4C8F5C7140BC4AF82D66AEC60EBAC11B69B02356FC7", "DE05C7D52E3497E260E44BB16B8B6F288BC2FC2CC5BEF24284133BAFB603128A", "DE2123905856D639DCF92B0D583344F27E273D850B3F43AD48504C009E52A9F0", "DE2BD30164EFAE1B7E7A921B84E3A69342E99A2F92337AE7FF4FD1502210EC1C" ] } }, channel: { endpoint: "[::ffff:172.27.0.1]:37830", peering_endpoint: "[::ffff:172.27.0.1]:37830", node_id: "474106DCA927E027F001D4759E1BCAD8565538C53D45F76E1794E429EA5FC1FA", socket: { remote_endpoint: "[::ffff:172.27.0.1]:37830", local_endpoint: "[::ffff:172.27.0.2]:17075" } }, dropped: false, traffic_type: "generic", drop_policy: "limiter", size: 496, should_pass: true, id: "0xdca79c3587702283"'
    properties = COMMON_PROPERTIES + \
        ['vote_count', 'vote_type', 'should_pass', 'dropped', 'size', 'id']
    store_message_test(line, ConfirmAckMessageSent, properties,
                       ['channels', 'votes', 'headers'])


def test_store_channel_confirm_ack_dropped():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-28 21:43:53.098] [channel_send] [trace] "confirm_ack" message: { header: { type: "confirm_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 49408 }, vote: { account: "398562D3A2945BE17E6676B3E43603E160142A0A555E85071E5A10D04010D8EC", timestamp: 18446744073709551615, hashes: [ "D77928ECA942C446D2E06482F6C05298AB148D24EC8FC0C5291A2481FFAD53F7", "DDD5605F877DEB8902132AB50B260B367AFAE077CBB2FBCFAF587E4505F3534D", "EA54DB6E95F09A0874C838CFDBF6BC92C91AB56701EB7E84056CF14965219A0C", "0A08CA811C16306DC6A44DE54637CE30229DE981361FF000CEF842EF47AA2509", "15F1DC741F8CE9491F74F9BDFF0CA7A03AD3EBCF46529DE946284EA52DEC64DD", "0FE6899C2BA9011F7BEEBBD772C2EF8348D0512F37534A0A4F94D4975BE87AA3", "1784660C4AE0719A7B4BFBB6F4670705BD99C13AC8EC323674F9B646AE5C1379", "27573AB772EEEDAB5314DFA844D751A1DC8F05B2545A3FD2B2756CAD5CE6B1D2", "1AA6AF5340E60E24FC9A577BE3C2FB4821F6EA9E917CEC2F5EFE61AC6ADF8A00", "127C62164928CFDACAEEF206E7108EE848CEA0C549844283BB6823C310B82293", "04DAF88213074A732534D464E22587E80EB99895346B129571D192C5319E0D2B", "08F118F921F1343533B5221B96A44CA9CA1E20924B6FDC7DD17D11E35E1B268E" ] } }, channel: { endpoint: "[::ffff:192.168.112.2]:17075", peering_endpoint: "[::ffff:192.168.112.2]:17075", node_id: "83886EC0215F7478F0881C93295E3F43EB901CDD36EBD2EF1932DF484C66E66C", socket: { remote_endpoint: "[::ffff:192.168.112.2]:17075", local_endpoint: "[::ffff:192.168.112.4]:53356" } }'
    properties = COMMON_PROPERTIES + ['vote_count', 'vote_type']
    store_message_test(line, ConfirmAckMessageSent, properties,
                       ['channels', 'votes', 'headers'])


def test_store_channel_confirm_req_sent():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-28 21:43:24.608] [channel_send] [trace] "confirm_req" message: { header: { type: "confirm_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 4352 }, block: null, roots: [ { root: "C608ECAD5CEEB3432C1BD591C16A67BD696178203D1BB02DCA277DE84238EF86", hash: "6D922AC3916233A76E8774614F330AE6AC9FD506EF2220EA1C0A74869F7AC203" } ] }, channel: { endpoint: "[::ffff:192.168.112.3]:17075", peering_endpoint: "[::ffff:192.168.112.3]:17075", node_id: "01F4C307028F5118F449AFED64DB25F5D7469E48312010429E90BA0B1274F607", socket: { remote_endpoint: "[::ffff:192.168.112.3]:17075", local_endpoint: "[::ffff:192.168.112.4]:41232" } }'
    properties = COMMON_PROPERTIES + ['root_count']
    store_message_test(line, ConfirmReqMessageSent, properties,
                       ['channels', 'roots', 'headers'])


def test_store_channel_confirm_req_dropped():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-07-28 21:43:52.798] [channel_send] [trace] "confirm_req" message: { header: { type: "confirm_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 28928 }, block: null, roots: [ { root: "518AFED5363B5CCBD0D9CCAB2B503DEA1D431DED6BF497E6B691B65A0F71A1EA", hash: "F3227AD806C3401C33D2BAC7C0FBCDB776B64D6756B8941E5F24C16B33BA7BD7" }, { root: "521568D573AF82192400C4515DD2E5F49A7AF0BCB9C9B80BEA82C99503B93859", hash: "2599637F9B63CD39AB8973EE9A75D8D40835CB6D3E5DF6E8B5FA8E02BA8F3C40" }, { root: "53A7057EC5FC18EDC9CB1615066D653D8BEC0E4CF3B0210D64E6C97933A45F05", hash: "EFCE772A06C91C38F9D0E58A024E7528C07A3FB884A9E98F484D6CBBC6B86820" }, { root: "554242624CEEBC4F67D47D810604B191C802F1CA5E5904318AFED3C5E0B9B504", hash: "2EE0B6FB86B25FA54D23C3AE4029F363F655221A030444FD3FC1C82ED3BCE38F" }, { root: "5B31558F8A4BA4A30E9C9CCD3CBAD7D1ABE7A67346329B80AEA66B538A4A4D54", hash: "4ADCA99AFD15ADF788645E602F46C44D4825876795EF118D68680D5FC4269A91" }, { root: "5E26F63E89A22BE344B1A1727C05BBEA90BBEACAF3E92C1B3F2AC5294DE4EA53", hash: "814B0818D65A9FCEB722D9A3A7F3E69B78FD06AD423EB7789C7C8C8EC3AD8282" }, { root: "60FD6A69B83790C65FFF9D9B7E7A8AE8EC117E1F7F871E9AE3BD19056FBE9CDD", hash: "F9DC56ACA6257F78C6701B815320BB2933482B0E0A4373DABD312D55FE55F220" } ] }, channel: { endpoint: "[::ffff:192.168.112.6]:17075", peering_endpoint: "[::ffff:192.168.112.6]:17075", node_id: "2C4327C0B3B302D1696E84D52480890E6FD5373523BACDF39BE45FC88C33FC78", socket: { remote_endpoint: "[::ffff:192.168.112.6]:17075", local_endpoint: "[::ffff:192.168.112.4]:39184" } }'
    properties = COMMON_PROPERTIES + ['root_count']
    store_message_test(line, ConfirmReqMessageSent, properties,
                       ['channels', 'roots', 'headers'])


def test_store_confirm_ack_message():
    # Prepare a sample ConfirmAckMessage
    line = '[2023-09-16 14:55:26.709] [network_processed] [trace] "confirm_ack" message: { header: { type: "confirm_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 49408 }, vote: { account: "04BD6D942F527F887196868C8927FF84340B4A9AC491BE69DB3AFC31AAF36F57", timestamp: 1694876126713, hashes: [ "78B11BCD0BF111384F55BB35278440D359277B76617D09A7B9CE9B9D926091AC", "7B4701FD467FCCECE440BF282018BC378DC86437785271096DCD1D2B44AC3AB5", "7F8C2635A34955EA5309132AAD50283A8572F84F7DA52C99516B90C35F571B80", "832C898424A5BB8CE14E3A0034458088A6FCBDCFA937BB3D725C0F5A1F5CC90B", "8551457C54F1D71E62023F61945FB20AA6D462825F99316621ECD3694A38242E", "863217576F3355A7BE405E8E2891D190C83977CF15307326BCD4A3A0A08E8881", "87200E8C3271228AEF3C0308657C786CA12E4E85C66B6B1F142294DF0064B814", "898F8FE37B73231EE172B5B7FE447B3BFC7F752AB55E7DA594EFED4FE4C3E11F", "8C7117D649362D0AB2A4AEF870B39CC6014242099642F7939E9D7280273F8421", "8C78C19FB1555918E45F6E5ADCEFB4EA53464D33CB58680EBBDF919DD3D622C1", "8D55C5FA0E3C82317DFCAEC8D8BF42735B74937425C59296B54C1FBC903CA585", "8DCA1298D8885AE6219C0A013F5189F2B5678844B13E4F5AFBD09ECA511C5B6E" ] } }'
    properties = COMMON_PROPERTIES + ['vote_count']
    store_message_test(line, ConfirmAckMessageReceived, properties,
                       ['votes', 'headers'])


def test_store_message_confirm_req():
    # Define sample lines for each message type
    line = '[2023-07-15 14:19:44.805] [network_processed] [trace] "confirm_req" message: { header: { type: "confirm_req", network: "live", network_int: 21059, version: 19, version_min: 18, version_max: 19, extensions: 4352 }, roots: [ { root: "3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash: "54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEA" } , { root: "3903175F5E19C5D772319EC9EB2B8BC4728F669EA4F7DD22BB6699D0A8CA455D", hash: "54108799F7FBC6ABCCEF37D7761B019F3FA86DDE8F094AB57BDA1CFE588F3FEB" }] }'
    properties = COMMON_PROPERTIES + ['root_count']
    store_message_test(line, ConfirmReqMessageReceived, properties,
                       ['roots', 'headers'])


def test_store_flush_message():
    line = '[2023-07-24 08:24:57.000] [confirmation_solicitor] [trace] "flush" channel: "[::ffff:192.168.96.6]:17075", confirm_req: { header: { type: "confirm_req", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 28928 }, block: null, roots: [ { root: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", hash: "F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E" }, { root: "C8084749BBD422A8C946E934FDE0702471F850B817D34450BB0FE5E574C9E56E", hash: "5583791E4DE40CCA877E394F471E605C494DB038BD5F2FFB5AB41FE709F463E9" }, { root: "18136735ACAECDC6AC775F3D739E5A10C5101C132F990EB6F338F2F1493ACD5B", hash: "1DD6232FA752C96A6F20AF451003B38EAA4799AB2A1837222E21C6EAF2C87ECB" }, { root: "122A088010D2B6BC88E9658EE06C893DC02E3504B400D6486F9F13AC888698BB", hash: "36687C628781978911AEC91FE95C249161BA11CCC804A4933751BE0B10CF780D" }, { root: "260394945DACEDDF531AE01796278AEDD6C26A68FC56BAB05797EE5746B73D1C", hash: "0A4BA7AB62B2C96987377860050687C7FCBD2DC0E0D24986EF128F928C655683" }, { root: "3DC77E847676662685995955C8148F8B335624AF549863C108D5FE3A9AA38786", hash: "E692A698B99E1136369B62401F4BB7B16098D7A0542DA7EF2438905C3F1E4B60" }, { root: "4CB6B82860EF803C5CD77B18C3ECC9C2F414E28E1FDB6351DC165BA16D5D76EA", hash: "A295A8E032EE234F1996311768712C86061322304F87F752A62D0B91717455B8" } ] }'
    properties = COMMON_PROPERTIES + ['channel', 'root_count']
    store_message_test(line, FlushMessage, properties, ['headers', 'roots'])


def test_store_activetransactionsstopped_message():
    # line = '[2023-07-19 08:24:43.749] [active_transactions] [trace] "active_stopped" root="68F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE3268F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE32", hashes=[ "77B0B617A49B12B6A5F1CE6D063337A1DD8B365EBCA1CD18FD92D761037D1F3E" ], behaviour="normal", confirmed=true'
    line = '[2023-07-28 10:49:27.298] [active_transactions] [trace] "active_stopped" election: { root: "F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4EF4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E", behaviour: "hinted", state: "expired_confirmed", confirmed: true, winner: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", tally_amount: "199987308019747226638731596728893410000", final_tally_amount: "149987308019747226638731596728893410000", blocks: [ { type: "state", hash: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1690541363, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "C8563DF2ADE096D4551819C3F4178C359C2DF8C8FE121E46ECCA9F9BD6E85C43", previous: "F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "616A9A2D255FCE81DDD3CBFF8DCD8DBB73B45007699D7393C4ABC9A442F6CDF6CC6987095153A287A55F6ED15CD8562B0CFF4A33872BDB12AAD169D43240FF03", work: 11857312774462321081 } ], votes: [ { account: "nano_137xfpc4ynmzj3rsf3nej6mzz33n3f7boj6jqsnxpgqw88oh8utqcq7nska8", time: 5510392306330110, timestamp: 18446744073709551615, hash: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account: "nano_3sz3bi6mpeg5jipr1up3hotxde6gxum8jotr55rzbu9run8e3wxjq1rod9a6", time: 5510394001887203, timestamp: 18446744073709551615, hash: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account: "nano_1ge7edbt774uw7z8exomwiu19rd14io1nocyin5jwpiit3133p9eaaxn74ub", time: 5510391903726501, timestamp: 18446744073709551615, hash: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account: "nano_3z93fykzixk7uoswh8fmx7ezefdo7d78xy8sykarpf7mtqi1w4tpg7ejn18h", time: 5510391606052922, timestamp: 1690541364592, hash: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account: "nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os", time: 5510391606051222, timestamp: 0, hash: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" } ], tally: [ { amount: "199987308019747226638731596728893410000", hash: "6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" } ] }'
    properties = COMMON_PROPERTIES + ['election_root', 'election_behaviour', 'election_state',
                                      'election_confirmed', 'election_winner', 'election_tally_amount', 'election_final_tally_amount']
    store_message_test(line, ActiveStoppedMessage, properties,
                       ['blocks', 'votes', 'tallies'])


def test_store_asc_pull_ack_message():
    line = '[2023-07-28 21:45:05.699] [network_processed] [trace] "asc_pull_ack" message: { header: { type: "asc_pull_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 1 }, type: "blocks", id: 10086824885354889104, blocks: [ { type: "state", hash: "DCF722205BEF1AFEB2BBACC151DC929289B8ADC481FA9A97E488E431D8D61FCD", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1690580620, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "FBC87CEB29775E4E15CA1F8D5E41C044A8097DC5D8486A094AB4A9BEFDFD62FD", previous: "EB18C2D46C8A5BABD7E15AE702A090E57C06A1522F0C585CEF23F52D8309807F", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "32DC1BB053B223450FC474FDF98063F44B1F2CAB36B9F6F738C88FD7D6C22A387D37C93D91F96407039E994E316D892693CBAB458CF06F6F7F7038636AF0CA0A", work: 816583997527443416 } ] }, channel: { endpoint: "[::ffff:172.25.0.2]:17075", peering_endpoint: "[::ffff:172.25.0.2]:17075", node_id: "26E58749092BF0BFAC1004F784C389A8CAE8A461CDA7837ADFBC149860CD7764", socket: { remote_endpoint: "[::ffff:172.25.0.2]:17075", local_endpoint: "[::ffff:172.25.0.4]:37556" } }, dropped: false'
    properties = COMMON_PROPERTIES + ['message_id', "message_type"]
    store_message_test(line, AscPullAckMessageReceived,
                       properties, ['headers', 'blocks'])


def test_store_asc_pull_ack_message_sent():
    line = '[2023-07-28 21:44:01.597] [channel_send] [trace] "asc_pull_ack" message: { header: { type: "asc_pull_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 218 }, type: "blocks", id: 16573031038784598660, blocks: [ { type: "state", hash: "DCF722205BEF1AFEB2BBACC151DC929289B8ADC481FA9A97E488E431D8D61FCD", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1690580620, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "FBC87CEB29775E4E15CA1F8D5E41C044A8097DC5D8486A094AB4A9BEFDFD62FD", previous: "EB18C2D46C8A5BABD7E15AE702A090E57C06A1522F0C585CEF23F52D8309807F", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "32DC1BB053B223450FC474FDF98063F44B1F2CAB36B9F6F738C88FD7D6C22A387D37C93D91F96407039E994E316D892693CBAB458CF06F6F7F7038636AF0CA0A", work: 816583997527443416 } ] }, channel: { endpoint: "[::ffff:192.168.112.6]:17075", peering_endpoint: "[::ffff:192.168.112.6]:17075", node_id: "2C4327C0B3B302D1696E84D52480890E6FD5373523BACDF39BE45FC88C33FC78", socket: { remote_endpoint: "[::ffff:192.168.112.6]:17075", local_endpoint: "[::ffff:192.168.112.4]:39184" } }'
    properties = COMMON_PROPERTIES + ['message_id', "message_type"]
    store_message_test(line, AscPullAckMessageSent,
                       properties, ['headers', 'blocks', 'channels'])


def test_store_asc_pull_ack_message_dropped():
    line = '[2023-07-28 21:44:01.597] [channel_send] [trace] "asc_pull_ack" message: { header: { type: "asc_pull_ack", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 218 }, type: "blocks", id: 16573031038784598660, blocks: [ { type: "state", hash: "DCF722205BEF1AFEB2BBACC151DC929289B8ADC481FA9A97E488E431D8D61FCD", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1690580620, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "FBC87CEB29775E4E15CA1F8D5E41C044A8097DC5D8486A094AB4A9BEFDFD62FD", previous: "EB18C2D46C8A5BABD7E15AE702A090E57C06A1522F0C585CEF23F52D8309807F", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "32DC1BB053B223450FC474FDF98063F44B1F2CAB36B9F6F738C88FD7D6C22A387D37C93D91F96407039E994E316D892693CBAB458CF06F6F7F7038636AF0CA0A", work: 816583997527443416 } ] }, channel: { endpoint: "[::ffff:192.168.112.6]:17075", peering_endpoint: "[::ffff:192.168.112.6]:17075", node_id: "2C4327C0B3B302D1696E84D52480890E6FD5373523BACDF39BE45FC88C33FC78", socket: { remote_endpoint: "[::ffff:192.168.112.6]:17075", local_endpoint: "[::ffff:192.168.112.4]:39184" } }'
    properties = COMMON_PROPERTIES + ['message_id', "message_type"]
    store_message_test(line, AscPullAckMessageSent,
                       properties, ['headers', 'blocks', 'channels'])


def test_store_nodeprocessconfirmed_message():
    line = '[2023-07-18 20:46:14.798] [node] [trace] "process_confirmed" block: { type: "state", hash: "85EE57C6AB8E09FFDD1E656F47F7CC6598ADD48BE2F7B9F8B811CD9096E77C06", sideband: { successor: "0000000000000000000000000000000000000000000000000000000000000000", account: "0000000000000000000000000000000000000000000000000000000000000000", balance: "00000000000000000000000000000000", height: 2, timestamp: 1689713164, source_epoch: "epoch_begin", details: { epoch: "epoch_2", is_send: false, is_receive: false, is_epoch: false } }, account: "4005DB9BB6BC221383E80FBA1D5924C73580EA8573349513DA2EFA30F2D1A23C", previous: "2A38C093945A920DC68F35F45195A88446A37E58F110FF022C71FD61C10D4D1C", representative: "39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance: "00000000000000000000000000000001", link: "0000000000000000000000000000000000000000000000000000000000000000", signature: "7A3D8EC7DA648010853C3F7BEEC8D6E760B7C8CC940D8393362068558A086230DFF14D1ED88921E41EEFE5AD57D66D2332D1250159758AFA31943CEA2B137D02", work: 2438566069390192728 }'
    properties = COMMON_PROPERTIES
    store_message_test(line, ProcessConfirmedMessage, properties, 'blocks')


def test_store_keepalive_message():
    line = '[2023-07-15 14:19:44.867] [network_processed] [trace] "keepalive" message: { header: { type: "keepalive", network: "live", network_int: 21059, version: 19, version_min: 18, version_max: 19, extensions: 0 }, peers: [ "[::ffff:94.130.135.50]:7075", "[::]:0", "[::ffff:174.138.4.198]:7075", "[::ffff:54.77.3.59]:7075", "[::ffff:139.180.168.194]:7075", "[::ffff:98.35.209.116]:7075", "[::ffff:154.26.158.112]:7075", "[::ffff:13.213.221.153]:7075" ] }'
    properties = COMMON_PROPERTIES
    store_message_test(line, KeepAliveMessageReceived,
                       properties, ['headers', 'peers'])


def test_store_keepalive_message_sent():
    line = '[2023-07-28 21:43:54.697] [channel_send] [trace] "keepalive" message: { header: { type: "keepalive", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 }, peers: [ "[::ffff:192.168.112.6]:17075", "[::ffff:192.168.112.5]:17075", "[::ffff:192.168.112.2]:17075", "[::]:0", "[::]:0", "[::]:0", "[::]:0", "[::]:0" ] }, channel: { endpoint: "[::ffff:192.168.112.1]:60100", peering_endpoint: "[::ffff:192.168.112.1]:60100", node_id: "1F0BCFE8B38CB6EEA797246258D221320EF6605B04E5E6804B513D7AA939FC9C", socket: { remote_endpoint: "[::ffff:192.168.112.1]:60100", local_endpoint: "0.0.0.0:0" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, KeepAliveMessageSent,
                       properties, ['headers', 'peers', 'channels'])


def test_store_keepalive_message_dropped():
    line = '[2023-07-28 21:43:54.697] [channel_send] [trace] "keepalive" message: { header: { type: "keepalive", network: "test", network_int: 21080, version: 19, version_min: 18, version_max: 19, extensions: 0 }, peers: [ "[::ffff:192.168.112.6]:17075", "[::ffff:192.168.112.5]:17075", "[::ffff:192.168.112.2]:17075", "[::]:0", "[::]:0", "[::]:0", "[::]:0", "[::]:0" ] }, channel: { endpoint: "[::ffff:192.168.112.1]:60100", peering_endpoint: "[::ffff:192.168.112.1]:60100", node_id: "1F0BCFE8B38CB6EEA797246258D221320EF6605B04E5E6804B513D7AA939FC9C", socket: { remote_endpoint: "[::ffff:192.168.112.1]:60100", local_endpoint: "0.0.0.0:0" } }'
    properties = COMMON_PROPERTIES
    store_message_test(line, KeepAliveMessageSent,
                       properties, ['headers', 'peers', 'channels'])


def test_channel_send_result():
    line = '[2023-09-21 20:55:07.027] [channel_send_result] [trace] "confirm_req" error: "system:0", size: 72, id: "0x438cae1d2ad4964f", success: true\n"'
    properties = COMMON_PROPERTIES
    store_message_test(line, ChannelSendResultMessage,
                       properties, [])

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
    assert_data_in_table(storage, message, properties)
    if isinstance(relationships, list):
        for relation in relationships:
            assert_related_entities_in_table(
                storage, message.class_name, relation)
    else:
        assert_related_entities_in_table(
            storage, message.class_name, relationships)


def assert_related_entities_in_table(storage, message_class_name, relationship):

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
        assert stored_message_entities_list[i]['relation_id'] == entity['sql_id']
        assert stored_message_entities_list[i][
            'message_type'] == message_class_name.lower()
        assert stored_message_entities_list[i]['message_id'] == 1
        assert stored_message_entities_list[i]['relation_id'] == i + 1


def assert_data_in_table(storage, message, properties):
    # Retrieve the stored message
    cursor = storage.repository.conn.cursor()
    cursor.execute(f"SELECT * FROM {message.class_name.lower()};")

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
            assert stored_message_dict[property] is not None, f"{property} is None"
