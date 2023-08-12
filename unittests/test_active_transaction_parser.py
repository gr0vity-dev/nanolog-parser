from nanolog_parser.src.messages import *
from nanolog_parser.src.parser import MessageFactory

active_stopped_log = '[2023-07-28 10:49:27.298] [active_transactions] [trace] "active_stopped" election={ root="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4EF4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E", behaviour="hinted", state="expired_confirmed", confirmed=true, winner="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", tally_amount="199987308019747226638731596728893410000", final_tally_amount="149987308019747226638731596728893410000", blocks=[ { type="state", hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1690541363, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="C8563DF2ADE096D4551819C3F4178C359C2DF8C8FE121E46ECCA9F9BD6E85C43", previous="F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="616A9A2D255FCE81DDD3CBFF8DCD8DBB73B45007699D7393C4ABC9A442F6CDF6CC6987095153A287A55F6ED15CD8562B0CFF4A33872BDB12AAD169D43240FF03", work=11857312774462321081 } ], votes=[ { account="nano_137xfpc4ynmzj3rsf3nej6mzz33n3f7boj6jqsnxpgqw88oh8utqcq7nska8", time=5510392306330110, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_3sz3bi6mpeg5jipr1up3hotxde6gxum8jotr55rzbu9run8e3wxjq1rod9a6", time=5510394001887203, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_1ge7edbt774uw7z8exomwiu19rd14io1nocyin5jwpiit3133p9eaaxn74ub", time=5510391903726501, timestamp=18446744073709551615, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_3z93fykzixk7uoswh8fmx7ezefdo7d78xy8sykarpf7mtqi1w4tpg7ejn18h", time=5510391606052922, timestamp=1690541364592, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" }, { account="nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os", time=5510391606051222, timestamp=0, hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" } ], tally=[ { amount="199987308019747226638731596728893410000", hash="6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA" } ] }'
active_started_log = '[2023-07-28 10:49:26.805] [active_transactions] [trace] "active_started" election={ root="4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF", behaviour="normal", state="passive", confirmed=false, winner="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3", tally_amount="0", final_tally_amount="0", blocks=[ { type="state", hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3", sideband={ successor="0000000000000000000000000000000000000000000000000000000000000000", account="0000000000000000000000000000000000000000000000000000000000000000", balance="00000000000000000000000000000000", height=2, timestamp=1690541366, source_epoch="epoch_begin", details={ epoch="epoch_2", is_send=false, is_receive=false, is_epoch=false } }, account="9697595FE72336CD35206C0D708F6523CFD06C40D79B439C00C9CC41670FBEBF", previous="4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF", representative="39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835", balance="00000000000000000000000000000001", link="0000000000000000000000000000000000000000000000000000000000000000", signature="C1DE613980803B4D34E1DF2E4F750AEE782CBFB9F4A2F9D09C27A29E29F7A6591E3AA0B5671C7806E50327B11F5EE993ED41B1CD75ED1C08AFD8ABF0D8EB0509", work=5711933947752905247 } ], votes=[ { account="nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os", time=5510393808356621, timestamp=0, hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3" } ], tally=[ { amount="0", hash="578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3" } ] }'


def test_active_transactions_started_message_parsing():
    # line_started = '[2023-07-19 08:24:43.500] [active_transactions] [trace] "active_started" root="385D9F01FCEBBE15F123FA80AEC4D86EEA7991EBBCCB6370A0E4260E2B8B920A385D9F01FCEBBE15F123FA80AEC4D86EEA7991EBBCCB6370A0E4260E2B8B920A", hash="CE40A97D9ACA6A6890F28B076ADE1CC6001B0BA017D3A629D02D31F1B2C03A98", behaviour="normal"'

    log = MessageFactory.create_message(active_started_log)
    assert isinstance(log, ActiveStartedMessage)

    # Assertions for base attributes
    # Validate the parsed data
    assert log.log_timestamp == "2023-07-28 10:49:26.805"
    assert log.log_process == "active_transactions"
    assert log.log_level == "trace"
    assert log.log_event == "active_started"
    assert log.class_name == "ActiveStartedMessage"

    # Check election fields
    assert log.election["root"] == "4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF4026BE6A8459EE671C093F4AE1B6C05F13CF883827DA95548B471D78CA1E5CDF"
    assert log.election["behaviour"] == "normal"
    assert log.election["state"] == "passive"
    assert log.election["confirmed"] == False
    assert log.election["winner"] == "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3"
    assert log.election["tally_amount"] == "0"
    assert log.election["final_tally_amount"] == "0"

    # Check blocks
    for block in log.election["blocks"]:
        assert block["type"] == "state"
        assert block["hash"] == "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3"
        assert block["sideband"]["successor"] == "0000000000000000000000000000000000000000000000000000000000000000"
        assert block["sideband"]["account"] == "0000000000000000000000000000000000000000000000000000000000000000"
        assert block["sideband"]["balance"] == "00000000000000000000000000000000"
        assert block["sideband"]["height"] == 2
        assert block["sideband"]["timestamp"] == 1690541366
        assert block["sideband"]["source_epoch"] == "epoch_begin"
        assert block["sideband"]["details"]["epoch"] == "epoch_2"
        assert block["sideband"]["details"]["is_send"] == False
        assert block["sideband"]["details"]["is_receive"] == False
        assert block["sideband"]["details"]["is_epoch"] == False

    # Check votes
    for vote in log.election["votes"]:
        assert vote["account"] == "nano_18m7oo1r5gjqtcqyksk7qpwd3xpohj57nr88hktw1tc4o8n11pf9hjo8r4os"
        assert vote["time"] == 5510393808356621
        assert vote["timestamp"] == 0
        assert vote["hash"] == "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3"

    # Check tally
    for tally in log.election["tally"]:
        assert tally["amount"] == "0"
        assert tally["hash"] == "578BE2455A067B4F5796C76903CA19ADBA6CFEBB7A1969F0B5AD299DFE3CC0E3"


def test_active_transactions_stopped_message_parsing():
    # line_stopped = '[2023-07-19 08:24:43.749] [active_transactions] [trace] "active_stopped" root="68F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE3268F074B216C89322BC26ACB7AEA3BBE9928EF091A80CBD2B4008E1A731D8BE32", hashes=[ "77B0B617A49B12B6A5F1CE6D063337A1DD8B365EBCA1CD18FD92D761037D1F3E" ], behaviour="normal", confirmed=true'

    log = MessageFactory.create_message(active_stopped_log)
    assert isinstance(log, ActiveStoppedMessage)

    assert log.log_timestamp == '2023-07-28 10:49:27.298'
    assert log.log_process == 'active_transactions'
    assert log.log_level == 'trace'
    assert log.log_event == 'active_stopped'
    assert log.class_name == 'ActiveStoppedMessage'

    # Election asserts
    assert log.election["root"] == 'F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4EF4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E'
    assert log.election["behaviour"] == 'hinted'
    assert log.election["state"] == 'expired_confirmed'
    assert log.election["confirmed"] == True
    assert log.election["winner"] == '6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA'
    assert log.election["tally_amount"] == '199987308019747226638731596728893410000'
    assert log.election["final_tally_amount"] == '149987308019747226638731596728893410000'

    # Blocks asserts
    assert log.election["blocks"][0]["type"] == 'state'
    assert log.election["blocks"][0]["hash"] == '6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA'
    assert log.election["blocks"][0]["account"] == 'C8563DF2ADE096D4551819C3F4178C359C2DF8C8FE121E46ECCA9F9BD6E85C43'
    assert log.election["blocks"][0]["previous"] == 'F4E0F29524503FC2C794F90BF83B91F20834F331B776800A0DA350507B08CC4E'
    assert log.election["blocks"][0]["representative"] == '39870A8DC9C5D73DB1E53CBB69D5A4A59AAC46C579CB009D2D31C0BFD8058835'
    assert log.election["blocks"][0]["balance"] == '00000000000000000000000000000001'

    # Votes asserts
    assert log.election["votes"][0]["account"] == 'nano_137xfpc4ynmzj3rsf3nej6mzz33n3f7boj6jqsnxpgqw88oh8utqcq7nska8'
    assert log.election["votes"][0]["timestamp"] == 18446744073709551615
    assert log.election["votes"][0]["hash"] == '6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA'

    # Tally asserts
    assert log.election["tally"][0]["amount"] == '199987308019747226638731596728893410000'
    assert log.election["tally"][0]["hash"] == '6D42FB40A4DEDBD2A38CB18565E0AA4D17F1B81036CEB1A53D4DB8B4309748AA'
