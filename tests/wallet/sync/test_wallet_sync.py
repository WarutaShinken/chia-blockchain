from typing import List, Optional, Set

import pytest
from colorlog import getLogger

from chia.consensus.block_record import BlockRecord
from chia.consensus.block_rewards import calculate_base_farmer_reward, calculate_pool_reward
from chia.full_node.full_node_api import FullNodeAPI
from chia.protocols import full_node_protocol, wallet_protocol
from chia.protocols.protocol_message_types import ProtocolMessageTypes
from chia.protocols.shared_protocol import Capability
from chia.protocols.wallet_protocol import RequestAdditions, RespondAdditions, RespondBlockHeaders, SendTransaction
from chia.server.outbound_message import Message
from chia.simulator.simulator_protocol import FarmNewBlockProtocol
from chia.types.peer_info import PeerInfo
from chia.util.hash import std_hash
from chia.util.ints import uint16, uint32, uint64
from chia.wallet.transaction_record import TransactionRecord
from chia.wallet.util.wallet_types import AmountWithPuzzlehash
from tests.connection_utils import disconnect_all_and_reconnect
from tests.pools.test_pool_rpc import wallet_is_synced
from tests.setup_nodes import test_constants
from tests.time_out_assert import time_out_assert
from chia.wallet.wallet_coin_record import WalletCoinRecord

def wallet_height_at_least(wallet_node, h):
    height = wallet_node.wallet_state_manager.blockchain.get_peak_height()
    if height == h:
        return True
    return False


log = getLogger(__name__)


class TestWalletSync:
    @pytest.mark.asyncio
    async def test_request_block_headers(self, bt, wallet_node, default_1000_blocks):
        # Tests the edge case of receiving funds right before the recent blocks  in weight proof
        full_node_api: FullNodeAPI
        full_node_api, wallet_node, full_node_server, wallet_server = wallet_node

        wallet = wallet_node.wallet_state_manager.main_wallet
        ph = await wallet.get_new_puzzlehash()
        for block in default_1000_blocks[:100]:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(10), uint32(15), False)
        )
        assert msg.type == ProtocolMessageTypes.respond_block_headers.value
        res_block_headers = RespondBlockHeaders.from_bytes(msg.data)
        bh = res_block_headers.header_blocks
        assert len(bh) == 6
        assert [x.reward_chain_block.height for x in default_1000_blocks[10:16]] == [
            x.reward_chain_block.height for x in bh
        ]

        assert [x.foliage for x in default_1000_blocks[10:16]] == [x.foliage for x in bh]

        assert [x.transactions_filter for x in bh] == [b"\x00"] * 6

        num_blocks = 20
        new_blocks = bt.get_consecutive_blocks(
            num_blocks, block_list_input=default_1000_blocks, pool_reward_puzzle_hash=ph
        )
        for i in range(0, len(new_blocks)):
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(new_blocks[i]))

        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(110), uint32(115), True)
        )
        res_block_headers = RespondBlockHeaders.from_bytes(msg.data)
        bh = res_block_headers.header_blocks
        assert len(bh) == 6

    # @pytest.mark.parametrize(
    #     "test_case",
    #     [(1000000, 10000010, False, ProtocolMessageTypes.reject_block_headers)],
    #     [(80, 99, False, ProtocolMessageTypes.respond_block_headers)],
    #     [(10, 8, False, None)],
    # )
    @pytest.mark.asyncio
    async def test_request_block_headers_rejected(self, bt, wallet_node, default_1000_blocks):
        # Tests the edge case of receiving funds right before the recent blocks  in weight proof
        full_node_api: FullNodeAPI
        full_node_api, wallet_node, full_node_server, wallet_server = wallet_node

        # start_height, end_height, return_filter, expected_res = test_case

        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(1000000), uint32(1000010), False)
        )
        assert msg.type == ProtocolMessageTypes.reject_block_headers.value

        for block in default_1000_blocks[:150]:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(80), uint32(99), False)
        )
        assert msg.type == ProtocolMessageTypes.respond_block_headers.value
        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(10), uint32(8), False)
        )
        assert msg.type == ProtocolMessageTypes.reject_block_headers.value

        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(10), uint32(8), True)
        )
        assert msg.type == ProtocolMessageTypes.reject_block_headers.value

        # test for 128 blocks to fetch at once limit
        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(10), uint32(140), True)
        )
        assert msg.type == ProtocolMessageTypes.reject_block_headers.value

        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(90), uint32(160), False)
        )
        assert msg.type == ProtocolMessageTypes.reject_block_headers.value
        msg = await full_node_api.request_block_headers(
            wallet_protocol.RequestBlockHeaders(uint32(90), uint32(160), True)
        )
        assert msg.type == ProtocolMessageTypes.reject_block_headers.value

    @pytest.mark.parametrize(
        "two_wallet_nodes",
        [
            dict(
                disable_capabilities=[Capability.BLOCK_HEADERS],
            ),
            dict(
                disable_capabilities=[Capability.BASE],
            ),
        ],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_basic_sync_wallet(self, bt, two_wallet_nodes, default_400_blocks, self_hostname):
        full_nodes, wallets = two_wallet_nodes
        full_node_api = full_nodes[0]
        full_node_server = full_node_api.full_node.server

        # Trusted node sync
        wallets[0][0].config["trusted_peers"] = {full_node_server.node_id.hex(): full_node_server.node_id.hex()}

        # Untrusted node sync
        wallets[1][0].config["trusted_peers"] = {}

        for block in default_400_blocks:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        for wallet_node, wallet_server in wallets:
            await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_server._port)), None)

        for wallet_node, wallet_server in wallets:
            await time_out_assert(100, wallet_height_at_least, True, wallet_node, len(default_400_blocks) - 1)

        # Tests a reorg with the wallet
        num_blocks = 30
        blocks_reorg = bt.get_consecutive_blocks(num_blocks, block_list_input=default_400_blocks[:-5])
        for i in range(1, len(blocks_reorg)):
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(blocks_reorg[i]))

        for wallet_node, wallet_server in wallets:
            await disconnect_all_and_reconnect(wallet_server, full_node_server, self_hostname)

        for wallet_node, wallet_server in wallets:
            await time_out_assert(
                100, wallet_height_at_least, True, wallet_node, len(default_400_blocks) + num_blocks - 5 - 1
            )

    @pytest.mark.parametrize(
        "two_wallet_nodes",
        [
            dict(
                disable_capabilities=[Capability.BLOCK_HEADERS],
            ),
            dict(
                disable_capabilities=[Capability.BASE],
            ),
        ],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_almost_recent(self, bt, two_wallet_nodes, default_400_blocks, self_hostname):
        # Tests the edge case of receiving funds right before the recent blocks  in weight proof
        full_nodes, wallets = two_wallet_nodes
        full_node_api = full_nodes[0]
        full_node_server = full_node_api.full_node.server

        # Trusted node sync
        wallets[0][0].config["trusted_peers"] = {full_node_server.node_id.hex(): full_node_server.node_id.hex()}

        # Untrusted node sync
        wallets[1][0].config["trusted_peers"] = {}

        base_num_blocks = 400
        for block in default_400_blocks:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))
        all_blocks = default_400_blocks
        both_phs = []
        for wallet_node, wallet_server in wallets:
            wallet = wallet_node.wallet_state_manager.main_wallet
            both_phs.append(await wallet.get_new_puzzlehash())

        for i in range(20):
            # Tests a reorg with the wallet
            ph = both_phs[i % 2]
            all_blocks = bt.get_consecutive_blocks(1, block_list_input=all_blocks, pool_reward_puzzle_hash=ph)
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(all_blocks[-1]))

        new_blocks = bt.get_consecutive_blocks(
            test_constants.WEIGHT_PROOF_RECENT_BLOCKS + 10, block_list_input=all_blocks
        )
        for i in range(base_num_blocks + 20, len(new_blocks)):
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(new_blocks[i]))

        for wallet_node, wallet_server in wallets:
            wallet = wallet_node.wallet_state_manager.main_wallet
            await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_server._port)), None)
            await time_out_assert(30, wallet.get_confirmed_balance, 10 * calculate_pool_reward(uint32(1000)))

    @pytest.mark.asyncio
    async def test_backtrack_sync_wallet(self, two_wallet_nodes, default_400_blocks, self_hostname):
        full_nodes, wallets = two_wallet_nodes
        full_node_api = full_nodes[0]
        full_node_server = full_node_api.full_node.server

        # Trusted node sync
        wallets[0][0].config["trusted_peers"] = {full_node_server.node_id.hex(): full_node_server.node_id.hex()}

        # Untrusted node sync
        wallets[1][0].config["trusted_peers"] = {}

        for block in default_400_blocks[:20]:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        for wallet_node, wallet_server in wallets:
            await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_server._port)), None)

        for wallet_node, wallet_server in wallets:
            await time_out_assert(100, wallet_height_at_least, True, wallet_node, 19)

    # Tests a reorg with the wallet
    @pytest.mark.asyncio
    async def test_short_batch_sync_wallet(self, two_wallet_nodes, default_400_blocks, self_hostname):
        full_nodes, wallets = two_wallet_nodes
        full_node_api = full_nodes[0]
        full_node_server = full_node_api.full_node.server

        # Trusted node sync
        wallets[0][0].config["trusted_peers"] = {full_node_server.node_id.hex(): full_node_server.node_id.hex()}

        # Untrusted node sync
        wallets[1][0].config["trusted_peers"] = {}

        for block in default_400_blocks[:200]:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        for wallet_node, wallet_server in wallets:
            await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_server._port)), None)

        for wallet_node, wallet_server in wallets:
            await time_out_assert(100, wallet_height_at_least, True, wallet_node, 199)

    @pytest.mark.asyncio
    async def test_long_sync_wallet(self, bt, two_wallet_nodes, default_1000_blocks, default_400_blocks, self_hostname):
        full_nodes, wallets = two_wallet_nodes
        full_node_api = full_nodes[0]
        full_node_server = full_node_api.full_node.server

        # Trusted node sync
        wallets[0][0].config["trusted_peers"] = {full_node_server.node_id.hex(): full_node_server.node_id.hex()}

        # Untrusted node sync
        wallets[1][0].config["trusted_peers"] = {}

        for block in default_400_blocks:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        for wallet_node, wallet_server in wallets:
            await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_server._port)), None)

        for wallet_node, wallet_server in wallets:
            await time_out_assert(600, wallet_height_at_least, True, wallet_node, len(default_400_blocks) - 1)

        # Tests a long reorg
        for block in default_1000_blocks:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        for wallet_node, wallet_server in wallets:
            await disconnect_all_and_reconnect(wallet_server, full_node_server, self_hostname)

            log.info(f"wallet node height is {wallet_node.wallet_state_manager.blockchain.get_peak_height()}")
            await time_out_assert(600, wallet_height_at_least, True, wallet_node, len(default_1000_blocks) - 1)

            await disconnect_all_and_reconnect(wallet_server, full_node_server, self_hostname)

        # Tests a short reorg
        num_blocks = 30
        blocks_reorg = bt.get_consecutive_blocks(num_blocks, block_list_input=default_1000_blocks[:-5])

        for i in range(len(blocks_reorg) - num_blocks - 10, len(blocks_reorg)):
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(blocks_reorg[i]))

        for wallet_node, wallet_server in wallets:
            await time_out_assert(
                600, wallet_height_at_least, True, wallet_node, len(default_1000_blocks) + num_blocks - 5 - 1
            )

    @pytest.mark.asyncio
    async def test_wallet_reorg_sync(self, bt, two_wallet_nodes, default_400_blocks, self_hostname):
        num_blocks = 5
        full_nodes, wallets = two_wallet_nodes
        full_node_api = full_nodes[0]
        full_node_server = full_node_api.full_node.server

        # Trusted node sync
        wallets[0][0].config["trusted_peers"] = {full_node_server.node_id.hex(): full_node_server.node_id.hex()}

        # Untrusted node sync
        wallets[1][0].config["trusted_peers"] = {}

        phs = []
        for wallet_node, wallet_server in wallets:
            wallet = wallet_node.wallet_state_manager.main_wallet
            phs.append(await wallet.get_new_puzzlehash())
            await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_server._port)), None)

        # Insert 400 blocks
        for block in default_400_blocks:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        # Farm few more with reward
        for i in range(0, num_blocks - 1):
            await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(phs[0]))

        for i in range(0, num_blocks):
            await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(phs[1]))

        # Confirm we have the funds
        funds = sum(
            [calculate_pool_reward(uint32(i)) + calculate_base_farmer_reward(uint32(i)) for i in range(1, num_blocks)]
        )

        async def get_tx_count(wsm, wallet_id):
            txs = await wsm.get_all_transactions(wallet_id)
            return len(txs)

        for wallet_node, wallet_server in wallets:
            wallet = wallet_node.wallet_state_manager.main_wallet
            await time_out_assert(5, wallet.get_confirmed_balance, funds)
            await time_out_assert(5, get_tx_count, 2 * (num_blocks - 1), wallet_node.wallet_state_manager, 1)

        # Reorg blocks that carry reward
        num_blocks = 30
        blocks_reorg = bt.get_consecutive_blocks(num_blocks, block_list_input=default_400_blocks[:-5])

        for block in blocks_reorg[-30:]:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        for wallet_node, wallet_server in wallets:
            wallet = wallet_node.wallet_state_manager.main_wallet
            await time_out_assert(5, get_tx_count, 0, wallet_node.wallet_state_manager, 1)
            await time_out_assert(5, wallet.get_confirmed_balance, 0)

    @pytest.mark.asyncio
    async def test_wallet_reorg_get_coinbase(self, bt, two_wallet_nodes, default_400_blocks, self_hostname):
        full_nodes, wallets = two_wallet_nodes
        full_node_api = full_nodes[0]
        full_node_server = full_node_api.full_node.server

        # Trusted node sync
        wallets[0][0].config["trusted_peers"] = {full_node_server.node_id.hex(): full_node_server.node_id.hex()}

        # Untrusted node sync
        wallets[1][0].config["trusted_peers"] = {}

        for wallet_node, wallet_server in wallets:
            await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_server._port)), None)

        # Insert 400 blocks
        for block in default_400_blocks:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        # Reorg blocks that carry reward
        num_blocks_reorg = 30
        blocks_reorg = bt.get_consecutive_blocks(num_blocks_reorg, block_list_input=default_400_blocks[:-5])

        for block in blocks_reorg[:-5]:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        async def get_tx_count(wsm, wallet_id):
            txs = await wsm.get_all_transactions(wallet_id)
            return len(txs)

        for wallet_node, wallet_server in wallets:
            await time_out_assert(10, get_tx_count, 0, wallet_node.wallet_state_manager, 1)
            await time_out_assert(30, wallet_is_synced, True, wallet_node, full_node_api)

        num_blocks_reorg_1 = 40
        all_blocks_reorg_2 = blocks_reorg[:-30]
        for wallet_node, wallet_server in wallets:
            wallet = wallet_node.wallet_state_manager.main_wallet
            ph = await wallet.get_new_puzzlehash()
            all_blocks_reorg_2 = bt.get_consecutive_blocks(
                1, pool_reward_puzzle_hash=ph, farmer_reward_puzzle_hash=ph, block_list_input=all_blocks_reorg_2
            )
        blocks_reorg_2 = bt.get_consecutive_blocks(num_blocks_reorg_1, block_list_input=all_blocks_reorg_2)

        for block in blocks_reorg_2[-44:]:
            await full_node_api.full_node.respond_block(full_node_protocol.RespondBlock(block))

        for wallet_node, wallet_server in wallets:
            await disconnect_all_and_reconnect(wallet_server, full_node_server, self_hostname)

        # Confirm we have the funds
        funds = calculate_pool_reward(uint32(len(all_blocks_reorg_2))) + calculate_base_farmer_reward(
            uint32(len(all_blocks_reorg_2))
        )

        for wallet_node, wallet_server in wallets:
            wallet = wallet_node.wallet_state_manager.main_wallet
            await time_out_assert(60, wallet_is_synced, True, wallet_node, full_node_api)
            await time_out_assert(20, get_tx_count, 2, wallet_node.wallet_state_manager, 1)
            await time_out_assert(20, wallet.get_confirmed_balance, funds)

    @pytest.mark.asyncio
    async def test_request_additions_errors(self, wallet_node_sim_and_wallet, self_hostname):
        full_nodes, wallets = wallet_node_sim_and_wallet
        wallet_node, wallet_server = wallets[0]
        wallet = wallet_node.wallet_state_manager.main_wallet
        ph = await wallet.get_new_puzzlehash()

        full_node_api = full_nodes[0]
        await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_api.full_node.server._port)), None)

        for i in range(2):
            await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))

        await time_out_assert(20, wallet_is_synced, True, wallet_node, full_node_api)

        last_block: Optional[BlockRecord] = full_node_api.full_node.blockchain.get_peak()
        assert last_block is not None

        # Invalid height
        with pytest.raises(ValueError):
            await full_node_api.request_additions(RequestAdditions(uint64(100), last_block.header_hash, [ph]))

        # Invalid header hash
        with pytest.raises(ValueError):
            await full_node_api.request_additions(RequestAdditions(last_block.height, std_hash(b""), [ph]))

        # No results
        res1: Optional[Message] = await full_node_api.request_additions(
            RequestAdditions(last_block.height, last_block.header_hash, [std_hash(b"")])
        )
        assert res1 is not None
        response = RespondAdditions.from_bytes(res1.data)
        assert response.height == last_block.height
        assert response.header_hash == last_block.header_hash
        assert len(response.proofs) == 1
        assert len(response.coins) == 1

        assert response.proofs[0][0] == std_hash(b"")
        assert response.proofs[0][1] is not None
        assert response.proofs[0][2] is None

    @pytest.mark.asyncio
    async def test_request_additions_success(self, wallet_node_sim_and_wallet, self_hostname):
        full_nodes, wallets = wallet_node_sim_and_wallet
        wallet_node, wallet_server = wallets[0]
        wallet = wallet_node.wallet_state_manager.main_wallet
        ph = await wallet.get_new_puzzlehash()

        full_node_api = full_nodes[0]
        await wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_api.full_node.server._port)), None)

        for i in range(2):
            await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))

        await time_out_assert(20, wallet_is_synced, True, wallet_node, full_node_api)

        payees: List[AmountWithPuzzlehash] = []
        for i in range(10):
            payee_ph = await wallet.get_new_puzzlehash()
            payees.append({"amount": uint64(i + 100), "puzzlehash": payee_ph, "memos": []})
            payees.append({"amount": uint64(i + 200), "puzzlehash": payee_ph, "memos": []})

        tx: TransactionRecord = await wallet.generate_signed_transaction(uint64(0), ph, primaries=payees)
        await full_node_api.send_transaction(SendTransaction(tx.spend_bundle))

        await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))

        last_block: Optional[BlockRecord] = full_node_api.full_node.blockchain.get_peak()
        assert last_block is not None
        await time_out_assert(20, wallet_is_synced, True, wallet_node, full_node_api)
        res2: Optional[Message] = await full_node_api.request_additions(
            RequestAdditions(
                last_block.height,
                None,
                [payees[0]["puzzlehash"], payees[2]["puzzlehash"], std_hash(b"1")],
            )
        )

        assert res2 is not None
        response = RespondAdditions.from_bytes(res2.data)
        assert response.height == last_block.height
        assert response.header_hash == last_block.header_hash
        assert len(response.proofs) == 3

        # First two PHs are included
        for i in range(2):
            assert response.proofs[i][0] in {payees[j]["puzzlehash"] for j in (0, 2)}
            assert response.proofs[i][1] is not None
            assert response.proofs[i][2] is not None

        # Third PH is not included
        assert response.proofs[2][2] is None

        coin_list_dict = {p: coin_list for p, coin_list in response.coins}

        assert len(coin_list_dict) == 3
        for p, coin_list in coin_list_dict.items():
            if p == std_hash(b"1"):
                # this is the one that is not included
                assert len(coin_list) == 0
            else:
                for coin in coin_list:
                    assert coin.puzzle_hash == p
                # The other ones are included
                assert len(coin_list) == 2

        # None for puzzle hashes returns all coins and no proofs
        res3: Optional[Message] = await full_node_api.request_additions(
            RequestAdditions(last_block.height, last_block.header_hash, None)
        )

        assert res3 is not None
        response = RespondAdditions.from_bytes(res3.data)
        assert response.height == last_block.height
        assert response.header_hash == last_block.header_hash
        assert response.proofs is None
        assert len(response.coins) == 12
        assert sum([len(c_list) for _, c_list in response.coins]) == 24

        # [] for puzzle hashes returns nothing
        res4: Optional[Message] = await full_node_api.request_additions(
            RequestAdditions(last_block.height, last_block.header_hash, [])
        )
        assert res4 is not None
        response = RespondAdditions.from_bytes(res4.data)
        assert response.proofs == []
        assert len(response.coins) == 0

    """
    This tests that a wallet filters out the dust properly.
    It runs in five phases:
    1. Create a single dust coin. 
       Typically (though there are edge cases), this coin will not be filtered.
    2. Create dust coins until the filter threshold has been reached. 
       At this point, none of the dust should be filtered.
    3. Create 10 coins that are exactly the size of the filter threshold. 
       These should not be filtered because they are not dust.
    4. Create one more dust coin. This coin should be filtered.
    5. Create 5 coins below the threshold and 5 at or above.
       Those below the threshold should get filtered, and those above should not.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("spam_filter_after_n_txs, xch_spam_amount, dust_value", [
        # In the following tests, the filter is run right away:
        (0, 1, 1), # nothing is filtered

        # In the following tests, 1 coin will be created in part 1, and 9 in part 2:
        (10, 10000000000, 1), # everything is dust
        (10, 10000000000, 10000000000), # max dust threshold, dust is same size so not filtered

        # Test with more coins
        (100, 1000000, 1), # default filter level (1m mojos), default dust size (1)
    ])
    async def test_dusted_wallet(self, self_hostname, 
        two_wallet_nodes_custom_spam_filtering, spam_filter_after_n_txs, xch_spam_amount, dust_value):
    
        full_nodes, wallets = two_wallet_nodes_custom_spam_filtering

        farm_wallet_node, farm_wallet_server = wallets[0]
        dust_wallet_node, dust_wallet_server = wallets[1]

        # Create two wallets, one for farming (not used for testing), and one for testing dust.
        farm_wallet = farm_wallet_node.wallet_state_manager.main_wallet
        dust_wallet = dust_wallet_node.wallet_state_manager.main_wallet
        ph = await farm_wallet.get_new_puzzlehash()

        full_node_api = full_nodes[0]

        #It's also possible to obtain the current settings for spam_filter_after_n_txs and xch_spam_amount
        #spam_filter_after_n_txs = wallets[0][0].config["spam_filter_after_n_txs"]
        #xch_spam_amount = wallets[0][0].config["xch_spam_amount"]
        #dust_value=1

        # Verify legal values for the settings to be tested
        # If spam_filter_after_n_txs is greater than 1000, this test will take a long time to run.
        # Current max value for xch_spam_amount is 0.01 XCH.
        # If needed, this could be increased but we would need to farm more blocks.
        # The max dust_value could be increased, but would require farming more blocks.
        assert spam_filter_after_n_txs >= 0
        assert spam_filter_after_n_txs <= 1000
        assert xch_spam_amount >= 1
        assert xch_spam_amount <= 10000000000
        assert dust_value >= 1
        assert dust_value <= 10000000000

        # start both clients
        await farm_wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_api.full_node.server._port)), None)
        await dust_wallet_server.start_client(PeerInfo(self_hostname, uint16(full_node_api.full_node.server._port)), None)

        # Farm two blocks
        for i in range(2):
            await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))

        #sync both nodes
        await time_out_assert(20, wallet_is_synced, True, farm_wallet_node, full_node_api)
        await time_out_assert(20, wallet_is_synced, True, dust_wallet_node, full_node_api)

        # Part 1: create a single dust coin
        payees: List[AmountWithPuzzlehash] = []
        payee_ph = await dust_wallet.get_new_puzzlehash()
        payees.append({"amount": uint64(dust_value), "puzzlehash": payee_ph, "memos": []})

        # construct and send tx
        tx: TransactionRecord = await farm_wallet.generate_signed_transaction(uint64(0), ph, primaries=payees)
        await full_node_api.send_transaction(SendTransaction(tx.spend_bundle))

        # advance the chain and sync both wallets
        await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))
        last_block: Optional[BlockRecord] = full_node_api.full_node.blockchain.get_peak()
        assert last_block is not None
        await time_out_assert(20, wallet_is_synced, True, farm_wallet_node, full_node_api)
        await time_out_assert(20, wallet_is_synced, True, dust_wallet_node, full_node_api)

        # The dust is only filtered at this point if spam_filter_after_n_txs is 0 and xch_spam_amount is > dust_value.
        if spam_filter_after_n_txs > 0:
            dust_coins = 1
            large_dust_coins = 0
            large_dust_balance = 0
        elif xch_spam_amount <= dust_value:
            dust_coins = 0
            large_dust_coins = 1
            large_dust_balance = dust_value
        else:
            dust_coins = 0
            large_dust_coins = 0
            large_dust_balance = 0

        # Obtain and log important values
        all_unspent: Set[WalletCoinRecord] = dust_wallet_node.wallet_state_manager.coin_store.get_all_unspent_coins()
        small_unspent_count = len([r for r in all_unspent if r.coin.amount < xch_spam_amount])
        balance: Optional[Message] = await dust_wallet.get_confirmed_balance()
        num_coins: Optional[Message] = len(await dust_wallet.select_coins(balance))

        log.info(f"Small coin count is {small_unspent_count}")
        log.info(f"Wallet balance is {balance}")
        log.info(f"Number of coins is {num_coins}")

        log.info(f"spam_filter_after_n_txs {spam_filter_after_n_txs}")
        log.info(f"xch_spam_amount {xch_spam_amount}")
        log.info(f"dust_value {dust_value}")

        # Verify balance and number of coins not filtered.
        assert balance == dust_coins * dust_value + large_dust_balance
        assert num_coins == dust_coins + large_dust_coins

        # Part 2: Create dust coins until the filter threshold has been reached.
        # Nothing should be filtered yet (unless spam_filter_after_n_txs is 0).
        payees: List[AmountWithPuzzlehash] = []

        # Determine how much dust to create, recalling that there already is one dust coin.
        new_dust = spam_filter_after_n_txs - 1
        dust_remaining = new_dust
        
        while dust_remaining > 0:            
            payee_ph = await dust_wallet.get_new_puzzlehash()
            payees.append({"amount": uint64(dust_value), "puzzlehash": payee_ph, "memos": []})

            # After every 100 (at most) coins added, push the tx and advance the chain
            # This greatly speeds up the overall process 
            if dust_remaining % 100 == 0 and dust_remaining != new_dust:
                # construct and send tx
                tx: TransactionRecord = await farm_wallet.generate_signed_transaction(uint64(0), ph, primaries=payees)
                await full_node_api.send_transaction(SendTransaction(tx.spend_bundle))
                await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))
                last_block: Optional[BlockRecord] = full_node_api.full_node.blockchain.get_peak()
                assert last_block is not None
                # reset payees
                payees: List[AmountWithPuzzlehash] = []
                
            dust_remaining -= 1

        # Only need to create tx if there was new dust to be added
        if new_dust >= 1:
            # construct and send tx
            tx: TransactionRecord = await farm_wallet.generate_signed_transaction(uint64(0), ph, primaries=payees)
            await full_node_api.send_transaction(SendTransaction(tx.spend_bundle))

            # advance the chain and sync both wallets
            await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))
            last_block: Optional[BlockRecord] = full_node_api.full_node.blockchain.get_peak()
            assert last_block is not None
            await time_out_assert(60, wallet_is_synced, True, farm_wallet_node, full_node_api)
            await time_out_assert(60, wallet_is_synced, True, dust_wallet_node, full_node_api)

        # Obtain and log important values
        all_unspent: Set[WalletCoinRecord] = dust_wallet_node.wallet_state_manager.coin_store.get_all_unspent_coins()
        small_unspent_count = len([r for r in all_unspent if r.coin.amount < xch_spam_amount])
        balance: Optional[Message] = await dust_wallet.get_confirmed_balance()
        # Selecting coins by using the wallet's coin selection algorithm won't work for large
        # numbers of coins, so we'll use the state manager for the rest of the test
        # num_coins: Optional[Message] = len(await dust_wallet.select_coins(balance))
        num_coins: Optional[Message] = len(list(
            await dust_wallet_node.wallet_state_manager.get_spendable_coins_for_wallet(1)
        ))

        log.info(f"Small coin count is {small_unspent_count}")
        log.info(f"Wallet balance is {balance}")
        log.info(f"Number of coins is {num_coins}")

        # obtain the total expected coins (new_dust could be negative)
        if new_dust > 0:
            dust_coins += new_dust

        # Make sure the number of coins matches the expected number.
        # At this point, nothing should be getting filtered unless spam_filter_after_n_txs is 0.
        assert dust_coins == spam_filter_after_n_txs
        assert balance == dust_coins * dust_value + large_dust_balance
        assert num_coins == dust_coins + large_dust_coins
        
        # Part 3: Create 10 coins that are exactly the size of the filter threshold.
        # These should not get filtered.
        large_coins = 10
        
        payees: List[AmountWithPuzzlehash] = []

        for i in range(large_coins):
            payee_ph = await dust_wallet.get_new_puzzlehash()
            payees.append({"amount": uint64(xch_spam_amount), "puzzlehash": payee_ph, "memos": []})

        # construct and send tx
        tx: TransactionRecord = await farm_wallet.generate_signed_transaction(uint64(0), ph, primaries=payees)
        await full_node_api.send_transaction(SendTransaction(tx.spend_bundle))

        # advance the chain and sync both wallets
        await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))
        last_block: Optional[BlockRecord] = full_node_api.full_node.blockchain.get_peak()
        assert last_block is not None
        await time_out_assert(20, wallet_is_synced, True, farm_wallet_node, full_node_api)
        await time_out_assert(20, wallet_is_synced, True, dust_wallet_node, full_node_api)

        # Obtain and log important values
        all_unspent: Set[WalletCoinRecord] = dust_wallet_node.wallet_state_manager.coin_store.get_all_unspent_coins()
        small_unspent_count = len([r for r in all_unspent if r.coin.amount < xch_spam_amount])
        balance: Optional[Message] = await dust_wallet.get_confirmed_balance()
        num_coins: Optional[Message] = len(list(
            await dust_wallet_node.wallet_state_manager.get_spendable_coins_for_wallet(1)
        ))

        log.info(f"Small coin count is {small_unspent_count}")
        log.info(f"Wallet balance is {balance}")
        log.info(f"Number of coins is {num_coins}")

        large_coin_balance = large_coins * xch_spam_amount

        # Determine whether the filter should have been activated. 
        # Make sure the number of coins matches the expected number.
        # At this point, nothing should be getting filtered unless spam_filter_after_n_txs is 0.
        assert dust_coins == spam_filter_after_n_txs
        assert balance == dust_coins * dust_value + large_coins * xch_spam_amount + large_dust_balance
        assert num_coins == dust_coins + large_coins + large_dust_coins
        
        # Part 4: Create one more dust coin to test the threshold
        payees: List[AmountWithPuzzlehash] = []

        payee_ph = await dust_wallet.get_new_puzzlehash()
        payees.append({"amount": uint64(dust_value), "puzzlehash": payee_ph, "memos": []})

        # construct and send tx
        tx: TransactionRecord = await farm_wallet.generate_signed_transaction(uint64(0), ph, primaries=payees)
        await full_node_api.send_transaction(SendTransaction(tx.spend_bundle))

        # advance the chain and sync both wallets
        await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))
        last_block: Optional[BlockRecord] = full_node_api.full_node.blockchain.get_peak()
        assert last_block is not None
        await time_out_assert(20, wallet_is_synced, True, farm_wallet_node, full_node_api)
        await time_out_assert(20, wallet_is_synced, True, dust_wallet_node, full_node_api)

        # Obtain and log important values
        all_unspent: Set[WalletCoinRecord] = dust_wallet_node.wallet_state_manager.coin_store.get_all_unspent_coins()
        small_unspent_count = len([r for r in all_unspent if r.coin.amount < xch_spam_amount])
        balance: Optional[Message] = await dust_wallet.get_confirmed_balance()
        num_coins: Optional[Message] = len(list(
            await dust_wallet_node.wallet_state_manager.get_spendable_coins_for_wallet(1)
        ))

        log.info(f"Small coin count is {small_unspent_count}")
        log.info(f"Wallet balance is {balance}")
        log.info(f"Number of coins is {num_coins}")

        # In the edge case where the new "dust" is larger than the threshold, 
        # then it is actually a large dust coin that won't get filtered.
        if dust_value >= xch_spam_amount:
            large_dust_coins += 1
            large_dust_balance += dust_value
        
        assert dust_coins == spam_filter_after_n_txs    
        assert balance == dust_coins * dust_value + large_coins * xch_spam_amount + large_dust_balance
        assert num_coins == dust_coins + large_dust_coins + large_coins
        
        # Part 5: Create 5 coins below the threshold and 5 at or above.
        # Those below the threshold should get filtered, and those above should not.
        payees: List[AmountWithPuzzlehash] = []

        for i in range(5):
            payee_ph = await dust_wallet.get_new_puzzlehash()
            
            # Create a large coin and add on the appropriate balance.
            payees.append({"amount": uint64(xch_spam_amount+i), "puzzlehash": payee_ph, "memos": []})
            large_coins += 1
            large_coin_balance += xch_spam_amount+i

            payee_ph = await dust_wallet.get_new_puzzlehash()
            
            # Make sure we are always creating coins with a positive value.
            if xch_spam_amount-dust_value-i > 0:
                payees.append({"amount": uint64(xch_spam_amount-dust_value-i), "puzzlehash": payee_ph, "memos": []})
            else:
                payees.append({"amount": uint64(dust_value), "puzzlehash": payee_ph, "memos": []})
            # In cases where xch_spam_amount is sufficiently low, 
            # the new dust should be considered a large coina and not be filtered.
            if xch_spam_amount <= dust_value:
                large_dust_coins += 1
                large_dust_balance += dust_value

        # construct and send tx
        tx: TransactionRecord = await farm_wallet.generate_signed_transaction(uint64(0), ph, primaries=payees)
        await full_node_api.send_transaction(SendTransaction(tx.spend_bundle))

        # advance the chain and sync both wallets
        await full_node_api.farm_new_transaction_block(FarmNewBlockProtocol(ph))
        last_block: Optional[BlockRecord] = full_node_api.full_node.blockchain.get_peak()
        assert last_block is not None
        await time_out_assert(20, wallet_is_synced, True, farm_wallet_node, full_node_api)
        await time_out_assert(20, wallet_is_synced, True, dust_wallet_node, full_node_api)

        # Obtain and log important values
        all_unspent: Set[WalletCoinRecord] = dust_wallet_node.wallet_state_manager.coin_store.get_all_unspent_coins()
        small_unspent_count = len([r for r in all_unspent if r.coin.amount < xch_spam_amount])
        balance: Optional[Message] = await dust_wallet.get_confirmed_balance()
        num_coins: Optional[Message] = len(list(
            await dust_wallet_node.wallet_state_manager.get_spendable_coins_for_wallet(1)
        ))

        log.info(f"Small coin count is {small_unspent_count}")
        log.info(f"Wallet balance is {balance}")
        log.info(f"Number of coins is {num_coins}")

        # The filter should have automatically been activated by now, regardless of filter value
        assert dust_coins == spam_filter_after_n_txs    
        assert balance == dust_coins * dust_value + large_coin_balance + large_dust_balance
        assert num_coins == dust_coins + large_dust_coins + large_coins