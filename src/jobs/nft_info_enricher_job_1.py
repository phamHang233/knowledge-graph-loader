import time
from typing import Dict

from defi_services.utils.get_fees import get_fees
from multithread_processing.base_job import BaseJob
from web3 import Web3

from artifacts.abis.dexes.uniswap_v3_nft_manage_abi import UNISWAP_V3_NFT_MANAGER_ABI
from artifacts.abis.multicall_abi import MULTICALL_ABI
from src.constants.mongo_constants import DexNFTManagerCollections
from src.constants.network_constants import Networks, MulticallContract, Chains
from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_dex import MongoDBDex
from src.models.nfts import NFT
from src.services.blockchain.batch_queries_service import decode_data_response_ignore_error, add_rpc_call
from src.services.blockchain.multicall import W3Multicall, add_rpc_multicall
from src.services.blockchain.state_query_service import StateQueryService

from src.utils.logger_utils import get_logger

logger = get_logger('NFT Info Enricher Job')


class NFTInfoEnricherJob(BaseJob):
    def __init__(
            self, _db, _exporter, max_workers, batch_size, nfts_batch,
            chain_id, provider_uri, db_prefix, n_cpu=1, cpu=1, ):
        self.n_cpu = n_cpu
        self.cpu = cpu - 1

        self._klg_db = _db
        self._exporter = _exporter
        self.db_prefix = db_prefix

        self.batch_size = batch_size
        self.number_of_nfts_batch = nfts_batch

        self.chain_id = chain_id
        self.state_querier = StateQueryService(provider_uri)
        self._w3 = Web3(Web3.HTTPProvider(provider_uri))

        work_iterable = self._get_work_iterable()
        super().__init__(work_iterable, batch_size, max_workers)

    def _get_work_iterable(self):
        work_iterable = [idx for idx in range(1, self.number_of_nfts_batch + 1) if
                         (idx - 1) % self.n_cpu == self.cpu]
        return work_iterable

    def _start(self):
        self.dex_db = MongoDBDex()
        self._etl_db = BlockchainETL(db_prefix=self.db_prefix)
        self.deleted_tokens = []
        self.pools = {}
        self.pools_fee = {}
        self.cnt = 0
        # self.invalid_pool = []
        # self.token_price = {}
        self.end_block = self._etl_db.get_last_block_number()
        current_day_timestamp = int(time.time())
        cursor = self._etl_db.get_block_by_timestamp(current_day_timestamp - 24 * 1 * 3600 + 3600 -1)
        if cursor:
            self.before_timestamp = cursor['block_number']
        # self.before_timestamp = 220460428

    def _execute_batch(self, nfts_batch_indicates):
        for batch_idx in nfts_batch_indicates:
            try:
                start_time = time.time()
                batch_cursor = self._klg_db.get_nfts_by_filter(
                    _filter={"flagged": batch_idx, 'liquidity': {"$gt": 0}, 'chainId': self.chain_id,
                             "nftManagerAddress": "0xc36442b4a4522e871399cd717abdd847ab11fe88"})
                # batch_cursor = self._klg_db.get_nfts_by_filter({'chainId': self.chain_id, 'tokenId': "2613061"})
                batch_cursor = list(batch_cursor)
                new_batch_cursor = self.check_available_nft(batch_cursor)
                current_data_response, before_data_response, pools_in_batch = self.prepare_enrich(new_batch_cursor)

                cursor = self.dex_db.get_pairs_with_addresses(chain_id=self.chain_id, addresses=list(pools_in_batch))

                self.pools.update({doc['address']: doc for doc in cursor})
                updated_nfts = self.enrich_data(new_batch_cursor, current_data_response, before_data_response, )
                self._export(updated_nfts)

                logger.info(f'Time to execute of batch [{batch_idx}] is {time.time() - start_time} seconds')
            except Exception as e:
                # logger.error(e)
                logger.exception(e)
                continue

    def check_available_nft(self, cursor_batch):

        list_rpc_call = []
        list_call_id = []
        for doc in cursor_batch:
            add_rpc_call(
                abi=UNISWAP_V3_NFT_MANAGER_ABI, contract_address=doc['nftManagerAddress'],
                fn_name="positions", block_number='latest', fn_paras=int(doc['tokenId']),
                list_call_id=list_call_id, list_rpc_call=list_rpc_call
            )
        data_response = self.state_querier.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=1000)
        decoded_data = decode_data_response_ignore_error(data_response, list_call_id)
        new_batch_cursor = []
        for idx, doc in enumerate(cursor_batch):
            doc = cursor_batch[idx]
            positions = decoded_data.get(f'positions_{doc["nftManagerAddress"]}_{int(doc["tokenId"])}_latest'.lower())

            if not positions:
                self.deleted_tokens.append(doc["_id"])
            elif positions[7] != 0:
                new_batch_cursor.append(doc)
        return new_batch_cursor

    def prepare_enrich(self, cursor_batch):
        pools_in_batch = []
        start_time = time.time()
        # unchanged_nfts = []
        # changed_nfts = []

        for _, doc in enumerate(cursor_batch):
            pool_address = doc['poolAddress']
            pools_in_batch.append(pool_address)

            # liquidity_change_logs = doc.get('liquidityChangeLogs', {})
            # unchanged_nft = True
            # blocks = []
            # for block_number in liquidity_change_logs:
            #     blocks.append(int(block_number))
            #     if int(block_number) > self.before_timestamp:
            #         unchanged_nft = False
            #
            # if unchanged_nft:
            #     unchanged_nfts.append({
            #         'tokenId': doc['tokenId'],
            #         'poolAddress': pool_address,
            #         'tickLower': doc['tickLower'],
            #         "tickUpper": doc['tickUpper'],
            #         'nftManagerAddress': doc['nftManagerAddress'],
            #         'blockNumber': min(blocks) if blocks else 0
            #
            #     })
            # else:
            # if blocks:
            #     changed_nfts.append({
            #         'tokenId': doc['tokenId'],
            #         'poolAddress': pool_address,
            #         'tickLower': doc['tickLower'],
            #         "tickUpper": doc['tickUpper'],
            #         'nftManagerAddress': doc['nftManagerAddress'],
            #         'blockNumber': min(blocks)
            #     })
        w3_multicall = W3Multicall(self._w3, address=MulticallContract.get_multicall_contract(self.chain_id))

        current_decoded_data = self.state_querier.get_batch_nft_fee_with_block_number(
            nfts=cursor_batch, pools=self.pools_fee, w3_multicall=w3_multicall, latest=True)

        # contract_ = self._w3.eth.contract(Web3.to_checksum_address(w3_multicall.address), abi=MULTICALL_ABI)
        # inputs = w3_multicall.get_params()
        # response = contract_.functions.aggregate(*inputs).call(block_identifier='latest')
        # current_decoded_data = w3_multicall.decode(response)
        before_decoded_data = {}
        for idx in range(0, len(cursor_batch), 20):
            w3_multicall.calls = {}
            nft_batchs = cursor_batch[idx: idx + 20]
            # self.state_querier.get_batch_nft_fee_with_block_number(
            #     nfts=nft_batchs, pools=self.pools_fee, latest=False,w3_multicall=w3_multicall)
            # contract_ = self._w3.eth.contract(Web3.to_checksum_address(w3_multicall.address), abi=MULTICALL_ABI)
            # inputs = w3_multicall.get_params()
            # response = contract_.functions.aggregate(*inputs).call(block_identifier=self.before_timestamp)
            # before_decoded_data.update(w3_multicall.decode(response))
            before_decoded_data.update(self.state_querier.get_batch_nft_fee_with_block_number(
                nfts=nft_batchs, pools=self.pools_fee, w3_multicall=w3_multicall, block_number=self.before_timestamp,
                latest=False))

        logger.info(f"Query process toke {time.time() - start_time}")

        return current_decoded_data, before_decoded_data, pools_in_batch

    def enrich_data(self, batch_cursor, current_data_response, before_data_response, ) -> Dict[str, NFT]:
        updated_nfts: Dict[str, NFT] = {}
        for doc in batch_cursor:
            try:
                token_id = doc['tokenId']
                nft = NFT(id=token_id, chain=self.chain_id)
                nft.from_dict(doc)
                idx = doc['_id']
                pool_address = nft.pool_address
                pool_info = self.pools.get(pool_address)
                if not pool_info or not pool_info.get('tick'):
                    #     self.invalid_pool.append(pool_address)
                    continue

                # unchanged_nft = True
                # liquidity_change_logs = nft.liquidity_change_logs
                # blocks = []
                # for block_number in liquidity_change_logs:
                #     blocks.append(int(block_number))
                #     if int(block_number) > self.before_timestamp:
                #         unchanged_nft = False
                #
                # if unchanged_nft:
                self.calculate_apr(nft, current_data_response, before_data_response)

                # if blocks:
                #     nft.apr, nft.pnl, nft.invested_asset_in_usd = self.calculate_apr(doc, data_response, start_block=min(blocks))
                updated_nfts[idx] = nft
            except Exception as e:
                # raise e
                logger.exception(e)
                continue
        return updated_nfts

    def calculate_apr(self, nft, current_data_response, before_data_response):
        token_id = nft.token_id
        pool_address = nft.pool_address
        nft_manager_contract = nft.nft_manager_address
        tick_lower = nft.tick_lower
        tick_upper = nft.tick_upper
        pool_info = self.pools.get(pool_address)
        if not self.pools_fee.get(pool_address, {}):
            fee_growth_global_0 = current_data_response.get(f'feeGrowthGlobal0X128_{pool_address}_latest'.lower())
            fee_growth_global_1 = current_data_response.get(f'feeGrowthGlobal1X128_{pool_address}_latest'.lower())
            fee_growth_global_0_before = before_data_response.get(
                f'feeGrowthGlobal0X128_{pool_address}_{self.before_timestamp}'.lower())
            fee_growth_global_1_before = before_data_response.get(
                f'feeGrowthGlobal1X128_{pool_address}_{self.before_timestamp}'.lower())
            slot0 = before_data_response.get(f'slot0_{pool_address}_{self.before_timestamp}'.lower())

            self.pools_fee[pool_address] = {
                "feeGrowthGlobal0X128": fee_growth_global_0,
                "feeGrowthGlobal1X128": fee_growth_global_1,
                "feeGrowthGlobal0X128Before": fee_growth_global_0_before,
                "feeGrowthGlobal1X128Before": fee_growth_global_1_before,
                'tick': slot0[1] if slot0 else None
            }
        fee_growth_global_0 = self.pools_fee[pool_address]['feeGrowthGlobal0X128']
        fee_growth_global_1 = self.pools_fee[pool_address]['feeGrowthGlobal1X128']
        fee_growth_global_0_before = self.pools_fee[pool_address]['feeGrowthGlobal0X128Before']
        fee_growth_global_1_before = self.pools_fee[pool_address]['feeGrowthGlobal1X128Before']
        tick_before = self.pools_fee[pool_address]['tick']

        fee_growth_low_x128_before = before_data_response.get(
            f'ticks_{pool_address}_{tick_lower}_{self.before_timestamp}'.lower())
        fee_growth_hi_x128_before = before_data_response.get(
            f'ticks_{pool_address}_{tick_upper}_{self.before_timestamp}'.lower())

        positions_before = before_data_response.get(
            f'positions_{nft_manager_contract}_{token_id}_{self.before_timestamp}'.lower())

        fee_growth_low_x128 = current_data_response.get(f'ticks_{pool_address}_{tick_lower}_latest'.lower())
        fee_growth_hi_x128 = current_data_response.get(f'ticks_{pool_address}_{tick_upper}_latest'.lower())
        positions = current_data_response.get(f'positions_{nft_manager_contract}_{token_id}_latest'.lower())
        if not positions:
            self.deleted_tokens.append(f"{self.chain_id}_{nft.nft_manager_address}_{nft.token_id}")
        ###

        nft.liquidity = float(positions[7])
        if abs(positions_before[7] - float(positions[7])) > 0.01:
            return
        tick = pool_info['tick']
        tokens = pool_info['tokens']
        token0_decimals = tokens[0]['decimals']
        token1_decimals = tokens[1]['decimals']
        token0_reward, token1_reward = get_fees(
            fee_growth_global_0=fee_growth_global_0,
            fee_growth_global_1=fee_growth_global_1,
            fee_growth_0_low=fee_growth_low_x128[2],
            fee_growth_1_low=fee_growth_low_x128[3],
            fee_growth_0_hi=fee_growth_hi_x128[2],
            fee_growth_1_hi=fee_growth_hi_x128[3],
            fee_growth_inside_0=positions[8],
            fee_growth_inside_1=positions[9],
            liquidity=nft.liquidity, tick_lower=tick_lower,
            tick_upper=tick_upper, tick_current=pool_info['tick'],
            decimals0=token0_decimals, decimals1=token1_decimals
        )
        nft.uncollected_fee[tokens[0]['address']] = token0_reward
        nft.uncollected_fee[tokens[1]['address']] = token1_reward
        nft.last_updated_fee_at = self.end_block

        token0_reward_before, token1_reward_before = get_fees(
            fee_growth_global_0=fee_growth_global_0_before,
            fee_growth_global_1=fee_growth_global_1_before,
            fee_growth_0_low=fee_growth_low_x128_before[2],
            fee_growth_1_low=fee_growth_low_x128_before[3],
            fee_growth_0_hi=fee_growth_hi_x128_before[2],
            fee_growth_1_hi=fee_growth_hi_x128_before[3],
            fee_growth_inside_0=positions_before[8],
            fee_growth_inside_1=positions_before[9],
            liquidity=positions_before[7], tick_lower=tick_lower,
            tick_upper=tick_upper, tick_current=tick_before,
            decimals0=token0_decimals, decimals1=token1_decimals
        )
        nft.cal_apr_in_month(self.before_timestamp, token0_reward_before,
                             token1_reward_before, pool_info, tick_before, tick)

    def _export(self, updated_nfts: Dict[str, NFT]):
        data = [nft.to_dict() for _, nft in updated_nfts.items()]

        if data:
            self._exporter.export_dex_nfts(data)
            logger.info(f'Update {len(data)} nfts')
            self.cnt += len(data)

        if self.deleted_tokens:
            self._exporter.removed_docs(collection_name=DexNFTManagerCollections.dex_nfts, keys=self.deleted_tokens)
            logger.info(f'Remove {len(self.deleted_tokens)} nfts')

    def _end(self):
        logger.info(f'Update total {self.cnt} nfts')
