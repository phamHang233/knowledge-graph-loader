import time
from typing import Dict

import requests
from cli_scheduler.scheduler_job import SchedulerJob
from defi_services.utils.get_fees import get_fees
from web3 import Web3

from artifacts.abis.tcv_abi import TCV_VAULT_ABI
from src.constants.mongo_constants import DexNFTManagerCollections
from src.constants.network_constants import MulticallContract
from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_dex import MongoDBDex
from src.models.nfts import NFT
from src.services.blockchain.multicall import W3Multicall
from src.services.blockchain.state_query_service import StateQueryService

from src.utils.logger_utils import get_logger

logger = get_logger('NFT Info Enricher Job')


class NFTInfoEnricherJob(SchedulerJob):
    def __init__(
            self, _db, _exporter, nfts_batch,
            chain_id, provider_uri, db_prefix, scheduler=None):
        self.dex_nft_db = _db
        self._exporter = _exporter
        self.db_prefix = db_prefix
        self.number_of_nfts_batch = nfts_batch

        self.chain_id = chain_id
        self.state_querier = StateQueryService(provider_uri)
        self._w3 = Web3(Web3.HTTPProvider(provider_uri))

        super().__init__(scheduler)

    def _start(self):
        self.dex_db = MongoDBDex()
        self._etl_db = BlockchainETL(db_prefix=self.db_prefix)
        self.pools = {}
        self.pools_fee = {}
        self.cnt = 0
        self.end_block = self._etl_db.get_last_block_number()
        self.wrong_apr = []
        cursor = self._etl_db.get_block_by_timestamp(int(time.time()) - 24 * 30 * 3600 - 1)
        self.before_block = cursor['block_number']

    def _execute(self, *args, **kwargs):
        for batch_idx in reversed(range(1, self.number_of_nfts_batch + 1)):
            # if batch_idx <=37:
            #     continue
            try:
                start_time = time.time()

                # batch_cursor = self.dex_nft_db.get_nfts_by_filter(
                #     _filter={"flagged": batch_idx, 'chainId': self.chain_id})
                # batch_cursor = self.dex_nft_db.get_nfts_by_filter(
                #     _filter={"flagged": 68, 'liquidity': {"$gt": 0}, 'chainId': self.chain_id, 'poolAddress': "0xaebdca1bc8d89177ebe2308d62af5e74885dccc3"})
                batch_cursor = self.dex_nft_db.get_nfts_by_filter(
                    {'chainId': self.chain_id, 'tokenId': {"$in": ["3157653"]}})
                # new_batch_cursor = list(batch_cursor)
                self.get_information_of_batch_cursor(batch_cursor)
                logger.info(f'Time to execute of batch [{batch_idx}] is {time.time() - start_time} seconds')
            except Exception as e:
                logger.exception(f"[{batch_idx}] has exception-{e}")
                continue

    def get_information_of_batch_cursor(self, nfts):
        current_data_response, before_data_response, pools_in_batch, new_nfts = self.prepare_enrich(nfts)
        cursor = self.dex_db.get_pairs_with_addresses(chain_id=self.chain_id, addresses=list(pools_in_batch))
        self.pools.update({doc['address']: doc for doc in cursor})
        updated_nfts, deleted_tokens = self.enrich_data(new_nfts, current_data_response, before_data_response)
        self._export(updated_nfts, deleted_tokens)

    def prepare_enrich(self, cursor_batch):
        start_time = time.time()

        w3_multicall = W3Multicall(self._w3, address=MulticallContract.get_multicall_contract(self.chain_id))

        current_decoded_data, nfts, important_nfts, pools_in_batch = self.state_querier.get_batch_nft_fee_with_current_block(
            nfts=cursor_batch, pools=self.pools_fee, w3_multicall=w3_multicall)

        before_decoded_data = {}
        w3_multicall.calls = {}
        before_decoded_data.update(self.state_querier.get_batch_nft_fee_with_block_number(nfts=important_nfts,
                                                                                          pools=self.pools_fee,
                                                                                          w3_multicall=w3_multicall,
                                                                                          a_day_ago_block_number=self.before_block,
                                                                                          ))
        logger.info(f"Query process toke {time.time() - start_time}")

        return current_decoded_data, before_decoded_data, pools_in_batch, nfts

    def enrich_data(self, batch_cursor, current_data_response, before_data_response):
        deleted_tokens = []
        updated_nfts: Dict[str, NFT] = {}
        for doc in batch_cursor:
            try:
                token_id = doc['tokenId']
                positions = current_data_response.get(f'positions_{doc["nftManagerAddress"]}_{token_id}_latest'.lower())
                if not positions:
                    deleted_tokens.append(doc["_id"])
                    continue
                nft = NFT(id=token_id, chain=self.chain_id)
                nft.from_dict(doc)
                idx = doc['_id']
                pool_address = nft.pool_address
                pool_info = self.pools.get(pool_address)
                nft_manager_contract = nft.nft_manager_address
                slot0_before = before_data_response.get(f'slot0_{pool_address}_{self.before_block}'.lower())
                if not pool_info or not pool_info.get('tick') or not slot0_before:
                    continue

                nft.liquidity = float(positions[7])
                positions_before = before_data_response.get(
                    f'positions_{nft_manager_contract}_{token_id}_{self.before_block}'.lower())

                if nft.liquidity > 0 and positions_before and abs(positions_before[7] - float(positions[7])) < 0.01:
                    self.calculate_apr(nft, current_data_response, before_data_response)
                else:
                    nft.apr_in_month = 0
                updated_nfts[idx] = nft
            except Exception as e:
                logger.exception(e)
                continue

        return updated_nfts, deleted_tokens

    def calculate_apr(self, nft: NFT, current_data_response, before_data_response):
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
                f'feeGrowthGlobal0X128_{pool_address}_{self.before_block}'.lower())
            fee_growth_global_1_before = before_data_response.get(
                f'feeGrowthGlobal1X128_{pool_address}_{self.before_block}'.lower())
            slot0 = before_data_response.get(f'slot0_{pool_address}_{self.before_block}'.lower())

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
            f'ticks_{pool_address}_{tick_lower}_{self.before_block}'.lower())
        fee_growth_hi_x128_before = before_data_response.get(
            f'ticks_{pool_address}_{tick_upper}_{self.before_block}'.lower())
        positions_before = before_data_response.get(
            f'positions_{nft_manager_contract}_{token_id}_{self.before_block}'.lower())

        positions = current_data_response.get(f'positions_{nft_manager_contract}_{token_id}_latest'.lower())
        fee_growth_low_x128 = current_data_response.get(f'ticks_{pool_address}_{tick_lower}_latest'.lower())
        fee_growth_hi_x128 = current_data_response.get(f'ticks_{pool_address}_{tick_upper}_latest'.lower())

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
        accepted_apr = nft.cal_apr_in_month(self.before_block, token0_reward_before,
                                            token1_reward_before, pool_info, tick_before, tick)
        if not accepted_apr:
            self.wrong_apr.append(nft.to_dict())

    def _export(self, updated_nfts: Dict[str, NFT], deleted_tokens):
        data = [nft.to_dict() for _, nft in updated_nfts.items()]

        if data:
            self._exporter.export_dex_nfts(data)
            logger.info(f'Update {len(data)} nfts')
            self.cnt += len(data)

        if deleted_tokens:
            self._exporter.removed_docs(collection_name=DexNFTManagerCollections.dex_nfts, keys=deleted_tokens)
            logger.info(f'Remove {len(deleted_tokens)} nfts')

    def _end(self):
        self.get_information_of_batch_cursor(self.wrong_apr)
        logger.info(f'Update total {self.wrong_apr} nfts')
