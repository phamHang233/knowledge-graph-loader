import time
from typing import Dict

from defi_services.utils.get_fees import get_fees
from multithread_processing.base_job import BaseJob
from src.constants.mongo_constants import DexNFTManagerCollections
from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_dex import MongoDBDex
from src.models.nfts import NFT
from src.services.blockchain.batch_queries_service import decode_data_response_ignore_error
from src.services.blockchain.state_query_service import StateQueryService

from src.utils.logger_utils import get_logger
logger = get_logger('NFT Info Enricher Job')


class NFTInfoEnricherJob(BaseJob):
    def __init__(
            self, _db, _exporter, max_workers, batch_size, nfts_batch,
            chain_id, provider_uri, db_prefix, n_cpu=1, cpu=1,):
        self.n_cpu = n_cpu
        self.cpu = cpu - 1

        self._klg_db = _db
        self._exporter = _exporter
        self.db_prefix = db_prefix

        self.batch_size = batch_size
        self.number_of_nfts_batch = nfts_batch

        self.chain_id = chain_id
        # self.client_querier = ClientQuerier(provider_url=provider_uri)
        self.state_querier = StateQueryService(provider_uri)

        work_iterable = self._get_work_iterable()
        super().__init__(work_iterable, batch_size, max_workers)

    def _get_work_iterable(self):
        work_iterable = [idx for idx in range(1, self.number_of_nfts_batch + 1) if
                         (idx - 1) % self.n_cpu == self.cpu]
        return work_iterable

    def _start(self):
        self.dex_db = MongoDBDex()
        self._etl_db = BlockchainETL(db_prefix=self.db_prefix)
        self.nfts: Dict[str, NFT] = {}
        self.deleted_tokens = []
        self.pools = {}
        self.current_pools_info = {}
        self.invalid_pool = []
        self.token_price = {}
        self.end_block = self._etl_db.get_last_block_number()
        current_day_timestamp = int(int(time.time()) / 24 / 3600) * 24 * 3600
        cursor = self._etl_db.get_block_by_timestamp(current_day_timestamp - 24 * 30 * 3600 + 3600 -1)
        for doc in cursor:
            self.before_30_days_block = doc['number']
        # self.before_30_days_block = 19331243

    def _execute_batch(self, nfts_batch_indicates):
        for batch_idx in nfts_batch_indicates:
            start_time = time.time()
            # batch_cursor = self._klg_db.get_nfts_by_flag(_filter={"liquidity": {"$gt": 0}, "flagged": batch_idx})
            batch_cursor = self._klg_db.get_nfts_by_flag(_filter={"tokenId": "690066"})
            batch_cursor = list(batch_cursor)
            data_response, pools_in_batch = self.prepare_enrich(batch_cursor)

            cursor = self.dex_db.get_pairs_with_addresses(chain_id=self.chain_id, addresses=list(pools_in_batch))

            self.pools.update({doc['address']: doc for doc in cursor})
            updated_nfts = self.enrich_data(batch_cursor, data_response)
            self._export(updated_nfts)

            logger.info(f'Time to execute of batch [{batch_idx}] is {time.time() - start_time} seconds')

    def prepare_enrich(self, cursor_batch):
        list_rpc_call = []
        list_call_id = []
        pools_in_batch = []
        start_time = time.time()
        current_nfts = []
        for idx, doc in enumerate(cursor_batch):
            pool_address = doc['poolAddress']
            pools_in_batch.append(pool_address)

            liquidity_change_logs = doc.get('liquidityChangeLogs', {})
            unchanged_nft = True
            for block_number in liquidity_change_logs:
                if int(block_number) > self.before_30_days_block:
                    unchanged_nft = False
                    break
            if unchanged_nft:
                current_nfts.append({
                    'tokenId': doc['tokenId'],
                    'poolAddress': pool_address,
                    'tickLower': doc['tickLower'],
                    "tickUpper": doc['tickUpper'],
                    'nftManagerAddress': doc['nftManagerAddress'],
                })
        self.state_querier.get_batch_nft_fee_with_block_number(
            nfts=current_nfts, pools=self.current_pools_info, block_number='latest', list_rpc_call=list_rpc_call, list_call_id=list_call_id)
        self.state_querier.get_batch_nft_fee_with_block_number(
            nfts=current_nfts, pools=self.current_pools_info, list_rpc_call=list_rpc_call,list_call_id=list_call_id, block_number=self.before_30_days_block)

        data_response = self.state_querier.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=1000)
        decoded_data = decode_data_response_ignore_error(data_response, list_call_id)
        logger.info(f"Query process toke {time.time() - start_time}")
        return decoded_data, pools_in_batch

    def enrich_data(self, batch_cursor, data_response) -> Dict[str, NFT]:
        updated_nfts: Dict[str, NFT] = {}
        for doc in batch_cursor:
            try:
                token_id = doc['tokenId']
                idx = doc['_id']
                nft = NFT(id=token_id, chain=self.chain_id)
                nft.from_dict(doc)
                pool_address = nft.pool_address
                nft_manager_contract = nft.nft_manager_address
                tick_lower = nft.tick_lower
                tick_upper = nft.tick_upper
                pool_info = self.pools.get(pool_address)
                if not pool_info or not pool_info.get('tick'):
                    self.invalid_pool.append(pool_address)
                    continue

                unchanged_nft = True
                liquidity_change_logs = nft.liquidity_change_logs
                for block in liquidity_change_logs:
                    if int(block) > self.before_30_days_block:
                        unchanged_nft = False
                        break

                if unchanged_nft:
                    tick = pool_info['tick']
                    if not self.current_pools_info.get(pool_address, {}):
                        fee_growth_global_0 = data_response.get(f'feeGrowthGlobal0X128_{pool_address}_latest'.lower())
                        fee_growth_global_1 = data_response.get(f'feeGrowthGlobal1X128_{pool_address}_latest'.lower())
                        fee_growth_global_0_before = data_response.get(
                            f'feeGrowthGlobal0X128_{pool_address}_{self.before_30_days_block}'.lower())
                        fee_growth_global_1_before = data_response.get(
                            f'feeGrowthGlobal1X128_{pool_address}_{self.before_30_days_block}'.lower())
                        slot0 = data_response.get(f'slot0_{pool_address}_{self.before_30_days_block}'.lower())

                        self.current_pools_info[pool_address] = {
                            "feeGrowthGlobal0X128": fee_growth_global_0,
                            "feeGrowthGlobal1X128": fee_growth_global_1,
                            "feeGrowthGlobal0X128Before": fee_growth_global_0_before,
                            "feeGrowthGlobal1X128Before": fee_growth_global_1_before,
                            'tick': slot0[1] if slot0 else None
                        }
                    fee_growth_global_0 = self.current_pools_info[pool_address]['feeGrowthGlobal0X128']
                    fee_growth_global_1 = self.current_pools_info[pool_address]['feeGrowthGlobal1X128']
                    fee_growth_global_0_before = self.current_pools_info[pool_address]['feeGrowthGlobal0X128Before']
                    fee_growth_global_1_before = self.current_pools_info[pool_address]['feeGrowthGlobal1X128Before']
                    tick_before = self.current_pools_info[pool_address]['tick']

                    fee_growth_low_x128_before = data_response.get(
                        f'ticks_{pool_address}_{tick_lower}_{self.before_30_days_block}'.lower())
                    fee_growth_hi_x128_before = data_response.get(
                        f'ticks_{pool_address}_{tick_upper}_{self.before_30_days_block}'.lower())

                    positions_before = data_response.get(
                        f'positions_{nft_manager_contract}_{token_id}_{self.before_30_days_block}'.lower())

                    fee_growth_low_x128 = data_response.get(f'ticks_{pool_address}_{tick_lower}_latest'.lower())
                    fee_growth_hi_x128 = data_response.get(f'ticks_{pool_address}_{tick_upper}_latest'.lower())
                    positions = data_response.get(f'positions_{nft_manager_contract}_{token_id}_latest'.lower())
    
                    ###
                    if positions:
                        if positions[7] == 0:
                            continue
                        nft.liquidity = float(positions[7])
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
                            tick_upper=tick_upper, tick_current=tick,
                            decimals0=token0_decimals, decimals1=token1_decimals
                        )
                        nft.uncollected_fee[tokens[0]['address']] = token0_reward
                        nft.uncollected_fee[tokens[1]['address']] = token1_reward
                        nft.last_updated_fee_at = self.end_block

                        if tick_before is not None:
                            if positions[7] != positions_before[7]:
                                print(f"token id {token_id} has smt wrong!!")
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
                            nft.apr_in_month = nft.cal_apr_in_month(self.before_30_days_block, token0_reward_before,
                                                                    token1_reward_before, pool_info, tick_before, tick)

                    else:
                        self.deleted_tokens.append(idx)
                    updated_nfts[idx] = nft
            except Exception as e:
                # raise e
                logger.error(e)
                continue
        return updated_nfts

    def _export(self, updated_nfts: Dict[str, NFT]):
        data = [nft.to_dict() for _, nft in updated_nfts.items()]

        if data:
            self._exporter.export_dex_nfts(data)
            logger.info(f'Update {len(data)} nfts')

        if self.deleted_tokens:
            self._exporter.removed_docs(collection_name=DexNFTManagerCollections.dex_nfts, keys=self.deleted_tokens)
            logger.info(f'Remove {len(self.deleted_tokens)} nfts')
