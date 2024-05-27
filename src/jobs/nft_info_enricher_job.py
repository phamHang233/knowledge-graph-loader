import time
from typing import Dict

from cli_scheduler.scheduler_job import SchedulerJob
from defi_services.utils.get_fees import get_fees
from query_state_lib.client.client_querier import ClientQuerier

from artifacts.abis.dexes.uniswap_v3_nft_manage_abi import UNISWAP_V3_NFT_MANAGER_ABI
from artifacts.abis.dexes.uniswap_v3_pool_abi import UNISWAP_V3_POOL_ABI
from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_dex import MongoDBDex
from src.models.nfts import NFT
from src.services.blockchain.batch_queries_service import add_rpc_call, decode_data_response_ignore_error

from src.utils.logger_utils import get_logger
logger = get_logger('NFT Info Enricher Job')


class NFTInfoEnricherJob(SchedulerJob):
    def __init__(
            self, _db, _exporter, scheduler, batch_size,
            chain_id, provider_uri, db_prefix):
        self._klg_db = _db
        self._exporter = _exporter
        self.db_prefix = db_prefix
        # self.end_block = end_block
        # self.start_timestamp = start_timestamp
        self.batch_size = batch_size
        self.chain_id = chain_id
        # self.last_synced_file = last_synced_file
        self.client_querier = ClientQuerier(provider_url=provider_uri)
        super().__init__(scheduler=scheduler)

    def _pre_start(self):
        self.dex_db = MongoDBDex()
        # progress_logger = logging.getLogger('ProgressLogger')
        self._etl_db = BlockchainETL(db_prefix=self.db_prefix)

    def _start(self):
        self.nfts: Dict[str, NFT] = {}
        self.deleted_tokens = []
        self.pools = {}
        self.pools_info_with_provider = {}
        self.invalid_pool = []
        self.token_price = {}
        self.end_block = self._etl_db.get_last_block_number()
        current_day_timestamp = int(int(time.time()) / 24 / 3600) * 24 * 3600
        cursor = self._etl_db.get_block_by_timestamp(current_day_timestamp - 24 * 30 * 3600)
        for doc in cursor:
            self.before_30_days_block = doc['number']

    def _execute(self, *args, **kwargs):
        cursor = self._klg_db.get_all_nfts({"liquidity": {"$gt": 0}}) #TODO: need fix limit
        cursor = list(cursor)
        for idx in range(0, len(cursor), self.batch_size):
            batch = cursor[idx:idx + self.batch_size]
            while True:
                try:
                    self._execute_batch(batch)
                    break
                except Exception as ex:
                    logger.exception(ex)
                    logger.warning(f"Batch id {idx} has something wrong!")
                    if self.retry:
                        self._retry()

    def _execute_batch(self, batch_cursor):
        data_response, pools_in_batch = self.prepare_enrich(batch_cursor)

        cursor = self.dex_db.get_pairs_with_addresses(chain_id=self.chain_id, addresses=list(pools_in_batch))
        # for doc in cursor:
        #
        #     self.pools[doc['address']] = doc
        self.pools.update({doc['address'] : doc for doc in cursor})
        updated_nfts = self.enrich_data(batch_cursor, data_response)
        self._export(updated_nfts)

    def prepare_enrich(self, cursor_batch):
        list_rpc_call = []
        list_call_id = []
        pools_in_batch = set()
        for doc in cursor_batch:
            token_id = doc['tokenId']
            nft = NFT(id=token_id, chain=self.chain_id)
            nft.from_dict(doc)
            pool_address = doc.get('poolAddress')
            if pool_address not in self.pools and pool_address not in self.invalid_pool:
                pools_in_batch.add(pool_address)

            tick_lower = nft.tick_lower
            tick_upper = nft.tick_upper
            nft_manager_address = nft.nft_manager_address
            pool_address = nft.pool_address

            if not self.pools_info_with_provider.get(pool_address, {}).get("feeGrowthGlobal0X128"):
                add_rpc_call(
                    abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                    fn_name="feeGrowthGlobal0X128", block_number='latest',
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )

                add_rpc_call(
                    abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                    fn_name="feeGrowthGlobal1X128", block_number='latest',
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )
            add_rpc_call(
                abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                fn_name="ticks", block_number='latest', fn_paras=tick_lower,
                list_call_id=list_call_id, list_rpc_call=list_rpc_call
            )
            add_rpc_call(
                abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                fn_name="ticks", block_number='latest', fn_paras=tick_upper,
                list_call_id=list_call_id, list_rpc_call=list_rpc_call
            )
            add_rpc_call(
                abi=UNISWAP_V3_NFT_MANAGER_ABI, contract_address=nft_manager_address,
                fn_name="positions", block_number='latest', fn_paras=int(token_id),
                list_call_id=list_call_id, list_rpc_call=list_rpc_call
            )
            unchanged_nft = True
            liquidity_change_logs = nft.liquidity_change_logs
            for block in liquidity_change_logs:
                if int(block) > self.before_30_days_block:
                    unchanged_nft = False
                    break

            ###
            if unchanged_nft:
                if not self.pools_info_with_provider.get(pool_address, {}).get("feeGrowthGlobal0X128Before"):
                    add_rpc_call(
                        abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                        fn_name="feeGrowthGlobal0X128", block_number=self.before_30_days_block,
                        list_call_id=list_call_id, list_rpc_call=list_rpc_call
                    )

                    add_rpc_call(
                        abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                        fn_name="feeGrowthGlobal1X128", block_number=self.before_30_days_block,
                        list_call_id=list_call_id, list_rpc_call=list_rpc_call
                    )
                    add_rpc_call(
                        abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                        fn_name="slot0", block_number=self.before_30_days_block,
                        list_call_id=list_call_id, list_rpc_call=list_rpc_call
                    )
                add_rpc_call(
                    abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                    fn_name="ticks", block_number=self.before_30_days_block, fn_paras=tick_lower,
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )
                add_rpc_call(
                    abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
                    fn_name="ticks", block_number=self.before_30_days_block, fn_paras=tick_upper,
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )
                add_rpc_call(
                    abi=UNISWAP_V3_NFT_MANAGER_ABI, contract_address=nft_manager_address,
                    fn_name="positions", block_number=self.before_30_days_block, fn_paras=int(token_id),
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )
        try:
            data_response = self.client_querier.sent_batch_to_provider(list_rpc_call)
            decoded_data = decode_data_response_ignore_error(data_response, list_call_id)
        except Exception as e:
            raise e

        return decoded_data, pools_in_batch

    def enrich_data(self, batch_cursor, data_response) -> Dict[str, NFT]:
        updated_nfts: Dict[str, NFT] = {}
        for doc in batch_cursor:
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

            tick = pool_info['tick']

            # if tick_lower < tick < tick_upper:
            if not self.pools_info_with_provider.get(pool_address, {}).get("feeGrowthGlobal0X128"):
                fee_growth_global_0 = data_response.get(f'feeGrowthGlobal0X128_{pool_address}_latest'.lower())
                fee_growth_global_1 = data_response.get(f'feeGrowthGlobal1X128_{pool_address}_latest'.lower())
                self.pools_info_with_provider[pool_address] = {
                    "feeGrowthGlobal0X128": fee_growth_global_0,
                    "feeGrowthGlobal1X128": fee_growth_global_1,
                }
            else:
                fee_growth_global_0 = self.pools_info_with_provider[pool_address]['feeGrowthGlobal0X128']
                fee_growth_global_1 = self.pools_info_with_provider[pool_address]['feeGrowthGlobal1X128']

            fee_growth_low_x128 = data_response.get(f'ticks_{pool_address}_{tick_lower}_latest'.lower())
            fee_growth_hi_x128 = data_response.get(f'ticks_{pool_address}_{tick_upper}_latest'.lower())
            positions = data_response.get(f'positions_{nft_manager_contract}_{token_id}_latest'.lower())

            ###
            if positions:
                nft.liquidity = positions[7]
                if nft.liquidity == 0:
                    print(nft.token_id)
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

                unchanged_nft = True
                liquidity_change_logs = nft.liquidity_change_logs
                for block in liquidity_change_logs:
                    if int(block) > self.before_30_days_block:
                        unchanged_nft = False
                        break
                if unchanged_nft:
                    if not self.pools_info_with_provider.get(pool_address, {}).get("feeGrowthGlobal0X128Before"):
                        fee_growth_global_0_before = data_response.get(
                            f'feeGrowthGlobal0X128_{pool_address}_{self.before_30_days_block}'.lower())
                        fee_growth_global_1_before = data_response.get(
                            f'feeGrowthGlobal1X128_{pool_address}_{self.before_30_days_block}'.lower())
                        slot0 = data_response.get(f'slot0_{pool_address}_{self.before_30_days_block}'.lower())
                        self.pools_info_with_provider[pool_address] = {
                            "feeGrowthGlobal0X128Before": fee_growth_global_0_before,
                            "feeGrowthGlobal1X128Before": fee_growth_global_1_before,
                            'tick': slot0[1]
                        }
                    fee_growth_global_0_before = self.pools_info_with_provider[pool_address]['feeGrowthGlobal0X128Before']
                    fee_growth_global_1_before = self.pools_info_with_provider[pool_address]['feeGrowthGlobal1X128Before']
                    tick_before = self.pools_info_with_provider[pool_address]['tick']

                    fee_growth_low_x128_before = data_response.get(
                        f'ticks_{pool_address}_{tick_lower}_{self.before_30_days_block}'.lower())
                    fee_growth_hi_x128_before = data_response.get(
                        f'ticks_{pool_address}_{tick_upper}_{self.before_30_days_block}'.lower())
                    positions_before = data_response.get(
                        f'positions_{nft_manager_contract}_{token_id}_{self.before_30_days_block}'.lower())
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
        return updated_nfts

    def _export(self, updated_nfts: Dict[str, NFT]):
        data = [nft.to_dict() for _, nft in updated_nfts.items()]

        if data:
            self._exporter.export_dex_nfts(data)
            logger.info(f'Update {len(data)} nfts')

        if self.deleted_tokens:
            self._exporter.removed_docs(collection_name='dex_nfts', keys=self.deleted_tokens)
            logger.info(f'Remove {len(self.deleted_tokens)} nfts')
