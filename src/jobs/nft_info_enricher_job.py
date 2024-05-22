
import time
from typing import Dict

from defi_services.utils.get_fees import get_fees
from query_state_lib.client.client_querier import ClientQuerier

from artifacts.abis.dexes.uniswap_v3_nft_manage_abi import UNISWAP_V3_NFT_MANAGER_ABI
from artifacts.abis.dexes.uniswap_v3_pool_abi import UNISWAP_V3_POOL_ABI
from src.constants.time_constants import TimeConstants
from src.databases.mongodb_dex import MongoDBDex
from src.jobs.base.cli_job import CLIJob
from src.models.nfts import NFT
from src.services.blockchain.batch_queries_service import add_rpc_call, decode_data_response_ignore_error
from src.utils.file_utils import write_last_synced_file
from src.utils.logger_utils import get_logger

logger = get_logger('NFT Info Enricher Job')


class NFTInfoEnricherJob(CLIJob):
    def __init__(
            self, _db, _exporter, start_timestamp, end_timestamp, interval, max_workers, batch_size,
            chain_id, last_synced_file, provider_uri,
            n_cpu, cpu, stream_id, monitor, end_block, before_30_days_block):
        self._klg_db = _db
        self._exporter = _exporter
        self.monitor = monitor
        self.end_block = end_block
        self.before_30_days_block = before_30_days_block
        self.stream_id = stream_id
        self.cpu = cpu
        self.n_cpu = n_cpu
        self.provider_uri = provider_uri
        self.start_timestamp = start_timestamp
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.chain_id = chain_id
        self.last_synced_file = last_synced_file
        self.dex_db = MongoDBDex()
        self.client_querier = ClientQuerier(provider_url=provider_uri)
        super().__init__(interval, end_timestamp, retry=False)

    def _start(self):
        cursor = self._klg_db.get_all_nfts()
        self.nfts: Dict[str, NFT] = {}
        self.deleted_tokens = []
        self.pools = {}
        self.pools_info_with_provider = {}
        self.invalid_pool = []
        cursor = list(cursor)
        for idx in range(0, len(cursor), self.batch_size):
            batch = cursor[idx:idx + self.batch_size]
            self._execute_batch(batch)

    # def _execute(self, *args, **kwargs):

    def _execute_batch(self, batch_cursor):
        pools_in_batch = set()
        nfts_batch = {}
        for doc in batch_cursor:
            token_id = doc['tokenId']
            nft = NFT(id=token_id, chain=self.chain_id)
            nft.from_dict(doc)
            nfts_batch[doc["_id"]] = nft
            pool_address = doc.get('poolAddress')
            if pool_address not in self.pools and pool_address not in self.invalid_pool:
                pools_in_batch.add(pool_address)

        cursor = self.dex_db.get_pairs_with_addresses(chain_id=self.chain_id, addresses=list(pools_in_batch))
        self.pools.update({doc['address']: doc for doc in cursor})

        data_response = self.prepare_enrich(nfts_batch, pools_in_batch)
        self.enrich_data(nfts_batch, data_response)
        self._export(nfts_batch)

    def prepare_enrich(self, nfts_batch: Dict[str, NFT], pool_contracts):
        list_rpc_call = []
        list_call_id = []

        for idx, nft in nfts_batch.items():
            token_id = int(nft.token_id)
            tick_lower = nft.tick_lower
            tick_upper = nft.tick_upper
            nft_manager_address = nft.nft_manager_address
            liquidity = nft.liquidity

            pool_address = nft.pool_address
            pool_info = self.pools.get(pool_address)
            if not pool_info or not pool_info.get('tick'):
                self.invalid_pool.append(pool_address)
                continue

            if not nft.wallet:
                add_rpc_call(
                    abi=UNISWAP_V3_NFT_MANAGER_ABI, contract_address=nft_manager_address,
                    fn_name="ownerOf", fn_paras=token_id, block_number='latest',
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )
            tick = pool_info['tick']
            if tick_lower < tick < tick_upper and liquidity > 0:
                if pool_address not in self.pools_info_with_provider:
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
                    fn_name="positions", block_number='latest', fn_paras=token_id,
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
                    if pool_address not in self.pools_info_with_provider:
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
                        fn_name="positions", block_number=self.before_30_days_block, fn_paras=token_id,
                        list_call_id=list_call_id, list_rpc_call=list_rpc_call
                    )
        try:
            data_response = self.client_querier.sent_batch_to_provider(list_rpc_call)
            decoded_data = decode_data_response_ignore_error(data_response, list_call_id)
        except Exception as e:
            raise e

        return decoded_data

    def enrich_data(self, nfts: Dict[str, NFT], data_response):
        for idx, nft in nfts.items():
            token_id = nft.token_id
            pool_address = nft.pool_address
            nft_manager_contract = nft.nft_manager_address
            tick_lower = nft.tick_lower
            tick_upper = nft.tick_upper
            block_number = 'latest'
            liquidity = nft.liquidity
            pool_info = self.pools.get(pool_address)
            if not pool_info or not pool_info.get('tick'):
                self.invalid_pool.append(pool_address)
                continue

            tick = pool_info['tick']
            if not nft.wallet:
                nft.wallet = data_response.get(
                    f'ownerOf_{nft_manager_contract}_{token_id}_{block_number}'.lower())

            if tick_lower < tick < tick_upper and liquidity > 0:
                if pool_address not in self.pools_info_with_provider:
                    fee_growth_global_0 = data_response.get(
                        f'feeGrowthGlobal0X128_{pool_address}_{block_number}'.lower())
                    fee_growth_global_1 = data_response.get(
                        f'feeGrowthGlobal1X128_{pool_address}_{block_number}'.lower())
                    self.pools_info_with_provider[pool_address] = {
                        "feeGrowthGlobal0X128": fee_growth_global_0,
                        "feeGrowthGlobal1X128": fee_growth_global_1,
                    }
                else:
                    fee_growth_global_0 = self.pools_info_with_provider[pool_address]['feeGrowthGlobal0X128']
                    fee_growth_global_1 = self.pools_info_with_provider[pool_address]['feeGrowthGlobal1X128']

                fee_growth_low_x128 = data_response.get(
                    f'ticks_{pool_address}_{tick_lower}_{block_number}'.lower())
                fee_growth_hi_x128 = data_response.get(
                    f'ticks_{pool_address}_{tick_upper}_{block_number}'.lower())
                positions = data_response.get(f'positions_{nft_manager_contract}_{token_id}_{block_number}'.lower())

                unchanged_nft = True
                liquidity_change_logs = nft.liquidity_change_logs
                for block in liquidity_change_logs:
                    if block > self.before_30_days_block:
                        unchanged_nft = False
                        break

                ###
                if positions:
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
                        liquidity=liquidity, tick_lower=tick_lower,
                        tick_upper=tick_upper, tick_current=pool_info.get("tick"),
                        decimals0=token0_decimals, decimals1=token1_decimals
                    )
                    if unchanged_nft:
                        fee_growth_global_0_before = data_response.get(
                            f'feeGrowthGlobal0X128_{pool_address}_{block_number}'.lower())
                        fee_growth_global_1_before = data_response.get(
                            f'feeGrowthGlobal1X128_{pool_address}_{block_number}'.lower())
                        fee_growth_low_x128_before = data_response.get(
                            f'ticks_{pool_address}_{tick_lower}_{block_number}'.lower())
                        fee_growth_hi_x128_before = data_response.get(
                            f'ticks_{pool_address}_{tick_upper}_{block_number}'.lower())
                        positions_before = data_response.get(
                            f'positions_{nft_manager_contract}_{token_id}_{block_number}'.lower())

                        token0_reward_before, token1_reward_before = get_fees(
                            fee_growth_global_0=fee_growth_global_0_before,
                            fee_growth_global_1=fee_growth_global_1_before,
                            fee_growth_0_low=fee_growth_low_x128_before[2],
                            fee_growth_1_low=fee_growth_low_x128_before[3],
                            fee_growth_0_hi=fee_growth_hi_x128_before[2],
                            fee_growth_1_hi=fee_growth_hi_x128_before[3],
                            fee_growth_inside_0=positions_before[8],
                            fee_growth_inside_1=positions_before[9],
                            liquidity=liquidity, tick_lower=tick_lower,
                            tick_upper=tick_upper, tick_current=tick,
                            decimals0=token0_decimals, decimals1=token1_decimals
                        )
                        nft.fee_30_days_before[tokens[0]['address']] = token0_reward_before / 10 ** token0_decimals
                        nft.fee_30_days_before[tokens[1]['address']] = token1_reward_before / 10 ** token1_decimals
                        nft.last_updated_fee_at = block_number
                    nft.uncollected_fee[tokens[0]['address']] = token0_reward / 10 ** token0_decimals
                    nft.uncollected_fee[tokens[1]['address']] = token1_reward / 10 ** token0_decimals
                else:
                    self.deleted_tokens.append(idx)

    def _export(self, nfts_batch: Dict[str, NFT]):
        data = [nft.to_dict() for _, nft in nfts_batch.items()]

        if data:
            self._exporter.export_dex_nfts(data)
            logger.info(f'Update {len(data)} nfts')

        if self.deleted_tokens:
            self._exporter.removed_docs(collection_name='dex_nfts', keys=self.deleted_tokens)
            logger.info(f'Remove {len(self.deleted_tokens)} nfts')

    def run(self, *args, **kwargs):
        while True:
            try:
                self._start()
                self._execute(*args, **kwargs)
                write_last_synced_file(self.last_synced_file, self.start_timestamp)
            except Exception as ex:
                logger.exception(ex)
                logger.warning('Something went wrong!!!')
                if self.retry:
                    self._retry()
                    continue
                raise ex

            self._end()

            # Check if not repeat
            if not self.interval:
                break

            # Check if finish
            next_synced_timestamp = self._get_next_synced_timestamp()
            if self._check_finish(next_synced_timestamp):
                break

            # Sleep to next synced time
            time_sleep = next_synced_timestamp - time.time() + TimeConstants.MINUTES_5
            if time_sleep > 0:
                logger.info(f'Sleep {round(time_sleep, 3)} seconds')
                time.sleep(time_sleep)

        self._follow_end()
