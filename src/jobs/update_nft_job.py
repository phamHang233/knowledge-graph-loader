from typing import Dict

from multithread_processing.base_job import BaseJob

from src.constants.network_constants import Networks, Chains
from src.databases.mongodb_dex import MongoDBDex
from src.exporters import MongoDBExporter
from src.models.nfts import NFT
from src.services.blockchain.state_query_service import StateQueryService
from src.utils.logger_utils import get_logger

logger = get_logger('Liquidity Pools Sync Job')


class UpdateNftInfoJob(BaseJob):
    def __init__(
            self, start_block, end_block,
            batch_size=4, max_workers=8,
            importer=None, exporter: MongoDBExporter = None,
            chain_id=None, query_batch_size=100
    ):
        self.chain_id = chain_id

        self.importer = importer
        self.exporter = exporter

        self.state_querier = StateQueryService(Networks.archive_node.get(Chains.names[self.chain_id]))
        self.query_batch_size = query_batch_size

        self.end_block = end_block
        self.start_block = start_block
        self.dex_db = MongoDBDex()

        work_iterable = range(start_block, end_block + 1)
        super().__init__(work_iterable, batch_size, max_workers)

    def _start(self):
        self.updated_nfts: Dict[str, NFT] = {}
        self.updated_factory_nft = self.exporter.get_config(f"{self.chain_id}_factory_nft_contract")["addresses"]
        if not self.updated_factory_nft:
            self.updated_factory_nft = {}
        self.pools = {}

    def _end(self):
        self.batch_executor.shutdown()

        self._export()

    def _execute_batch(self, works):
        start_block = works[0]
        end_block = works[-1]

        events_cursor = self.importer.get_dex_events_in_block_range(start_block, end_block,
                                                                    event_types=['INCREASELIQUIDITY',
                                                                                 "DECREASELIQUIDITY", "COLLECT"])
        events = list(events_cursor)
        new_pools = set()
        if events:
            token_ids = [f"{self.chain_id}_{event['contract_address']}_{event['tokenId']}" for event in events]
            cursor = self.exporter.get_nfts(token_ids)
            for nft in cursor:
                token_id = nft['tokenId']
                new_pools.add(nft.get('poolAddress'))
                if token_id not in self.updated_nfts:
                    self.updated_nfts[token_id] = NFT(token_id, chain=self.chain_id)
                    self.updated_nfts[token_id].from_dict(nft)

            missing_nfts_info = self.collecting_missing_nft(events, new_pools)

            pool_keys = [pool for pool in new_pools if pool not in self.pools]
            cursor = self.dex_db.get_pairs_with_addresses(chain_id=self.chain_id, addresses=pool_keys)
            self.pools.update({doc['address']: doc for doc in cursor})
            self.process_event(events, missing_nfts_info)

    def collecting_missing_nft(self, events, new_pools):
        missing_nfts = []
        for event in events:
            token_id = event['tokenId']
            contract_address = event['contract_address']
            if token_id not in self.updated_nfts:
                missing_nfts.append({
                    'token_id': token_id,
                    'block_number': event['block_number'],
                    'contract_address': contract_address
                })
        if missing_nfts:
            data = self.state_querier.get_batch_nft_info_with_block_number(
                missing_nfts, factory_nft_contracts=self.updated_factory_nft, new_pools=new_pools)
            return data
        return {}

    def process_event(self, events, data):
        for event in events:
            if event['event_type'] == 'INCREASELIQUIDITY' or event['event_type'] == 'DECREASELIQUIDITY':
                self.aggregate_change_liquidity_event(event, data)

            if event['event_type'] == 'COLLECT':
                self.aggregate_collect_event(event, data)

    def aggregate_change_liquidity_event(self, event, data):
        token_id = event['tokenId']
        liquidity = float(event['liquidity'])
        nft_info = self.updated_nfts.get(token_id)
        block_number = event['block_number']
        if data and data.get(token_id) and not nft_info:
            query_info = data[token_id]
            self.updated_nfts[token_id] = NFT(token_id, self.chain_id)
            nft_info = self.updated_nfts.get(token_id)
            nft_info.liquidity = query_info.get('liquidity')
            nft_info.tick_upper = query_info.get('tick_upper')
            nft_info.tick_lower = query_info.get('tick_lower')
            nft_info.first_called_at = query_info.get('first_called_at')
            nft_info.pool_address = query_info.get('pool_address')
            nft_info.nft_manager_address = event['contract_address']
        else:
            return

        if block_number > nft_info.last_interact_at:
            if event['event_type'] == "INCREASELIQUIDITY":
                nft_info.liquidity += liquidity
            if event['event_type'] == "DECREASELIQUIDITY":
                nft_info.liquidity -= liquidity

        nft_info.last_interact_at = block_number
        nft_info.liquidity_change_logs[str(block_number)] = nft_info.liquidity

    def aggregate_collect_event(self, event, data):
        token_id = event['tokenId']
        nft_info = self.updated_nfts.get(token_id)
        block_number = event['block_number']
        if data and data.get(token_id) and not nft_info:
            query_info = data[token_id]
            self.updated_nfts[token_id] = NFT(token_id, self.chain_id)
            pool_address = query_info.get('pool_address')
            nft_info = self.updated_nfts.get(token_id)
            nft_info.liquidity = query_info.get('liquidity')
            nft_info.tick_upper = query_info.get('tick_upper')
            nft_info.tick_lower = query_info.get('tick_lower')
            nft_info.first_called_at = query_info.get('first_called_at')
            nft_info.pool_address = pool_address
            nft_info.nft_manager_address = event['contract_address']
            nft_info.last_interact_at = query_info.get('first_called_at')
        else:
            return

        pool_info = self.pools.get(pool_address)
        if pool_info and pool_info.get("tokens"):
            tokens = pool_info['tokens']
            for idx, token in enumerate(tokens):
                address = token.get('address')
                amount = float(event[f'amount{idx}'])
                decimals = token.get('decimals')
                if address not in nft_info.collected_fee:
                    nft_info.collected_fee[address] = 0
                nft_info.collected_fee[address] += amount / 10 ** decimals
                nft_info.fee_change_logs[str(block_number)] = nft_info.collected_fee
        nft_info.last_interact_at = block_number

    def _export(self):
        data = [p.to_dict() for pool_address, p in self.updated_nfts.items()]
        config = {
            "id": f"{self.chain_id}_factory_nft_contract",
            "addresses": self.updated_factory_nft,
            "chainId": self.chain_id
        }
        self.exporter.export_config(config)
        if data:
            self.exporter.export_dex_nfts(data)
            logger.info(f'Exported {len(data)} nfts')
