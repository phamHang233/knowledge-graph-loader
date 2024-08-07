from typing import Dict

from multithread_processing.base_job import BaseJob

from src.constants.network_constants import Networks, Chains
from src.databases.mongodb_dex import MongoDBDex
from src.exporters.nft_mongodb_exporter import NFTMongoDBExporter
from src.models.nfts import NFT
from src.models.wallet import Wallet
from src.services.blockchain.state_query_service import StateQueryService
from src.utils.logger_utils import get_logger

logger = get_logger('Liquidity Pools Sync Job')


class UpdateNftInfoJob(BaseJob):
    def __init__(
            self, start_block, end_block,
            batch_size=4, max_workers=8,
            importer=None, exporter: NFTMongoDBExporter = None,
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
        self.updated_wallet: Dict[str, Wallet] = {}

        work_iterable = range(start_block, end_block + 1)
        super().__init__(work_iterable, batch_size, max_workers)

    def _start(self):
        self.updated_nfts: Dict[str, NFT] = {}
        self.updated_factory_nft = {}
        cursor = self.exporter.get_config(f"{self.chain_id}_factory_nft_contract")
        if cursor:
            self.updated_factory_nft = cursor['addresses']
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
            token_keys = [f"{self.chain_id}_{event['contract_address']}_{event['tokenId']}" for event in events]
            cursor = self.exporter.get_nfts(token_keys)
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
                self.aggregate_collect_event(event, events, data)

    def aggregate_change_liquidity_event(self, event, data):
        token_id = event['tokenId']
        liquidity = float(event['liquidity'])
        nft_info = self.updated_nfts.get(token_id)
        block_number = event['block_number']
        if nft_info is None and data and data.get(token_id):
            query_info = data[token_id]
            self.updated_nfts[token_id] = NFT(token_id, self.chain_id)
            nft_info = self.updated_nfts.get(token_id)
            nft_info.liquidity = query_info.get('liquidity')
            nft_info.tick_upper = query_info.get('tick_upper')
            nft_info.tick_lower = query_info.get('tick_lower')
            nft_info.last_called_at = query_info.get('last_called_at')
            nft_info.pool_address = query_info.get('pool_address')
            nft_info.nft_manager_address = event['contract_address']
            nft_info.wallet = query_info.get('wallet')
            # nft_info.last_interact_at = query_info.get('last_called_at')

        if not nft_info:
            return
        if block_number > nft_info.last_called_at:
            if event['event_type'] == "INCREASELIQUIDITY":
                nft_info.liquidity += liquidity
            if event['event_type'] == "DECREASELIQUIDITY":
                nft_info.liquidity -= liquidity

        # nft_info.liquidity_change_logs[str(block_number)] = nft_info.liquidity
        nft_info.last_interact_at = max(nft_info.last_interact_at, block_number)

    def aggregate_collect_event(self, event, events, data):
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
            nft_info.last_called_at = query_info.get('last_called_at')
            nft_info.pool_address = pool_address
            nft_info.nft_manager_address = event['contract_address']
            nft_info.wallet = query_info.get('wallet')
        if not nft_info:
            return

        decrease_event = None
        for e in events:
            if e['transaction_hash'] == event['transaction_hash'] and e['event_type'] == 'DECREASELIQUIDITY':
                decrease_event = e
                break

        # nft_info.last_interact_at = block_number
        pool_info = self.pools.get(nft_info.pool_address)
        if pool_info and pool_info.get("tokens"):
            tokens = pool_info['tokens']
            nft_info.fee_change_logs[str(block_number)] = {}
            for idx, token in enumerate(tokens):
                address = token.get('address')
                collect_amount = float(event[f'amount{idx}'])
                fee_amount = collect_amount if not decrease_event else collect_amount - float(decrease_event[f'amount{idx}'])
                decimals = token.get('decimals')
                if address not in nft_info.collected_fee:
                    nft_info.collected_fee[address] = 0
                nft_info.collected_fee[address] += fee_amount / 10 ** decimals
                nft_info.fee_change_logs[str(block_number)][address] = fee_amount / 10 ** decimals

    def update_wallet_info(self):
        addresses = [nft_info.wallet for _, nft_info in self.updated_nfts.items()]
        cursor = self.exporter.get_wallets(addresses=addresses, chain_id=self.chain_id)
        for doc in cursor:
            address = doc['address']
            self.updated_wallet[address] = Wallet(address, chain_id=self.chain_id)
            self.updated_wallet[address].from_dict(doc)
        for token_id, nft_info in self.updated_nfts.items():
            address = nft_info.wallet
            if address not in self.updated_wallet:
                self.updated_wallet[address] = Wallet(address, chain_id=self.chain_id)
            wallet = self.updated_wallet[address]
            wallet.nfts.append(f"{self.chain_id}_{nft_info.nft_manager_address}_{token_id}")

    def _export(self):
        self.update_wallet_info()
        config = {
            "id": f"{self.chain_id}_factory_nft_contract",
            "addresses": self.updated_factory_nft,
            "chainId": self.chain_id
        }
        self.exporter.export_config(config)
        # wallet_data = [w.to_dict() for _, w in self.updated_wallet.items()]
        # if wallet_data:
        #     self.exporter.export_wallets(wallet_data)
        #     logger.info(f'Exported {len(wallet_data)} wallets')


        data = [p.to_dict() for pool_address, p in self.updated_nfts.items()]
        if data:
            self.exporter.export_dex_nfts(data)
            logger.info(f'Exported {len(data)} nfts')
