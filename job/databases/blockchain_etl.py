from pymongo import MongoClient

from config import TestBlockchainETLConfig, BlockchainETLConfig
from job.constants.blockchain_etl_constants import BlockchainETLCollections
from job.utils.logger_utils import get_logger

logger = get_logger('Blockchain ETL')


class BlockchainETL:
    def __init__(self, connection_url=None, db_prefix=""):
        self._conn = None
        if not connection_url:
            connection_url = TestBlockchainETLConfig.CONNECTION_URL

        # self.connection_url = connection_url.split('@')[-1]
        self.connection_url = connection_url
        self.connection = MongoClient(connection_url)
        if db_prefix:
            self.db_name = db_prefix + "_" + BlockchainETLConfig.DATABASE
        else:
            self.db_name = BlockchainETLConfig.DATABASE

        self.mongo_db = self.connection[self.db_name]

        self.block_collection = self.mongo_db[BlockchainETLCollections.blocks]
        self.transaction_collection = self.mongo_db[BlockchainETLCollections.transactions]
        self.internal_transaction_collection = self.mongo_db['internal_transactions']
        self.collector_collection = self.mongo_db[BlockchainETLCollections.collectors]
        self.lending_events_collection = self.mongo_db['lending_events']
        self.events_collection = self.mongo_db['events']
        self.dex_events_collection = self.mongo_db['dex_events']
        self.projects_collection = self.mongo_db['projects']
        self.logs_collection = self.mongo_db['logs']

    def get_collect_events_by_token_id(self, tokenids):
        filter_params = {'tokenId': {"$in": tokenids}, 'event_type': 'COLLECT'}
        cursor = self.dex_events_collection.find(filter=filter_params, batch_size=1000)
        return cursor
