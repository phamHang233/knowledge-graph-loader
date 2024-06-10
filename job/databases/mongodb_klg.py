import sys

from pymongo import MongoClient

from config import MongoDBConfig
from job.constants.mongo_constants import MongoDBCollections
from job.utils.logger_utils import get_logger

logger = get_logger('MongoDB')


class MongoDB:
    def __init__(self, connection_url=None, database=MongoDBConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]

        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

        self._wallets_col = self.mongo_db[MongoDBCollections.wallets]
        self._multichain_wallets_col = self.mongo_db[MongoDBCollections.multichain_wallets]
        self._multichain_wallets_score_col = self.mongo_db[MongoDBCollections.multichain_wallets_credit_scores]
        self._projects_col = self.mongo_db[MongoDBCollections.projects]
        self._smart_contracts_col = self.mongo_db[MongoDBCollections.smart_contracts]
        self._relationships_col = self.mongo_db[MongoDBCollections.relationships]
        self._call_smart_contracts_col = self.mongo_db[MongoDBCollections.call_smart_contracts]
        self._interactions_col = self.mongo_db[MongoDBCollections.interactions]

        self._abi_col = self.mongo_db[MongoDBCollections.abi]
        self._configs_col = self.mongo_db[MongoDBCollections.configs]
        self._is_part_ofs_col = self.mongo_db[MongoDBCollections.is_part_ofs]
        self.dex_nfts = self.mongo_db['dex_nfts']

        # self._create_index()

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return {}

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

    def get_wallets(self, chain_id=None, batch_idx=None, projection=None, batch_size=50000):
        filter_statement = {}
        if chain_id:
            filter_statement['chainId'] = chain_id
        if batch_idx:
            filter_statement['flagged'] = batch_idx

        projection_statement = self.get_projection_statement(projection)
        if projection_statement:
            cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement,
                                            batch_size=batch_size)
        else:
            cursor = self._wallets_col.find(filter=filter_statement, batch_size=batch_size)
        return cursor


    def get_wallets_by_keys(self, keys, projection=None):
        projection_statement = self.get_projection_statement(projection)
        try:
            filter_statement = {
                "_id": {"$in": keys}
            }
            if projection_statement:
                cursor = self._wallets_col.find(
                    filter=filter_statement, projection=projection_statement, batch_size=1000)
            else:
                cursor = self._wallets_col.find(
                    filter=filter_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None


    def get_tokens_by_keys(self, keys, projection=None):
        filter_statement = {
            "idCoingecko": {"$exists": True},
            "_id": {"$in": keys}
        }
        cursor = self._smart_contracts_col.find(filter_statement, projection)
        return cursor

    def get_nfts_by_pool(self, chain_id, pool_address):
        cursor = self.dex_nfts.find({"poolAddress": pool_address, "chainId": chain_id})
        return cursor