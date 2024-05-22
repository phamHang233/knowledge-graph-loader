import pymongo
from pymongo import MongoClient, UpdateOne

from config import MongoDBDexConfig
from src.constants.mongo_constants import MongoDBDexCollections
from src.utils.dict_utils import flatten_dict
from src.utils.logger_utils import get_logger
from src.utils.retry_handler import retry_handler
from src.utils.time_execute_decorator import sync_log_time_exe, TimeExeTag

logger = get_logger('MongoDB DEX')


class MongoDBDex:
    def __init__(self, connection_url=None, database=MongoDBDexConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBDexConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.client = MongoClient(connection_url)
        self.db = self.client[database]

        self._dexes_col = self.db[MongoDBDexCollections.dexes]
        self._pairs_col = self.db[MongoDBDexCollections.pairs]
        self._cexes_col = self.db[MongoDBDexCollections.cexes]
        self._nfts_col = self.db[MongoDBDexCollections.nfts]
        self._tickers_col = self.db[MongoDBDexCollections.tickers]
        self._exchanges_col = self.db[MongoDBDexCollections.exchanges]

        self._configs_col = self.db[MongoDBDexCollections.configs]

        self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        # Pairs index
        pairs_col_indexes = self._pairs_col.index_information()
        if 'pairs_index' not in pairs_col_indexes:
            self._pairs_col.create_index(
                [('lastInteractedAt', pymongo.ASCENDING), ('liquidityValueInUSD', pymongo.ASCENDING)],
                name='pairs_index', background=True
            )

    #######################
    #        DEXes        #
    #######################

    @retry_handler
    def update_dexes(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._dexes_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_dexes(self, projection=None):
        try:
            cursor = self._dexes_col.find(filter={}, projection=projection, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #        Pairs        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_pairs(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._pairs_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs(self, chain_id, project_id, last_interacted_at=None, batch_size=1000):
        try:
            filter_statement = {'chainId': chain_id, 'project': project_id}
            if last_interacted_at is not None:
                filter_statement.update({'lastInteractedAt': {'$gt': last_interacted_at}})

            cursor = self._pairs_col.find(filter=filter_statement, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs_with_addresses(self, chain_id, addresses, batch_size=1000):
        try:
            keys = [f'{chain_id}_{address}' for address in addresses]
            filter_statement = {'_id': {'$in': keys}}
            cursor = self._pairs_col.find(filter=filter_statement, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_pairs_with_tokens(self, chain_id, tokens, projects=None, projection=None):
        filter_statement = {
            'chainId': chain_id,
            'tokens': {
                '$elemMatch': {
                    'address': {'$in': tokens}
                }
            },
            'liquidityValueInUSD': {'$gt': 100000}
        }

        if projects is not None:
            filter_statement['project'] = {'$in': projects}
        cursor = self._pairs_col.find(filter_statement, projection=projection)
        return cursor

    #######################
    #        CEXes        #
    #######################

    @retry_handler
    def update_cexes(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._cexes_col.bulk_write(bulk_operations)

    #######################
    #        NFTs         #
    #######################

    @retry_handler
    def update_nfts(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._nfts_col.bulk_write(bulk_operations)

    #######################
    #       Tickers       #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_tickers(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._tickers_col.bulk_write(bulk_operations)

    #######################
    #      Exchanges      #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_exchanges(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._exchanges_col.bulk_write(bulk_operations)

    #######################
    #       Configs       #
    #######################

    def get_config(self, key):
        try:
            config = self._configs_col.find_one({'_id': key})
            return config
        except Exception as ex:
            logger.exception(ex)
        return None

    def update_config(self, config, merge=True):
        try:
            if merge:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": flatten_dict(config)}, upsert=True)]
            else:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": config}, upsert=True)]
            self._configs_col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)
